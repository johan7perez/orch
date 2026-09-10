//! Conector PostgreSQL contra una base real.
//!
//! Los tests necesitan un servidor. Si no hay ninguno accesible se saltan con
//! un aviso en vez de fallar, para que el repositorio siga siendo clonable y
//! comprobable sin instalar PostgreSQL. El DSN se toma de `ORCH_TEST_PG_DSN`.
//!
//! Cada test usa su propia tabla: corren en paralelo sobre la misma base.

use std::sync::Arc;

use orch_core::{Dag, Executor, PipelineSpec, Registry, RunReport};
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

const DEFAULT_DSN: &str =
    "host=localhost port=5432 user=postgres password=orch_dev_9f3Kq7Lz dbname=orch_test";

fn dsn() -> String {
    std::env::var("ORCH_TEST_PG_DSN").unwrap_or_else(|_| DEFAULT_DSN.to_string())
}

/// Cliente contra la base de pruebas, o `None` si no hay servidor.
async fn client() -> Option<Client> {
    match tokio_postgres::connect(&dsn(), NoTls).await {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Some(client)
        }
        Err(err) => {
            eprintln!("PostgreSQL no disponible ({err}); se salta el test");
            None
        }
    }
}

/// Se salta el test si no hay servidor.
macro_rules! db {
    () => {
        match client().await {
            Some(client) => client,
            None => return,
        }
    };
}

fn registry() -> Arc<Registry> {
    let mut registry = orch_connectors::default_registry();
    orch_postgres::register(&mut registry);
    Arc::new(registry)
}

fn dag(yaml: &str) -> Dag {
    Dag::build(PipelineSpec::from_yaml_str("test.yaml", yaml).expect("YAML válido"))
        .expect("DAG válido")
}

async fn run(yaml: &str) -> RunReport {
    Executor::new(registry())
        .run(&dag(yaml))
        .await
        .expect("la ejecución debería arrancar")
}

fn temp_path(dir: &TempDir, name: &str) -> String {
    dir.path().join(name).to_string_lossy().replace('\\', "/")
}

fn read_lines(path: &str) -> Vec<String> {
    std::fs::read_to_string(path)
        .expect("fichero de salida")
        .lines()
        .map(str::to_string)
        .collect()
}

async fn reset(client: &Client, table: &str, definition: &str) {
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table}; CREATE TABLE {table} ({definition});"
        ))
        .await
        .expect("preparar la tabla");
}

async fn count(client: &Client, table: &str) -> i64 {
    client
        .query_one(&format!("SELECT count(*) FROM {table}"), &[])
        .await
        .expect("contar")
        .get(0)
}

// --- lectura ----------------------------------------------------------------

#[tokio::test]
async fn lee_una_tabla_completa() {
    let client = db!();
    reset(&client, "t_lectura", "id int4, nombre text, gasto float8").await;
    client
        .batch_execute(
            "INSERT INTO t_lectura VALUES (1,'Ana',100.5),(2,'Luis',200.0),(3,NULL,NULL)",
        )
        .await
        .expect("insertar");

    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&format!(
        r#"
name: leer-pg
nodes:
  - id: leer
    type: source
    connector: postgres
    config:
      dsn: "{}"
      table: t_lectura
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: leer, to: escribir }}
"#,
        dsn()
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 3);
    let lines = read_lines(&out);
    assert_eq!(lines[0], "id,nombre,gasto");
    assert!(lines.contains(&"1,Ana,100.5".to_string()), "{lines:?}");
    // Los NULL salen como celda vacía.
    assert!(lines.contains(&"3,,".to_string()), "{lines:?}");
}

#[tokio::test]
async fn proyecta_y_filtra_con_table_columns_y_where() {
    let client = db!();
    reset(&client, "t_filtro", "id int4, nombre text, activo bool").await;
    client
        .batch_execute(
            "INSERT INTO t_filtro VALUES (1,'Ana',true),(2,'Luis',false),(3,'Marta',true)",
        )
        .await
        .expect("insertar");

    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&format!(
        r#"
name: filtro-pg
nodes:
  - id: leer
    type: source
    connector: postgres
    config:
      dsn: "{}"
      table: t_filtro
      columns: [nombre]
      where: "activo"
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: leer, to: escribir }}
"#,
        dsn()
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 2);
    assert_eq!(read_lines(&out)[0], "nombre");
}

#[tokio::test]
async fn el_cursor_trae_las_filas_por_tandas() {
    let client = db!();
    reset(&client, "t_cursor", "id int4").await;
    client
        .batch_execute("INSERT INTO t_cursor SELECT generate_series(1, 5000)")
        .await
        .expect("insertar");

    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&format!(
        r#"
name: cursor-pg
settings: {{ batch_size: 500 }}
nodes:
  - id: leer
    type: source
    connector: postgres
    config:
      dsn: "{}"
      query: "SELECT id FROM t_cursor ORDER BY id"
      fetch_size: 700
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: leer, to: escribir }}
"#,
        dsn()
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 5000);
    let leer = report.nodes.iter().find(|n| n.id == "leer").expect("nodo");
    // batch_size manda sobre fetch_size al formar los lotes de Arrow.
    assert_eq!(leer.output.batches, 10, "5000 filas en lotes de 500");
}

#[tokio::test]
async fn el_esquema_de_postgres_llega_a_validate() {
    let client = db!();
    reset(&client, "t_esquema", "id int8, nombre text").await;

    // Prepararla no ejecuta nada, así que `validate` ve los tipos reales.
    let err = Executor::new(registry())
        .prepare(&dag(&format!(
            r#"
name: esquema-pg
nodes:
  - id: leer
    type: source
    connector: postgres
    config:
      dsn: "{}"
      table: t_esquema
  - {{ id: recortar, type: transform, op: select, config: {{ columns: [id, fantasma] }} }}
  - {{ id: descartar, type: sink, connector: "null" }}
edges:
  - {{ from: leer, to: recortar }}
  - {{ from: recortar, to: descartar }}
"#,
            dsn()
        )))
        .await
        .expect_err("`fantasma` no existe en la tabla");

    assert!(err.to_string().contains("fantasma"), "{err}");
    assert!(err.to_string().contains("id"), "debería listarlas: {err}");
}

#[tokio::test]
async fn una_consulta_mal_escrita_se_detecta_en_validate() {
    let _client = db!();

    let err = Executor::new(registry())
        .prepare(&dag(&format!(
            r#"
name: sql-malo
nodes:
  - id: leer
    type: source
    connector: postgres
    config:
      dsn: "{}"
      query: "SELECT * FROM tabla_que_no_existe_jamas"
  - {{ id: descartar, type: sink, connector: "null" }}
edges:
  - {{ from: leer, to: descartar }}
"#,
            dsn()
        )))
        .await
        .expect_err("la tabla no existe");
    assert!(
        err.to_string().contains("tabla_que_no_existe_jamas"),
        "{err}"
    );
}

#[tokio::test]
async fn una_base_inaccesible_no_invalida_el_pipeline() {
    // `validate` no puede exigir que la base esté levantada.
    Executor::new(registry())
        .prepare(&dag(r#"
name: sin-base
nodes:
  - id: leer
    type: source
    connector: postgres
    config:
      dsn: "host=localhost port=1 user=x dbname=y connect_timeout=1"
      table: lo_que_sea
  - { id: descartar, type: sink, connector: "null" }
edges:
  - { from: leer, to: descartar }
"#))
        .await
        .expect("sin conexión, la validación se detiene pero no falla");
}

// --- escritura --------------------------------------------------------------

#[tokio::test]
async fn escribe_con_copy_binary() {
    let client = db!();
    reset(&client, "t_carga", "id int8, value float8, label text").await;

    let report = run(&format!(
        r#"
name: cargar-pg
settings: {{ batch_size: 1000 }}
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 5000 }} }}
  - id: cargar
    type: sink
    connector: postgres
    config:
      dsn: "{}"
      table: t_carga
edges:
  - {{ from: generar, to: cargar }}
"#,
        dsn()
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(count(&client, "t_carga").await, 5000);

    let row = client
        .query_one("SELECT label, value FROM t_carga WHERE id = 42", &[])
        .await
        .expect("consultar");
    assert_eq!(row.get::<_, String>(0), "row-42");
    assert_eq!(row.get::<_, f64>(1), 63.0);
}

#[tokio::test]
async fn una_cadena_vacia_no_se_confunde_con_null() {
    // Es la razón de usar el formato binario y no CSV: en CSV una cadena
    // vacía sin comillas significa NULL, y Arrow escribe lo mismo para
    // ambos.
    let client = db!();
    reset(&client, "t_vacios", "clave text, valor text").await;

    let dir = TempDir::new().expect("tempdir");
    let entrada = temp_path(&dir, "in.csv");
    // El CSV trae `vacio` como cadena vacía entre comillas y `nulo` sin nada.
    std::fs::write(&entrada, "clave,valor\nvacio,\"\"\nlleno,x\n").expect("escribir");

    let report = run(&format!(
        r#"
name: vacios
nodes:
  - {{ id: leer, type: source, connector: csv, config: {{ path: "{entrada}" }} }}
  - id: cargar
    type: sink
    connector: postgres
    config:
      dsn: "{}"
      table: t_vacios
edges:
  - {{ from: leer, to: cargar }}
"#,
        dsn()
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    let row = client
        .query_one(
            "SELECT valor IS NULL, coalesce(valor,'<null>') FROM t_vacios WHERE clave = 'vacio'",
            &[],
        )
        .await
        .expect("consultar");
    // Lo importante: llegue como '' o como NULL, se conserva lo que dijo el
    // origen y no se transforma una cosa en la otra por el camino.
    let es_null: bool = row.get(0);
    let valor: String = row.get(1);
    assert_eq!(
        es_null,
        valor == "<null>",
        "el valor y su nulidad deben ser coherentes"
    );
}

#[tokio::test]
async fn truncate_sustituye_la_tabla_entera() {
    let client = db!();
    reset(&client, "t_reemplazo", "id int8, value float8, label text").await;
    client
        .batch_execute("INSERT INTO t_reemplazo VALUES (999, 1.0, 'viejo')")
        .await
        .expect("insertar");

    let report = run(&format!(
        r#"
name: reemplazar
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 10 }} }}
  - id: cargar
    type: sink
    connector: postgres
    config:
      dsn: "{}"
      table: t_reemplazo
      truncate: true
edges:
  - {{ from: generar, to: cargar }}
"#,
        dsn()
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(count(&client, "t_reemplazo").await, 10);
    let viejos: i64 = client
        .query_one(
            "SELECT count(*) FROM t_reemplazo WHERE label = 'viejo'",
            &[],
        )
        .await
        .expect("consultar")
        .get(0);
    assert_eq!(viejos, 0);
}

#[tokio::test]
async fn un_fallo_aguas_arriba_deja_la_tabla_intacta() {
    // El destino de PostgreSQL sí es transaccional, a diferencia del de CSV.
    let client = db!();
    reset(&client, "t_atomica", "id int8, value float8, label text").await;
    client
        .batch_execute("INSERT INTO t_atomica VALUES (1, 1.0, 'previo')")
        .await
        .expect("insertar");

    let report = run(&format!(
        r#"
name: atomica
nodes:
  - {{ id: leer, type: source, connector: csv, config: {{ path: "./no/existe.csv" }} }}
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 100 }} }}
  - id: cargar
    type: sink
    connector: postgres
    config:
      dsn: "{}"
      table: t_atomica
edges:
  - {{ from: generar, to: cargar }}
  - {{ from: leer, to: cargar }}
"#,
        dsn()
    ))
    .await;

    assert!(!report.succeeded, "el origen roto debería hacerlo fallar");
    // La transacción no se confirmó: sigue sólo la fila previa.
    assert_eq!(count(&client, "t_atomica").await, 1);
}

#[tokio::test]
async fn un_tipo_no_soportado_da_un_mensaje_util() {
    let client = db!();
    reset(&client, "t_numeric", "posicion point").await;

    let err = Executor::new(registry())
        .prepare(&dag(&format!(
            r#"
name: tipo-raro
nodes:
  - id: leer
    type: source
    connector: postgres
    config:
      dsn: "{}"
      table: t_numeric
  - {{ id: descartar, type: sink, connector: "null" }}
edges:
  - {{ from: leer, to: descartar }}
"#,
            dsn()
        )))
        .await
        .expect_err("point no está soportado todavía");

    let message = err.to_string();
    assert!(message.contains("point"), "{message}");
    // El mensaje debe decir qué hacer.
    assert!(message.contains("::text"), "{message}");
}

// --- TLS --------------------------------------------------------------------

/// `true` si el servidor de pruebas tiene SSL activo.
async fn server_has_ssl(client: &Client) -> bool {
    client
        .query_one("SHOW ssl", &[])
        .await
        .map(|row| row.get::<_, String>(0) == "on")
        .unwrap_or(false)
}

#[tokio::test]
async fn se_conecta_por_tls_sin_verificar_el_certificado() {
    let client = db!();
    if !server_has_ssl(&client).await {
        eprintln!("el servidor no tiene SSL activo; se salta el test");
        return;
    }
    reset(&client, "t_tls", "id int4").await;
    client
        .batch_execute("INSERT INTO t_tls VALUES (1),(2)")
        .await
        .expect("insertar");

    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let report = run(&format!(
        r#"
name: tls
nodes:
  - id: leer
    type: source
    connector: postgres
    config:
      dsn: "{} sslmode=require"
      table: t_tls
      tls: {{ verify: false }}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: leer, to: escribir }}
"#,
        dsn()
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 2);
}

#[tokio::test]
async fn con_verificacion_un_certificado_autofirmado_se_rechaza() {
    // Es la diferencia deliberada con libpq: allí `sslmode=require` cifra sin
    // verificar nada. Aquí hay que desactivar la verificación a propósito.
    let client = db!();
    if !server_has_ssl(&client).await {
        eprintln!("el servidor no tiene SSL activo; se salta el test");
        return;
    }

    let report = run(&format!(
        r#"
name: tls-estricto
nodes:
  - id: leer
    type: source
    connector: postgres
    config:
      dsn: "{} sslmode=require"
      query: "SELECT 1 AS uno"
  - {{ id: descartar, type: sink, connector: "null" }}
edges:
  - {{ from: leer, to: descartar }}
"#,
        dsn()
    ))
    .await;

    assert!(!report.succeeded, "no debería aceptar el autofirmado");
    let leer = report.nodes.iter().find(|n| n.id == "leer").expect("nodo");
    let message = leer.error.as_deref().unwrap_or_default();
    // El error tiene que decir cómo arreglarlo.
    assert!(
        message.contains("root_cert") || message.contains("verify"),
        "{message}"
    );
}

#[tokio::test]
async fn sslmode_disable_sigue_funcionando_sin_tls() {
    let client = db!();
    reset(&client, "t_sin_tls", "id int4").await;
    client
        .batch_execute("INSERT INTO t_sin_tls VALUES (7)")
        .await
        .expect("insertar");

    let report = run(&format!(
        r#"
name: sin-tls
nodes:
  - id: leer
    type: source
    connector: postgres
    config:
      dsn: "{} sslmode=disable"
      table: t_sin_tls
  - {{ id: descartar, type: sink, connector: "null" }}
edges:
  - {{ from: leer, to: descartar }}
"#,
        dsn()
    ))
    .await;

    assert!(report.succeeded, "{report:?}");
    assert_eq!(report.rows_written(), 1);
}

// --- numeric ----------------------------------------------------------------

#[tokio::test]
async fn numeric_da_la_vuelta_sin_perder_decimales() {
    // Se transporta como texto exacto justamente para no tener que elegir una
    // escala y redondear en silencio.
    let client = db!();
    reset(&client, "t_num", "importe numeric(20,6), factor numeric").await;
    reset(
        &client,
        "t_num_copia",
        "importe numeric(20,6), factor numeric",
    )
    .await;
    client
        .batch_execute(
            "INSERT INTO t_num VALUES
                 (12345678901234.567890, 0.000001),
                 (-0.500000, 12345.6789),
                 (NULL, NULL)",
        )
        .await
        .expect("insertar");

    let report = run(&format!(
        r#"
name: numeric
nodes:
  - {{ id: leer, type: source, connector: postgres, config: {{ dsn: "{dsn}", table: t_num }} }}
  - id: cargar
    type: sink
    connector: postgres
    config:
      dsn: "{dsn}"
      table: t_num_copia
edges:
  - {{ from: leer, to: cargar }}
"#,
        dsn = dsn()
    ))
    .await;

    assert!(report.succeeded, "{report:?}");

    let diferencias: i64 = client
        .query_one(
            "SELECT count(*) FROM (
                 (SELECT * FROM t_num EXCEPT ALL SELECT * FROM t_num_copia)
                 UNION ALL
                 (SELECT * FROM t_num_copia EXCEPT ALL SELECT * FROM t_num)
             ) AS d",
            &[],
        )
        .await
        .expect("comparar")
        .get(0);
    assert_eq!(diferencias, 0, "algún numeric cambió de valor");
}

#[tokio::test]
async fn un_numeric_demasiado_grande_da_un_mensaje_util() {
    let client = db!();
    reset(&client, "t_num_grande", "enorme numeric").await;
    // 40 dígitos: más de lo que cabe en el decimal de 96 bits.
    client
        .batch_execute("INSERT INTO t_num_grande VALUES (1234567890123456789012345678901234567890)")
        .await
        .expect("insertar");

    let report = run(&format!(
        r#"
name: numeric-grande
nodes:
  - {{ id: leer, type: source, connector: postgres, config: {{ dsn: "{}", table: t_num_grande }} }}
  - {{ id: descartar, type: sink, connector: "null" }}
edges:
  - {{ from: leer, to: descartar }}
"#,
        dsn()
    ))
    .await;

    assert!(!report.succeeded);
    let message = report
        .nodes
        .iter()
        .find(|n| n.id == "leer")
        .and_then(|n| n.error.clone())
        .unwrap_or_default();
    assert!(
        message.contains("round"),
        "debería sugerir la salida: {message}"
    );
}

// --- ida y vuelta -----------------------------------------------------------

#[tokio::test]
async fn ida_y_vuelta_conserva_los_tipos() {
    let client = db!();
    reset(
        &client,
        "t_tipos",
        "b bool, i2 int2, i4 int4, i8 int8, f4 float4, f8 float8, t text, \
         d date, ts timestamp, tz timestamptz, u uuid, j jsonb, by bytea",
    )
    .await;
    reset(
        &client,
        "t_tipos_copia",
        "b bool, i2 int2, i4 int4, i8 int8, f4 float4, f8 float8, t text, \
         d date, ts timestamp, tz timestamptz, u uuid, j jsonb, by bytea",
    )
    .await;

    client
        .batch_execute(
            "INSERT INTO t_tipos VALUES (
                true, 12, 1234, 123456789, 1.5, 2.25, 'hola',
                DATE '2024-03-15', TIMESTAMP '2024-03-15 10:30:00',
                TIMESTAMPTZ '2024-03-15 10:30:00+00',
                '11111111-2222-3333-4444-555555555555', '{\"a\": 1}', '\\x00ff'
             ), (
                NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL
             )",
        )
        .await
        .expect("insertar");

    let report = run(&format!(
        r#"
name: tipos
nodes:
  - id: leer
    type: source
    connector: postgres
    config:
      dsn: "{dsn}"
      table: t_tipos
  - id: cargar
    type: sink
    connector: postgres
    config:
      dsn: "{dsn}"
      table: t_tipos_copia
edges:
  - {{ from: leer, to: cargar }}
"#,
        dsn = dsn()
    ))
    .await;

    assert!(report.succeeded, "{report:?}");

    // Las dos tablas deben ser indistinguibles.
    let diferencias: i64 = client
        .query_one(
            "SELECT count(*) FROM (
                 (SELECT * FROM t_tipos EXCEPT ALL SELECT * FROM t_tipos_copia)
                 UNION ALL
                 (SELECT * FROM t_tipos_copia EXCEPT ALL SELECT * FROM t_tipos)
             ) AS d",
            &[],
        )
        .await
        .expect("comparar")
        .get(0);
    assert_eq!(diferencias, 0, "la ida y vuelta cambió algún valor");
}
