//! Lineage por columna: de qué columnas de origen sale cada columna.
//!
//! Se prueba con pipelines completos —orígenes reales, transformaciones
//! nativas y SQL, sinks— porque lo que importa es la cadena entera, no cada
//! salto por separado.

use std::sync::Arc;

use async_trait::async_trait;
use orch_core::arrow::datatypes::SchemaRef;
use orch_core::{
    lineage, Dag, Derivation, Executor, Input, InputSchemas, NodeColumn, NodeContext, NodeLineage,
    Output, PipelineSpec, Registry, Result, Transform,
};
use tempfile::TempDir;

fn registry() -> Arc<Registry> {
    let mut registry = orch_connectors::default_registry();
    orch_sql::register(&mut registry);
    Arc::new(registry)
}

fn dag(yaml: &str) -> Dag {
    Dag::build(PipelineSpec::from_yaml_str("test.yaml", yaml).expect("YAML válido"))
        .expect("DAG válido")
}

async fn lineage_with(registry: Arc<Registry>, yaml: &str) -> Vec<NodeLineage> {
    Executor::new(registry)
        .lineage(&dag(yaml))
        .await
        .expect("el lineage debería calcularse")
}

async fn lineage_of(yaml: &str) -> Vec<NodeLineage> {
    lineage_with(registry(), yaml).await
}

fn node<'a>(all: &'a [NodeLineage], id: &str) -> &'a NodeLineage {
    all.iter()
        .find(|n| n.node == id)
        .unwrap_or_else(|| panic!("no hay lineage de `{id}`"))
}

/// `columna ← nodo.col, nodo.col (derivación)`, para comparar de un vistazo.
fn columns(node: &NodeLineage) -> Vec<String> {
    node.columns
        .as_ref()
        .unwrap_or_else(|| panic!("`{}` no tiene lineage: {node:?}", node.node))
        .iter()
        .map(|c| {
            let origins: Vec<String> = c.origins.iter().map(ToString::to_string).collect();
            format!(
                "{} ← {} ({})",
                c.column,
                origins.join(", "),
                c.derivation.label()
            )
        })
        .collect()
}

fn origin(node: &str, column: &str) -> NodeColumn {
    NodeColumn {
        node: node.to_string(),
        column: column.to_string(),
    }
}

fn write(dir: &TempDir, name: &str, contents: &str) -> String {
    let path = dir.path().join(name);
    std::fs::write(&path, contents).expect("escribir el CSV");
    path.to_string_lossy().replace('\\', "/")
}

fn temp_path(dir: &TempDir, name: &str) -> String {
    dir.path().join(name).to_string_lossy().replace('\\', "/")
}

const CLIENTES: &str = "id,name,city,segment,spend\n1,Ana,Santiago,retail,1200.5\n";
const PEDIDOS: &str = "order_id,customer_id,amount,status\n1001,1,320.5,pagado\n";

/// El pipeline de `examples/pipelines/join.yaml`, con rutas temporales.
fn join_yaml(dir: &TempDir) -> String {
    let clientes = write(dir, "customers.csv", CLIENTES);
    let pedidos = write(dir, "orders.csv", PEDIDOS);
    let out = temp_path(dir, "out.csv");
    format!(
        r#"
name: pedidos-por-cliente
nodes:
  - {{ id: leer_clientes, type: source, connector: csv, config: {{ path: "{clientes}" }} }}
  - {{ id: leer_pedidos, type: source, connector: csv, config: {{ path: "{pedidos}" }} }}
  - {{ id: pagados, type: transform, op: filter, config: {{ where: "status = 'pagado'" }} }}
  - id: cruzar
    type: transform
    op: sql
    config:
      query: >
        SELECT c.city AS ciudad,
               count(DISTINCT c.id) AS clientes,
               count(*) AS pedidos,
               round(sum(p.amount), 2) AS facturado
        FROM clientes c
        JOIN pedidos p ON c.id = p.customer_id
        GROUP BY c.city
        ORDER BY facturado DESC
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: leer_clientes, to: cruzar, port: clientes }}
  - {{ from: leer_pedidos, to: pagados }}
  - {{ from: pagados, to: cruzar, port: pedidos }}
  - {{ from: cruzar, to: escribir }}
"#
    )
}

// --- de la salida a los orígenes ----------------------------------------------

#[tokio::test]
async fn un_join_con_agregados_dice_de_donde_sale_cada_columna() {
    let dir = TempDir::new().expect("tempdir");
    let todo = lineage_of(&join_yaml(&dir)).await;

    assert_eq!(
        columns(node(&todo, "escribir")),
        vec![
            "ciudad ← leer_clientes.city (copia)",
            "clientes ← leer_clientes.id (agregada)",
            // `count(*)` cuenta filas: no lee ninguna columna.
            "pedidos ←  (agregada)",
            "facturado ← leer_pedidos.amount (agregada)",
        ]
    );

    // El tipo viaja con la columna.
    let escribir = node(&todo, "escribir");
    assert_eq!(escribir.column("ciudad").unwrap().data_type, "Utf8");
    assert_eq!(escribir.column("pedidos").unwrap().data_type, "Int64");
    assert_eq!(escribir.kind, "sink");
}

#[tokio::test]
async fn las_columnas_que_solo_deciden_salen_como_indirectas() {
    let dir = TempDir::new().expect("tempdir");
    let todo = lineage_of(&join_yaml(&dir)).await;

    // El filtro lee `status` para decidir qué filas pasan. Que además la
    // copie no le quita lo otro: quitarla rompe el filtro.
    assert_eq!(
        node(&todo, "pagados").indirect,
        vec![origin("leer_pedidos", "status")]
    );

    // El join lee las claves; agrupar, la ciudad; ordenar, el total.
    let cruzar = &node(&todo, "cruzar").indirect;
    for esperado in [
        origin("leer_clientes", "id"),
        origin("leer_pedidos", "customer_id"),
        origin("leer_clientes", "city"),
        origin("leer_pedidos", "amount"),
    ] {
        assert!(cruzar.contains(&esperado), "falta {esperado} en {cruzar:?}");
    }
    assert!(!cruzar.contains(&origin("leer_pedidos", "status")));

    // Un sink escribe lo que le llega: no decide nada.
    assert!(node(&todo, "escribir").indirect.is_empty());
}

// --- del origen a las salidas -----------------------------------------------

#[tokio::test]
async fn el_impacto_de_una_columna_incluye_donde_se_usa_sin_salir() {
    let dir = TempDir::new().expect("tempdir");
    let todo = lineage_of(&join_yaml(&dir)).await;

    let amount = lineage::impact(&todo, &origin("leer_pedidos", "amount"));
    assert_eq!(amount.outputs, vec![origin("escribir", "facturado")]);

    // La clave del join no llega a ninguna salida, pero quitarla lo rompe.
    let clave = lineage::impact(&todo, &origin("leer_pedidos", "customer_id"));
    assert!(clave.outputs.is_empty());
    assert_eq!(clave.used_by, vec!["cruzar".to_string()]);

    let estado = lineage::impact(&todo, &origin("leer_pedidos", "status"));
    assert!(estado.outputs.is_empty());
    assert_eq!(estado.used_by, vec!["pagados".to_string()]);

    // Y una que no se usa en ningún sitio.
    assert!(lineage::impact(&todo, &origin("leer_clientes", "segment")).is_empty());
}

// --- cómo se transforma el valor por el camino --------------------------------

#[tokio::test]
async fn una_cadena_de_pasos_se_queda_con_el_mas_fuerte() {
    let dir = TempDir::new().expect("tempdir");
    let clientes = write(&dir, "customers.csv", CLIENTES);
    let out = temp_path(&dir, "out.csv");

    // `examples/pipelines/sql_resumen.yaml`: calcular, agregar y redondear.
    let todo = lineage_of(&format!(
        r#"
name: clientes-por-ciudad
nodes:
  - {{ id: leer, type: source, connector: csv, config: {{ path: "{clientes}" }} }}
  - {{ id: relevantes, type: transform, op: filter, config: {{ where: "spend >= 500" }} }}
  - id: con_impuesto
    type: transform
    op: derive
    config:
      columns: {{ spend_con_itbis: "spend * 1.18", origen: "'csv'" }}
  - id: por_ciudad
    type: transform
    op: aggregate
    config:
      group_by: [city]
      aggregates: {{ clientes: "count(*)", gasto_total: "sum(spend_con_itbis)" }}
  - id: ordenar
    type: transform
    op: sql
    config:
      query: "SELECT city, clientes, round(gasto_total, 2) AS gasto_total FROM input"
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: leer, to: relevantes }}
  - {{ from: relevantes, to: con_impuesto }}
  - {{ from: con_impuesto, to: por_ciudad }}
  - {{ from: por_ciudad, to: ordenar }}
  - {{ from: ordenar, to: escribir }}
"#
    ))
    .await;

    // `derive` añade a lo que había: las de antes pasan tal cual.
    let derive = columns(node(&todo, "con_impuesto"));
    assert!(derive.contains(&"spend ← leer.spend (copia)".to_string()));
    assert!(derive.contains(&"spend_con_itbis ← leer.spend (calculada)".to_string()));
    // Un literal no lee ninguna columna.
    assert!(derive.contains(&"origen ←  (calculada)".to_string()));

    assert_eq!(
        columns(node(&todo, "escribir")),
        vec![
            "city ← leer.city (copia)",
            "clientes ←  (agregada)",
            // Calculada, agregada y redondeada: lo más fuerte es agregar.
            "gasto_total ← leer.spend (agregada)",
        ]
    );
}

#[tokio::test]
async fn select_rename_y_limit_copian_con_otro_nombre() {
    let dir = TempDir::new().expect("tempdir");
    let clientes = write(&dir, "customers.csv", CLIENTES);
    let out = temp_path(&dir, "out.csv");

    let todo = lineage_of(&format!(
        r#"
name: copias
nodes:
  - {{ id: leer, type: source, connector: csv, config: {{ path: "{clientes}" }} }}
  - {{ id: elegir, type: transform, op: select, config: {{ columns: [spend, name] }} }}
  - {{ id: renombrar, type: transform, op: rename, config: {{ columns: {{ name: nombre }} }} }}
  - {{ id: primeros, type: transform, op: limit, config: {{ rows: 10 }} }}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: leer, to: elegir }}
  - {{ from: elegir, to: renombrar }}
  - {{ from: renombrar, to: primeros }}
  - {{ from: primeros, to: escribir }}
"#
    ))
    .await;

    assert_eq!(
        columns(node(&todo, "escribir")),
        vec!["spend ← leer.spend (copia)", "nombre ← leer.name (copia)"]
    );
    // El orden del resultado es el de los nodos en el fichero.
    let ids: Vec<&str> = todo.iter().map(|n| n.node.as_str()).collect();
    assert_eq!(ids, ["leer", "elegir", "renombrar", "primeros", "escribir"]);
    // Un origen es su propio origen.
    assert_eq!(
        columns(node(&todo, "leer"))[0],
        "id ← leer.id (copia)".to_string()
    );
}

#[tokio::test]
async fn el_fan_in_suma_los_origenes_de_cada_entrada() {
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let todo = lineage_of(&format!(
        r#"
name: fan-in
nodes:
  - {{ id: lunes, type: source, connector: generator, config: {{ rows: 10 }} }}
  - {{ id: martes, type: source, connector: generator, config: {{ rows: 10 }} }}
  - {{ id: juntar, type: transform, op: select, config: {{ columns: [id] }} }}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: lunes, to: juntar }}
  - {{ from: martes, to: juntar }}
  - {{ from: juntar, to: escribir }}
"#
    ))
    .await;

    assert_eq!(
        columns(node(&todo, "escribir")),
        vec!["id ← lunes.id, martes.id (copia)"]
    );
}

#[tokio::test]
async fn sql_union_ventanas_cte_y_subconsultas() {
    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");

    let todo = lineage_of(&format!(
        r#"
name: sql-variado
nodes:
  - {{ id: a, type: source, connector: generator, config: {{ rows: 10 }} }}
  - {{ id: b, type: source, connector: generator, config: {{ rows: 10 }} }}
  - id: q
    type: transform
    op: sql
    config:
      query: >
        WITH todo AS (
          SELECT id, value FROM a
          UNION ALL
          SELECT id, value * 2 FROM b
        )
        SELECT id,
               sum(value) OVER (ORDER BY id) AS acumulado,
               (SELECT max(label) FROM b) AS ultima,
               CAST(id AS VARCHAR) AS texto
        FROM todo
        WHERE id > 0
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: a, to: q }}
  - {{ from: b, to: q }}
  - {{ from: q, to: escribir }}
"#
    ))
    .await;

    assert_eq!(
        columns(node(&todo, "escribir")),
        vec![
            // UNION ALL: cada columna sale de las dos ramas; una la multiplica.
            "id ← a.id, b.id (copia)",
            // La ventana se ordena por `id`: también lee de ella.
            "acumulado ← a.id, a.value, b.id, b.value (agregada)",
            "ultima ← b.label (agregada)",
            "texto ← a.id, b.id (calculada)",
        ]
    );
    let q = &node(&todo, "q").indirect;
    assert!(q.contains(&origin("a", "id")), "{q:?}");
}

// --- donde no se puede saber ------------------------------------------------

#[tokio::test]
async fn un_origen_sin_esquema_corta_la_cadena_y_dice_donde() {
    let dir = TempDir::new().expect("tempdir");
    // El fichero todavía no existe: lo generaría otro paso.
    let falta = temp_path(&dir, "aun_no.csv");
    let out = temp_path(&dir, "out.csv");

    let todo = lineage_of(&format!(
        r#"
name: sin-esquema
nodes:
  - {{ id: leer, type: source, connector: csv, config: {{ path: "{falta}" }} }}
  - {{ id: filtrar, type: transform, op: filter, config: {{ where: "x > 1" }} }}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: leer, to: filtrar }}
  - {{ from: filtrar, to: escribir }}
"#
    ))
    .await;

    for id in ["leer", "filtrar", "escribir"] {
        let n = node(&todo, id);
        assert!(n.columns.is_none(), "{n:?}");
        assert_eq!(n.unknown_at.as_deref(), Some("leer"), "{n:?}");
    }
}

/// Una transformación que declara su esquema pero no su lineage: como un
/// plugin que no lo implementa.
struct Opaca;

#[async_trait]
impl Transform for Opaca {
    fn op(&self) -> &str {
        "opaca"
    }

    async fn plan(&self, inputs: &InputSchemas) -> Result<Option<SchemaRef>> {
        inputs.concatenated("opaca")
    }

    async fn apply(&self, _ctx: &NodeContext, input: &mut Input, output: &Output) -> Result<()> {
        while let Some(batch) = input.recv().await? {
            output.send(batch).await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn una_transformacion_que_no_declara_lineage_lo_corta_ahi() {
    let mut registry = orch_connectors::default_registry();
    orch_sql::register(&mut registry);
    registry.register_transform("opaca", |_node, _: orch_core::NoConfig| {
        let t: Arc<dyn Transform> = Arc::new(Opaca);
        Ok(t)
    });

    let dir = TempDir::new().expect("tempdir");
    let out = temp_path(&dir, "out.csv");
    let todo = lineage_with(
        Arc::new(registry),
        &format!(
            r#"
name: opaca
nodes:
  - {{ id: generar, type: source, connector: generator, config: {{ rows: 10 }} }}
  - {{ id: misterio, type: transform, op: opaca }}
  - {{ id: escribir, type: sink, connector: csv, config: {{ path: "{out}" }} }}
edges:
  - {{ from: generar, to: misterio }}
  - {{ from: misterio, to: escribir }}
"#
        ),
    )
    .await;

    assert!(node(&todo, "generar").columns.is_some());
    for id in ["misterio", "escribir"] {
        let n = node(&todo, id);
        assert!(n.columns.is_none(), "{n:?}");
        assert_eq!(n.unknown_at.as_deref(), Some("misterio"), "{n:?}");
    }
}

#[test]
fn encadenar_pasos_se_queda_con_el_mas_fuerte() {
    use Derivation::*;
    assert_eq!(Copy.then(Computed), Computed);
    assert_eq!(Aggregated.then(Computed), Aggregated);
    assert_eq!(Copy.then(Copy), Copy);
    assert_eq!(Aggregated.then(Unknown), Unknown);
}
