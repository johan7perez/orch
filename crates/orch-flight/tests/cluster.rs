//! Un pipeline repartido entre workers de verdad: cada uno un servicio
//! Flight con TLS mutuo en su propio puerto de esta máquina.

mod comun;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use comun::{error_de, ids, nodo, puerto, Pki, Recogido};
use orch_core::{Dag, EdgeSpec, Executor, NodeKind, PipelineSpec, Plazo, RunEvent};
use orch_flight::cluster::{dividir, servir_worker, ClusterConfig, ClusterTls, Coordinador, Donde};

fn spec(yaml: &str) -> PipelineSpec {
    PipelineSpec::from_yaml_str_as_written("test.yaml", yaml).expect("YAML válido")
}

fn tls(pki: &Pki) -> ClusterTls {
    ClusterTls {
        ca: pki.ca.clone(),
        cert: pki.servidor_cert.clone(),
        key: pki.servidor_key.clone(),
    }
}

/// Arranca un worker y devuelve su url.
async fn worker(pki: &Pki) -> String {
    let escucha = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("https://127.0.0.1:{}", escucha.local_addr().unwrap().port());
    let tls = tls(pki);
    let registry = comun::registro(&Recogido::default());
    tokio::spawn(async move {
        servir_worker(
            escucha,
            &tls,
            registry,
            PathBuf::from("."),
            std::future::pending(),
        )
        .await
        .unwrap();
    });
    url
}

fn cluster(pki: &Pki, workers: &[(&str, &str)], escucha: Option<u16>) -> ClusterConfig {
    ClusterConfig {
        tls: tls(pki),
        workers: workers
            .iter()
            .map(|(n, u)| (n.to_string(), u.to_string()))
            .collect(),
        listen: escucha.map(|p| format!("127.0.0.1:{p}")),
        url: escucha.map(|p| format!("https://127.0.0.1:{p}")),
        wait: Plazo(Duration::from_secs(10)),
    }
}

// --- Partir el pipeline -------------------------------------------------------

const REPARTIDO: &str = r#"
name: repartido
nodes:
  - { id: leer, type: source, connector: generator, worker: a, config: { rows: 10 } }
  - { id: limpiar, type: transform, op: filter, worker: b, config: { where: "id > 1" } }
  - { id: contar, type: transform, op: sql, worker: b, config: { query: "SELECT count(*) AS n FROM leer" } }
  - { id: guardar, type: sink, connector: guardar }
  - { id: guardar_n, type: sink, connector: guardar }
edges:
  - { from: leer, to: limpiar }
  - { from: leer, to: contar }
  - { from: limpiar, to: guardar }
  - { from: contar, to: guardar_n }
"#;

fn urls() -> BTreeMap<String, String> {
    [("a", "https://a:1"), ("b", "https://b:1")]
        .into_iter()
        .map(|(n, u)| (n.to_string(), u.to_string()))
        .collect()
}

#[test]
fn partir_pone_una_arista_por_cada_salto_entre_maquinas() {
    let partes = dividir(
        &spec(REPARTIDO),
        "RUN",
        &urls(),
        None,
        Plazo(Duration::from_secs(5)),
    )
    .unwrap();
    let por: BTreeMap<_, _> = partes.iter().map(|p| (p.donde.clone(), &p.spec)).collect();
    let ids = |d: Donde| -> Vec<String> { por[&d].nodes.iter().map(|n| n.id.clone()).collect() };

    // `leer` alimenta a dos nodos de `b`: viaja una sola vez.
    assert_eq!(ids(Donde::Worker("a".into())), ["leer", "leer→b"]);
    assert_eq!(
        ids(Donde::Worker("b".into())),
        [
            "limpiar",
            "contar",
            "leer@b",
            "limpiar→local",
            "contar→local"
        ]
    );
    assert_eq!(
        ids(Donde::Local),
        ["guardar", "guardar_n", "limpiar@local", "contar@local"]
    );

    let b = por[&Donde::Worker("b".into())];
    let leer = b.nodes.iter().find(|n| n.id == "leer@b").unwrap();
    let NodeKind::Source { connector, config } = &leer.kind else {
        panic!("{leer:?}")
    };
    assert_eq!(connector, "cluster_in");
    assert_eq!(config["url"], "https://a:1");
    // El ticket lleva la ejecución: dos a la vez no se cruzan.
    assert_eq!(config["ticket"], "RUN/leer/b");
    // El puerto sigue llamándose `leer`: la query lee la tabla `leer`.
    assert!(b.edges.iter().any(|e: &EdgeSpec| e.from == "leer@b"
        && e.to == "contar"
        && e.port.as_deref() == Some("leer")));
    // Nadie lleva ya `worker`: cada parte corre entera en su máquina.
    assert!(partes
        .iter()
        .flat_map(|p| &p.spec.nodes)
        .all(|n| n.worker.is_none()));
    // Cada parte es un pipeline válido por sí misma.
    for p in &partes {
        Dag::build(p.spec.clone()).unwrap_or_else(|e| panic!("{:?}: {e}", p.donde));
    }
}

#[test]
fn partir_rechaza_lo_que_no_puede_repartir() {
    let plazo = Plazo(Duration::from_secs(5));
    let error = |yaml: &str| {
        dividir(&spec(yaml), "R", &urls(), None, plazo)
            .unwrap_err()
            .to_string()
    };

    let desconocido = REPARTIDO.replace("worker: b", "worker: z");
    assert!(
        error(&desconocido).contains("`z`"),
        "{}",
        error(&desconocido)
    );

    // Lo de aquí alimenta a un worker y el coordinador no escucha.
    let desde_aqui = r#"
name: x
nodes:
  - { id: leer, type: source, connector: generator, config: { rows: 1 } }
  - { id: tirar, type: sink, connector: "null", worker: a }
edges:
  - { from: leer, to: tirar }
"#;
    assert!(
        error(desde_aqui).contains("listen"),
        "{}",
        error(desde_aqui)
    );
    assert!(dividir(&spec(desde_aqui), "R", &urls(), Some("https://yo:1"), plazo).is_ok());

    let espera_cruzada = r#"
name: x
nodes:
  - { id: leer, type: source, connector: generator, worker: a, config: { rows: 1 } }
  - { id: tirar, type: sink, connector: "null", worker: a }
  - { id: otro, type: source, connector: generator, after: [tirar], config: { rows: 1 } }
  - { id: tirar2, type: sink, connector: "null" }
edges:
  - { from: leer, to: tirar }
  - { from: otro, to: tirar2 }
"#;
    assert!(
        error(espera_cruzada).contains("after"),
        "{}",
        error(espera_cruzada)
    );
}

#[tokio::test]
async fn el_motor_local_no_ejecuta_lo_que_va_en_un_worker() {
    let registry = Arc::new(comun::registro(&Recogido::default()));
    let dag = Dag::build(spec(REPARTIDO)).unwrap();
    let error = Executor::new(registry)
        .run(&dag)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("--cluster"), "{error}");
}

// --- Ejecutar repartido -------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn de_worker_a_worker_y_de_vuelta_aqui() {
    let pki = Pki::nueva();
    let (a, b) = (worker(&pki).await, worker(&pki).await);
    let recogido = Recogido::default();
    let coordinador = Coordinador::new(
        comun::registro(&recogido),
        cluster(&pki, &[("a", &a), ("b", &b)], None),
    )
    .unwrap();
    let mut eventos = coordinador.subscribe();

    let informe = coordinador
        .run(
            &spec(
                r#"
name: repartido
nodes:
  - { id: generar, type: source, connector: generator, worker: a, config: { rows: 100000, with_text: false } }
  - { id: pares, type: transform, op: filter, worker: b, config: { where: "id % 2 = 0" } }
  - { id: guardar, type: sink, connector: guardar }
edges:
  - { from: generar, to: pares }
  - { from: pares, to: guardar }
"#,
            ),
            &PathBuf::from("."),
        )
        .await
        .unwrap();

    assert!(informe.succeeded, "{informe:?}");
    // Un nodo SQL no promete el orden sin `ORDER BY` (DataFusion reparte
    // los lotes entre particiones), igual que en una sola máquina: se
    // comprueba que llegan todas, no en qué orden.
    let mut llegados = ids(&recogido);
    llegados.sort_unstable();
    assert_eq!(llegados, (0..100_000).step_by(2).collect::<Vec<i64>>());
    // El informe tiene los nodos del pipeline primero, de todas las máquinas.
    let primeros: Vec<&str> = informe
        .nodes
        .iter()
        .take(3)
        .map(|n| n.id.as_str())
        .collect();
    assert_eq!(primeros, ["generar", "pares", "guardar"]);
    assert_eq!(nodo(&informe, "generar").output.rows, 100_000);
    assert_eq!(nodo(&informe, "pares").output.rows, 50_000);
    // Las aristas entre máquinas no cuentan como escritura: sólo `guardar`.
    assert_eq!(nodo(&informe, "generar→b").kind, "edge");
    assert_eq!(nodo(&informe, "pares@local").kind, "edge");
    assert_eq!(informe.rows_written(), 50_000);

    // Una sola ejecución para quien escucha: un arranque, un final, y los
    // eventos de los nodos de todas las máquinas con el mismo id.
    let mut vistos = Vec::new();
    while let Ok(evento) = eventos.try_recv() {
        vistos.push(evento);
    }
    let arranques = vistos
        .iter()
        .filter(|e| matches!(e, RunEvent::RunStarted { .. }))
        .count();
    let finales = vistos
        .iter()
        .filter(|e| matches!(e, RunEvent::RunFinished { .. }))
        .count();
    assert_eq!((arranques, finales), (1, 1));
    assert!(vistos.iter().all(|e| e.run_id() == informe.run_id));
    assert!(vistos
        .iter()
        .any(|e| matches!(e, RunEvent::NodeFinished { node, .. } if node == "generar")));
    assert!(matches!(
        vistos.last(),
        Some(RunEvent::RunFinished {
            succeeded: true,
            ..
        })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn de_aqui_a_un_worker_y_de_vuelta() {
    let pki = Pki::nueva();
    let a = worker(&pki).await;
    let recogido = Recogido::default();
    let coordinador = Coordinador::new(
        comun::registro(&recogido),
        cluster(&pki, &[("a", &a)], Some(puerto())),
    )
    .unwrap();

    let informe = coordinador
        .run(
            &spec(
                r#"
name: ida-y-vuelta
nodes:
  - { id: generar, type: source, connector: generator, config: { rows: 30000, with_text: false } }
  - id: contar
    type: transform
    op: sql
    worker: a
    config: { query: "SELECT count(*) AS id FROM generar" }
  - { id: guardar, type: sink, connector: guardar }
edges:
  - { from: generar, to: contar }
  - { from: contar, to: guardar }
"#,
            ),
            &PathBuf::from("."),
        )
        .await
        .unwrap();
    assert!(informe.succeeded, "{informe:?}");
    assert_eq!(ids(&recogido), [30_000]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn un_fallo_en_un_worker_llega_al_informe() {
    let pki = Pki::nueva();
    let (a, b) = (worker(&pki).await, worker(&pki).await);
    let coordinador = Coordinador::new(
        comun::registro(&Recogido::default()),
        cluster(&pki, &[("a", &a), ("b", &b)], None),
    )
    .unwrap();

    let informe = coordinador
        .run(
            &spec(
                r#"
name: falla-en-b
nodes:
  - { id: generar, type: source, connector: generator, worker: a, config: { rows: 100000, with_text: false } }
  - { id: romper, type: transform, op: derive, worker: b, config: { columns: { x: "10 / (id - 50000)" } } }
  - { id: guardar, type: sink, connector: guardar }
edges:
  - { from: generar, to: romper }
  - { from: romper, to: guardar }
"#,
            ),
            &PathBuf::from("."),
        )
        .await
        .unwrap();
    assert!(!informe.succeeded);
    assert!(
        error_de(&informe, "romper")
            .to_lowercase()
            .contains("divide"),
        "{informe:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn un_worker_que_no_responde_cancela_lo_demas_sin_esperar() {
    let pki = Pki::nueva();
    let a = worker(&pki).await;
    // Nadie escucha en este puerto.
    let muerto = format!("https://127.0.0.1:{}", puerto());
    let coordinador = Coordinador::new(
        comun::registro(&Recogido::default()),
        cluster(&pki, &[("a", &a), ("muerto", &muerto)], None),
    )
    .unwrap();

    let inicio = Instant::now();
    let error = coordinador
        .run(
            &spec(
                r#"
name: con-un-muerto
nodes:
  - { id: generar, type: source, connector: generator, worker: a, config: { rows: 1000 } }
  - { id: pasar, type: transform, op: limit, worker: muerto, config: { rows: 10 } }
  - { id: guardar, type: sink, connector: guardar }
edges:
  - { from: generar, to: pasar }
  - { from: pasar, to: guardar }
"#,
            ),
            &PathBuf::from("."),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("worker `muerto`"), "{error}");
    // Las otras partes esperaban hasta 10 s una arista que no iba a llegar:
    // se cancelan en vez de agotarlo.
    assert!(
        inicio.elapsed() < Duration::from_secs(8),
        "{:?}",
        inicio.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn un_worker_sin_el_conector_lo_dice_con_su_nombre() {
    let pki = Pki::nueva();
    let a = worker(&pki).await;
    let coordinador = Coordinador::new(
        comun::registro(&Recogido::default()),
        cluster(&pki, &[("a", &a)], None),
    )
    .unwrap();
    let error = coordinador
        .run(
            &spec(
                r#"
name: falta-algo
nodes:
  - { id: leer, type: source, connector: no_existe, worker: a }
  - { id: guardar, type: sink, connector: guardar }
edges:
  - { from: leer, to: guardar }
"#,
            ),
            &PathBuf::from("."),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("worker `a`") && error.contains("no_existe"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn un_coordinador_de_otra_ca_no_entra() {
    let pki = Pki::nueva();
    let a = worker(&pki).await;
    let mut config = cluster(&pki, &[("a", &a)], None);
    config.tls.cert = pki.intruso_cert.clone();
    config.tls.key = pki.intruso_key.clone();
    let coordinador = Coordinador::new(comun::registro(&Recogido::default()), config).unwrap();

    let resultado = coordinador
        .run(
            &spec(
                r#"
name: intruso
nodes:
  - { id: leer, type: source, connector: generator, worker: a, config: { rows: 10 } }
  - { id: guardar, type: sink, connector: guardar }
edges:
  - { from: leer, to: guardar }
"#,
            ),
            &PathBuf::from("."),
        )
        .await;
    let error = resultado.unwrap_err().to_string();
    assert!(error.contains("worker `a`"), "{error}");
}
