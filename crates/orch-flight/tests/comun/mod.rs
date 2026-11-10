//! Lo que comparten los tests de Flight: un sink que guarda lo que recibe,
//! puertos libres, ejecutar un pipeline y certificados de usar y tirar.

#![allow(dead_code)]

use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use orch_core::arrow::array::{Array, Int64Array};
use orch_core::arrow::record_batch::RecordBatch;
use orch_core::{
    Dag, Executor, Input, NoConfig, NodeContext, NodeReport, PipelineSpec, Registry, RunReport,
    Sink,
};
use tempfile::TempDir;

pub type Recogido = Arc<Mutex<Vec<RecordBatch>>>;

/// Sink que guarda lo que recibe, para mirarlo después.
struct Guardar(Recogido);

#[async_trait]
impl Sink for Guardar {
    fn connector(&self) -> &str {
        "guardar"
    }

    async fn write(&self, _ctx: &NodeContext, input: &mut Input) -> orch_core::Result<()> {
        while let Some(lote) = input.recv().await? {
            self.0.lock().unwrap().push(lote);
        }
        Ok(())
    }
}

/// Todo lo que trae Orch, más `flight` y el sink `guardar`.
pub fn registro(recogido: &Recogido) -> Registry {
    let mut registry = orch_connectors::default_registry();
    orch_sql::register(&mut registry);
    orch_flight::register(&mut registry);
    let r = Arc::clone(recogido);
    registry.register_sink("guardar", move |_node, _: NoConfig| {
        let s: Arc<dyn Sink> = Arc::new(Guardar(Arc::clone(&r)));
        Ok(s)
    });
    registry
}

/// Un puerto libre de esta máquina.
pub fn puerto() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub async fn correr(registry: &Arc<Registry>, yaml: String) -> RunReport {
    let dag = Dag::build(PipelineSpec::from_yaml_str("test.yaml", &yaml).expect("YAML válido"))
        .expect("DAG válido");
    Executor::new(Arc::clone(registry))
        .run(&dag)
        .await
        .expect("el pipeline debería arrancar")
}

pub fn nodo<'a>(informe: &'a RunReport, id: &str) -> &'a NodeReport {
    informe
        .nodes
        .iter()
        .find(|n| n.id == id)
        .unwrap_or_else(|| panic!("no hay nodo `{id}` en {informe:?}"))
}

pub fn error_de(informe: &RunReport, id: &str) -> String {
    nodo(informe, id).error.clone().unwrap_or_default()
}

/// Los `id` que llegaron, en orden.
pub fn ids(recogido: &Recogido) -> Vec<i64> {
    recogido
        .lock()
        .unwrap()
        .iter()
        .flat_map(|lote| {
            let col = lote
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            (0..col.len()).map(move |i| col.value(i))
        })
        .collect()
}

/// Certificados de usar y tirar: una CA con un servidor y un cliente, y
/// otra CA ajena con su propio cliente, para lo que tiene que fallar.
pub struct Pki {
    _dir: TempDir,
    pub ca: PathBuf,
    pub servidor_cert: PathBuf,
    pub servidor_key: PathBuf,
    pub cliente_cert: PathBuf,
    pub cliente_key: PathBuf,
    pub otra_ca: PathBuf,
    pub intruso_cert: PathBuf,
    pub intruso_key: PathBuf,
}

impl Pki {
    pub fn nueva() -> Pki {
        use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};

        let dir = TempDir::new().unwrap();
        let escribir = |nombre: &str, texto: String| -> PathBuf {
            let ruta = dir.path().join(nombre);
            std::fs::write(&ruta, texto).unwrap();
            ruta
        };
        let ca = |nombre: &str| {
            let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, nombre);
            CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap()
        };
        let firmado = |nombres: Vec<String>, emisor: &CertifiedIssuer<'_, KeyPair>| {
            let clave = KeyPair::generate().unwrap();
            let cert = CertificateParams::new(nombres)
                .unwrap()
                .signed_by(&clave, emisor)
                .unwrap();
            (cert.pem(), clave.serialize_pem())
        };

        let propia = ca("CA de pruebas de Orch");
        let ajena = ca("Otra CA");
        let (servidor_cert, servidor_key) =
            firmado(vec!["localhost".into(), "127.0.0.1".into()], &propia);
        let (cliente_cert, cliente_key) = firmado(vec!["coordinador".into()], &propia);
        let (intruso_cert, intruso_key) = firmado(vec!["intruso".into()], &ajena);

        Pki {
            ca: escribir("ca.pem", propia.pem()),
            servidor_cert: escribir("servidor.pem", servidor_cert),
            servidor_key: escribir("servidor.key", servidor_key),
            cliente_cert: escribir("cliente.pem", cliente_cert),
            cliente_key: escribir("cliente.key", cliente_key),
            otra_ca: escribir("otra_ca.pem", ajena.pem()),
            intruso_cert: escribir("intruso.pem", intruso_cert),
            intruso_key: escribir("intruso.key", intruso_key),
            _dir: dir,
        }
    }
}

/// Una ruta como va dentro de un YAML entre comillas.
pub fn r(ruta: &std::path::Path) -> String {
    ruta.to_string_lossy().replace('\\', "/")
}
