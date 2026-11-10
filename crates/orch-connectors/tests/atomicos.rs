//! Los sinks de fichero escriben todo o nada.
//!
//! Un pipeline que falla a mitad no puede dejar un CSV o un Parquet cortado
//! que parezca bueno: el destino se queda como estaba (con el resultado
//! anterior, o sin fichero), y no quedan temporales por ahí.

use std::sync::Arc;

use async_trait::async_trait;
use orch_core::arrow::array::Int64Array;
use orch_core::arrow::datatypes::{DataType, Field, Schema};
use orch_core::arrow::record_batch::RecordBatch;
use orch_core::{
    Dag, Executor, NoConfig, NodeContext, OrchError, Output, PipelineSpec, Result, RunReport,
    Source,
};
use tempfile::TempDir;

/// Manda unos lotes y luego falla: lo que pasaría con una API que se cae a
/// mitad o un fichero corrupto.
struct FallaAMitad;

#[async_trait]
impl Source for FallaAMitad {
    fn connector(&self) -> &str {
        "falla_a_mitad"
    }

    async fn read(&self, _ctx: &NodeContext, output: &Output) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for lote in 0..3i64 {
            let ids = Int64Array::from_iter_values(lote * 100..lote * 100 + 100);
            output
                .send(RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(ids)],
                )?)
                .await?;
        }
        Err(OrchError::Other("se cayó la conexión a mitad".into()))
    }
}

async fn correr(yaml: &str) -> RunReport {
    let mut registry = orch_connectors::default_registry();
    registry.register_source("falla_a_mitad", |_node, _: NoConfig| {
        let s: Arc<dyn Source> = Arc::new(FallaAMitad);
        Ok(s)
    });
    let dag = Dag::build(PipelineSpec::from_yaml_str("test.yaml", yaml).unwrap()).unwrap();
    Executor::new(Arc::new(registry)).run(&dag).await.unwrap()
}

fn pipeline(origen: &str, sink: &str, destino: &str) -> String {
    format!(
        r#"
name: atomico
nodes:
  - {{ id: leer, type: source, connector: {origen} }}
  - {{ id: escribir, type: sink, connector: {sink}, config: {{ path: "{destino}" }} }}
edges:
  - {{ from: leer, to: escribir }}
"#
    )
}

/// Lo que hay en la carpeta, sin contar lo que se puso a propósito.
fn restos(dir: &TempDir) -> Vec<String> {
    std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains("orch-tmp"))
        .collect()
}

fn ruta(dir: &TempDir, nombre: &str) -> (std::path::PathBuf, String) {
    let p = dir.path().join(nombre);
    let s = p.to_string_lossy().replace('\\', "/");
    (p, s)
}

#[tokio::test]
async fn un_csv_a_medias_no_pisa_el_resultado_anterior() {
    let dir = TempDir::new().unwrap();
    let (destino, destino_yaml) = ruta(&dir, "salida.csv");
    std::fs::write(&destino, "id\n1\n2\n").unwrap();

    let informe = correr(&pipeline("falla_a_mitad", "csv", &destino_yaml)).await;
    assert!(!informe.succeeded);
    // El de ayer sigue ahí, entero; el de hoy no apareció a medias.
    assert_eq!(std::fs::read_to_string(&destino).unwrap(), "id\n1\n2\n");
    assert!(restos(&dir).is_empty(), "{:?}", restos(&dir));
}

#[tokio::test]
async fn sin_resultado_anterior_no_queda_ninguno() {
    let dir = TempDir::new().unwrap();
    let (destino, destino_yaml) = ruta(&dir, "salida.csv");

    let informe = correr(&pipeline("falla_a_mitad", "csv", &destino_yaml)).await;
    assert!(!informe.succeeded);
    assert!(!destino.exists(), "un CSV cortado parecería un resultado");
    assert!(restos(&dir).is_empty(), "{:?}", restos(&dir));
}

#[tokio::test]
async fn un_parquet_a_medias_tampoco() {
    let dir = TempDir::new().unwrap();
    let (destino, destino_yaml) = ruta(&dir, "salida.parquet");
    std::fs::write(&destino, b"el de ayer").unwrap();

    let informe = correr(&pipeline("falla_a_mitad", "parquet", &destino_yaml)).await;
    assert!(!informe.succeeded);
    assert_eq!(std::fs::read(&destino).unwrap(), b"el de ayer");
    assert!(restos(&dir).is_empty(), "{:?}", restos(&dir));
}

#[tokio::test]
async fn lo_que_termina_bien_sustituye_al_anterior_entero() {
    let dir = TempDir::new().unwrap();
    let (csv, csv_yaml) = ruta(&dir, "salida.csv");
    let (parquet, parquet_yaml) = ruta(&dir, "salida.parquet");
    std::fs::write(&csv, "lo de ayer\n").unwrap();

    for (sink, destino) in [("csv", &csv_yaml), ("parquet", &parquet_yaml)] {
        let yaml = format!(
            r#"
name: bien
nodes:
  - {{ id: leer, type: source, connector: generator, config: {{ rows: 1000, with_text: false }} }}
  - {{ id: escribir, type: sink, connector: {sink}, config: {{ path: "{destino}" }} }}
edges:
  - {{ from: leer, to: escribir }}
"#
        );
        let informe = correr(&yaml).await;
        assert!(informe.succeeded, "{informe:?}");
    }
    let texto = std::fs::read_to_string(&csv).unwrap();
    assert!(texto.starts_with("id,value"), "{texto}");
    assert_eq!(texto.lines().count(), 1001);
    // El Parquet se lee: tiene su pie.
    let lector = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(&parquet).unwrap(),
    )
    .unwrap();
    assert_eq!(lector.metadata().file_metadata().num_rows(), 1000);
    assert!(restos(&dir).is_empty(), "{:?}", restos(&dir));
}
