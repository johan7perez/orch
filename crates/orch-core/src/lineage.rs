//! Lineage por columna: de qué columnas de origen sale cada columna.
//!
//! Se calcula sin leer datos, por el mismo camino que los esquemas. Cada
//! transformación dice, para cada columna que produce, de qué columnas de sus
//! entradas sale: un solo salto ([`StepLineage`]). El ejecutor encadena esos
//! saltos en orden topológico hasta llegar a los orígenes
//! ([`Executor::lineage`](crate::Executor::lineage)).
//!
//! Se distinguen dos maneras de depender de una columna:
//!
//! - **Directa**: su valor acaba en la columna de salida, tal cual o
//!   transformado. Es lo que dice [`ColumnLineage::origins`].
//! - **Indirecta**: se lee para decidir qué filas pasan o en qué orden —el
//!   `where` de un filtro, la clave de un join, el `ORDER BY`— pero su valor
//!   no llega a ninguna salida. Es [`NodeLineage::indirect`].
//!
//! Sin la segunda, el análisis de impacto mentiría: la clave de un join no
//! llega a ninguna columna de salida, pero quitarla rompe el pipeline.
//!
//! Igual que la propagación de esquemas, es *best effort*. Un nodo que no
//! sabe su esquema (un plugin, un origen que sólo lo descubre al leer) corta
//! la cadena; lo que va detrás queda como desconocido y dice dónde se cortó.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::Schema;
use serde::Serialize;

use crate::schema::InputSchemas;
use crate::spec::NodeId;

/// Qué le pasa al valor por el camino, de menos a más.
///
/// El orden importa: encadenar dos pasos se queda con el más fuerte. Una
/// suma de una columna calculada es una columna agregada.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Derivation {
    /// Llega tal cual, como mucho con otro nombre.
    Copy,
    /// Se calcula fila a fila a partir de sus orígenes (`spend * 1.18`).
    Computed,
    /// Resume varias filas: `sum(amount)`, `count(*)`, una ventana.
    Aggregated,
    /// Algún paso del camino no se pudo seguir. Los orígenes que tenga son
    /// los que sí se encontraron, no necesariamente todos.
    Unknown,
}

impl Derivation {
    /// El resultado de encadenar dos pasos.
    pub fn then(self, other: Self) -> Self {
        self.max(other)
    }

    /// Para enseñarlo a una persona.
    pub fn label(self) -> &'static str {
        match self {
            Derivation::Copy => "copia",
            Derivation::Computed => "calculada",
            Derivation::Aggregated => "agregada",
            Derivation::Unknown => "incierta",
        }
    }
}

/// Una columna de una de las entradas de un nodo.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InputColumn {
    /// El puerto, que por defecto es el id del nodo de origen.
    pub port: String,
    pub column: String,
}

impl InputColumn {
    pub fn new(port: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            port: port.into(),
            column: column.into(),
        }
    }
}

/// De qué columnas de entrada sale una columna de salida, en un solo salto.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDerivation {
    pub column: String,
    /// Vacío en una constante o un `count(*)`: no lee ninguna columna.
    pub inputs: Vec<InputColumn>,
    pub derivation: Derivation,
}

impl ColumnDerivation {
    /// `column` copiada de la columna `from` de **todas** las entradas.
    ///
    /// Es lo que hace el fan-in: apila flujos de la misma forma, así que una
    /// columna de la salida sale de la del mismo nombre en cada entrada.
    pub fn copy_of(inputs: &InputSchemas, from: &str, column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            inputs: inputs
                .ports()
                .iter()
                .map(|p| InputColumn::new(&p.port, from))
                .collect(),
            derivation: Derivation::Copy,
        }
    }

    /// Todas las columnas de `schema`, tal cual: un `limit`, un sink.
    pub fn passthrough(inputs: &InputSchemas, schema: &Schema) -> Vec<Self> {
        schema
            .fields()
            .iter()
            .map(|f| Self::copy_of(inputs, f.name(), f.name()))
            .collect()
    }
}

/// Lo que una transformación sabe de sus columnas: un solo salto.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StepLineage {
    /// Cada columna de salida, en el orden en que sale.
    pub columns: Vec<ColumnDerivation>,
    /// Columnas que lee sin que su valor pase a ninguna salida: el `where`
    /// de un filtro, la clave de un join, el `ORDER BY`.
    pub indirect: Vec<InputColumn>,
}

/// Una columna de un nodo: de un origen, cuando es de donde sale otra, o de
/// un sink, cuando es adonde llega.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct NodeColumn {
    /// El pipeline del nodo, cuando no es el que se está mirando: una columna
    /// que sale de un fichero que escribe otro pipeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    pub node: NodeId,
    pub column: String,
}

impl NodeColumn {
    /// Una columna de un nodo de este mismo pipeline.
    pub fn new(node: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            pipeline: None,
            node: node.into(),
            column: column.into(),
        }
    }
}

impl std::fmt::Display for NodeColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.pipeline {
            Some(p) => write!(f, "{p}:{}.{}", self.node, self.column),
            None => write!(f, "{}.{}", self.node, self.column),
        }
    }
}

/// Una columna de un nodo, resuelta hasta los orígenes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ColumnLineage {
    pub column: String,
    /// Tipo Arrow tal y como se escribe (`Int64`, `Utf8`…).
    pub data_type: String,
    /// Las columnas de los orígenes de las que sale, sin repetir y en orden.
    /// Vacía si no lee ninguna: una constante, un `count(*)`.
    pub origins: Vec<NodeColumn>,
    pub derivation: Derivation,
}

/// El lineage de un nodo.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeLineage {
    pub node: NodeId,
    /// `source`, `transform` o `sink`.
    pub kind: &'static str,
    /// Las columnas que produce o, en un sink, las que escribe. `None` si no
    /// se pueden saber sin ejecutar.
    pub columns: Option<Vec<ColumnLineage>>,
    /// Columnas de los orígenes que este nodo lee sin pasarlas a su salida.
    /// Sólo las de este nodo, no las de los anteriores.
    pub indirect: Vec<NodeColumn>,
    /// Si no se sabe, el nodo donde se cortó la cadena: este mismo o uno
    /// anterior.
    pub unknown_at: Option<NodeId>,
}

impl NodeLineage {
    pub(crate) fn unknown(node: &str, kind: &'static str, at: &str) -> Self {
        Self {
            node: node.to_string(),
            kind,
            columns: None,
            indirect: Vec::new(),
            unknown_at: Some(at.to_string()),
        }
    }

    pub fn column(&self, name: &str) -> Option<&ColumnLineage> {
        self.columns.as_ref()?.iter().find(|c| c.column == name)
    }
}

/// Las columnas de un origen: cada una es su propio origen.
pub(crate) fn of_source(node: &str, schema: &Schema) -> NodeLineage {
    NodeLineage {
        node: node.to_string(),
        kind: "source",
        columns: Some(
            schema
                .fields()
                .iter()
                .map(|f| ColumnLineage {
                    column: f.name().clone(),
                    data_type: f.data_type().to_string(),
                    origins: vec![NodeColumn::new(node, f.name())],
                    derivation: Derivation::Copy,
                })
                .collect(),
        ),
        indirect: Vec::new(),
        unknown_at: None,
    }
}

// --- Ficheros que escribe uno y lee otro ----------------------------------------

/// Un sink que escribe un fichero que lee un origen: lo que el origen hereda.
#[derive(Debug, Clone)]
pub struct Productor {
    /// El pipeline del sink; `None` si es el mismo que el del origen.
    pub pipeline: Option<String>,
    pub nodo: NodeId,
    /// Lo que escribe, si se sabe: el esquema que tendrá el fichero.
    pub schema: Option<arrow::datatypes::SchemaRef>,
    /// Sus columnas, con sus orígenes.
    pub columnas: Vec<ColumnLineage>,
}

/// Los ficheros que escriben otros pipelines, por su ruta normalizada.
pub type Externos = std::collections::HashMap<String, Productor>;

/// El fichero que lee o escribe un nodo: su `path`, la única clave que en la
/// config de un nodo significa «fichero en disco».
pub fn ruta_de(kind: &crate::spec::NodeKind) -> Option<String> {
    use crate::spec::NodeKind;
    let config = match kind {
        NodeKind::Source { config, .. } | NodeKind::Sink { config, .. } => config,
        NodeKind::Transform { .. } => return None,
    };
    config.get("path")?.as_str().map(normalizar_ruta)
}

/// Quita `.` y resuelve `..` sin tocar el disco: el fichero puede no existir
/// todavía, y `out/x.csv` y `./out/../out/x.csv` son el mismo.
pub fn normalizar_ruta(ruta: &str) -> String {
    use std::path::{Component, PathBuf};
    let mut limpia = PathBuf::new();
    for c in std::path::Path::new(ruta).components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                if !limpia.pop() {
                    limpia.push("..");
                }
            }
            otro => limpia.push(otro.as_os_str()),
        }
    }
    limpia.to_string_lossy().into_owned()
}

/// El esquema de lo que escribió `productor`, como lo leerá un origen: si el
/// origen sólo pide unas `columns`, sólo esas y en ese orden. Es la misma
/// convención que usa el pushdown para recortar columnas en el origen.
pub(crate) fn esquema_leido(
    escrito: &arrow::datatypes::SchemaRef,
    config: &serde_json::Value,
) -> Option<arrow::datatypes::SchemaRef> {
    let Some(columnas) = config.get("columns").and_then(|c| c.as_array()) else {
        return Some(arrow::datatypes::SchemaRef::clone(escrito));
    };
    let indices: Option<Vec<usize>> = columnas
        .iter()
        .map(|c| c.as_str().and_then(|n| escrito.index_of(n).ok()))
        .collect();
    escrito.project(&indices?).ok().map(std::sync::Arc::new)
}

/// Las columnas de un origen que lee lo que escribe `productor`: cada una
/// hereda los orígenes de la columna del mismo nombre del sink. Lo que el
/// sink no escribe (un fichero más viejo, con otra forma) se queda como
/// origen propio.
pub(crate) fn of_source_enlazado(
    node: &str,
    schema: &Schema,
    productor: &Productor,
) -> NodeLineage {
    let mut lineage = of_source(node, schema);
    for columna in lineage.columns.iter_mut().flatten() {
        if let Some(de_sink) = productor
            .columnas
            .iter()
            .find(|c| c.column == columna.column)
        {
            columna.origins = de_sink.origins.clone();
            columna.derivation = de_sink.derivation;
        }
    }
    lineage
}

/// Encadena un salto con lo ya resuelto en las entradas.
///
/// `upstream` da, para cada puerto, el lineage del nodo del que viene.
/// `schema` es lo que el nodo produce (o lo que escribe un sink), de donde
/// salen los tipos.
pub(crate) fn resolve(
    node: &str,
    kind: &'static str,
    step: StepLineage,
    upstream: &BTreeMap<String, &NodeLineage>,
    schema: Option<&Schema>,
) -> NodeLineage {
    let origins_of = |input: &InputColumn| -> Option<(Vec<NodeColumn>, Derivation)> {
        let column = upstream.get(&input.port)?.column(&input.column)?;
        Some((column.origins.clone(), column.derivation))
    };

    // Los tipos se buscan por posición si cuadran —una query puede devolver
    // dos columnas con el mismo nombre— y si no, por nombre.
    let by_position = schema.is_some_and(|s| s.fields().len() == step.columns.len());
    let data_type = |i: usize, name: &str| -> String {
        let Some(schema) = schema else {
            return String::new();
        };
        let field = if by_position {
            Some(schema.field(i))
        } else {
            schema.field_with_name(name).ok()
        };
        field.map(|f| f.data_type().to_string()).unwrap_or_default()
    };

    let columns: Vec<ColumnLineage> = step
        .columns
        .into_iter()
        .enumerate()
        .map(|(i, derived)| {
            let mut origins = BTreeSet::new();
            let mut derivation = derived.derivation;
            for input in &derived.inputs {
                match origins_of(input) {
                    Some((found, how)) => {
                        origins.extend(found);
                        derivation = derivation.then(how);
                    }
                    None => derivation = Derivation::Unknown,
                }
            }
            ColumnLineage {
                data_type: data_type(i, &derived.column),
                column: derived.column,
                origins: origins.into_iter().collect(),
                derivation,
            }
        })
        .collect();

    // Una columna puede pasar a la salida y además decidir qué filas pasan
    // (`SELECT * … WHERE status = 'pagado'`): sale en los dos sitios, porque
    // quitarla rompe las dos cosas.
    let indirect: BTreeSet<NodeColumn> = step
        .indirect
        .iter()
        .filter_map(origins_of)
        .flat_map(|(found, _)| found)
        .collect();

    NodeLineage {
        node: node.to_string(),
        kind,
        columns: Some(columns),
        indirect: indirect.into_iter().collect(),
        unknown_at: None,
    }
}

/// Lo que se hace con una columna de un origen: a qué salidas llega y en qué
/// nodos se usa sin pasar a la salida.
///
/// Es la pregunta del análisis de impacto: «si cambio esta columna, ¿qué
/// se rompe?».
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Impact {
    /// Columnas de los sinks que salen de ella.
    pub outputs: Vec<NodeColumn>,
    /// Nodos que la leen para filtrar, unir u ordenar.
    pub used_by: Vec<NodeId>,
}

impl Impact {
    pub fn is_empty(&self) -> bool {
        self.outputs.is_empty() && self.used_by.is_empty()
    }
}

/// El impacto de una columna de origen sobre el pipeline entero.
pub fn impact(lineage: &[NodeLineage], origin: &NodeColumn) -> Impact {
    let mut impact = Impact::default();
    for node in lineage {
        if node.kind == "sink" {
            for column in node.columns.iter().flatten() {
                if column.origins.contains(origin) {
                    impact
                        .outputs
                        .push(NodeColumn::new(&node.node, &column.column));
                }
            }
        }
        if node.indirect.contains(origin) {
            impact.used_by.push(node.node.clone());
        }
    }
    impact
}

/// El lineage de varios pipelines a la vez —una carpeta—, siguiendo los
/// ficheros que escribe uno y lee otro: una columna de la salida de `b` que
/// sale de un fichero que escribe `a` llega hasta los orígenes de `a`.
///
/// Se calculan en orden: primero quien escribe, luego quien lee. Si dos se
/// leen mutuamente (un ciclo), lo que no se puede ordenar se calcula sin
/// enlazar, en vez de dar vueltas. El resultado va en el orden de entrada.
pub async fn de_varios(
    executor: &crate::Executor,
    pipelines: &[(String, crate::Dag)],
) -> Vec<crate::Result<Vec<NodeLineage>>> {
    use crate::spec::NodeKind;
    use std::collections::BTreeSet;

    let rutas = |dag: &crate::Dag, de_sink: bool| -> BTreeSet<String> {
        dag.nodes()
            .iter()
            .filter(|n| matches!(n.kind, NodeKind::Sink { .. }) == de_sink)
            .filter(|n| !matches!(n.kind, NodeKind::Transform { .. }))
            .filter_map(|n| ruta_de(&n.kind))
            .collect()
    };
    let escribe: Vec<BTreeSet<String>> = pipelines.iter().map(|(_, d)| rutas(d, true)).collect();
    let lee: Vec<BTreeSet<String>> = pipelines.iter().map(|(_, d)| rutas(d, false)).collect();

    // Kahn: `i` antes que `j` si `j` lee algo que escribe `i`.
    let n = pipelines.len();
    let depende = |j: usize, i: usize| i != j && !escribe[i].is_disjoint(&lee[j]);
    let mut pendientes: Vec<usize> = (0..n).collect();
    let mut orden = Vec::with_capacity(n);
    while !pendientes.is_empty() {
        let libre = pendientes
            .iter()
            .position(|&j| !pendientes.iter().any(|&i| depende(j, i)));
        // Con un ciclo no hay ninguno libre: se sigue por el primero.
        let k = libre.unwrap_or(0);
        orden.push(pendientes.remove(k));
    }

    let mut externos = Externos::new();
    let mut resultados: Vec<Option<crate::Result<Vec<NodeLineage>>>> =
        (0..n).map(|_| None).collect();
    for i in orden {
        let (nombre, dag) = &pipelines[i];
        // Lo que escribe él mismo lo resuelve él, con sus propios sinks.
        let ajenos: Externos = externos
            .iter()
            .filter(|(ruta, _)| !escribe[i].contains(*ruta))
            .map(|(r, p)| (r.clone(), p.clone()))
            .collect();
        resultados[i] = Some(match executor.lineage_con(dag, &ajenos).await {
            Ok((nodos, escritos)) => {
                for (ruta, mut productor) in escritos {
                    productor.pipeline = Some(nombre.clone());
                    // Visto desde otro pipeline, lo que no dice de dónde es,
                    // es de éste.
                    for columna in &mut productor.columnas {
                        for origen in &mut columna.origins {
                            origen.pipeline.get_or_insert_with(|| nombre.clone());
                        }
                    }
                    externos.insert(ruta, productor);
                }
                Ok(nodos)
            }
            Err(err) => Err(err),
        });
    }
    resultados
        .into_iter()
        .map(|r| r.expect("cada pipeline se calcula una vez"))
        .collect()
}

/// A qué salidas de **otros** pipelines llega una columna de un origen de
/// `pipeline`: las que la leen a través de un fichero.
pub fn impacto_en_otros(
    otros: &[(String, Vec<NodeLineage>)],
    pipeline: &str,
    origen: &NodeColumn,
) -> Vec<NodeColumn> {
    let buscado = NodeColumn {
        pipeline: Some(pipeline.to_string()),
        ..origen.clone()
    };
    let mut salidas = Vec::new();
    for (nombre, nodos) in otros {
        for nodo in nodos.iter().filter(|n| n.kind == "sink") {
            for columna in nodo.columns.iter().flatten() {
                if columna.origins.contains(&buscado) {
                    salidas.push(NodeColumn {
                        pipeline: Some(nombre.clone()),
                        node: nodo.node.clone(),
                        column: columna.column.clone(),
                    });
                }
            }
        }
    }
    salidas
}
