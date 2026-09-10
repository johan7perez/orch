//! Definición declarativa de un pipeline (el "pipeline como código").
//!
//! El formato en disco es YAML, pero la config de cada nodo se guarda como
//! `serde_json::Value` para que cada conector deserialice su propio esquema.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Identificador de nodo dentro de un pipeline.
pub type NodeId = String;

/// Versión de formato soportada por este binario.
pub const SPEC_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    #[serde(default = "default_version")]
    pub version: u32,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub settings: RunSettings,
    pub nodes: Vec<NodeSpec>,
    #[serde(default)]
    pub edges: Vec<EdgeSpec>,
}

fn default_version() -> u32 {
    SPEC_VERSION
}

impl PipelineSpec {
    pub fn from_yaml_str(path: &str, yaml: &str) -> crate::Result<Self> {
        serde_yaml::from_str(yaml).map_err(|source| crate::OrchError::SpecParse {
            path: path.to_string(),
            source,
        })
    }

    pub fn from_path(path: impl AsRef<std::path::Path>) -> crate::Result<Self> {
        let path = path.as_ref();
        let display = path.display().to_string();
        let raw = std::fs::read_to_string(path).map_err(|source| crate::OrchError::SpecIo {
            path: display.clone(),
            source,
        })?;
        Self::from_yaml_str(&display, &raw)
    }
}

/// Parámetros de ejecución del pipeline completo.
///
/// No existe un "máximo de nodos en paralelo": en un motor de dataflow por
/// streaming todos los nodos corren a la vez y el que regula el ritmo es la
/// contrapresión de los canales. Limitar los nodos concurrentes con un
/// semáforo puede bloquear el pipeline (un productor con permiso esperando a
/// un consumidor que nunca obtiene el suyo). La palanca real es
/// [`RunSettings::channel_capacity`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSettings {
    /// Filas por `RecordBatch` que los orígenes deberían producir.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Batches en vuelo por arista antes de aplicar contrapresión.
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
}

fn default_batch_size() -> usize {
    8_192
}

fn default_channel_capacity() -> usize {
    4
}

impl Default for RunSettings {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            channel_capacity: default_channel_capacity(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSpec {
    pub id: NodeId,
    /// Dependencias de orden puro (sin flujo de datos): este nodo no arranca
    /// hasta que los nodos listados terminen.
    #[serde(default)]
    pub after: Vec<NodeId>,
    #[serde(default)]
    pub retry: RetryPolicy,
    // `flatten` es incompatible con `deny_unknown_fields`, por eso este struct
    // no lo lleva.
    #[serde(flatten)]
    pub kind: NodeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeKind {
    /// Produce batches desde un origen externo.
    Source {
        connector: String,
        #[serde(default)]
        config: Value,
    },
    /// Consume batches, produce batches.
    Transform {
        op: String,
        #[serde(default)]
        config: Value,
    },
    /// Consume batches y los escribe a un destino externo.
    Sink {
        connector: String,
        #[serde(default)]
        config: Value,
    },
}

impl NodeKind {
    pub fn label(&self) -> &'static str {
        match self {
            NodeKind::Source { .. } => "source",
            NodeKind::Transform { .. } => "transform",
            NodeKind::Sink { .. } => "sink",
        }
    }

    /// Nombre del componente registrado (conector o transformación).
    pub fn component(&self) -> &str {
        match self {
            NodeKind::Source { connector, .. } | NodeKind::Sink { connector, .. } => connector,
            NodeKind::Transform { op, .. } => op,
        }
    }

    pub fn config(&self) -> &Value {
        match self {
            NodeKind::Source { config, .. }
            | NodeKind::Transform { config, .. }
            | NodeKind::Sink { config, .. } => config,
        }
    }

    pub fn accepts_input(&self) -> bool {
        !matches!(self, NodeKind::Source { .. })
    }

    pub fn produces_output(&self) -> bool {
        !matches!(self, NodeKind::Sink { .. })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeSpec {
    pub from: NodeId,
    pub to: NodeId,
}

/// Política de reintentos de un nodo.
///
/// Un reintento sólo es seguro mientras el nodo no haya consumido ni emitido
/// ningún batch: una vez que hay datos en vuelo, repetir el nodo duplicaría o
/// perdería filas. El ejecutor aplica esa regla y degrada a fallo definitivo.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    /// Intentos totales, incluyendo el primero. `1` = sin reintentos.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

fn default_max_attempts() -> u32 {
    1
}
fn default_backoff_ms() -> u64 {
    250
}
fn default_backoff_multiplier() -> f64 {
    2.0
}
fn default_max_backoff_ms() -> u64 {
    30_000
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            backoff_ms: default_backoff_ms(),
            backoff_multiplier: default_backoff_multiplier(),
            max_backoff_ms: default_max_backoff_ms(),
        }
    }
}

impl RetryPolicy {
    /// Espera antes del intento `attempt` (1-indexado; `attempt == 1` no espera).
    pub fn backoff_for(&self, attempt: u32) -> std::time::Duration {
        if attempt <= 1 {
            return std::time::Duration::ZERO;
        }
        let factor = self.backoff_multiplier.max(1.0).powi(attempt as i32 - 2);
        let millis = (self.backoff_ms as f64 * factor).min(self.max_backoff_ms as f64);
        std::time::Duration::from_millis(millis as u64)
    }
}
