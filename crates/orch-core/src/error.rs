//! Tipos de error del motor.

/// Alias de `Result` con [`OrchError`] por defecto.
pub type Result<T, E = OrchError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum OrchError {
    #[error("no se pudo leer el pipeline `{path}`: {source}")]
    SpecIo {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("no se pudo parsear el pipeline `{path}`: {source}")]
    SpecParse {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },

    /// El pipeline es sintácticamente válido pero no forma un DAG ejecutable.
    #[error("pipeline inválido: {0}")]
    Validation(String),

    #[error("{kind} `{name}` desconocido (disponibles: {available})")]
    UnknownComponent {
        kind: &'static str,
        name: String,
        available: String,
    },

    #[error("config inválida en el nodo `{node}`: {message}")]
    Config { node: String, message: String },

    #[error("el nodo `{node}` falló: {message}")]
    Node { node: String, message: String },

    /// Un consumidor cerró su canal sin haber terminado bien.
    #[error("nodo `{node}`: el consumidor `{downstream}` cerró el canal sin completar")]
    DownstreamFailed { node: String, downstream: String },

    #[error("nodo `{node}`: el nodo del que depende (`{upstream}`) no completó")]
    UpstreamFailed { node: String, upstream: String },

    #[error("error de arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("error de E/S: {0}")]
    Io(#[from] std::io::Error),

    #[error("la tarea del nodo terminó de forma anómala: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("{0}")]
    Other(String),
}

impl OrchError {
    /// Error genérico de nodo a partir de cualquier mensaje.
    pub fn node(node: impl Into<String>, message: impl std::fmt::Display) -> Self {
        OrchError::Node {
            node: node.into(),
            message: message.to_string(),
        }
    }

    pub fn config(node: impl Into<String>, message: impl std::fmt::Display) -> Self {
        OrchError::Config {
            node: node.into(),
            message: message.to_string(),
        }
    }
}

/// Deserializa el bloque `config` de un nodo, atribuyendo el error al nodo.
pub fn parse_config<T: serde::de::DeserializeOwned>(
    node: &str,
    config: &serde_json::Value,
) -> Result<T> {
    serde_json::from_value(config.clone()).map_err(|e| OrchError::config(node, e))
}
