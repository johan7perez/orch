//! Catálogo de conectores y transformaciones disponibles en tiempo de ejecución.
//!
//! El core no conoce ninguna implementación concreta: `orch-connectors` (y, en
//! la Fase 2, los plugins WASM) se registran aquí y el ejecutor sólo resuelve
//! nombres.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::Value;

use crate::connector::{Sink, Source, Transform};
use crate::error::{OrchError, Result};

type SourceFactory = Box<dyn Fn(&str, &Value) -> Result<Arc<dyn Source>> + Send + Sync>;
type TransformFactory = Box<dyn Fn(&str, &Value) -> Result<Arc<dyn Transform>> + Send + Sync>;
type SinkFactory = Box<dyn Fn(&str, &Value) -> Result<Arc<dyn Sink>> + Send + Sync>;

#[derive(Default)]
pub struct Registry {
    sources: BTreeMap<String, SourceFactory>,
    transforms: BTreeMap<String, TransformFactory>,
    sinks: BTreeMap<String, SinkFactory>,
    /// Esquema de config por componente, indexado por «tipo/nombre».
    schemas: BTreeMap<String, Value>,
    pushdown: BTreeMap<String, crate::pushdown::PushdownHandler>,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("sources", &self.source_names())
            .field("transforms", &self.transform_names())
            .field("sinks", &self.sink_names())
            .finish()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }
    /// Registra un origen.
    ///
    /// El tipo de config se infiere de la anotación del cierre, y de él salen
    /// **las dos cosas**: el parseo y el esquema que enseña el inspector. No
    /// hay forma de que se desincronicen porque no hay dos definiciones.
    pub fn register_source<C, F>(&mut self, name: impl Into<String>, factory: F) -> &mut Self
    where
        C: schemars::JsonSchema + serde::de::DeserializeOwned,
        F: Fn(&str, C) -> Result<Arc<dyn Source>> + Send + Sync + 'static,
    {
        let name = name.into();
        self.schemas.insert(clave("source", &name), esquema::<C>());
        self.sources.insert(
            name,
            Box::new(move |node, config| factory(node, crate::parse_config::<C>(node, config)?)),
        );
        self
    }

    pub fn register_transform<C, F>(&mut self, name: impl Into<String>, factory: F) -> &mut Self
    where
        C: schemars::JsonSchema + serde::de::DeserializeOwned,
        F: Fn(&str, C) -> Result<Arc<dyn Transform>> + Send + Sync + 'static,
    {
        let name = name.into();
        self.schemas
            .insert(clave("transform", &name), esquema::<C>());
        self.transforms.insert(
            name,
            Box::new(move |node, config| factory(node, crate::parse_config::<C>(node, config)?)),
        );
        self
    }

    pub fn register_sink<C, F>(&mut self, name: impl Into<String>, factory: F) -> &mut Self
    where
        C: schemars::JsonSchema + serde::de::DeserializeOwned,
        F: Fn(&str, C) -> Result<Arc<dyn Sink>> + Send + Sync + 'static,
    {
        let name = name.into();
        self.schemas.insert(clave("sink", &name), esquema::<C>());
        self.sinks.insert(
            name,
            Box::new(move |node, config| factory(node, crate::parse_config::<C>(node, config)?)),
        );
        self
    }

    /// El esquema de config de un componente, para generar su formulario.
    pub fn schema(&self, kind: &str, name: &str) -> Option<&Value> {
        self.schemas.get(&clave(kind, name))
    }
    /// Declara qué operaciones del nodo siguiente sabe absorber un origen.
    ///
    /// El manejador recibe la config del origen y la operación, y devuelve
    /// `true` si la aceptó, dejando la config ya modificada.
    pub fn register_pushdown<F>(&mut self, connector: impl Into<String>, handler: F) -> &mut Self
    where
        F: Fn(&mut Value, &crate::pushdown::PushdownOp) -> bool + Send + Sync + 'static,
    {
        self.pushdown.insert(connector.into(), Box::new(handler));
        self
    }

    pub(crate) fn pushdown_handler(
        &self,
        connector: &str,
    ) -> Option<&crate::pushdown::PushdownHandler> {
        self.pushdown.get(connector)
    }

    /// Conectores que aceptan que se les empuje trabajo.
    pub fn pushdown_names(&self) -> Vec<&str> {
        self.pushdown.keys().map(String::as_str).collect()
    }

    pub fn source_names(&self) -> Vec<&str> {
        self.sources.keys().map(String::as_str).collect()
    }

    pub fn transform_names(&self) -> Vec<&str> {
        self.transforms.keys().map(String::as_str).collect()
    }

    pub fn sink_names(&self) -> Vec<&str> {
        self.sinks.keys().map(String::as_str).collect()
    }

    /// ¿Está registrado el componente que pide este nodo?
    ///
    /// El diseñador lo usa para marcar un nodo cuyo conector no existe en vez
    /// de dibujarlo como si fuera bueno y fallar sólo al ejecutar.
    pub fn has(&self, kind: &crate::spec::NodeKind) -> bool {
        use crate::spec::NodeKind;
        match kind {
            NodeKind::Source { connector, .. } => self.sources.contains_key(connector),
            NodeKind::Transform { op, .. } => self.transforms.contains_key(op),
            NodeKind::Sink { connector, .. } => self.sinks.contains_key(connector),
        }
    }

    pub fn build_source(&self, name: &str, node: &str, config: &Value) -> Result<Arc<dyn Source>> {
        let factory = self
            .sources
            .get(name)
            .ok_or_else(|| unknown("source", name, self.source_names()))?;
        factory(node, config)
    }

    pub fn build_transform(
        &self,
        name: &str,
        node: &str,
        config: &Value,
    ) -> Result<Arc<dyn Transform>> {
        let factory = self
            .transforms
            .get(name)
            .ok_or_else(|| unknown("transform", name, self.transform_names()))?;
        factory(node, config)
    }

    pub fn build_sink(&self, name: &str, node: &str, config: &Value) -> Result<Arc<dyn Sink>> {
        let factory = self
            .sinks
            .get(name)
            .ok_or_else(|| unknown("sink", name, self.sink_names()))?;
        factory(node, config)
    }
}

fn clave(kind: &str, name: &str) -> String {
    format!("{kind}/{name}")
}

/// El esquema JSON del tipo de config, ya como `Value` para cruzar a la UI.
fn esquema<C: schemars::JsonSchema>() -> Value {
    serde_json::to_value(schemars::schema_for!(C)).unwrap_or(Value::Null)
}

fn unknown(kind: &'static str, name: &str, available: Vec<&str>) -> OrchError {
    OrchError::UnknownComponent {
        kind,
        name: name.to_string(),
        available: if available.is_empty() {
            "ninguno".to_string()
        } else {
            available.join(", ")
        },
    }
}
