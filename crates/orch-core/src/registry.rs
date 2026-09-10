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

    pub fn register_source<F>(&mut self, name: impl Into<String>, factory: F) -> &mut Self
    where
        F: Fn(&str, &Value) -> Result<Arc<dyn Source>> + Send + Sync + 'static,
    {
        self.sources.insert(name.into(), Box::new(factory));
        self
    }

    pub fn register_transform<F>(&mut self, name: impl Into<String>, factory: F) -> &mut Self
    where
        F: Fn(&str, &Value) -> Result<Arc<dyn Transform>> + Send + Sync + 'static,
    {
        self.transforms.insert(name.into(), Box::new(factory));
        self
    }

    pub fn register_sink<F>(&mut self, name: impl Into<String>, factory: F) -> &mut Self
    where
        F: Fn(&str, &Value) -> Result<Arc<dyn Sink>> + Send + Sync + 'static,
    {
        self.sinks.insert(name.into(), Box::new(factory));
        self
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
