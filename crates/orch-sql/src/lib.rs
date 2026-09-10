//! # orch-sql
//!
//! Transformaciones con expresiones, sobre Apache DataFusion.
//!
//! La integración es como **transformación aislada**: cada nodo SQL levanta su
//! propio `SessionContext` y consume el flujo del nodo anterior a través de un
//! `StreamingTable`. DataFusion tira de nuestros batches en vez de exigir el
//! dataset materializado, así que un `WHERE` o un `SELECT` mantienen el uso de
//! memoria acotado por los lotes en vuelo, igual que el resto del motor.
//!
//! La alternativa —dejar que DataFusion planifique sub-grafos enteros del
//! pipeline— daría un plan global optimizable (empujar filtros hasta el
//! origen, fundir proyecciones), pero obligaría a que los conectores fueran
//! `TableProvider` de DataFusion y ataría el motor a su modelo de ejecución.
//! Queda pendiente de medir antes de decidir; ver `docs/PLAN.md`.

mod engine;
mod table;
mod transforms;

use std::sync::Arc;

use orch_core::{Registry, Transform};

pub use transforms::{
    build_aggregate, build_derive, build_filter, build_sql, AggregateConfig, DeriveConfig,
    FilterConfig, SqlConfig, SqlTransform,
};

/// Añade las transformaciones SQL a un registro existente.
pub fn register(registry: &mut Registry) {
    registry.register_transform("sql", |node, config| {
        let t: Arc<dyn Transform> = Arc::new(build_sql(node, config)?);
        Ok(t)
    });
    registry.register_transform("filter", |node, config| {
        let t: Arc<dyn Transform> = Arc::new(build_filter(node, config)?);
        Ok(t)
    });
    registry.register_transform("derive", |node, config| {
        let t: Arc<dyn Transform> = Arc::new(build_derive(node, config)?);
        Ok(t)
    });
    registry.register_transform("aggregate", |node, config| {
        let t: Arc<dyn Transform> = Arc::new(build_aggregate(node, config)?);
        Ok(t)
    });
}
