//! # orch-core
//!
//! Motor de orquestación de Orch. Contiene el modelo declarativo de pipeline,
//! su validación como DAG, los contratos que implementan los conectores y el
//! ejecutor asíncrono.
//!
//! Deliberadamente **no** conoce ninguna implementación de conector: se le
//! entrega un [`Registry`] con lo que haya disponible. Así el mismo motor
//! sirve a la CLI de la Fase 0, al backend de Tauri de la Fase 1 y a los
//! plugins WASM de la Fase 2.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use orch_core::{Dag, Executor, PipelineSpec, Registry};
//! # async fn ejemplo() -> orch_core::Result<()> {
//! let spec = PipelineSpec::from_path("pipeline.yaml")?;
//! let dag = Dag::build(spec)?;
//! let executor = Executor::new(Arc::new(Registry::new()));
//! let report = executor.run(&dag).await?;
//! assert!(report.succeeded);
//! # Ok(())
//! # }
//! ```

pub mod connector;
pub mod dag;
pub mod error;
pub mod event;
pub mod executor;
pub mod io;
pub mod pushdown;
pub mod registry;
pub mod schema;
pub mod secrets;
pub mod spec;

pub use connector::{NodeContext, Sink, Source, Transform};
pub use dag::Dag;
pub use error::{parse_config, OrchError, Result};
pub use event::RunEvent;
pub use executor::{Executor, NodeReport, NodeStatus, RunReport};
pub use io::{Input, InputPort, IoStats, NodeSignal, Output};
pub use pushdown::{PushdownOp, Pushed};
pub use registry::Registry;
pub use schema::{InputSchemas, PortSchema};
pub use spec::{
    Concurrency, EdgeSpec, NodeId, NodeKind, NodeSpec, PipelineSpec, RetryPolicy, RunSettings,
    ScheduleSpec, SPEC_VERSION,
};

/// Re-exportado para que los conectores no tengan que fijar su propia versión
/// de `arrow` y arriesgarse a un choque de tipos entre crates.
pub use arrow;
