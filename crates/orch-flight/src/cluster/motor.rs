//! Lo que ejecuta un pipeline: el motor de siempre o, si reparte nodos entre
//! máquinas, el coordinador.
//!
//! Lo usan igual `orch run`, el demonio y la aplicación. Para quien escucha
//! sus eventos (el visor, el historial, las alertas) son lo mismo: cada
//! ejecución tiene su propio canal, aunque el coordinador sea uno para
//! todas.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use orch_core::event::EVENT_CHANNEL_CAPACITY;
use orch_core::{Dag, Executor, OrchError, PipelineSpec, Registry, Result, RunEvent, RunReport};
use tokio::sync::broadcast;

use super::Coordinador;

pub enum Motor {
    Local {
        ejecutor: Executor,
        dag: Dag,
    },
    Repartido {
        coordinador: Arc<Coordinador>,
        /// Tal y como está escrito: cada máquina resuelve lo suyo.
        spec: PipelineSpec,
        base: PathBuf,
        eventos: broadcast::Sender<RunEvent>,
    },
}

/// Si algún nodo del pipeline va en un worker.
pub fn reparte(spec: &PipelineSpec) -> bool {
    spec.nodes.iter().any(|n| n.worker.is_some())
}

impl Motor {
    /// Prepara la ejecución de `texto`, el contenido del pipeline en
    /// `ruta`, leído una sola vez.
    ///
    /// Si algún nodo lleva `worker`, va repartido y hace falta un
    /// coordinador; si no, el motor de siempre, con `registry`.
    pub fn para(
        ruta: &Path,
        texto: &str,
        registry: Arc<Registry>,
        coordinador: Option<Arc<Coordinador>>,
    ) -> Result<Motor> {
        let escrito = PipelineSpec::from_yaml_str_as_written(&ruta.display().to_string(), texto)?;
        if !reparte(&escrito) {
            let mut spec = PipelineSpec::from_text_at(ruta, texto)?;
            orch_core::pushdown::apply(&mut spec, &registry);
            return Ok(Motor::Local {
                ejecutor: Executor::new(registry),
                dag: Dag::build(spec)?,
            });
        }
        let coordinador = coordinador.ok_or_else(|| sin_cluster(&escrito))?;
        let (eventos, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Ok(Motor::Repartido {
            coordinador,
            spec: escrito,
            base: ruta
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(".")),
            eventos,
        })
    }

    /// Los eventos de esta ejecución. Suscribirse antes de [`run`](Self::run).
    pub fn subscribe(&self) -> broadcast::Receiver<RunEvent> {
        match self {
            Motor::Local { ejecutor, .. } => ejecutor.subscribe(),
            Motor::Repartido { eventos, .. } => eventos.subscribe(),
        }
    }

    pub fn spec(&self) -> &PipelineSpec {
        match self {
            Motor::Local { dag, .. } => dag.spec(),
            Motor::Repartido { spec, .. } => spec,
        }
    }

    pub async fn run(&self) -> Result<RunReport> {
        match self {
            Motor::Local { ejecutor, dag } => ejecutor.run(dag).await,
            Motor::Repartido {
                coordinador,
                spec,
                base,
                eventos,
            } => coordinador.run_con(spec, base, eventos).await,
        }
    }
}

/// Lo que se dice cuando un pipeline repartido no tiene clúster con el que
/// correr.
pub fn sin_cluster(spec: &PipelineSpec) -> OrchError {
    let nodo = spec
        .nodes
        .iter()
        .find(|n| n.worker.is_some())
        .map(|n| {
            format!(
                "el nodo `{}` va en el worker `{}`",
                n.id,
                n.worker.as_deref().unwrap_or_default()
            )
        })
        .unwrap_or_default();
    OrchError::Validation(format!(
        "{nodo}: este pipeline reparte nodos entre máquinas y hace falta el fichero del \
         clúster (`--cluster` o la variable `ORCH_CLUSTER`)"
    ))
}
