//! Alertas: avisar cuando una ejecución va mal.
//!
//! Un vigilante escucha el mismo canal de eventos que la terminal, la ventana
//! y el historial —es un oyente más— y avisa de tres cosas:
//!
//! - **Que falló**, si `on_failure`. Es lo único que se vigila sin bloque
//!   `alerts`: el caso que nadie quiere perderse.
//! - **Que está colgada**: pasa `stalled_after` sin que se mueva una fila en
//!   todo el pipeline. El motor sólo publica progreso cuando algo se mueve,
//!   así que «no llega nada» es exactamente lo que hay que vigilar.
//! - **Que va más lenta de lo normal**: lleva más de `slower_than` veces la
//!   mediana de sus últimas ejecuciones correctas. Se avisa en cuanto pasa,
//!   sin esperar a que termine: una ejecución que suele tardar diez minutos y
//!   lleva media hora es justo la que interesa saber ahora.
//!
//! Cada alerta va a quien vigila (la terminal, la ventana), al historial y, si
//! el pipeline lo pide, a un webhook. Que una entrega falle no impide las
//! demás, y nunca frena la ejecución: la vigilancia es otro oyente del canal.

use std::sync::Arc;
use std::time::Duration;

use orch_core::{AlertsSpec, PipelineSpec, RunEvent};
use orch_store::{AlertRow, Store};
use serde::Serialize;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// Cuántas ejecuciones correctas cuentan para saber «lo que suele tardar».
const HISTORIA: usize = 10;

/// Cada cuánto se mira si está colgada o va lenta.
const REVISION: Duration = Duration::from_secs(1);

/// Lo que puede tardar el webhook en contestar antes de darlo por perdido.
const PLAZO_WEBHOOK: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Tipo {
    Fallo,
    Colgada,
    Lenta,
}

impl Tipo {
    pub fn as_str(self) -> &'static str {
        match self {
            Tipo::Fallo => "fallo",
            Tipo::Colgada => "colgada",
            Tipo::Lenta => "lenta",
        }
    }
}

/// Algo de lo que avisar.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Alerta {
    pub tipo: Tipo,
    pub pipeline: String,
    pub run_id: String,
    /// El nodo que lo causó, si se sabe.
    pub nodo: Option<String>,
    /// Lo que se le dice a una persona.
    pub mensaje: String,
}

/// Lo que hace falta para vigilar una ejecución de un pipeline.
#[derive(Debug, Clone)]
pub struct Vigilancia {
    pub pipeline: String,
    pub config: AlertsSpec,
    /// Lo que suele tardar, si el historial tiene con qué saberlo.
    pub tipica: Option<Duration>,
}

impl Vigilancia {
    /// La vigilancia de un pipeline: su bloque `alerts` (o lo de por defecto:
    /// avisar de los fallos) y, si va a hacer falta, lo que suele tardar.
    pub fn para(spec: &PipelineSpec, store: Option<&Store>) -> Self {
        let config = spec.alerts.clone().unwrap_or_default();
        let tipica = config
            .slower_than
            .and(store)
            .and_then(|store| {
                store
                    .typical_elapsed_ms(&spec.name, HISTORIA)
                    .ok()
                    .flatten()
            })
            .map(Duration::from_millis);
        Self {
            pipeline: spec.name.clone(),
            config,
            tipica,
        }
    }

    /// Escucha una ejecución y devuelve sus alertas según se detectan.
    ///
    /// La tarea termina con el `RunFinished` o cuando se cierra el canal.
    pub fn detectar(
        self,
        eventos: broadcast::Receiver<RunEvent>,
    ) -> (JoinHandle<()>, mpsc::UnboundedReceiver<Alerta>) {
        let (avisos, alertas) = mpsc::unbounded_channel();
        (tokio::spawn(detectar(self, eventos, avisos)), alertas)
    }
}

async fn detectar(
    v: Vigilancia,
    mut eventos: broadcast::Receiver<RunEvent>,
    avisos: mpsc::UnboundedSender<Alerta>,
) {
    let mut run_id = String::new();
    let mut inicio = Instant::now();
    let mut ultimo_movimiento = Instant::now();
    let mut avisada_colgada = false;
    let mut avisada_lenta = false;
    let mut primer_fallo: Option<(String, String)> = None;
    let mut revision = tokio::time::interval(REVISION);
    revision.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let alerta = |tipo, run_id: &str, nodo: Option<String>, mensaje: String| Alerta {
        tipo,
        pipeline: v.pipeline.clone(),
        run_id: run_id.to_string(),
        nodo,
        mensaje,
    };

    loop {
        tokio::select! {
            recibido = eventos.recv() => match recibido {
                Ok(evento) => {
                    let ahora = Instant::now();
                    if run_id.is_empty() {
                        run_id = evento.run_id().to_string();
                        inicio = ahora;
                    }
                    match &evento {
                        RunEvent::RunStarted { .. } => {
                            inicio = ahora;
                            ultimo_movimiento = ahora;
                        }
                        RunEvent::NodeFailed { node, error, will_retry: false, .. } => {
                            primer_fallo.get_or_insert((node.clone(), error.clone()));
                            ultimo_movimiento = ahora;
                        }
                        RunEvent::RunFinished { succeeded, .. } => {
                            if !succeeded && v.config.on_failure {
                                let (nodo, mensaje) = match primer_fallo.take() {
                                    Some((nodo, error)) => {
                                        // El error de un nodo ya empieza diciendo
                                        // qué nodo falló; repetirlo sobra.
                                        let prefijo = format!("el nodo `{nodo}` falló: ");
                                        let error = error.strip_prefix(&prefijo).unwrap_or(&error);
                                        let mensaje = format!(
                                            "`{}` falló en el nodo `{nodo}`: {error}",
                                            v.pipeline
                                        );
                                        (Some(nodo.clone()), mensaje)
                                    }
                                    None => (None, format!("`{}` falló", v.pipeline)),
                                };
                                let _ = avisos.send(alerta(Tipo::Fallo, &run_id, nodo, mensaje));
                            }
                            return;
                        }
                        // Cualquier otro evento es que algo se movió.
                        _ => ultimo_movimiento = ahora,
                    }
                }
                // Si se perdieron eventos es que llegaban muchos: se movía.
                Err(RecvError::Lagged(_)) => ultimo_movimiento = Instant::now(),
                Err(RecvError::Closed) => return,
            },
            _ = revision.tick() => {
                if run_id.is_empty() {
                    continue;
                }
                let ahora = Instant::now();
                if let Some(plazo) = v.config.stalled_after {
                    if !avisada_colgada && ahora - ultimo_movimiento >= plazo.0 {
                        avisada_colgada = true;
                        let mensaje = format!(
                            "`{}` lleva {} sin mover una fila",
                            v.pipeline,
                            duracion(ahora - ultimo_movimiento)
                        );
                        let _ = avisos.send(alerta(Tipo::Colgada, &run_id, None, mensaje));
                    }
                }
                if let (Some(veces), Some(tipica)) = (v.config.slower_than, v.tipica) {
                    let lleva = ahora - inicio;
                    if !avisada_lenta && lleva > tipica.mul_f64(veces) {
                        avisada_lenta = true;
                        let mensaje = format!(
                            "`{}` lleva {}, más de {veces}× lo que suele tardar ({})",
                            v.pipeline,
                            duracion(lleva),
                            duracion(tipica)
                        );
                        let _ = avisos.send(alerta(Tipo::Lenta, &run_id, None, mensaje));
                    }
                }
            }
        }
    }
}

/// A dónde va cada alerta, además de a quien vigila.
#[derive(Clone)]
pub struct Entrega {
    store: Option<Arc<Store>>,
    webhook: Option<String>,
    http: reqwest::Client,
}

impl Entrega {
    pub fn nueva(store: Option<Arc<Store>>, webhook: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(PLAZO_WEBHOOK)
            .build()
            .unwrap_or_default();
        Self {
            store,
            webhook,
            http,
        }
    }

    /// Entrega una alerta. Devuelve lo que no se pudo entregar, y por qué.
    pub async fn entregar(&self, alerta: &Alerta) -> Vec<String> {
        let mut problemas = Vec::new();

        if let Some(store) = &self.store {
            let store = Arc::clone(store);
            let fila = AlertRow {
                run_id: alerta.run_id.clone(),
                pipeline: alerta.pipeline.clone(),
                at: chrono::Utc::now(),
                kind: alerta.tipo.as_str().to_string(),
                node: alerta.nodo.clone(),
                message: alerta.mensaje.clone(),
            };
            match tokio::task::spawn_blocking(move || store.record_alert(&fila)).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => problemas.push(format!("no se pudo guardar la alerta: {err}")),
                Err(err) => problemas.push(format!("no se pudo guardar la alerta: {err}")),
            }
        }

        if let Some(url) = &self.webhook {
            if let Err(err) = self.mandar(url, alerta).await {
                problemas.push(format!("no se pudo mandar la alerta al webhook: {err}"));
            }
        }
        problemas
    }

    async fn mandar(&self, url: &str, alerta: &Alerta) -> Result<(), String> {
        // `text` es lo que leen los webhooks entrantes de Slack y de Teams; el
        // resto es para quien quiera tratar la alerta en un programa.
        let cuerpo = serde_json::json!({
            "text": format!("⚠ {}", alerta.mensaje),
            "tipo": alerta.tipo,
            "pipeline": alerta.pipeline,
            "run_id": alerta.run_id,
            "nodo": alerta.nodo,
        });
        let respuesta = self
            .http
            .post(url)
            .json(&cuerpo)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if respuesta.status().is_success() {
            Ok(())
        } else {
            Err(format!("respondió {}", respuesta.status()))
        }
    }
}

/// Vigila una ejecución de principio a fin y entrega cada alerta.
///
/// `local` recibe cada alerta, con lo que no se pudo entregar de ella: es
/// quien se la enseña a una persona (la terminal, la ventana). La tarea
/// termina cuando termina la ejecución y se han entregado todas.
pub fn vigilar(
    vigilancia: Vigilancia,
    eventos: broadcast::Receiver<RunEvent>,
    entrega: Entrega,
    mut local: impl FnMut(&Alerta, &[String]) + Send + 'static,
) -> JoinHandle<()> {
    let (detector, mut alertas) = vigilancia.detectar(eventos);
    tokio::spawn(async move {
        while let Some(alerta) = alertas.recv().await {
            tracing::warn!(tipo = alerta.tipo.as_str(), "{}", alerta.mensaje);
            let problemas = entrega.entregar(&alerta).await;
            for problema in &problemas {
                tracing::warn!("{problema}");
            }
            local(&alerta, &problemas);
        }
        let _ = detector.await;
    })
}

/// Avisa de que un pipeline ni siquiera pudo arrancar.
///
/// Es el caso del demonio: a las tres de la mañana falta un secreto o alguien
/// dejó el YAML a medias, y no hay ejecución que vigilar. Es justo el fallo
/// que más interesa saber, así que se avisa igual.
///
/// El bloque `alerts` se lee por su cuenta, del YAML en bruto: si un nodo está
/// mal escrito, el pipeline entero no carga, pero su bloque de alertas sigue
/// valiendo, y es precisamente el que dice a quién avisar. El webhook se
/// expande si se puede; si su secreto es lo que falta, la alerta llega al
/// menos a quien vigila y al historial.
pub async fn avisar_arranque_fallido(
    ruta: &std::path::Path,
    pipeline: &str,
    error: &str,
    store: Option<Arc<Store>>,
    mut local: impl FnMut(&Alerta, &[String]),
) {
    let config = alertas_escritas(ruta).unwrap_or_default();
    if !config.on_failure {
        return;
    }
    let webhook = config
        .webhook
        .and_then(|url| match orch_core::secrets::expand("alerts", &url) {
            Ok(expandido) => Some(expandido.unwrap_or(url)),
            Err(_) => None,
        });
    let alerta = Alerta {
        tipo: Tipo::Fallo,
        pipeline: pipeline.to_string(),
        run_id: String::new(),
        nodo: None,
        mensaje: format!("`{pipeline}` no pudo arrancar: {error}"),
    };
    tracing::warn!(tipo = alerta.tipo.as_str(), "{}", alerta.mensaje);
    let problemas = Entrega::nueva(store, webhook).entregar(&alerta).await;
    local(&alerta, &problemas);
}

/// El bloque `alerts` de un fichero, aunque el resto no cargue.
fn alertas_escritas(ruta: &std::path::Path) -> Option<AlertsSpec> {
    let texto = std::fs::read_to_string(ruta).ok()?;
    let documento: serde_yaml::Value = serde_yaml::from_str(&texto).ok()?;
    let bloque = documento.get("alerts")?.clone();
    serde_yaml::from_value(bloque).ok()
}

/// Una duración como se dice: `45s`, `12m 30s`, `1h 05m`.
pub fn duracion(d: Duration) -> String {
    let s = d.as_secs();
    if s < 60 {
        format!("{s}s")
    } else if s < 3_600 {
        format!("{}m {:02}s", s / 60, s % 60)
    } else {
        format!("{}h {:02}m", s / 3_600, (s % 3_600) / 60)
    }
}
