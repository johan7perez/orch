//! Decide qué pipeline arranca y cuándo.
//!
//! Es deliberadamente puro: se le pregunta «qué toca a esta hora» y responde,
//! sin relojes ni tareas. El demonio es una capa fina encima. Así la parte
//! con reglas —concurrencia, encadenamientos, saltos— se prueba con fechas
//! escritas a mano en vez de con esperas.

use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use orch_core::{Concurrency, OrchError, PipelineSpec, Result, ScheduleSpec};

/// Un pipeline que el demonio vigila.
#[derive(Debug, Clone)]
pub struct Entry {
    pub name: String,
    pub path: std::path::PathBuf,
    pub cron: Option<CronTrigger>,
    /// Pipelines cuyo éxito lo dispara.
    pub after: Vec<String>,
    pub concurrency: Concurrency,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct CronTrigger {
    pub expression: String,
    pub schedule: cron::Schedule,
    pub timezone: Tz,
}

impl Entry {
    /// Construye la entrada a partir del pipeline ya cargado.
    pub fn from_spec(spec: &PipelineSpec, path: impl Into<std::path::PathBuf>) -> Result<Self> {
        let schedule = spec.schedule.clone().unwrap_or_default();
        Ok(Self {
            name: spec.name.clone(),
            path: path.into(),
            cron: cron_trigger(&spec.name, &schedule)?,
            after: schedule.after.clone(),
            concurrency: schedule.concurrency,
            enabled: schedule.enabled,
        })
    }

    /// `true` si el pipeline arranca solo por algún motivo.
    pub fn is_triggered(&self) -> bool {
        self.enabled && (self.cron.is_some() || !self.after.is_empty())
    }

    pub fn describe_trigger(&self) -> String {
        let mut parts = Vec::new();
        if let Some(cron) = &self.cron {
            parts.push(format!("cron `{}` ({})", cron.expression, cron.timezone));
        }
        if !self.after.is_empty() {
            parts.push(format!("tras {}", self.after.join(", ")));
        }
        if parts.is_empty() {
            return "a mano".to_string();
        }
        if !self.enabled {
            parts.push("desactivado".to_string());
        }
        parts.join(" · ")
    }
}

fn cron_trigger(pipeline: &str, schedule: &ScheduleSpec) -> Result<Option<CronTrigger>> {
    let Some(expression) = &schedule.cron else {
        return Ok(None);
    };
    let timezone: Tz = match &schedule.timezone {
        None => chrono_tz::UTC,
        Some(name) => name.parse().map_err(|_| {
            OrchError::Validation(format!(
                "`{pipeline}`: la zona horaria `{name}` no existe; se esperan nombres \
                 de la base IANA como `America/Santo_Domingo`"
            ))
        })?,
    };
    Ok(Some(CronTrigger {
        expression: expression.clone(),
        schedule: crate::cron::parse(pipeline, expression)?,
        timezone,
    }))
}

/// Por qué arrancó una ejecución.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Cron,
    /// Lo disparó el éxito de otro pipeline.
    Upstream,
    /// Estaba encolado porque su turno pilló al anterior corriendo.
    Queued,
}

/// Un arranque decidido por el planificador.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Launch {
    pub name: String,
    pub reason: Reason,
}

struct State {
    entry: Entry,
    /// Próxima vez que le toca por cron.
    next_fire: Option<DateTime<Utc>>,
    queued: bool,
}

pub struct Scheduler {
    states: Vec<State>,
    index: HashMap<String, usize>,
    running: HashSet<String>,
    /// Orden en que se encolaron, para no favorecer a nadie.
    pending: VecDeque<String>,
    skipped: u64,
}

impl Scheduler {
    /// Arranca el planificador con la hora de referencia.
    pub fn new(entries: Vec<Entry>, now: DateTime<Utc>) -> Self {
        let mut index = HashMap::with_capacity(entries.len());
        let states = entries
            .into_iter()
            .enumerate()
            .map(|(position, entry)| {
                index.insert(entry.name.clone(), position);
                let next_fire = next_after(&entry, now);
                State {
                    entry,
                    next_fire,
                    queued: false,
                }
            })
            .collect();

        Self {
            states,
            index,
            running: HashSet::new(),
            pending: VecDeque::new(),
            skipped: 0,
        }
    }

    /// Cambia los pipelines que se vigilan: lo que hay en la carpeta ahora.
    ///
    /// Un pipeline cuyo disparador no cambió conserva su estado —su próximo
    /// disparo, su turno en cola—; uno que cambió se recalcula desde `now`, y
    /// uno que desaparece deja de dispararse. Lo que esté corriendo sigue
    /// hasta terminar: este cambio es sobre lo que arranca a partir de ahora.
    pub fn replace(&mut self, entries: Vec<Entry>, now: DateTime<Utc>) -> Changes {
        let mut previos: HashMap<String, State> = std::mem::take(&mut self.states)
            .into_iter()
            .map(|s| (s.entry.name.clone(), s))
            .collect();
        let mut cambios = Changes::default();

        for entry in entries {
            let state = match previos.remove(&entry.name) {
                Some(previo) if mismo_disparador(&previo.entry, &entry) => {
                    State { entry, ..previo }
                }
                Some(previo) => {
                    cambios.changed.push(entry.name.clone());
                    State {
                        next_fire: next_after(&entry, now),
                        // Un turno en cola se respeta mientras siga activo.
                        queued: previo.queued && entry.enabled,
                        entry,
                    }
                }
                None => {
                    cambios.added.push(entry.name.clone());
                    State {
                        next_fire: next_after(&entry, now),
                        queued: false,
                        entry,
                    }
                }
            };
            self.states.push(state);
        }

        cambios.removed = previos.into_keys().collect();
        cambios.removed.sort();
        self.index = self
            .states
            .iter()
            .enumerate()
            .map(|(i, s)| (s.entry.name.clone(), i))
            .collect();
        // Lo que esperaba turno y ya no está (o ya no puede arrancar), fuera.
        let vivos: HashSet<&str> = self
            .states
            .iter()
            .filter(|s| s.queued)
            .map(|s| s.entry.name.as_str())
            .collect();
        self.pending.retain(|name| vivos.contains(name.as_str()));
        cambios
    }

    pub fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.states.iter().map(|state| &state.entry)
    }

    /// Cuántos disparos se han saltado por concurrencia.
    pub fn skipped(&self) -> u64 {
        self.skipped
    }

    /// Momento del próximo disparo por cron, si hay alguno.
    pub fn next_wakeup(&self) -> Option<DateTime<Utc>> {
        self.states.iter().filter_map(|state| state.next_fire).min()
    }

    /// Qué debe arrancar a esta hora.
    pub fn due(&mut self, now: DateTime<Utc>) -> Vec<Launch> {
        let mut launches = Vec::new();

        // Primero lo que esperaba turno: si se liberó el hueco, va antes que
        // un disparo nuevo.
        let mut still_pending = VecDeque::new();
        while let Some(name) = self.pending.pop_front() {
            if self.try_launch(&name, Reason::Queued, &mut launches) {
                if let Some(state) = self.state_mut(&name) {
                    state.queued = false;
                }
            } else {
                still_pending.push_back(name);
            }
        }
        self.pending = still_pending;

        // Y ahora los que les toca por reloj.
        let fired: Vec<String> = self
            .states
            .iter_mut()
            .filter_map(|state| {
                let next = state.next_fire?;
                if next > now {
                    return None;
                }
                // Se recalcula desde `now` y no desde `next`: si el proceso
                // estuvo parado un rato, no se dispara una vez por cada
                // minuto perdido.
                state.next_fire = next_after(&state.entry, now);
                Some(state.entry.name.clone())
            })
            .collect();

        for name in fired {
            self.try_launch(&name, Reason::Cron, &mut launches);
        }

        launches
    }

    /// Avisa de que un pipeline arrancó por su cuenta (a mano, por ejemplo).
    pub fn started(&mut self, name: &str) {
        self.running.insert(name.to_string());
    }

    /// Avisa de que terminó, y devuelve lo que eso dispara.
    pub fn finished(&mut self, name: &str, succeeded: bool) -> Vec<Launch> {
        self.running.remove(name);
        let mut launches = Vec::new();

        // Los encadenamientos sólo cuentan si terminó bien: encadenar tras
        // un fallo propagaría datos a medias.
        if succeeded {
            let downstream: Vec<String> = self
                .states
                .iter()
                .filter(|state| state.entry.enabled && state.entry.after.iter().any(|a| a == name))
                .map(|state| state.entry.name.clone())
                .collect();
            for target in downstream {
                self.try_launch(&target, Reason::Upstream, &mut launches);
            }
        }

        launches
    }

    fn state_mut(&mut self, name: &str) -> Option<&mut State> {
        let position = *self.index.get(name)?;
        self.states.get_mut(position)
    }

    /// Intenta arrancar, respetando la política de concurrencia.
    fn try_launch(&mut self, name: &str, reason: Reason, out: &mut Vec<Launch>) -> bool {
        let busy = self.running.contains(name);
        let Some(position) = self.index.get(name).copied() else {
            return false;
        };
        let concurrency = self.states[position].entry.concurrency;
        let enabled = self.states[position].entry.enabled;
        if !enabled {
            return false;
        }

        if !busy || concurrency == Concurrency::Allow {
            self.running.insert(name.to_string());
            out.push(Launch {
                name: name.to_string(),
                reason,
            });
            return true;
        }

        match concurrency {
            Concurrency::Skip => {
                self.skipped += 1;
                tracing::warn!(
                    pipeline = %name,
                    "todavía corría la anterior: se salta este disparo"
                );
            }
            Concurrency::Queue => {
                // Uno como mucho: encolar cada disparo perdido convertiría
                // un atasco de una hora en cien ejecuciones seguidas.
                if !self.states[position].queued {
                    self.states[position].queued = true;
                    self.pending.push_back(name.to_string());
                } else {
                    self.skipped += 1;
                }
            }
            Concurrency::Allow => unreachable!("se resolvió arriba"),
        }
        false
    }
}

/// Lo que cambió al reemplazar los pipelines vigilados.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Changes {
    pub added: Vec<String>,
    /// Cambió cuándo arranca: su cron, su zona, de quién depende, si está
    /// activo o qué hace si le toca mientras corre.
    pub changed: Vec<String>,
    pub removed: Vec<String>,
}

impl Changes {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.changed.is_empty() && self.removed.is_empty()
    }
}

/// Si dos versiones de un pipeline arrancan exactamente igual.
fn mismo_disparador(a: &Entry, b: &Entry) -> bool {
    a.cron.as_ref().map(|c| (&c.expression, c.timezone))
        == b.cron.as_ref().map(|c| (&c.expression, c.timezone))
        && a.after == b.after
        && a.concurrency == b.concurrency
        && a.enabled == b.enabled
        && a.path == b.path
}

/// Siguiente disparo por cron después de `now`, en la zona configurada.
fn next_after(entry: &Entry, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    if !entry.enabled {
        return None;
    }
    let cron = entry.cron.as_ref()?;
    cron.schedule
        .after(&now.with_timezone(&cron.timezone))
        .next()
        .map(|moment| moment.with_timezone(&Utc))
}
