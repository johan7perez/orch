//! # orch-schedule
//!
//! Disparadores: cron y encadenamiento entre pipelines.
//!
//! El planificador es puro —se le pregunta qué toca a una hora dada y
//! responde— para que las reglas que de verdad importan (concurrencia,
//! encadenamientos, disparos saltados) se prueben con fechas escritas a mano
//! en vez de con esperas reales. El demonio de la CLI es una capa fina que
//! le pasa el reloj.

mod cron;
mod discover;
mod scheduler;

pub use discover::discover;
pub use scheduler::{CronTrigger, Entry, Launch, Reason, Scheduler};
