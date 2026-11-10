//! La conexión al fichero del historial, compartido entre procesos.
//!
//! DuckDB sólo deja que **un** proceso tenga el fichero abierto a la vez.
//! Con la conexión abierta toda la vida del proceso, la aplicación abierta
//! impedía un `orch run` con el mismo historial (y al revés).
//!
//! Así que cada proceso abre el fichero cuando lo necesita y lo suelta en
//! cuanto lleva [`REPOSO`] sin usarlo. Abrir cuesta (unos 90 ms en debug, por
//! el checkpoint al cerrar), así que no se abre y cierra en cada operación:
//! una ráfaga de operaciones reutiliza la misma conexión. Si al abrir lo
//! tiene otro, se reintenta durante [`ESPERA`]: el otro lo soltará en cuanto
//! deje de usarlo.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use duckdb::Connection;
use orch_core::{OrchError, Result};

/// Sin usarlo este tiempo, el fichero se suelta para que entre otro proceso.
pub const REPOSO: Duration = Duration::from_millis(300);
/// Cuánto se espera a que otro proceso suelte el fichero.
pub const ESPERA: Duration = Duration::from_secs(15);

pub(crate) enum Fuente {
    /// Para tests: una base en memoria desaparece al cerrarla, así que no se
    /// suelta nunca.
    Memoria(Mutex<Connection>),
    Fichero(Arc<Compartido>),
}

pub(crate) struct Compartido {
    path: PathBuf,
    conn: Mutex<Option<Connection>>,
    ultimo_uso: Mutex<Instant>,
}

/// Una conexión prestada mientras dura la operación.
pub(crate) enum Prestada<'a> {
    Memoria(MutexGuard<'a, Connection>),
    Fichero(MutexGuard<'a, Option<Connection>>, &'a Compartido),
}

impl std::ops::Deref for Prestada<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        match self {
            Prestada::Memoria(c) => c,
            Prestada::Fichero(c, _) => c.as_ref().expect("abierta al prestarla"),
        }
    }
}

impl Drop for Prestada<'_> {
    fn drop(&mut self) {
        if let Prestada::Fichero(_, compartido) = self {
            *bloquear(&compartido.ultimo_uso) = Instant::now();
        }
    }
}

fn bloquear<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock()
        .unwrap_or_else(|envenenado| envenenado.into_inner())
}

fn fallo(contexto: &str, err: impl std::fmt::Display) -> OrchError {
    OrchError::Other(format!("almacén: {contexto}: {err}"))
}

impl Fuente {
    pub(crate) fn memoria() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(|e| fallo("no se pudo abrir", e))?;
        Ok(Fuente::Memoria(Mutex::new(conn)))
    }

    pub(crate) fn fichero(path: &Path) -> Self {
        let compartido = Arc::new(Compartido {
            path: path.to_path_buf(),
            conn: Mutex::new(None),
            ultimo_uso: Mutex::new(Instant::now()),
        });
        soltar_en_reposo(Arc::downgrade(&compartido));
        Fuente::Fichero(compartido)
    }

    pub(crate) fn prestar(&self) -> Result<Prestada<'_>> {
        match self {
            Fuente::Memoria(c) => Ok(Prestada::Memoria(bloquear(c))),
            Fuente::Fichero(compartido) => {
                let mut conn = bloquear(&compartido.conn);
                if conn.is_none() {
                    *conn = Some(abrir(&compartido.path)?);
                }
                Ok(Prestada::Fichero(conn, compartido))
            }
        }
    }
}

/// Abre el fichero, esperando a que otro proceso lo suelte si hace falta.
fn abrir(path: &Path) -> Result<Connection> {
    let inicio = Instant::now();
    let mut pausa = Duration::from_millis(10);
    loop {
        match Connection::open(path) {
            Ok(conn) => return Ok(conn),
            Err(err) => {
                let mensaje = err.to_string();
                let ocupado =
                    mensaje.contains("Could not set lock") || mensaje.contains("Conflicting lock");
                if !ocupado {
                    return Err(fallo(
                        &format!("no se pudo abrir `{}`", path.display()),
                        err,
                    ));
                }
                if inicio.elapsed() >= ESPERA {
                    return Err(fallo(
                        &format!(
                            "`{}` lo tiene abierto otro proceso y no lo soltó en {} s",
                            path.display(),
                            ESPERA.as_secs()
                        ),
                        err,
                    ));
                }
                std::thread::sleep(pausa);
                pausa = (pausa * 2).min(Duration::from_millis(200));
            }
        }
    }
}

/// Un hilo que suelta el fichero cuando lleva [`REPOSO`] sin usarse. Muere
/// con el almacén.
fn soltar_en_reposo(compartido: Weak<Compartido>) {
    let _ = std::thread::Builder::new()
        .name("orch-store-reposo".into())
        .spawn(move || loop {
            std::thread::sleep(REPOSO / 3);
            let Some(compartido) = compartido.upgrade() else {
                return;
            };
            if bloquear(&compartido.ultimo_uso).elapsed() < REPOSO {
                continue;
            }
            // Si alguien lo está usando, ya se soltará en la siguiente vuelta.
            let intento = compartido.conn.try_lock();
            if let Ok(mut conn) = intento {
                if conn.is_some() && bloquear(&compartido.ultimo_uso).elapsed() >= REPOSO {
                    *conn = None;
                }
            }
        });
}
