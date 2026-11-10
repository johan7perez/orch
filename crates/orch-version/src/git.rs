//! Lo poco de git que necesita Orch, a través del binario `git`.
//!
//! Se llama al `git` instalado y no a una librería. Así se respeta la
//! configuración del usuario —autor, firma de commits, hooks— y no hay otra
//! dependencia en C que compilar. Sin git instalado, o con el pipeline fuera
//! de un repositorio, [`Repo::of`] devuelve `None` y lo demás sigue
//! funcionando: simplemente no hay commits que enseñar.
//!
//! Tres precauciones en cada llamada:
//!
//! - `--literal-pathspecs`: una ruta es una ruta, aunque lleve `*`.
//! - `core.quotepath=false`: las rutas con tildes salen tal cual, no como
//!   secuencias octales.
//! - `core.fsmonitor=false`: un repositorio puede declarar un programa que git
//!   ejecuta al mirar el estado. Orch mira el estado sin que nadie lo pida, al
//!   arrancar cada ejecución, así que no lo ejecuta.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use chrono::{DateTime, Utc};
use orch_core::{OrchError, Result};
use serde::Serialize;

/// Hasta dónde se mira atrás al buscar una versión por su hash.
const HISTORIA_MAXIMA: usize = 10_000;

/// Un commit, tal y como se enseña.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Commit {
    pub hash: String,
    pub author: String,
    pub date: DateTime<Utc>,
    /// La primera línea del mensaje.
    pub subject: String,
    /// Ruta del fichero en ese commit, relativa a la raíz del repositorio.
    /// Cambia si el fichero se renombró después.
    #[serde(skip)]
    pub path: String,
}

impl Commit {
    /// Los siete primeros caracteres, como los enseña git.
    pub fn short(&self) -> &str {
        short(&self.hash)
    }
}

/// Los siete primeros caracteres de un hash.
pub fn short(hash: &str) -> &str {
    hash.get(..7).unwrap_or(hash)
}

/// Cómo está un fichero respecto a git.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FileState {
    /// El último commit que lo tocó. `None` si nunca se ha guardado en git.
    pub commit: Option<Commit>,
    /// Su contenido no es el de ese commit: tiene cambios sin guardar (o no
    /// está en git).
    pub modified: bool,
}

/// Un repositorio de git que contiene pipelines.
#[derive(Debug, Clone)]
pub struct Repo {
    root: PathBuf,
}

impl Repo {
    /// El repositorio que contiene `path`, si lo hay y git está instalado.
    pub fn of(path: &Path) -> Option<Repo> {
        let dir = carpeta(path);
        match git(&dir, &["rev-parse", "--show-toplevel"]) {
            Ok(salida) => {
                let root = PathBuf::from(salida.trim_end_matches(['\n', '\r']));
                // La raíz se canonicaliza igual que las rutas que se le pidan,
                // o `strip_prefix` fallaría por un enlace simbólico.
                Some(Repo {
                    root: root.canonicalize().unwrap_or(root),
                })
            }
            Err(err) => {
                tracing::debug!(path = %path.display(), error = %err, "sin repositorio de git");
                None
            }
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// La ruta de `path` dentro del repositorio, con `/` como separador.
    pub fn relative(&self, path: &Path) -> Result<String> {
        let absoluta = path.canonicalize().map_err(|err| {
            OrchError::Other(format!("no se pudo resolver `{}`: {err}", path.display()))
        })?;
        let relativa = absoluta.strip_prefix(&self.root).map_err(|_| {
            OrchError::Other(format!(
                "`{}` no está dentro del repositorio `{}`",
                path.display(),
                self.root.display()
            ))
        })?;
        Ok(relativa
            .components()
            .map(|c| c.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/"))
    }

    /// El último commit que tocó el fichero y si tiene cambios sin guardar.
    pub fn state(&self, path: &Path) -> Result<FileState> {
        let rel = self.relative(path)?;
        let estado = self.run(&["status", "--porcelain=v1", "-z", "--", &rel])?;
        let commit = self.log(path, 1)?.into_iter().next();
        Ok(FileState {
            modified: !estado.is_empty() || commit.is_none(),
            commit,
        })
    }

    /// Los commits que tocaron el fichero, del más nuevo al más viejo,
    /// siguiéndolo a través de los renombrados.
    pub fn log(&self, path: &Path, limit: usize) -> Result<Vec<Commit>> {
        let rel = self.relative(path)?;
        // En un repositorio recién creado `git log` falla en vez de devolver
        // nada: no hay historial que enseñar.
        if self
            .run(&["rev-parse", "--verify", "--quiet", "HEAD"])
            .is_err()
        {
            return Ok(Vec::new());
        }
        let limite = limit.max(1).to_string();
        // Cada commit empieza por un separador de registro; los campos van
        // separados por el de unidad. Ninguno de los dos aparece en un
        // nombre ni en un asunto.
        let salida = self.run(&[
            "log",
            "--follow",
            "--name-only",
            "--format=%x1e%H%x1f%an%x1f%aI%x1f%s",
            "-n",
            &limite,
            "--",
            &rel,
        ])?;
        Ok(salida
            .split('\u{1e}')
            .filter_map(|registro| parse_commit(registro, &rel))
            .collect())
    }

    /// El hash completo de una referencia (`HEAD`, `a1b2c3`, `main~2`…).
    pub fn resolve(&self, rev: &str) -> Result<String> {
        if rev.starts_with('-') {
            return Err(OrchError::Other(format!("`{rev}` no es una versión")));
        }
        self.run(&[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{rev}^{{commit}}"),
        ])
        .map(|s| s.trim().to_string())
        .map_err(|_| OrchError::Other(format!("git no conoce la versión `{rev}`")))
    }

    /// El texto del fichero en una versión.
    ///
    /// Si el fichero se llamaba de otra forma en esa versión, se busca con
    /// el nombre de entonces.
    pub fn show(&self, rev: &str, path: &Path) -> Result<(Commit, String)> {
        let hash = self.resolve(rev)?;
        let historia = self.log(path, HISTORIA_MAXIMA)?;
        let commit = match historia.iter().find(|c| c.hash == hash) {
            Some(c) => c.clone(),
            // La versión existe pero no tocó el fichero: su contenido es el
            // del último commit anterior que sí lo hizo. Se busca con cada
            // nombre que tuvo, del actual hacia atrás, por si se renombró
            // después de esa versión.
            None => {
                let mut nombres: Vec<String> = vec![self.relative(path)?];
                for c in &historia {
                    if !nombres.contains(&c.path) {
                        nombres.push(c.path.clone());
                    }
                }
                nombres
                    .iter()
                    .find_map(|nombre| self.commit_at(&hash, nombre).ok())
                    .ok_or_else(|| {
                        OrchError::Other(format!(
                            "`{}` no existía en la versión {}",
                            path.display(),
                            short(&hash)
                        ))
                    })?
            }
        };
        let texto = self.run(&["show", &format!("{hash}:{}", commit.path)])?;
        Ok((commit, texto))
    }

    /// El texto del fichero en un commit de su historial ([`Repo::log`]).
    pub fn text_of(&self, commit: &Commit) -> Result<String> {
        self.run(&["show", &format!("{}:{}", commit.hash, commit.path)])
    }

    /// El último commit que tocó `rel` desde `hash`, con el nombre de entonces.
    fn commit_at(&self, hash: &str, rel: &str) -> Result<Commit> {
        let salida = self.run(&[
            "log",
            "--name-only",
            "--format=%x1e%H%x1f%an%x1f%aI%x1f%s",
            "-n",
            "1",
            hash,
            "--",
            rel,
        ])?;
        salida
            .split('\u{1e}')
            .find_map(|registro| parse_commit(registro, rel))
            .ok_or_else(|| {
                OrchError::Other(format!("`{rel}` no existía en la versión {}", short(hash)))
            })
    }

    /// Guarda el fichero como una versión nueva: `git add` y `git commit`
    /// sólo de ese fichero, aunque haya otros cambios preparados.
    pub fn commit(&self, path: &Path, message: &str) -> Result<Commit> {
        let mensaje = message.trim();
        if mensaje.is_empty() {
            return Err(OrchError::Other(
                "una versión necesita un mensaje que diga qué cambia".into(),
            ));
        }
        if !self.state(path)?.modified {
            return Err(OrchError::Other(
                "no hay cambios que guardar: el fichero es igual que su última versión".into(),
            ));
        }
        let rel = self.relative(path)?;
        self.run(&["add", "--", &rel])?;
        // Con rutas, `git commit` guarda sólo esas: lo que el usuario tuviera
        // preparado de otros ficheros se queda donde estaba.
        self.run(&["commit", "--quiet", "-m", mensaje, "--", &rel])?;
        self.log(path, 1)?
            .into_iter()
            .next()
            .ok_or_else(|| OrchError::Other("el commit no aparece en el historial".into()))
    }

    /// Deja el fichero como en su última versión, descartando los cambios.
    pub fn discard(&self, path: &Path) -> Result<()> {
        let estado = self.state(path)?;
        if estado.commit.is_none() {
            return Err(OrchError::Other(
                "el fichero nunca se guardó en git: no hay versión a la que volver".into(),
            ));
        }
        let rel = self.relative(path)?;
        self.run(&["checkout", "HEAD", "--", &rel]).map(|_| ())
    }

    fn run(&self, args: &[&str]) -> Result<String> {
        git(&self.root, args)
    }
}

fn parse_commit(registro: &str, por_defecto: &str) -> Option<Commit> {
    let mut lineas = registro.lines();
    let cabecera = lineas.next()?;
    let mut campos = cabecera.split('\u{1f}');
    let (hash, author, fecha, subject) = (
        campos.next()?,
        campos.next()?,
        campos.next()?,
        campos.next().unwrap_or(""),
    );
    if hash.is_empty() {
        return None;
    }
    let path = lineas
        .map(str::trim)
        .rfind(|l| !l.is_empty())
        .unwrap_or(por_defecto)
        .to_string();
    Some(Commit {
        hash: hash.to_string(),
        author: author.to_string(),
        date: DateTime::parse_from_rfc3339(fecha)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_default(),
        subject: subject.to_string(),
        path,
    })
}

fn carpeta(path: &Path) -> PathBuf {
    let dir = if path.is_dir() {
        path
    } else {
        path.parent().unwrap_or(Path::new("."))
    };
    if dir.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        dir.to_path_buf()
    }
}

fn git(dir: &Path, args: &[&str]) -> Result<String> {
    let salida = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args([
            "--literal-pathspecs",
            "-c",
            "core.quotepath=false",
            "-c",
            "core.fsmonitor=false",
        ])
        .args(args)
        // Nunca preguntar nada: no hay terminal a la que preguntar.
        .env("GIT_TERMINAL_PROMPT", "0")
        // Mirar el estado no debe pelear por el índice con el git del usuario.
        .env("GIT_OPTIONAL_LOCKS", "0")
        .stdin(Stdio::null())
        .output()
        .map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                OrchError::Other("git no está instalado".into())
            } else {
                OrchError::Other(format!("no se pudo lanzar git: {err}"))
            }
        })?;
    if salida.status.success() {
        Ok(String::from_utf8_lossy(&salida.stdout).into_owned())
    } else {
        let error = String::from_utf8_lossy(&salida.stderr);
        Err(OrchError::Other(format!(
            "git {}: {}",
            args.first().copied().unwrap_or_default(),
            error.trim()
        )))
    }
}
