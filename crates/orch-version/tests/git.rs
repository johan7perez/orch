//! Versiones en git, contra repositorios temporales de verdad.
//!
//! Cada test crea su propio repositorio y le pone identidad, sin firma y sin
//! hooks: no depende de la configuración global de quien los ejecute ni toca
//! ningún otro repositorio.

use std::path::{Path, PathBuf};
use std::process::Command;

use orch_version::{Repo, Snapshot};
use tempfile::TempDir;

const V1: &str = "name: demo\nnodes:\n  - { id: a, type: source, connector: generator, config: { rows: 1 } }\n  - { id: b, type: sink, connector: \"null\" }\nedges:\n  - { from: a, to: b }\n";
const V2: &str = "name: demo\nnodes:\n  - { id: a, type: source, connector: generator, config: { rows: 2 } }\n  - { id: b, type: sink, connector: \"null\" }\nedges:\n  - { from: a, to: b }\n";

fn git(dir: &Path, args: &[&str]) -> String {
    let salida = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("git instalado");
    assert!(
        salida.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&salida.stderr)
    );
    String::from_utf8_lossy(&salida.stdout).into_owned()
}

/// Un repositorio vacío con lo justo para poder commitear.
fn repo() -> TempDir {
    let dir = TempDir::new().expect("tempdir");
    let p = dir.path();
    git(p, &["init", "--quiet"]);
    git(p, &["config", "user.name", "Pruebas"]);
    git(p, &["config", "user.email", "pruebas@example.invalid"]);
    git(p, &["config", "commit.gpgsign", "false"]);
    let sin_hooks = p.join(".sin-hooks");
    git(
        p,
        &["config", "core.hooksPath", &sin_hooks.to_string_lossy()],
    );
    dir
}

fn escribir(dir: &TempDir, nombre: &str, texto: &str) -> PathBuf {
    let ruta = dir.path().join(nombre);
    if let Some(padre) = ruta.parent() {
        std::fs::create_dir_all(padre).unwrap();
    }
    std::fs::write(&ruta, texto).unwrap();
    ruta
}

#[test]
fn fuera_de_git_la_version_es_la_huella() {
    let dir = TempDir::new().unwrap();
    let ruta = escribir(&dir, "p.yaml", V1);

    assert!(Repo::of(&ruta).is_none());
    let foto = Snapshot::take(&ruta).unwrap();
    assert!(foto.git.is_none());
    assert_eq!(foto.label(), format!("sin git (#{})", &foto.hash[..7]));
    assert_eq!(foto.hash, orch_core::fingerprint(V1));
    assert_eq!(foto.text, V1);
    // El pipeline que se ejecuta sale de ese mismo texto.
    assert_eq!(foto.spec().unwrap().name, "demo");
}

#[test]
fn guardar_una_version_y_luego_cambiarla() {
    let dir = repo();
    let ruta = escribir(&dir, "pipelines/p.yaml", V1);
    let repo = Repo::of(&ruta).expect("dentro de un repositorio");

    // Sin guardar todavía: no hay commit al que atribuirlo.
    let foto = Snapshot::take(&ruta).unwrap();
    let estado = foto.git.as_ref().unwrap();
    assert!(estado.commit.is_none() && estado.modified);
    assert_eq!(foto.label(), "sin guardar en git");

    let primero = repo.commit(&ruta, "Primera versión").unwrap();
    assert_eq!(primero.subject, "Primera versión");
    assert_eq!(primero.author, "Pruebas");
    assert_eq!(primero.path, "pipelines/p.yaml");

    let foto = Snapshot::take(&ruta).unwrap();
    assert_eq!(
        foto.commit().map(|c| c.hash.as_str()),
        Some(primero.hash.as_str())
    );
    assert_eq!(foto.label(), primero.short());

    // Un cambio sin guardar: la versión es «ese commit, con cambios».
    std::fs::write(&ruta, V2).unwrap();
    let foto = Snapshot::take(&ruta).unwrap();
    assert!(foto.commit().is_none());
    assert_eq!(foto.label(), format!("{} con cambios", primero.short()));

    let segundo = repo.commit(&ruta, "Dos filas").unwrap();
    let historia = repo.log(&ruta, 10).unwrap();
    assert_eq!(
        historia
            .iter()
            .map(|c| c.subject.as_str())
            .collect::<Vec<_>>(),
        ["Dos filas", "Primera versión"]
    );
    assert_eq!(historia[0].hash, segundo.hash);

    // El texto de una versión anterior, por hash o por referencia.
    let (commit, texto) = repo.show(&primero.hash[..8], &ruta).unwrap();
    assert_eq!(texto, V1);
    assert_eq!(commit.hash, primero.hash);
    assert_eq!(repo.show("HEAD", &ruta).unwrap().1, V2);
}

#[test]
fn sin_cambios_o_sin_mensaje_no_se_guarda_nada() {
    let dir = repo();
    let ruta = escribir(&dir, "p.yaml", V1);
    let repo = Repo::of(&ruta).unwrap();

    let sin_mensaje = repo.commit(&ruta, "   ").unwrap_err().to_string();
    assert!(sin_mensaje.contains("mensaje"), "{sin_mensaje}");

    repo.commit(&ruta, "Primera").unwrap();
    let sin_cambios = repo.commit(&ruta, "Otra").unwrap_err().to_string();
    assert!(sin_cambios.contains("no hay cambios"), "{sin_cambios}");
}

#[test]
fn guardar_solo_toca_el_pipeline() {
    let dir = repo();
    let ruta = escribir(&dir, "p.yaml", V1);
    // Algo que el usuario tenía preparado para otro commit.
    escribir(&dir, "notas.txt", "pendiente\n");
    git(dir.path(), &["add", "notas.txt"]);

    let repo = Repo::of(&ruta).unwrap();
    repo.commit(&ruta, "Sólo el pipeline").unwrap();

    let en_el_commit = git(dir.path(), &["show", "--name-only", "--format=", "HEAD"]);
    assert_eq!(en_el_commit.trim(), "p.yaml");
    // Lo preparado sigue preparado, sin commitear.
    let estado = git(dir.path(), &["status", "--porcelain"]);
    assert!(estado.contains("A  notas.txt"), "{estado}");
}

#[test]
fn descartar_vuelve_a_la_ultima_version() {
    let dir = repo();
    let ruta = escribir(&dir, "p.yaml", V1);
    let repo = Repo::of(&ruta).unwrap();

    // Sin versión guardada no hay a qué volver: no se borra nada.
    assert!(repo.discard(&ruta).is_err());
    assert_eq!(std::fs::read_to_string(&ruta).unwrap(), V1);

    repo.commit(&ruta, "Primera").unwrap();
    std::fs::write(&ruta, V2).unwrap();
    repo.discard(&ruta).unwrap();
    assert_eq!(std::fs::read_to_string(&ruta).unwrap(), V1);
    assert!(!repo.state(&ruta).unwrap().modified);
}

#[test]
fn el_historial_sigue_al_fichero_renombrado() {
    let dir = repo();
    let viejo = escribir(&dir, "viejo.yaml", V1);
    let repo = Repo::of(&viejo).unwrap();
    let primero = repo.commit(&viejo, "Con el nombre viejo").unwrap();

    git(dir.path(), &["mv", "viejo.yaml", "nuevo.yaml"]);
    git(dir.path(), &["commit", "--quiet", "-m", "Renombrado"]);
    let nuevo = dir.path().join("nuevo.yaml");
    std::fs::write(&nuevo, V2).unwrap();
    repo.commit(&nuevo, "Con el nombre nuevo").unwrap();

    let historia = repo.log(&nuevo, 10).unwrap();
    assert_eq!(
        historia
            .iter()
            .map(|c| c.subject.as_str())
            .collect::<Vec<_>>(),
        ["Con el nombre nuevo", "Renombrado", "Con el nombre viejo"]
    );
    assert_eq!(historia[2].path, "viejo.yaml");

    // La versión de antes del renombrado se lee con el nombre de entonces.
    let (_, texto) = repo.show(&primero.hash, &nuevo).unwrap();
    assert_eq!(texto, V1);
}

#[test]
fn una_version_que_no_toco_el_fichero_tiene_el_contenido_anterior() {
    let dir = repo();
    let ruta = escribir(&dir, "p.yaml", V1);
    let repo = Repo::of(&ruta).unwrap();
    let primero = repo.commit(&ruta, "Pipeline").unwrap();

    escribir(&dir, "otro.txt", "x\n");
    git(dir.path(), &["add", "otro.txt"]);
    git(dir.path(), &["commit", "--quiet", "-m", "Otra cosa"]);

    let (commit, texto) = repo.show("HEAD", &ruta).unwrap();
    assert_eq!(texto, V1);
    // Se atribuye al commit que dejó el fichero así.
    assert_eq!(commit.hash, primero.hash);
    // Y el estado del fichero apunta a ese mismo commit.
    let estado = repo.state(&ruta).unwrap();
    assert_eq!(estado.commit.unwrap().hash, primero.hash);
    assert!(!estado.modified);
}

#[test]
fn una_version_que_no_existe_se_dice_claro() {
    let dir = repo();
    let ruta = escribir(&dir, "p.yaml", V1);
    let repo = Repo::of(&ruta).unwrap();
    repo.commit(&ruta, "Pipeline").unwrap();

    let error = repo.show("no-existe", &ruta).unwrap_err().to_string();
    assert!(error.contains("no conoce la versión"), "{error}");
    // Algo con pinta de opción no se le pasa a git.
    assert!(repo.show("--output=/tmp/x", &ruta).is_err());
}
