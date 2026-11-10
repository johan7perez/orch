//! Edición del YAML de un pipeline sin reescribirlo.
//!
//! Un pipeline es código que la gente edita a mano: lleva comentarios que
//! explican el porqué, bloques de SQL formateados con cuidado y un estilo
//! propio. Serializar el spec entero después de cada cambio del lienzo lo
//! destrozaría todo. Aquí el texto se edita sobre un árbol sintáctico sin
//! pérdidas (`yaml-edit`) y se toca sólo lo que cambia.
//!
//! La garantía no depende de que el editor acierte siempre. Cada cambio se
//! aplica dos veces: al modelo, que dice lo que el pipeline debe significar
//! después, y al texto. Luego el texto resultante se vuelve a leer con el
//! mismo parser que usa el motor, y tiene que dar *exactamente* ese modelo; si
//! no, el cambio se rechaza y el fichero no se toca. Un editor que a veces
//! dice «esto no lo sé hacer» es aceptable. Uno que a veces cambia algo que
//! no le pediste, no.

mod modelo;
mod texto;

use std::path::Path;

use orch_core::PipelineSpec;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Un cambio sobre el pipeline, tal y como lo pide el lienzo.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Edit {
    /// Añade un nodo al final de `nodes`.
    AddNode {
        id: String,
        kind: Kind,
        /// Conector u operación: `csv`, `sql`…
        component: String,
        #[serde(default)]
        config: Map<String, Value>,
    },
    /// Quita un nodo, sus aristas y las esperas `after` que lo nombraban.
    RemoveNode { id: String },
    /// Añade una arista de datos.
    Connect {
        from: String,
        to: String,
        #[serde(default)]
        port: Option<String>,
    },
    /// Quita la arista de datos entre dos nodos.
    Disconnect { from: String, to: String },
    /// Pone un campo de la config de un nodo; sin `value`, lo quita.
    SetConfig {
        node: String,
        key: String,
        #[serde(default)]
        value: Option<Value>,
    },
    /// Sustituye la lista `after` de un nodo.
    SetAfter { node: String, after: Vec<String> },
    /// Cambia el id de un nodo y todo lo que lo nombra.
    RenameNode { from: String, to: String },
    /// Fija dónde dibuja el lienzo estos nodos, `[x, y]`. Se redondea a
    /// enteros: medio píxel no le importa a nadie y ensucia el fichero.
    SetLayout {
        positions: std::collections::BTreeMap<String, [f64; 2]>,
    },
    /// Borra todas las posiciones: el lienzo vuelve a colocar los nodos solo.
    ClearLayout,
}

/// Tipo de nodo, como se escribe en `type:`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Source,
    Transform,
    Sink,
}

#[derive(Debug, thiserror::Error)]
pub enum EditError {
    /// El fichero de partida no se lee como pipeline. Hay que arreglarlo a
    /// mano: sin un modelo fiable de lo que dice, no hay contra qué
    /// comprobar el cambio.
    #[error("el pipeline no se puede leer, así que no se puede editar desde el lienzo: {0}")]
    Unreadable(String),
    /// El cambio no tiene sentido sobre este pipeline.
    #[error("{0}")]
    Invalid(String),
    /// El fichero tiene una forma que el editor no sabe tocar sin reescribir
    /// más de lo que cambia.
    #[error("no sé hacer este cambio sin reescribir otras partes del fichero ({0}); hazlo a mano")]
    Unsupported(String),
    /// El texto resultante no dice lo que debía. Es un fallo del editor, y
    /// precisamente por eso el fichero no se toca.
    #[error("el cambio no se guardó: el resultado no coincidía con lo esperado en `{0}`")]
    Mismatch(String),
    /// Alguien cambió el fichero desde que se abrió (otro editor, git…).
    /// Escribir encima perdería su cambio.
    #[error(
        "el fichero cambió fuera de Orch desde que se abrió, así que el cambio no se guardó: \
         vuelve a hacerlo sobre lo que hay ahora"
    )]
    Stale,
    #[error("no se pudo leer o escribir `{path}`: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// Aplica los cambios, en orden, al texto de un pipeline.
///
/// Devuelve el texto nuevo. Si un cambio falla, falla todo: no hay
/// resultados a medias.
pub fn apply(texto: &str, edits: &[Edit]) -> Result<String, EditError> {
    // Dos detalles de codificación que no deben cambiar por editar desde el
    // lienzo: el BOM de los editores de Windows y los finales de línea CRLF.
    let (bom, cuerpo) = match texto.strip_prefix('\u{feff}') {
        Some(resto) => (true, resto),
        None => (false, texto),
    };
    let crlf = cuerpo.contains("\r\n");
    let mut actual = if crlf {
        cuerpo.replace("\r\n", "\n")
    } else {
        cuerpo.to_string()
    };

    for edit in edits {
        actual = apply_one(&actual, edit)?;
    }

    if crlf {
        actual = actual.replace('\n', "\r\n");
    }
    if bom {
        actual.insert(0, '\u{feff}');
    }
    Ok(actual)
}

fn apply_one(texto: &str, edit: &Edit) -> Result<String, EditError> {
    let antes = leer(texto).map_err(EditError::Unreadable)?;
    let esperado = modelo::aplicar(&antes, edit)?;
    let nuevo = texto::aplicar(texto, edit)?;
    let despues = leer(&nuevo)
        .map_err(|err| EditError::Mismatch(format!("el texto ya no se puede leer: {err}")))?;
    comparar(&despues, &esperado)?;
    Ok(nuevo)
}

pub(crate) fn leer(texto: &str) -> Result<PipelineSpec, String> {
    PipelineSpec::from_yaml_str_as_written("pipeline", texto).map_err(|err| err.to_string())
}

/// Lo que se leyó frente a lo que se esperaba, campo a campo.
fn comparar(despues: &PipelineSpec, esperado: &PipelineSpec) -> Result<(), EditError> {
    let a = normalizar(serde_json::to_value(despues).expect("un spec siempre se serializa"));
    let b = normalizar(serde_json::to_value(esperado).expect("un spec siempre se serializa"));
    match primera_diferencia(&a, &b, "") {
        None => Ok(()),
        Some(donde) => Err(EditError::Mismatch(donde)),
    }
}

/// Un nodo sin `config:` y uno con `config: {}` significan lo mismo para el
/// motor; para la comparación también.
fn normalizar(mut spec: Value) -> Value {
    if let Some(nodos) = spec.get_mut("nodes").and_then(Value::as_array_mut) {
        for nodo in nodos {
            if let Some(config) = nodo.get_mut("config") {
                if config.is_null() {
                    *config = Value::Object(Map::new());
                }
            }
        }
    }
    spec
}

fn primera_diferencia(a: &Value, b: &Value, ruta: &str) -> Option<String> {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let mut claves: Vec<&String> = x.keys().chain(y.keys()).collect();
            claves.sort();
            claves.dedup();
            claves.into_iter().find_map(|clave| {
                let hija = if ruta.is_empty() {
                    clave.clone()
                } else {
                    format!("{ruta}.{clave}")
                };
                match (x.get(clave), y.get(clave)) {
                    (Some(p), Some(q)) => primera_diferencia(p, q, &hija),
                    _ => Some(hija),
                }
            })
        }
        (Value::Array(x), Value::Array(y)) if x.len() == y.len() => x
            .iter()
            .zip(y)
            .enumerate()
            .find_map(|(i, (p, q))| primera_diferencia(p, q, &format!("{ruta}[{i}]"))),
        _ if a == b => None,
        _ => Some(if ruta.is_empty() {
            "(raíz)".into()
        } else {
            ruta.into()
        }),
    }
}

/// Huella del contenido de un fichero.
///
/// La UI la recibe al abrir el pipeline y la devuelve al editar: si no
/// coincide, alguien cambió el fichero por debajo. Es la misma huella con la
/// que el historial identifica qué versión corrió en cada ejecución.
pub fn revision(texto: &str) -> String {
    orch_core::fingerprint(texto)
}

/// Aplica los cambios a un fichero, si sigue siendo el que se abrió.
///
/// Devuelve el texto nuevo. La escritura es atómica: se escribe al lado y se
/// renombra encima, de modo que un corte a mitad nunca deja el pipeline a
/// medio escribir.
pub fn edit_file(
    path: &Path,
    revision_esperada: &str,
    edits: &[Edit],
) -> Result<String, EditError> {
    let io = |source| EditError::Io {
        path: path.display().to_string(),
        source,
    };
    let texto = std::fs::read_to_string(path).map_err(io)?;
    if revision(&texto) != revision_esperada {
        return Err(EditError::Stale);
    }
    let nuevo = apply(&texto, edits)?;
    if nuevo != texto {
        escribir_atomico(path, &nuevo).map_err(io)?;
    }
    Ok(nuevo)
}

/// Sustituye el texto entero del fichero, si sigue siendo el que se abrió.
///
/// Es lo que hace recuperar una versión anterior: el texto viejo vuelve al
/// fichero como un cambio más, sin tocar el historial. Igual que
/// [`edit_file`], no pisa un fichero que cambió por debajo y escribe de forma
/// atómica.
pub fn replace_file(path: &Path, revision_esperada: &str, nuevo: &str) -> Result<(), EditError> {
    let io = |source| EditError::Io {
        path: path.display().to_string(),
        source,
    };
    let texto = std::fs::read_to_string(path).map_err(io)?;
    if revision(&texto) != revision_esperada {
        return Err(EditError::Stale);
    }
    if nuevo != texto {
        escribir_atomico(path, nuevo).map_err(io)?;
    }
    Ok(())
}

fn escribir_atomico(path: &Path, texto: &str) -> std::io::Result<()> {
    use std::io::Write;

    let nombre = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    // Empieza por punto y no acaba en `.yaml`: ni el demonio ni la lista de
    // la ventana lo toman por un pipeline si llegan a verlo.
    let temporal = path.with_file_name(format!(".{nombre}.orch-tmp"));

    let resultado = (|| {
        let mut fichero = std::fs::File::create(&temporal)?;
        fichero.write_all(texto.as_bytes())?;
        fichero.sync_all()?;
        drop(fichero);
        if let Ok(meta) = std::fs::metadata(path) {
            std::fs::set_permissions(&temporal, meta.permissions())?;
        }
        std::fs::rename(&temporal, path)
    })();
    if resultado.is_err() {
        let _ = std::fs::remove_file(&temporal);
    }
    resultado
}
