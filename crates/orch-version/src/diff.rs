//! Qué cambió entre dos versiones de un pipeline, dicho como pipeline.
//!
//! Un diff de líneas de un YAML dice «cambió la línea 23»; éste dice «el
//! nodo `pagados` filtra ahora por `status IN (...)`». Compara los pipelines
//! ya leídos, así que lo que no cambia el significado no aparece: el orden de
//! las claves, los comentarios, escribir explícitamente un valor por defecto.
//!
//! - Los nodos se emparejan por id. Uno que desaparece y otro que aparece con
//!   el mismo contenido es un **renombrado**, y sus aristas no se cuentan
//!   como quitadas y añadidas.
//! - Los campos se comparan hasta la hoja: `config.where`, `retry.max_attempts`,
//!   `schedule.cron`. Las listas se comparan enteras.
//! - Las posiciones del lienzo no cambian lo que hace el pipeline: se dicen
//!   aparte y en una sola línea.

use std::collections::{BTreeMap, BTreeSet};

use orch_core::{NodeSpec, PipelineSpec};
use serde::Serialize;
use serde_json::{Map, Value};

/// Un cambio entre dos versiones.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum Change {
    /// Un campo del pipeline: `name`, `settings.batch_size`, `schedule.cron`…
    Pipeline {
        field: String,
        before: Option<Value>,
        after: Option<Value>,
    },
    NodeAdded {
        node: String,
        kind: String,
        component: String,
    },
    NodeRemoved {
        node: String,
        kind: String,
        component: String,
    },
    NodeRenamed {
        from: String,
        to: String,
    },
    /// Un campo de un nodo: `config.where`, `connector`, `after`…
    Node {
        node: String,
        field: String,
        before: Option<Value>,
        after: Option<Value>,
    },
    EdgeAdded {
        from: String,
        to: String,
        port: String,
    },
    EdgeRemoved {
        from: String,
        to: String,
        port: String,
    },
    /// Sólo se movieron estos nodos en el lienzo.
    Layout {
        nodes: Vec<String>,
    },
}

impl Change {
    /// Si cambia lo que hace el pipeline, y no sólo cómo se dibuja.
    pub fn is_semantic(&self) -> bool {
        !matches!(self, Change::Layout { .. })
    }
}

/// Lo que cambió de `before` a `after`, en orden de lectura: el pipeline,
/// luego los nodos, luego las aristas, y al final el lienzo.
pub fn diff(before: &PipelineSpec, after: &PipelineSpec) -> Vec<Change> {
    let mut cambios = Vec::new();

    // --- El pipeline -------------------------------------------------------
    let cabecera = |s: &PipelineSpec| {
        let mut m = Map::new();
        m.insert("version".into(), json(&s.version));
        m.insert("name".into(), json(&s.name));
        m.insert("description".into(), json(&s.description));
        m.insert("settings".into(), json(&s.settings));
        m.insert("schedule".into(), json(&s.schedule));
        m.insert("alerts".into(), json(&s.alerts));
        Value::Object(m)
    };
    let mut campos = Vec::new();
    hojas("", &cabecera(before), &cabecera(after), &mut campos);
    cambios.extend(
        campos
            .into_iter()
            .map(|(field, before, after)| Change::Pipeline {
                field,
                before,
                after,
            }),
    );

    // --- Los nodos ---------------------------------------------------------
    let antes: BTreeMap<&str, &NodeSpec> =
        before.nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    let ahora: BTreeMap<&str, &NodeSpec> = after.nodes.iter().map(|n| (n.id.as_str(), n)).collect();

    let mut quitados: Vec<&str> = before
        .nodes
        .iter()
        .map(|n| n.id.as_str())
        .filter(|id| !ahora.contains_key(id))
        .collect();
    let mut nuevos: Vec<&str> = after
        .nodes
        .iter()
        .map(|n| n.id.as_str())
        .filter(|id| !antes.contains_key(id))
        .collect();

    // Un renombrado: lo que desapareció y lo que apareció son el mismo nodo.
    // Las referencias a otros nodos (`after`) se comparan ya traducidas, por
    // si se renombraron dos que dependían uno de otro.
    let mut renombres: BTreeMap<&str, &str> = BTreeMap::new();
    loop {
        let traducir = |id: &str| renombres.get(id).copied().unwrap_or(id).to_string();
        let par = quitados.iter().find_map(|&viejo| {
            nuevos
                .iter()
                .find(|&&nuevo| {
                    cuerpo(antes[viejo], &traducir) == cuerpo(ahora[nuevo], &|id| id.to_string())
                })
                .map(|&nuevo| (viejo, nuevo))
        });
        let Some((viejo, nuevo)) = par else { break };
        renombres.insert(viejo, nuevo);
        quitados.retain(|&id| id != viejo);
        nuevos.retain(|&id| id != nuevo);
    }
    let traducir = |id: &str| renombres.get(id).copied().unwrap_or(id).to_string();

    for id in &quitados {
        let n = antes[id];
        cambios.push(Change::NodeRemoved {
            node: id.to_string(),
            kind: n.kind.label().to_string(),
            component: n.kind.component().to_string(),
        });
    }
    for id in &nuevos {
        let n = ahora[id];
        cambios.push(Change::NodeAdded {
            node: id.to_string(),
            kind: n.kind.label().to_string(),
            component: n.kind.component().to_string(),
        });
    }
    for (viejo, nuevo) in &renombres {
        cambios.push(Change::NodeRenamed {
            from: viejo.to_string(),
            to: nuevo.to_string(),
        });
    }
    // Los que siguen ahí, en el orden del fichero nuevo.
    for n in &after.nodes {
        let viejo = renombres
            .iter()
            .find(|(_, &nuevo)| nuevo == n.id)
            .map(|(&viejo, _)| viejo)
            .unwrap_or(n.id.as_str());
        let Some(&previo) = antes.get(viejo) else {
            continue;
        };
        let mut campos = Vec::new();
        hojas(
            "",
            &cuerpo(previo, &traducir),
            &cuerpo(n, &|id| id.to_string()),
            &mut campos,
        );
        cambios.extend(
            campos
                .into_iter()
                .map(|(field, before, after)| Change::Node {
                    node: n.id.clone(),
                    field,
                    before,
                    after,
                }),
        );
    }

    // --- Las aristas -------------------------------------------------------
    let aristas = |s: &PipelineSpec, traducir: &dyn Fn(&str) -> String| {
        s.edges
            .iter()
            .map(|e| {
                (
                    traducir(&e.from),
                    traducir(&e.to),
                    // El puerto por defecto es el id del origen: si sólo
                    // cambió porque se renombró el origen, no es un cambio.
                    match &e.port {
                        Some(p) => p.clone(),
                        None => traducir(&e.from),
                    },
                )
            })
            .collect::<BTreeSet<_>>()
    };
    let aristas_antes = aristas(before, &traducir);
    let aristas_ahora = aristas(after, &|id| id.to_string());
    for (from, to, port) in aristas_antes.difference(&aristas_ahora) {
        cambios.push(Change::EdgeRemoved {
            from: from.clone(),
            to: to.clone(),
            port: port.clone(),
        });
    }
    for (from, to, port) in aristas_ahora.difference(&aristas_antes) {
        cambios.push(Change::EdgeAdded {
            from: from.clone(),
            to: to.clone(),
            port: port.clone(),
        });
    }

    // --- El lienzo ---------------------------------------------------------
    let posiciones_antes: BTreeMap<String, [f64; 2]> = before
        .layout
        .iter()
        .map(|(id, p)| (traducir(id), *p))
        .collect();
    let movidos: Vec<String> = after
        .nodes
        .iter()
        .map(|n| n.id.clone())
        // Un nodo nuevo trae su posición: eso no es moverlo.
        .filter(|id| !nuevos.contains(&id.as_str()))
        .filter(|id| posiciones_antes.get(id) != after.layout.get(id))
        .collect();
    if !movidos.is_empty() {
        cambios.push(Change::Layout { nodes: movidos });
    }

    cambios
}

/// Un nodo sin su id, con las referencias a otros nodos traducidas.
fn cuerpo(nodo: &NodeSpec, traducir: &dyn Fn(&str) -> String) -> Value {
    let mut valor = json(nodo);
    if let Value::Object(m) = &mut valor {
        m.remove("id");
        m.insert(
            "after".into(),
            Value::Array(
                nodo.after
                    .iter()
                    .map(|id| Value::String(traducir(id)))
                    .collect(),
            ),
        );
    }
    valor
}

/// Los campos que difieren, hasta la hoja. Un objeto se recorre clave a
/// clave; todo lo demás (listas incluidas) se compara entero.
fn hojas(
    prefijo: &str,
    antes: &Value,
    ahora: &Value,
    salida: &mut Vec<(String, Option<Value>, Option<Value>)>,
) {
    match (antes, ahora) {
        (Value::Object(a), Value::Object(b)) => {
            let claves: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
            for clave in claves {
                let campo = if prefijo.is_empty() {
                    clave.clone()
                } else {
                    format!("{prefijo}.{clave}")
                };
                match (a.get(clave), b.get(clave)) {
                    (Some(x), Some(y)) => hojas(&campo, x, y, salida),
                    (x, y) => salida.push((campo, presente(x), presente(y))),
                }
            }
        }
        (a, b) if a != b => {
            salida.push((prefijo.to_string(), presente(Some(a)), presente(Some(b))))
        }
        _ => {}
    }
}

/// `null` y ausente son lo mismo para quien lee el diff.
fn presente(v: Option<&Value>) -> Option<Value> {
    v.filter(|v| !v.is_null()).cloned()
}

fn json<T: Serialize>(valor: &T) -> Value {
    serde_json::to_value(valor).unwrap_or(Value::Null)
}

/// Un valor tal y como se enseña en una línea: los textos entre comillas,
/// lo demás como JSON compacto.
pub fn show_value(valor: &Value) -> String {
    match valor {
        Value::String(s) => format!("\"{s}\""),
        otro => otro.to_string(),
    }
}

/// Diff de líneas entre dos textos, para un valor de varias líneas (una
/// query escrita con `|`). Cada línea va marcada con `-`, `+` o ` `.
pub fn line_diff<'a>(antes: &'a str, ahora: &'a str) -> Vec<(char, &'a str)> {
    let a: Vec<&str> = antes.lines().collect();
    let b: Vec<&str> = ahora.lines().collect();
    // Subsecuencia común más larga, de atrás hacia delante. Las queries son
    // cortas: la tabla cuadrática no importa.
    let mut lcs = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for i in (0..a.len()).rev() {
        for j in (0..b.len()).rev() {
            lcs[i][j] = if a[i] == b[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let (mut i, mut j) = (0, 0);
    let mut salida = Vec::new();
    while i < a.len() && j < b.len() {
        if a[i] == b[j] {
            salida.push((' ', a[i]));
            i += 1;
            j += 1;
        } else if lcs[i + 1][j] >= lcs[i][j + 1] {
            salida.push(('-', a[i]));
            i += 1;
        } else {
            salida.push(('+', b[j]));
            j += 1;
        }
    }
    salida.extend(a[i..].iter().map(|l| ('-', *l)));
    salida.extend(b[j..].iter().map(|l| ('+', *l)));
    salida
}
