//! Partir un pipeline por las aristas que cruzan de una máquina a otra.
//!
//! Cada arista `u → v` con `u` y `v` en máquinas distintas se convierte en:
//!
//! - en la máquina de `u`, un sink `cluster_out` llamado `u→<destino>`;
//! - en la máquina de `v`, un origen `cluster_in` llamado `u@<destino>`, y la
//!   arista hacia `v` con el puerto `u`. Así el nombre del puerto de `v` no
//!   cambia —una query que lee la tabla `u` sigue funcionando— y ningún
//!   nombre se repite al juntar los informes de todas las máquinas.
//!
//! Si `u` alimenta a varios nodos de la misma máquina de destino, viaja una
//! sola vez y se reparte allí. Como en el pushdown, la reescritura se hace
//! sobre el [`PipelineSpec`] y el ejecutor de cada máquina no se entera.

use std::collections::{BTreeMap, BTreeSet};

use orch_core::{EdgeSpec, NodeKind, NodeSpec, OrchError, PipelineSpec, Plazo, Result};
use serde_json::json;

use super::LOCAL;

/// Dónde corre una parte.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Donde {
    /// Donde se lanza el pipeline (el coordinador).
    Local,
    Worker(String),
}

impl Donde {
    fn nombre(&self) -> &str {
        match self {
            Donde::Local => LOCAL,
            Donde::Worker(n) => n,
        }
    }
}

/// Un trozo del pipeline que corre entero en una máquina.
#[derive(Debug, Clone)]
pub struct Parte {
    pub donde: Donde,
    pub spec: PipelineSpec,
}

/// Parte `spec` (tal y como está escrito) en un pipeline por máquina.
///
/// `urls` dice cómo llegar a cada worker; `url_local`, al coordinador, si
/// algún nodo de aquí alimenta a un worker. Los tickets llevan el `run_id`:
/// dos ejecuciones a la vez no se cruzan.
pub fn dividir(
    spec: &PipelineSpec,
    run_id: &str,
    urls: &BTreeMap<String, String>,
    url_local: Option<&str>,
    espera: Plazo,
) -> Result<Vec<Parte>> {
    let donde_de = |nodo: &NodeSpec| match &nodo.worker {
        Some(w) => Donde::Worker(w.clone()),
        None => Donde::Local,
    };
    let lugar: BTreeMap<&str, Donde> = spec
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), donde_de(n)))
        .collect();

    // Cada worker nombrado tiene que existir en el clúster.
    for nodo in &spec.nodes {
        if let Some(w) = &nodo.worker {
            if !urls.contains_key(w) {
                return Err(OrchError::Validation(format!(
                    "el nodo `{}` va en el worker `{w}`, que no está en el clúster (hay: {})",
                    nodo.id,
                    urls.keys().cloned().collect::<Vec<_>>().join(", ")
                )));
            }
        }
        // Una espera `after` entre máquinas necesitaría que el coordinador
        // ordenara las partes; de momento, no.
        for previo in &nodo.after {
            if lugar.get(previo.as_str()) != Some(&donde_de(nodo)) {
                return Err(OrchError::Validation(format!(
                    "el nodo `{}` espera (`after`) a `{previo}`, que corre en otra máquina: \
                     las esperas sólo valen dentro de la misma máquina",
                    nodo.id
                )));
            }
        }
    }

    let url_de = |d: &Donde, nodo: &str| -> Result<String> {
        match d {
            Donde::Worker(w) => Ok(urls[w].clone()),
            Donde::Local => url_local.map(str::to_string).ok_or_else(|| {
                OrchError::Validation(format!(
                    "el nodo `{nodo}` corre aquí y alimenta a un worker: el coordinador tiene que \
                     escuchar para servírselo (`listen` y `url` en el fichero del clúster)"
                ))
            }),
        }
    };

    let mut partes: BTreeMap<Donde, PipelineSpec> = BTreeMap::new();
    for nodo in &spec.nodes {
        let mut copia = nodo.clone();
        copia.worker = None;
        parte(&mut partes, &lugar[nodo.id.as_str()], spec)
            .nodes
            .push(copia);
    }

    // Lo que ya viaja: (nodo de origen, máquina de destino).
    let mut cruzadas: BTreeSet<(String, Donde)> = BTreeSet::new();
    for arista in &spec.edges {
        let desde = &lugar[arista.from.as_str()];
        let hasta = &lugar[arista.to.as_str()];
        if desde == hasta {
            parte(&mut partes, desde, spec).edges.push(arista.clone());
            continue;
        }

        let ticket = format!("{run_id}/{}/{}", arista.from, hasta.nombre());
        let entrada = format!("{}@{}", arista.from, hasta.nombre());
        if cruzadas.insert((arista.from.clone(), hasta.clone())) {
            let salida = format!("{}→{}", arista.from, hasta.nombre());
            let origen = parte(&mut partes, desde, spec);
            origen.nodes.push(sintetico(
                &salida,
                NodeKind::Sink {
                    connector: "cluster_out".into(),
                    config: json!({ "ticket": ticket, "wait": espera.to_string() }),
                },
            ));
            origen.edges.push(EdgeSpec {
                from: arista.from.clone(),
                to: salida,
                port: None,
            });

            let url = url_de(desde, &arista.from)?;
            parte(&mut partes, hasta, spec).nodes.push(sintetico(
                &entrada,
                NodeKind::Source {
                    connector: "cluster_in".into(),
                    config: json!({ "url": url, "ticket": ticket, "wait": espera.to_string() }),
                },
            ));
        }
        // Al otro lado, el puerto sigue llamándose como `u`.
        parte(&mut partes, hasta, spec).edges.push(EdgeSpec {
            from: entrada,
            to: arista.to.clone(),
            port: Some(arista.port_name().to_string()),
        });
    }

    Ok(partes
        .into_iter()
        .map(|(donde, spec)| Parte { donde, spec })
        .collect())
}

/// La parte de una máquina, creándola con lo común del pipeline la primera
/// vez. Sin programación, alertas ni lienzo: eso es del coordinador.
fn parte<'a>(
    partes: &'a mut BTreeMap<Donde, PipelineSpec>,
    donde: &Donde,
    spec: &PipelineSpec,
) -> &'a mut PipelineSpec {
    partes.entry(donde.clone()).or_insert_with(|| PipelineSpec {
        version: spec.version,
        name: spec.name.clone(),
        description: spec.description.clone(),
        settings: spec.settings.clone(),
        schedule: None,
        alerts: None,
        nodes: Vec::new(),
        edges: Vec::new(),
        layout: BTreeMap::new(),
    })
}

fn sintetico(id: &str, kind: NodeKind) -> NodeSpec {
    NodeSpec {
        id: id.to_string(),
        after: Vec::new(),
        retry: Default::default(),
        worker: None,
        kind,
    }
}
