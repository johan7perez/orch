//! Cada cambio, aplicado al texto.
//!
//! Dos herramientas, según el caso. Para cambiar valores dentro de un mapa
//! (un campo de la config, un `after`, un id) se usa `yaml-edit`, que
//! reescribe sólo ese valor y pone las comillas que hagan falta. Para añadir
//! y quitar elementos enteros de `nodes` y `edges` se corta y pega por
//! líneas, porque ahí importan cosas que el árbol no decide bien: la línea en
//! blanco que separa los nodos, el comentario que describe al que se quita y
//! el estilo `{ from: a, to: b }` de las aristas.
//!
//! Cada función relee el texto que recibe, de modo que los rangos con los
//! que corta son siempre los de ese texto y no los de uno anterior.

use std::ops::Range;
use std::str::FromStr;

use orch_core::{EdgeSpec, NodeKind, PipelineSpec};
use serde_json::{Map, Value};
use yaml_edit::{Document, Mapping, ScalarValue, Sequence};

use crate::modelo::{indice, lee_puertos};
use crate::{Edit, EditError, Kind};

pub(crate) fn aplicar(texto: &str, edit: &Edit) -> Result<String, EditError> {
    match edit {
        Edit::AddNode {
            id,
            kind,
            component,
            config,
        } => anadir_nodo(texto, id, *kind, component, config),
        Edit::RemoveNode { id } => quitar_nodo(texto, id),
        Edit::Connect { from, to, port } => conectar(texto, from, to, port.as_deref()),
        Edit::Disconnect { from, to } => quitar_aristas(texto, |e| e.from == *from && e.to == *to),
        Edit::SetConfig { node, key, value } => poner_config(texto, node, key, value.as_ref()),
        Edit::SetAfter { node, after } => {
            let mut lista: Vec<String> = Vec::with_capacity(after.len());
            for previo in after {
                if !lista.contains(previo) {
                    lista.push(previo.clone());
                }
            }
            poner_after(texto, node, &lista)
        }
        Edit::RenameNode { from, to } => renombrar(texto, from, to),
        Edit::SetLayout { positions } => poner_layout(texto, positions),
        Edit::ClearLayout => quitar_layout(texto),
    }
}

// --- Operaciones ----------------------------------------------------------

fn anadir_nodo(
    texto: &str,
    id: &str,
    kind: Kind,
    component: &str,
    config: &Map<String, Value>,
) -> Result<String, EditError> {
    let (doc, raiz, spec) = abrir(texto)?;
    let nodos = secuencia(&raiz, "nodes", spec.nodes.len())?;
    // Si los nodos que hay van separados por una línea en blanco, el nuevo
    // también. Es lo que hacen todos los ejemplos.
    let separar = separados(texto, &rangos(texto, &nodos), true);

    let nodo = Mapping::new();
    nodo.set("id", id);
    let (tipo, clave) = match kind {
        Kind::Source => ("source", "connector"),
        Kind::Transform => ("transform", "op"),
        Kind::Sink => ("sink", "connector"),
    };
    nodo.set("type", tipo);
    nodo.set(clave, component);
    if !config.is_empty() {
        let mapa = Mapping::new();
        for (campo, valor) in config {
            poner(&mapa, campo, valor);
        }
        nodo.set("config", mapa);
    }
    nodos.push(nodo);
    let texto = doc.to_string();

    if !separar {
        return Ok(texto);
    }
    // `yaml-edit` lo pega justo debajo del anterior; la línea en blanco se
    // pone aquí.
    let (_, raiz, spec) = abrir(&texto)?;
    let nodos = secuencia(&raiz, "nodes", spec.nodes.len())?;
    let ultimo = rangos(&texto, &nodos)
        .pop()
        .expect("se acaba de añadir uno");
    if guion(&texto, &ultimo).is_none() {
        return Ok(texto);
    }
    let linea = inicio_de_linea(&texto, ultimo.start);
    if linea >= 2 && &texto[linea - 2..linea] == "\n\n" {
        return Ok(texto);
    }
    let mut con_hueco = texto;
    con_hueco.insert(linea, '\n');
    Ok(con_hueco)
}

fn quitar_nodo(texto: &str, id: &str) -> Result<String, EditError> {
    // Primero lo que lo nombra; el nodo, al final.
    let mut texto = quitar_aristas(texto, |e| e.from == id || e.to == id)?;

    let spec = leer(&texto)?;
    for nodo in &spec.nodes {
        if nodo.after.iter().any(|previo| previo == id) {
            let resto: Vec<String> = nodo
                .after
                .iter()
                .filter(|previo| *previo != id)
                .cloned()
                .collect();
            texto = poner_after(&texto, &nodo.id, &resto)?;
        }
    }
    texto = quitar_de_layout(&texto, id)?;

    let (doc, raiz, spec) = abrir(&texto)?;
    let i = indice(&spec, id)?;
    let nodos = secuencia(&raiz, "nodes", spec.nodes.len())?;
    if nodos.len() == 1 {
        // Una secuencia sin elementos se queda en `nodes:` a secas, que se lee
        // como nulo y no como lista vacía.
        raiz.set("nodes", Sequence::new_flow());
        return Ok(doc.to_string());
    }
    quitar_elemento(&texto, rangos(&texto, &nodos)[i].clone())
}

fn quitar_aristas(texto: &str, quitar: impl Fn(&EdgeSpec) -> bool) -> Result<String, EditError> {
    let (doc, raiz, spec) = abrir(texto)?;
    let indices: Vec<usize> = spec
        .edges
        .iter()
        .enumerate()
        .filter(|(_, e)| quitar(e))
        .map(|(i, _)| i)
        .collect();
    if indices.is_empty() {
        return Ok(texto.to_string());
    }
    let aristas = secuencia(&raiz, "edges", spec.edges.len())?;
    if indices.len() == aristas.len() {
        raiz.set("edges", Sequence::new_flow());
        return Ok(doc.to_string());
    }

    let rangos = rangos(texto, &aristas);
    let mut texto = texto.to_string();
    // De atrás hacia delante: quitar un elemento no mueve los anteriores.
    for &i in indices.iter().rev() {
        texto = quitar_elemento(&texto, rangos[i].clone())?;
    }
    Ok(texto)
}

fn conectar(texto: &str, from: &str, to: &str, port: Option<&str>) -> Result<String, EditError> {
    let (doc, raiz, spec) = abrir(texto)?;

    let Some(aristas) = raiz.get_sequence("edges") else {
        return Ok(anadir_bloque_de_aristas(
            texto, &raiz, &spec, from, to, port,
        ));
    };
    if aristas.len() != spec.edges.len() {
        return Err(desfase("edges", aristas.len(), spec.edges.len()));
    }

    if aristas.is_empty() {
        // `edges: []`: se sustituye por un bloque normal.
        raiz.remove("edges");
        let sin = doc.to_string();
        let (_, raiz, spec) = abrir(&sin)?;
        return Ok(anadir_bloque_de_aristas(&sin, &raiz, &spec, from, to, port));
    }

    let rangos = rangos(texto, &aristas);
    let ultimo = rangos.last().expect("no está vacía").clone();
    let en_bloque = !aristas.is_flow_style() && guion(texto, &ultimo).is_some();
    let muestra = &texto[ultimo.clone()];

    if en_bloque && muestra.starts_with('{') {
        // El estilo de los ejemplos: una arista por línea, `{ from: a, to: b }`.
        let espacios = muestra.starts_with("{ ");
        let linea = arista_en_linea(from, to, port, espacios);
        return anadir_elemento(texto, &rangos, &linea, separados(texto, &rangos, false));
    }

    // Cualquier otro estilo: que `yaml-edit` imite el de los hermanos.
    let arista = if aristas.is_flow_style() {
        Mapping::new_flow()
    } else {
        Mapping::new()
    };
    arista.set("from", from);
    arista.set("to", to);
    if let Some(port) = port {
        arista.set("port", port);
    }
    aristas.push(arista);
    Ok(doc.to_string())
}

fn poner_config(
    texto: &str,
    node: &str,
    clave: &str,
    valor: Option<&Value>,
) -> Result<String, EditError> {
    let (doc, raiz, spec) = abrir(texto)?;
    let i = indice(&spec, node)?;
    let nodos = secuencia(&raiz, "nodes", spec.nodes.len())?;
    let nodo = mapa(&nodos, i, node)?;

    match valor {
        Some(valor) => match nodo.get_mapping("config") {
            Some(config) => poner(&config, clave, valor),
            None => {
                // Sin `config:`, o con `config:` vacío, que se lee como nulo.
                let config = Mapping::new();
                poner(&config, clave, valor);
                nodo.set("config", config);
            }
        },
        None => {
            if let Some(config) = nodo.get_mapping("config") {
                config.remove(clave);
                if config.is_empty() {
                    nodo.remove("config");
                }
            }
        }
    }
    Ok(doc.to_string())
}

fn poner_after(texto: &str, node: &str, lista: &[String]) -> Result<String, EditError> {
    let (doc, raiz, spec) = abrir(texto)?;
    let i = indice(&spec, node)?;
    let nodos = secuencia(&raiz, "nodes", spec.nodes.len())?;
    let nodo = mapa(&nodos, i, node)?;

    if lista.is_empty() {
        nodo.remove("after");
        return Ok(doc.to_string());
    }

    // En una línea, como en los ejemplos: `after: [cargar]`.
    let valor = Sequence::new_flow();
    for previo in lista {
        valor.push(previo.as_str());
    }
    if nodo.contains_key("after") {
        nodo.set("after", valor);
    } else {
        // Junto al componente y antes de la config, que es donde lo ponen
        // los ejemplos: se lee antes que el detalle.
        let clave = match spec.nodes[i].kind {
            NodeKind::Transform { .. } => "op",
            _ => "connector",
        };
        if !nodo.insert_after(clave, "after", &valor) {
            nodo.set("after", valor);
        }
    }
    Ok(doc.to_string())
}

fn renombrar(texto: &str, from: &str, to: &str) -> Result<String, EditError> {
    if from == to {
        return Ok(texto.to_string());
    }
    let (doc, raiz, spec) = abrir(texto)?;
    let i = indice(&spec, from)?;
    let nodos = secuencia(&raiz, "nodes", spec.nodes.len())?;

    mapa(&nodos, i, from)?.set("id", to);

    if !spec.edges.is_empty() {
        let aristas = secuencia(&raiz, "edges", spec.edges.len())?;
        for (j, arista) in spec.edges.iter().enumerate() {
            if arista.from != from && arista.to != from {
                continue;
            }
            let mapa = mapa(&aristas, j, "una arista")?;
            if arista.from == from {
                mapa.set("from", to);
                if arista.port.is_none() && lee_puertos(&spec, &arista.to) {
                    mapa.set("port", from);
                }
            }
            if arista.to == from {
                mapa.set("to", to);
            }
        }
    }

    for (j, nodo) in spec.nodes.iter().enumerate() {
        let posiciones: Vec<usize> = nodo
            .after
            .iter()
            .enumerate()
            .filter(|(_, previo)| *previo == from)
            .map(|(k, _)| k)
            .collect();
        if posiciones.is_empty() {
            continue;
        }
        let lista = mapa(&nodos, j, &nodo.id)?
            .get_sequence("after")
            .ok_or_else(|| EditError::Unsupported(format!("el `after` de `{}`", nodo.id)))?;
        for k in posiciones {
            lista.set(k, to);
        }
    }

    if let Some(layout) = raiz.get_mapping("layout") {
        if layout.contains_key(from) {
            layout.rename_key(from, to);
        }
    }

    Ok(doc.to_string())
}

// --- Posiciones --------------------------------------------------------------

/// Cabecera del bloque `layout`. Quien abre el YAML a mano tiene que saber
/// qué es eso del final y que puede borrarlo sin romper nada.
const CABECERA_LAYOUT: &str = "\
# Dónde dibuja cada nodo el lienzo de Orch. El motor no lo usa; si se borra,
# el lienzo vuelve a colocarlos solo.
";

fn poner_layout(
    texto: &str,
    posiciones: &std::collections::BTreeMap<String, [f64; 2]>,
) -> Result<String, EditError> {
    let (doc, raiz, spec) = abrir(texto)?;

    if let Some(layout) = raiz.get_mapping("layout") {
        // Ya existe: se cambian sus líneas y las que faltan van al final.
        for (id, &[x, y]) in posiciones {
            layout.set(id.as_str(), punto(x, y));
        }
        return Ok(doc.to_string());
    }
    if raiz.contains_key("layout") {
        // `layout:` vacío o con algo que no es un mapa: se sustituye.
        let layout = Mapping::new();
        for (id, &[x, y]) in posiciones {
            layout.set(id.as_str(), punto(x, y));
        }
        raiz.set("layout", layout);
        return Ok(doc.to_string());
    }

    // Bloque nuevo al final del fichero, lejos de lo que se edita a mano, y
    // en el orden de los nodos para que se lea igual que `nodes`.
    let mut nuevo = texto.to_string();
    if !nuevo.ends_with('\n') {
        nuevo.push('\n');
    }
    if !nuevo.ends_with("\n\n") {
        nuevo.push('\n');
    }
    nuevo.push_str(CABECERA_LAYOUT);
    nuevo.push_str("layout:\n");
    let en_orden = spec
        .nodes
        .iter()
        .filter_map(|n| posiciones.get_key_value(&n.id))
        .chain(
            posiciones
                .iter()
                .filter(|(id, _)| !spec.nodes.iter().any(|n| &n.id == *id)),
        );
    for (id, &[x, y]) in en_orden {
        nuevo.push_str(&format!(
            "  {}: [{}, {}]\n",
            escalar(id),
            x.round() as i64,
            y.round() as i64
        ));
    }
    Ok(nuevo)
}

fn punto(x: f64, y: f64) -> Sequence {
    let punto = Sequence::new_flow();
    punto.push(x.round() as i64);
    punto.push(y.round() as i64);
    punto
}

/// Quita la posición de un nodo; si era la última, el bloque entero.
fn quitar_de_layout(texto: &str, id: &str) -> Result<String, EditError> {
    let (doc, raiz, _) = abrir(texto)?;
    let Some(layout) = raiz.get_mapping("layout") else {
        return Ok(texto.to_string());
    };
    if !layout.contains_key(id) {
        return Ok(texto.to_string());
    }
    layout.remove(id);
    if layout.is_empty() {
        return quitar_layout(texto);
    }
    Ok(doc.to_string())
}

/// Quita el bloque `layout` entero, con su cabecera.
///
/// El bloque lo escribe Orch, así que basta con cortar por líneas: desde los
/// comentarios pegados encima de `layout:` hasta la primera línea que vuelve
/// a la columna 0.
fn quitar_layout(texto: &str) -> Result<String, EditError> {
    let Some(clave) = texto
        .match_indices("layout:")
        .map(|(i, _)| i)
        .find(|&i| i == 0 || texto.as_bytes()[i - 1] == b'\n')
    else {
        // No está escrito como una clave al principio de línea: o no existe,
        // o tiene una forma rara que mejor resuelve el árbol.
        let (doc, raiz, _) = abrir(texto)?;
        if raiz.contains_key("layout") {
            raiz.remove("layout");
            return Ok(doc.to_string());
        }
        return Ok(texto.to_string());
    };

    let mut desde = clave;
    while desde > 0 {
        let anterior = inicio_de_linea(texto, desde - 1);
        if texto[anterior..desde].starts_with('#') {
            desde = anterior;
        } else {
            break;
        }
    }

    let mut hasta = fin_de_linea(texto, clave);
    while hasta < texto.len() && texto[hasta..].starts_with([' ', '\t']) {
        hasta = fin_de_linea(texto, hasta);
    }

    // La línea en blanco que lo separaba de lo anterior sobra si detrás no
    // queda nada, o si detrás ya hay otra.
    if desde >= 2 && &texto[desde - 2..desde] == "\n\n" {
        let detras_en_blanco =
            hasta == texto.len() || texto[hasta..fin_de_linea(texto, hasta)].trim().is_empty();
        if detras_en_blanco {
            desde -= 1;
        }
    }

    Ok(format!("{}{}", &texto[..desde], &texto[hasta..]))
}

// --- Cortar y pegar por líneas ---------------------------------------------

/// Quita un elemento de una secuencia en bloque, con lo que le pertenece.
///
/// Le pertenecen los comentarios pegados encima a su misma sangría (son los
/// que lo describen), los comentarios más sangrados que lo siguen (un campo
/// comentado dentro del nodo) y una línea en blanco detrás, para que la
/// separación con el siguiente quede como estaba.
fn quitar_elemento(texto: &str, rango: Range<usize>) -> Result<String, EditError> {
    let columna = guion(texto, &rango)
        .ok_or_else(|| EditError::Unsupported("una lista escrita en una línea".into()))?;
    let resto = texto[rango.end..fin_de_linea(texto, rango.end)].trim();
    if !(resto.is_empty() || resto.starts_with('#')) {
        return Err(EditError::Unsupported(
            "un elemento que comparte línea con otro".into(),
        ));
    }

    let mut desde = inicio_de_linea(texto, rango.start);
    while desde > 0 {
        let anterior = inicio_de_linea(texto, desde - 1);
        let linea = &texto[anterior..desde];
        if linea.trim_start().starts_with('#') && sangria(linea) == columna {
            desde = anterior;
        } else {
            break;
        }
    }

    let mut hasta = tras_comentarios_propios(texto, fin_de_linea(texto, rango.end), columna);
    if hasta < texto.len() {
        let siguiente = fin_de_linea(texto, hasta);
        if texto[hasta..siguiente].trim().is_empty() {
            hasta = siguiente;
        }
    }

    Ok(format!("{}{}", &texto[..desde], &texto[hasta..]))
}

/// Añade un elemento de una línea al final de una secuencia en bloque.
fn anadir_elemento(
    texto: &str,
    rangos: &[Range<usize>],
    linea: &str,
    separar: bool,
) -> Result<String, EditError> {
    let ultimo = rangos.last().expect("la secuencia no está vacía");
    let columna = guion(texto, ultimo)
        .ok_or_else(|| EditError::Unsupported("una lista escrita en una línea".into()))?;
    let pos = tras_comentarios_propios(texto, fin_de_linea(texto, ultimo.end), columna);

    let mut nuevo = String::new();
    if pos == texto.len() && !texto.ends_with('\n') {
        nuevo.push('\n');
    }
    if separar {
        nuevo.push('\n');
    }
    nuevo.push_str(&" ".repeat(columna));
    nuevo.push_str("- ");
    nuevo.push_str(linea);
    nuevo.push('\n');

    let mut texto = texto.to_string();
    texto.insert_str(pos, &nuevo);
    Ok(texto)
}

/// Un pipeline sin `edges:` recibe el bloque entero al final del fichero, con
/// la misma sangría que los nodos.
fn anadir_bloque_de_aristas(
    texto: &str,
    raiz: &Mapping,
    spec: &PipelineSpec,
    from: &str,
    to: &str,
    port: Option<&str>,
) -> String {
    let columna = secuencia(raiz, "nodes", spec.nodes.len())
        .ok()
        .and_then(|nodos| rangos(texto, &nodos).first().and_then(|r| guion(texto, r)))
        .unwrap_or(2);

    let mut nuevo = texto.to_string();
    if !nuevo.ends_with('\n') {
        nuevo.push('\n');
    }
    if !nuevo.ends_with("\n\n") {
        nuevo.push('\n');
    }
    nuevo.push_str("edges:\n");
    nuevo.push_str(&" ".repeat(columna));
    nuevo.push_str("- ");
    nuevo.push_str(&arista_en_linea(from, to, port, true));
    nuevo.push('\n');
    nuevo
}

fn arista_en_linea(from: &str, to: &str, port: Option<&str>, espacios: bool) -> String {
    let mut campos = vec![
        format!("from: {}", escalar(from)),
        format!("to: {}", escalar(to)),
    ];
    if let Some(port) = port {
        campos.push(format!("port: {}", escalar(port)));
    }
    let dentro = campos.join(", ");
    if espacios {
        format!("{{ {dentro} }}")
    } else {
        format!("{{{dentro}}}")
    }
}

/// Un escalar que se puede escribir dentro de `{ ... }`.
///
/// Sin comillas si es un identificador que ningún parser de YAML confunda con
/// otra cosa; si no, entre comillas dobles. Un string de JSON es un escalar
/// de YAML válido con las mismas reglas de escape.
fn escalar(valor: &str) -> String {
    const RESERVADAS: &[&str] = &[
        "true", "false", "yes", "no", "on", "off", "y", "n", "null", "~",
    ];
    let mut letras = valor.chars();
    let plano = matches!(letras.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && letras.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && !RESERVADAS.contains(&valor.to_ascii_lowercase().as_str());
    if plano {
        valor.to_string()
    } else {
        serde_json::to_string(valor).expect("un string siempre se serializa")
    }
}

/// Si el elemento empieza en su propia línea (`  - id: ...`), la columna del
/// guion.
fn guion(texto: &str, rango: &Range<usize>) -> Option<usize> {
    let linea = inicio_de_linea(texto, rango.start);
    let prefijo = &texto[linea..rango.start];
    (prefijo.trim() == "-").then(|| sangria(prefijo))
}

/// Si los dos últimos elementos van separados por una línea en blanco. Con
/// menos de dos no hay nada que imitar y se usa `por_defecto`.
fn separados(texto: &str, rangos: &[Range<usize>], por_defecto: bool) -> bool {
    let [.., a, b] = rangos else {
        return por_defecto;
    };
    if guion(texto, b).is_none() {
        return false;
    }
    let desde = fin_de_linea(texto, a.end);
    let hasta = inicio_de_linea(texto, b.start);
    desde < hasta && texto[desde..hasta].lines().any(|l| l.trim().is_empty())
}

/// Avanza sobre los comentarios más sangrados que el guion: siguen siendo del
/// elemento aunque el árbol los deje fuera de su rango.
fn tras_comentarios_propios(texto: &str, mut pos: usize, columna: usize) -> usize {
    while pos < texto.len() {
        let fin = fin_de_linea(texto, pos);
        let linea = &texto[pos..fin];
        if linea.trim_start().starts_with('#') && sangria(linea) > columna {
            pos = fin;
        } else {
            break;
        }
    }
    pos
}

fn inicio_de_linea(texto: &str, pos: usize) -> usize {
    texto[..pos].rfind('\n').map_or(0, |i| i + 1)
}

fn fin_de_linea(texto: &str, pos: usize) -> usize {
    texto[pos..].find('\n').map_or(texto.len(), |i| pos + i + 1)
}

fn sangria(linea: &str) -> usize {
    linea.len() - linea.trim_start_matches(' ').len()
}

// --- El árbol --------------------------------------------------------------

fn leer(texto: &str) -> Result<PipelineSpec, EditError> {
    crate::leer(texto).map_err(EditError::Unreadable)
}

/// El árbol sin pérdidas, su raíz y el modelo del mismo texto.
fn abrir(texto: &str) -> Result<(Document, Mapping, PipelineSpec), EditError> {
    let spec = leer(texto)?;
    let doc = Document::from_str(texto)
        .map_err(|err| EditError::Unsupported(format!("el árbol sintáctico: {err}")))?;
    let raiz = doc
        .as_mapping()
        .ok_or_else(|| EditError::Unsupported("la raíz no es un mapa".into()))?;
    Ok((doc, raiz, spec))
}

/// Una secuencia de la raíz, comprobando que tiene tantos elementos como lee
/// el motor. Los nodos y las aristas se localizan por posición, así que si
/// las cuentas no cuadran no se toca nada.
fn secuencia(raiz: &Mapping, clave: &str, esperados: usize) -> Result<Sequence, EditError> {
    let lista = raiz
        .get_sequence(clave)
        .ok_or_else(|| EditError::Unsupported(format!("no encuentro la lista `{clave}`")))?;
    if lista.len() != esperados {
        return Err(desfase(clave, lista.len(), esperados));
    }
    Ok(lista)
}

fn desfase(clave: &str, vistos: usize, esperados: usize) -> EditError {
    EditError::Unsupported(format!(
        "en `{clave}` veo {vistos} elementos y el motor lee {esperados}"
    ))
}

fn mapa(lista: &Sequence, i: usize, que: &str) -> Result<Mapping, EditError> {
    lista
        .get(i)
        .and_then(|nodo| nodo.as_mapping().cloned())
        .ok_or_else(|| EditError::Unsupported(format!("`{que}` no está escrito como un mapa")))
}

/// Dónde está cada elemento en el texto, sin el blanco del final.
///
/// El árbol incluye el salto de línea final en el rango de un mapa en bloque
/// pero no en el de uno en línea (`{ ... }`). Recortado, todos acaban en su
/// último carácter visible y las cuentas por líneas valen para los dos.
fn rangos(texto: &str, lista: &Sequence) -> Vec<Range<usize>> {
    lista
        .values()
        .map(|nodo| {
            let rango = nodo.byte_range();
            let inicio = rango.start as usize;
            let fin = inicio + texto[inicio..rango.end as usize].trim_end().len();
            inicio..fin
        })
        .collect()
}

/// Pone un valor de JSON en un mapa del árbol.
fn poner(mapa: &Mapping, clave: &str, valor: &Value) {
    match valor {
        Value::Null => mapa.set(clave, ScalarValue::null()),
        Value::Bool(b) => mapa.set(clave, *b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                mapa.set(clave, i);
            } else if let Some(u) = n.as_u64() {
                mapa.set(clave, u);
            } else {
                mapa.set(clave, n.as_f64().unwrap_or_default());
            }
        }
        Value::String(s) => mapa.set(clave, s.as_str()),
        Value::Array(elementos) => mapa.set(clave, lista_de(elementos)),
        Value::Object(campos) => {
            let hijo = Mapping::new();
            for (k, v) in campos {
                poner(&hijo, k, v);
            }
            mapa.set(clave, hijo);
        }
    }
}

/// Una lista de escalares va en una línea, como `columns: [a, b]` en los
/// ejemplos; una de mapas o listas, en bloque.
fn lista_de(elementos: &[Value]) -> Sequence {
    let plana = elementos.iter().all(|v| !v.is_array() && !v.is_object());
    let lista = if plana {
        Sequence::new_flow()
    } else {
        Sequence::new()
    };
    for valor in elementos {
        match valor {
            Value::Null => lista.push(ScalarValue::null()),
            Value::Bool(b) => lista.push(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    lista.push(i);
                } else if let Some(u) = n.as_u64() {
                    lista.push(u);
                } else {
                    lista.push(n.as_f64().unwrap_or_default());
                }
            }
            Value::String(s) => lista.push(s.as_str()),
            Value::Array(hijos) => lista.push(lista_de(hijos)),
            Value::Object(campos) => {
                let hijo = Mapping::new();
                for (k, v) in campos {
                    poner(&hijo, k, v);
                }
                lista.push(hijo);
            }
        }
    }
    lista
}
