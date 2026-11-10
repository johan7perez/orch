//! El editor del lienzo: que cada cambio toque sólo lo que cambia.

use orch_core::PipelineSpec;
use orch_edit::{apply, edit_file, revision, Edit, EditError, Kind};
use serde_json::{json, Map, Value};

const BASE: &str = r#"# Cabecera: tiene que sobrevivir a cualquier cambio.

version: 1
name: prueba

nodes:
  - id: leer
    type: source
    connector: csv
    config:
      path: datos.csv

  # Se queda con lo que importa.
  - id: filtrar
    type: transform
    op: filter
    config:
      where: "x > 1"

  - id: cruzar
    type: transform
    op: sql
    config:
      query: |
        SELECT * FROM filtrar
      memory_limit_mb: 256

  - id: escribir
    type: sink
    connector: csv
    config:
      path: salida.csv

edges:
  - { from: leer, to: filtrar }
  - { from: filtrar, to: cruzar }
  - { from: cruzar, to: escribir }
"#;

fn uno(texto: &str, edit: Edit) -> Result<String, EditError> {
    apply(texto, &[edit])
}

fn config(valor: Value) -> Map<String, Value> {
    valor.as_object().cloned().unwrap_or_default()
}

/// Sustituye exactamente una vez; si el trozo no está, el test está mal.
fn cambiar(texto: &str, de: &str, a: &str) -> String {
    assert_eq!(
        texto.matches(de).count(),
        1,
        "`{de}` tiene que aparecer una vez"
    );
    texto.replacen(de, a, 1)
}

// --- El resultado exacto -----------------------------------------------------

#[test]
fn un_nodo_nuevo_va_al_final_separado_por_una_linea_en_blanco() {
    let nuevo = uno(
        BASE,
        Edit::AddNode {
            id: "limitar".into(),
            kind: Kind::Transform,
            component: "limit".into(),
            config: config(json!({ "rows": 10 })),
        },
    )
    .unwrap();
    let esperado = cambiar(
        BASE,
        "      path: salida.csv\n\nedges:",
        "      path: salida.csv\n\n  - id: limitar\n    type: transform\n    op: limit\n    config:\n      rows: 10\n\nedges:",
    );
    assert_eq!(nuevo, esperado);
}

#[test]
fn un_nodo_nuevo_sin_config_no_escribe_config() {
    let nuevo = uno(
        BASE,
        Edit::AddNode {
            id: "nada".into(),
            kind: Kind::Sink,
            component: "null".into(),
            config: Map::new(),
        },
    )
    .unwrap();
    assert!(
        nuevo.contains("  - id: nada\n    type: sink\n    connector: \"null\"\n\nedges:")
            || nuevo.contains("  - id: nada\n    type: sink\n    connector: 'null'\n\nedges:"),
        "{nuevo}"
    );
}

#[test]
fn quitar_un_nodo_se_lleva_su_comentario_sus_aristas_y_su_separacion() {
    let nuevo = uno(
        BASE,
        Edit::RemoveNode {
            id: "filtrar".into(),
        },
    )
    .unwrap();
    let esperado = cambiar(
        BASE,
        "  # Se queda con lo que importa.\n  - id: filtrar\n    type: transform\n    op: filter\n    config:\n      where: \"x > 1\"\n\n",
        "",
    );
    let esperado = cambiar(&esperado, "  - { from: leer, to: filtrar }\n", "");
    let esperado = cambiar(&esperado, "  - { from: filtrar, to: cruzar }\n", "");
    assert_eq!(nuevo, esperado);
}

#[test]
fn quitar_el_ultimo_nodo_no_deja_dos_lineas_en_blanco() {
    let nuevo = uno(
        BASE,
        Edit::RemoveNode {
            id: "escribir".into(),
        },
    )
    .unwrap();
    assert!(
        nuevo.contains("      memory_limit_mb: 256\n\nedges:"),
        "{nuevo}"
    );
    assert!(!nuevo.contains("escribir"));
}

#[test]
fn quitar_un_nodo_lo_borra_de_los_after_que_lo_nombran() {
    let con_after = uno(
        BASE,
        Edit::SetAfter {
            node: "escribir".into(),
            after: vec!["leer".into(), "filtrar".into()],
        },
    )
    .unwrap();
    let nuevo = uno(&con_after, Edit::RemoveNode { id: "leer".into() }).unwrap();
    assert!(nuevo.contains("    after: [filtrar]\n"), "{nuevo}");
}

#[test]
fn una_arista_nueva_imita_el_estilo_de_las_demas() {
    let nuevo = uno(
        BASE,
        Edit::Connect {
            from: "leer".into(),
            to: "escribir".into(),
            port: None,
        },
    )
    .unwrap();
    assert_eq!(nuevo, format!("{BASE}  - {{ from: leer, to: escribir }}\n"));

    let con_puerto = uno(
        BASE,
        Edit::Connect {
            from: "leer".into(),
            to: "cruzar".into(),
            port: Some("clientes".into()),
        },
    )
    .unwrap();
    assert_eq!(
        con_puerto,
        format!("{BASE}  - {{ from: leer, to: cruzar, port: clientes }}\n")
    );
}

#[test]
fn desconectar_quita_solo_esa_linea() {
    let nuevo = uno(
        BASE,
        Edit::Disconnect {
            from: "filtrar".into(),
            to: "cruzar".into(),
        },
    )
    .unwrap();
    assert_eq!(
        nuevo,
        cambiar(BASE, "  - { from: filtrar, to: cruzar }\n", "")
    );
}

#[test]
fn sin_aristas_queda_una_lista_vacia_y_se_puede_volver_a_conectar() {
    let mut texto = BASE.to_string();
    for (from, to) in [
        ("leer", "filtrar"),
        ("filtrar", "cruzar"),
        ("cruzar", "escribir"),
    ] {
        texto = uno(
            &texto,
            Edit::Disconnect {
                from: from.into(),
                to: to.into(),
            },
        )
        .unwrap();
    }
    assert!(texto.contains("edges: []"), "{texto}");

    let otra = uno(
        &texto,
        Edit::Connect {
            from: "leer".into(),
            to: "escribir".into(),
            port: None,
        },
    )
    .unwrap();
    assert!(
        otra.contains("edges:\n  - { from: leer, to: escribir }\n"),
        "{otra}"
    );
    assert!(!otra.contains("[]"), "{otra}");
}

#[test]
fn un_pipeline_sin_edges_recibe_el_bloque_al_final() {
    let sin = BASE.split("edges:").next().unwrap().trim_end().to_string() + "\n";
    let nuevo = uno(
        &sin,
        Edit::Connect {
            from: "leer".into(),
            to: "escribir".into(),
            port: None,
        },
    )
    .unwrap();
    assert_eq!(
        nuevo,
        format!("{sin}\nedges:\n  - {{ from: leer, to: escribir }}\n")
    );
}

#[test]
fn cambiar_un_campo_toca_solo_su_linea() {
    let nuevo = uno(
        BASE,
        Edit::SetConfig {
            node: "cruzar".into(),
            key: "memory_limit_mb".into(),
            value: Some(json!(512)),
        },
    )
    .unwrap();
    assert_eq!(
        nuevo,
        cambiar(BASE, "memory_limit_mb: 256", "memory_limit_mb: 512")
    );
}

#[test]
fn un_campo_nuevo_va_al_final_de_la_config() {
    let nuevo = uno(
        BASE,
        Edit::SetConfig {
            node: "leer".into(),
            key: "has_header".into(),
            value: Some(json!(false)),
        },
    )
    .unwrap();
    assert_eq!(
        nuevo,
        cambiar(
            BASE,
            "      path: datos.csv\n",
            "      path: datos.csv\n      has_header: false\n"
        )
    );
}

#[test]
fn quitar_un_campo_quita_su_linea_y_la_config_si_se_queda_vacia() {
    let nuevo = uno(
        BASE,
        Edit::SetConfig {
            node: "cruzar".into(),
            key: "memory_limit_mb".into(),
            value: None,
        },
    )
    .unwrap();
    assert_eq!(nuevo, cambiar(BASE, "      memory_limit_mb: 256\n", ""));

    let vacia = uno(
        BASE,
        Edit::SetConfig {
            node: "filtrar".into(),
            key: "where".into(),
            value: None,
        },
    )
    .unwrap();
    assert_eq!(
        vacia,
        cambiar(
            BASE,
            "    op: filter\n    config:\n      where: \"x > 1\"\n",
            "    op: filter\n"
        )
    );
}

#[test]
fn los_valores_delicados_vuelven_tal_cual() {
    // La comprobación interna ya lo garantiza; aquí se ve que no falla.
    for valor in [
        json!("true"),
        json!("123"),
        json!("${env:PG_DSN}"),
        json!("a: b # c"),
        json!(""),
        json!("SELECT a,\n       b\nFROM t\n"),
        json!(["id", "spend"]),
        json!({ "cliente": "name", "gasto": "spend" }),
        json!(2.5),
        json!(null),
    ] {
        uno(
            BASE,
            Edit::SetConfig {
                node: "leer".into(),
                key: "campo".into(),
                value: Some(valor.clone()),
            },
        )
        .unwrap_or_else(|err| panic!("{valor}: {err}"));
    }
}

#[test]
fn un_sql_multilinea_se_escribe_como_bloque() {
    let nuevo = uno(
        BASE,
        Edit::SetConfig {
            node: "cruzar".into(),
            key: "query".into(),
            value: Some(json!("SELECT x\nFROM filtrar\n")),
        },
    )
    .unwrap();
    assert!(
        nuevo.contains(
            "      query: |\n        SELECT x\n        FROM filtrar\n      memory_limit_mb: 256\n"
        ),
        "{nuevo}"
    );
}

#[test]
fn after_va_junto_al_componente() {
    let nuevo = uno(
        BASE,
        Edit::SetAfter {
            node: "escribir".into(),
            after: vec!["leer".into()],
        },
    )
    .unwrap();
    assert_eq!(
        nuevo,
        cambiar(
            BASE,
            "    type: sink\n    connector: csv\n",
            "    type: sink\n    connector: csv\n    after: [leer]\n"
        )
    );

    let sin = uno(
        &nuevo,
        Edit::SetAfter {
            node: "escribir".into(),
            after: vec![],
        },
    )
    .unwrap();
    assert_eq!(sin, BASE);
}

#[test]
fn renombrar_cambia_el_id_y_lo_que_lo_nombra() {
    let nuevo = uno(
        BASE,
        Edit::RenameNode {
            from: "leer".into(),
            to: "clientes".into(),
        },
    )
    .unwrap();
    let esperado = cambiar(BASE, "  - id: leer\n", "  - id: clientes\n");
    let esperado = cambiar(
        &esperado,
        "{ from: leer, to: filtrar }",
        "{ from: clientes, to: filtrar }",
    );
    assert_eq!(nuevo, esperado);
}

#[test]
fn renombrar_lo_que_entra_en_un_sql_fija_el_puerto() {
    // La query dice `FROM filtrar`: si el puerto pasara a llamarse como el
    // nodo nuevo, la tabla dejaría de existir.
    let nuevo = uno(
        BASE,
        Edit::RenameNode {
            from: "filtrar".into(),
            to: "filtrados".into(),
        },
    )
    .unwrap();
    let spec = PipelineSpec::from_yaml_str_as_written("t", &nuevo).unwrap();
    let arista = spec.edges.iter().find(|e| e.to == "cruzar").unwrap();
    assert_eq!(arista.from, "filtrados");
    assert_eq!(arista.port_name(), "filtrar");
    // Y la arista que no entra en un `sql` no se ensucia con un `port`.
    let otra = spec.edges.iter().find(|e| e.to == "filtrados").unwrap();
    assert_eq!(otra.port, None);
    assert!(nuevo.contains("SELECT * FROM filtrar"));
}

// --- Codificación ------------------------------------------------------------

#[test]
fn se_respetan_crlf_y_bom() {
    let windows = format!("\u{feff}{}", BASE.replace('\n', "\r\n"));
    let nuevo = uno(
        &windows,
        Edit::Connect {
            from: "leer".into(),
            to: "escribir".into(),
            port: None,
        },
    )
    .unwrap();
    assert!(nuevo.starts_with('\u{feff}'));
    assert_eq!(nuevo.matches('\n').count(), nuevo.matches("\r\n").count());
    assert_eq!(
        nuevo,
        format!("{windows}  - {{ from: leer, to: escribir }}\r\n")
    );
}

// --- Lo que se rechaza ---------------------------------------------------------

#[test]
fn se_rechazan_los_cambios_sin_sentido() {
    let casos = [
        Edit::RemoveNode { id: "nadie".into() },
        Edit::AddNode {
            id: "leer".into(),
            kind: Kind::Source,
            component: "csv".into(),
            config: Map::new(),
        },
        Edit::AddNode {
            id: "LeerOtro".into(),
            kind: Kind::Source,
            component: "csv".into(),
            config: Map::new(),
        },
        Edit::AddNode {
            id: "leer-otro".into(),
            kind: Kind::Source,
            component: "csv".into(),
            config: Map::new(),
        },
        Edit::Connect {
            from: "leer".into(),
            to: "filtrar".into(),
            port: None,
        },
        Edit::Disconnect {
            from: "leer".into(),
            to: "escribir".into(),
        },
        Edit::SetAfter {
            node: "leer".into(),
            after: vec!["leer".into()],
        },
        Edit::RenameNode {
            from: "leer".into(),
            to: "filtrar".into(),
        },
    ];
    for edit in casos {
        match uno(BASE, edit.clone()) {
            Err(EditError::Invalid(_)) => {}
            otro => panic!("{edit:?} debía rechazarse y dio {otro:?}"),
        }
    }
}

#[test]
fn un_cambio_que_falla_no_deja_nada_a_medias() {
    let resultado = apply(
        BASE,
        &[
            Edit::RemoveNode { id: "leer".into() },
            Edit::RemoveNode { id: "nadie".into() },
        ],
    );
    assert!(resultado.is_err());
}

#[test]
fn un_fichero_que_no_se_lee_no_se_edita() {
    let roto = "nodes: [\n";
    assert!(matches!(
        uno(roto, Edit::RemoveNode { id: "x".into() }),
        Err(EditError::Unreadable(_))
    ));
}

// --- El fichero ----------------------------------------------------------------

#[test]
fn edit_file_escribe_si_nadie_lo_cambio_por_debajo() {
    let dir = tempfile::tempdir().unwrap();
    let ruta = dir.path().join("p.yaml");
    std::fs::write(&ruta, BASE).unwrap();
    let edit = Edit::Disconnect {
        from: "cruzar".into(),
        to: "escribir".into(),
    };

    let vieja = revision("otra cosa");
    assert!(matches!(
        edit_file(&ruta, &vieja, std::slice::from_ref(&edit)),
        Err(EditError::Stale)
    ));
    assert_eq!(std::fs::read_to_string(&ruta).unwrap(), BASE);

    let nuevo = edit_file(&ruta, &revision(BASE), &[edit]).unwrap();
    assert_eq!(std::fs::read_to_string(&ruta).unwrap(), nuevo);
    // Sin restos del fichero temporal.
    let restos: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert_eq!(restos.len(), 1);
}

// --- Todos los ejemplos ----------------------------------------------------------

fn ejemplos() -> Vec<(String, String)> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/pipelines");
    let mut lista: Vec<(String, String)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "yaml"))
        .map(|p| {
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read_to_string(&p).unwrap(),
            )
        })
        .collect();
    lista.sort();
    assert!(lista.len() >= 5, "no se encontraron los ejemplos");
    lista
}

/// Líneas quitadas y añadidas entre dos textos (diff por LCS).
fn diferencias(a: &str, b: &str) -> (Vec<String>, Vec<String>) {
    let x: Vec<&str> = a.lines().collect();
    let y: Vec<&str> = b.lines().collect();
    let mut t = vec![vec![0usize; y.len() + 1]; x.len() + 1];
    for i in (0..x.len()).rev() {
        for j in (0..y.len()).rev() {
            t[i][j] = if x[i] == y[j] {
                t[i + 1][j + 1] + 1
            } else {
                t[i + 1][j].max(t[i][j + 1])
            };
        }
    }
    let (mut i, mut j) = (0, 0);
    let (mut quitadas, mut puestas) = (Vec::new(), Vec::new());
    while i < x.len() && j < y.len() {
        if x[i] == y[j] {
            i += 1;
            j += 1;
        } else if t[i + 1][j] >= t[i][j + 1] {
            quitadas.push(x[i].to_string());
            i += 1;
        } else {
            puestas.push(y[j].to_string());
            j += 1;
        }
    }
    quitadas.extend(x[i..].iter().map(|s| s.to_string()));
    puestas.extend(y[j..].iter().map(|s| s.to_string()));
    (quitadas, puestas)
}

fn spec(texto: &str) -> PipelineSpec {
    PipelineSpec::from_yaml_str_as_written("t", texto).unwrap()
}

#[test]
fn en_todos_los_ejemplos_cada_cambio_toca_solo_lo_suyo() {
    for (nombre, texto) in ejemplos() {
        let modelo = spec(&texto);
        let ctx = |que: &str| format!("{nombre}: {que}");

        // Un nodo nuevo: sólo líneas añadidas.
        let nuevo = uno(
            &texto,
            Edit::AddNode {
                id: "nodo_nuevo".into(),
                kind: Kind::Transform,
                component: "limit".into(),
                config: config(json!({ "rows": 5 })),
            },
        )
        .unwrap_or_else(|e| panic!("{}: {e}", ctx("añadir nodo")));
        let (quitadas, puestas) = diferencias(&texto, &nuevo);
        assert!(quitadas.is_empty(), "{}: {quitadas:?}", ctx("añadir nodo"));
        assert!(puestas.len() <= 6, "{}: {puestas:?}", ctx("añadir nodo"));

        for nodo in &modelo.nodes {
            let id = &nodo.id;

            // Un campo nuevo: una línea más (dos si no había `config:`).
            let con_campo = uno(
                &texto,
                Edit::SetConfig {
                    node: id.clone(),
                    key: "campo_de_prueba".into(),
                    value: Some(json!("valor")),
                },
            )
            .unwrap_or_else(|e| panic!("{}: {e}", ctx(&format!("campo nuevo en {id}"))));
            let (quitadas, puestas) = diferencias(&texto, &con_campo);
            assert!(quitadas.is_empty(), "{}: {quitadas:?}", ctx(id));
            assert!((1..=2).contains(&puestas.len()), "{}: {puestas:?}", ctx(id));

            // Cambiar un string de una línea: esa línea y ninguna más.
            if let Some((clave, valor)) = nodo.kind.config().as_object().and_then(|c| {
                c.iter()
                    .find(|(_, v)| v.as_str().is_some_and(|s| !s.contains('\n')))
            }) {
                let otro = format!("{}_2", valor.as_str().unwrap());
                let cambiado = uno(
                    &texto,
                    Edit::SetConfig {
                        node: id.clone(),
                        key: clave.clone(),
                        value: Some(json!(otro)),
                    },
                )
                .unwrap_or_else(|e| panic!("{}: {e}", ctx(&format!("{id}.{clave}"))));
                let (quitadas, puestas) = diferencias(&texto, &cambiado);
                assert_eq!(
                    (quitadas.len(), puestas.len()),
                    (1, 1),
                    "{}",
                    ctx(&format!("{id}.{clave}"))
                );
            }

            // Renombrar: sólo se sustituyen líneas que nombraban al nodo.
            let renombrado = uno(
                &texto,
                Edit::RenameNode {
                    from: id.clone(),
                    to: format!("{id}_x"),
                },
            )
            .unwrap_or_else(|e| panic!("{}: {e}", ctx(&format!("renombrar {id}"))));
            let (quitadas, puestas) = diferencias(&texto, &renombrado);
            assert_eq!(
                quitadas.len(),
                puestas.len(),
                "{}",
                ctx(&format!("renombrar {id}"))
            );
            assert!(
                quitadas.iter().all(|l| l.contains(id.as_str())),
                "{}: {quitadas:?}",
                ctx(&format!("renombrar {id}"))
            );

            // Quitarlo: la cabecera del fichero sigue entera.
            let sin = uno(&texto, Edit::RemoveNode { id: id.clone() })
                .unwrap_or_else(|e| panic!("{}: {e}", ctx(&format!("quitar {id}"))));
            let cabecera = texto.split("\nnodes:").next().unwrap();
            assert!(
                sin.starts_with(cabecera),
                "{}",
                ctx(&format!("quitar {id}"))
            );
            // Lo único que puede aparecer es la lista vacía, si se fueron
            // todas las aristas con él, o un `after` que lo nombraba.
            let (_, puestas) = diferencias(&texto, &sin);
            assert!(
                puestas
                    .iter()
                    .all(|l| l == "edges: []" || l.trim_start().starts_with("after: [")),
                "{}: {puestas:?}",
                ctx(&format!("quitar {id}"))
            );
        }

        // Cada arista se puede quitar, y sólo se va su línea.
        for arista in &modelo.edges {
            let sin = uno(
                &texto,
                Edit::Disconnect {
                    from: arista.from.clone(),
                    to: arista.to.clone(),
                },
            )
            .unwrap_or_else(|e| panic!("{}: {e}", ctx("desconectar")));
            let (quitadas, puestas) = diferencias(&texto, &sin);
            if modelo.edges.len() > 1 {
                assert_eq!(
                    (quitadas.len(), puestas.len()),
                    (1, 0),
                    "{}",
                    ctx("desconectar")
                );
            }
        }

        // Una arista nueva: una línea.
        let par = modelo
            .nodes
            .iter()
            .flat_map(|a| modelo.nodes.iter().map(move |b| (a, b)))
            .find(|(a, b)| {
                a.id != b.id
                    && a.kind.produces_output()
                    && b.kind.accepts_input()
                    && !modelo.edges.iter().any(|e| e.from == a.id && e.to == b.id)
            });
        if let Some((a, b)) = par {
            let conectado = uno(
                &texto,
                Edit::Connect {
                    from: a.id.clone(),
                    to: b.id.clone(),
                    port: None,
                },
            )
            .unwrap_or_else(|e| panic!("{}: {e}", ctx("conectar")));
            let (quitadas, puestas) = diferencias(&texto, &conectado);
            assert_eq!(
                (quitadas.len(), puestas.len()),
                (0, 1),
                "{}",
                ctx("conectar")
            );
        }
    }
}

// --- Posiciones ----------------------------------------------------------------

fn posiciones(lista: &[(&str, f64, f64)]) -> Edit {
    Edit::SetLayout {
        positions: lista
            .iter()
            .map(|(id, x, y)| (id.to_string(), [*x, *y]))
            .collect(),
    }
}

const CABECERA: &str = "# Dónde dibuja cada nodo el lienzo de Orch. El motor no lo usa; si se borra,\n# el lienzo vuelve a colocarlos solo.\n";

#[test]
fn las_posiciones_van_en_un_bloque_al_final_en_el_orden_de_los_nodos() {
    let nuevo = uno(
        BASE,
        posiciones(&[
            ("escribir", 732.0, 40.0),
            ("leer", 0.0, 40.4),
            ("filtrar", 244.6, 40.0),
        ]),
    )
    .unwrap();
    assert_eq!(
        nuevo,
        format!(
            "{BASE}\n{CABECERA}layout:\n  leer: [0, 40]\n  filtrar: [245, 40]\n  escribir: [732, 40]\n"
        )
    );
    let spec = spec(&nuevo);
    assert_eq!(spec.layout["filtrar"], [245.0, 40.0]);
}

#[test]
fn mover_un_nodo_cambia_solo_su_linea() {
    let colocado = uno(
        BASE,
        posiciones(&[("leer", 0.0, 0.0), ("filtrar", 200.0, 0.0)]),
    )
    .unwrap();
    let movido = uno(&colocado, posiciones(&[("filtrar", 210.0, 30.0)])).unwrap();
    assert_eq!(
        movido,
        cambiar(&colocado, "  filtrar: [200, 0]\n", "  filtrar: [210, 30]\n")
    );
}

#[test]
fn renombrar_y_quitar_arrastran_su_posicion() {
    let colocado = uno(
        BASE,
        posiciones(&[("leer", 0.0, 0.0), ("filtrar", 200.0, 0.0)]),
    )
    .unwrap();

    let renombrado = uno(
        &colocado,
        Edit::RenameNode {
            from: "filtrar".into(),
            to: "filtrados".into(),
        },
    )
    .unwrap();
    assert!(
        renombrado.contains("  filtrados: [200, 0]\n"),
        "{renombrado}"
    );
    assert!(!renombrado.contains("  filtrar: ["), "{renombrado}");

    let sin = uno(
        &colocado,
        Edit::RemoveNode {
            id: "filtrar".into(),
        },
    )
    .unwrap();
    assert!(sin.ends_with("layout:\n  leer: [0, 0]\n"), "{sin}");
}

#[test]
fn quitar_la_ultima_posicion_o_borrarlas_deja_el_fichero_como_estaba() {
    let colocado = uno(
        BASE,
        posiciones(&[("leer", 0.0, 0.0), ("escribir", 9.0, 9.0)]),
    )
    .unwrap();
    assert_eq!(uno(&colocado, Edit::ClearLayout).unwrap(), BASE);

    // Quitar los dos nodos con posición se lleva también el bloque.
    let solo_leer = uno(BASE, posiciones(&[("leer", 0.0, 0.0)])).unwrap();
    let sin = uno(&solo_leer, Edit::RemoveNode { id: "leer".into() }).unwrap();
    assert_eq!(
        sin,
        uno(BASE, Edit::RemoveNode { id: "leer".into() }).unwrap()
    );
}

#[test]
fn una_posicion_mal_escrita_no_impide_leer_el_pipeline() {
    let roto = format!("{BASE}layout:\n  leer: [0, 0]\n  filtrar: arriba\n  cruzar: [1]\n");
    let spec = spec(&roto);
    assert_eq!(spec.layout.len(), 1);
    assert_eq!(spec.layout["leer"], [0.0, 0.0]);
    // Y se puede seguir editando encima.
    let movido = uno(&roto, posiciones(&[("leer", 5.0, 5.0)])).unwrap();
    assert!(
        movido.contains("  leer: [5, 5]\n  filtrar: arriba\n"),
        "{movido}"
    );
}

#[test]
fn no_se_coloca_un_nodo_que_no_existe() {
    assert!(matches!(
        uno(BASE, posiciones(&[("nadie", 0.0, 0.0)])),
        Err(EditError::Invalid(_))
    ));
}

// --- Sustituir el texto entero (recuperar una versión) ------------------------

#[test]
fn recuperar_una_version_no_pisa_un_fichero_que_cambio() {
    let dir = tempfile::TempDir::new().unwrap();
    let ruta = dir.path().join("p.yaml");
    std::fs::write(&ruta, "name: uno\n").unwrap();
    let vista = revision("name: uno\n");

    // Alguien lo cambia mientras se miraba el historial.
    std::fs::write(&ruta, "name: dos\n").unwrap();
    let error = orch_edit::replace_file(&ruta, &vista, "name: vieja\n");
    assert!(matches!(error, Err(EditError::Stale)));
    assert_eq!(std::fs::read_to_string(&ruta).unwrap(), "name: dos\n");

    // Con la revisión buena, el texto viejo vuelve entero, sin temporales.
    orch_edit::replace_file(&ruta, &revision("name: dos\n"), "name: vieja\n").unwrap();
    assert_eq!(std::fs::read_to_string(&ruta).unwrap(), "name: vieja\n");
    let restos: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert_eq!(restos.len(), 1);
}
