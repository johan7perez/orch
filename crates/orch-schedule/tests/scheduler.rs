//! Reglas del planificador, probadas con fechas concretas.

use chrono::{DateTime, TimeZone, Utc};
use orch_core::PipelineSpec;
use orch_schedule::{discover, Entry, Reason, Scheduler};
use tempfile::TempDir;

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .expect("fecha válida")
        .with_timezone(&Utc)
}

fn entry(name: &str, schedule: &str) -> Entry {
    let yaml = format!(
        r#"
name: {name}
{schedule}
nodes:
  - {{ id: g, type: source, connector: generator, config: {{ rows: 1 }} }}
  - {{ id: d, type: sink, connector: "null" }}
edges:
  - {{ from: g, to: d }}
"#
    );
    let spec = PipelineSpec::from_yaml_str("test.yaml", &yaml).expect("YAML válido");
    Entry::from_spec(&spec, format!("{name}.yaml")).expect("entrada válida")
}

fn names(launches: &[orch_schedule::Launch]) -> Vec<&str> {
    launches.iter().map(|l| l.name.as_str()).collect()
}

// --- cron -------------------------------------------------------------------

#[test]
fn dispara_cuando_toca_y_no_antes() {
    let diario = entry("diario", "schedule: { cron: \"0 2 * * *\" }");
    let mut scheduler = Scheduler::new(vec![diario], at("2026-03-15T01:00:00Z"));

    assert!(scheduler.due(at("2026-03-15T01:59:00Z")).is_empty());
    assert_eq!(
        names(&scheduler.due(at("2026-03-15T02:00:00Z"))),
        vec!["diario"]
    );

    // Y no se repite dentro del mismo minuto.
    scheduler.finished("diario", true);
    assert!(scheduler.due(at("2026-03-15T02:00:30Z")).is_empty());
}

#[test]
fn el_dia_de_la_semana_usa_la_numeracion_de_unix() {
    // `1-5` tiene que ser lunes a viernes, no domingo a jueves.
    let laborables = entry("laborables", "schedule: { cron: \"0 9 * * 1-5\" }");
    let mut scheduler = Scheduler::new(vec![laborables], at("2026-03-15T00:00:00Z"));

    // 2026-03-15 es domingo: no debe disparar.
    assert!(scheduler.due(at("2026-03-15T09:00:00Z")).is_empty());
    // 2026-03-16 es lunes: sí.
    assert_eq!(scheduler.due(at("2026-03-16T09:00:00Z")).len(), 1);
}

#[test]
fn el_cron_se_interpreta_en_su_zona_horaria() {
    let nocturno = entry(
        "nocturno",
        "schedule: { cron: \"0 2 * * *\", timezone: America/Santo_Domingo }",
    );
    let mut scheduler = Scheduler::new(vec![nocturno], at("2026-03-15T00:00:00Z"));

    // Santo Domingo es UTC-4 todo el año: las 2:00 locales son las 6:00 UTC.
    assert!(scheduler.due(at("2026-03-15T02:00:00Z")).is_empty());
    assert_eq!(scheduler.due(at("2026-03-15T06:00:00Z")).len(), 1);
}

#[test]
fn una_parada_larga_no_provoca_una_avalancha() {
    // El proceso estuvo caído tres horas con un cron cada minuto: al volver
    // debe disparar una vez, no ciento ochenta.
    let frecuente = entry("frecuente", "schedule: { cron: \"* * * * *\" }");
    let mut scheduler = Scheduler::new(vec![frecuente], at("2026-03-15T00:00:00Z"));

    let launches = scheduler.due(at("2026-03-15T03:00:00Z"));
    assert_eq!(launches.len(), 1);
    assert_eq!(launches[0].reason, Reason::Cron);
}

#[test]
fn una_zona_horaria_inventada_se_rechaza() {
    let yaml = r#"
name: p
schedule: { cron: "0 2 * * *", timezone: "Marte/Olympus" }
nodes:
  - { id: g, type: source, connector: generator, config: { rows: 1 } }
  - { id: d, type: sink, connector: "null" }
edges:
  - { from: g, to: d }
"#;
    let spec = PipelineSpec::from_yaml_str("test.yaml", yaml).expect("YAML válido");
    let err = Entry::from_spec(&spec, "p.yaml").expect_err("zona inexistente");
    assert!(err.to_string().contains("Marte/Olympus"), "{err}");
}

// --- encadenamiento ---------------------------------------------------------

#[test]
fn el_exito_de_uno_dispara_al_siguiente() {
    let primero = entry("primero", "schedule: { cron: \"0 2 * * *\" }");
    let segundo = entry("segundo", "schedule: { after: [primero] }");
    let mut scheduler = Scheduler::new(vec![primero, segundo], at("2026-03-15T01:00:00Z"));

    assert_eq!(
        names(&scheduler.due(at("2026-03-15T02:00:00Z"))),
        vec!["primero"]
    );
    let encadenados = scheduler.finished("primero", true);
    assert_eq!(names(&encadenados), vec!["segundo"]);
    assert_eq!(encadenados[0].reason, Reason::Upstream);
}

#[test]
fn un_fallo_no_encadena() {
    // Encadenar tras un fallo propagaría datos a medias.
    let primero = entry("primero", "schedule: { cron: \"0 2 * * *\" }");
    let segundo = entry("segundo", "schedule: { after: [primero] }");
    let mut scheduler = Scheduler::new(vec![primero, segundo], at("2026-03-15T01:00:00Z"));

    scheduler.due(at("2026-03-15T02:00:00Z"));
    assert!(scheduler.finished("primero", false).is_empty());
}

#[test]
fn los_encadenamientos_se_propagan_en_cascada() {
    let a = entry("a", "schedule: { cron: \"0 2 * * *\" }");
    let b = entry("b", "schedule: { after: [a] }");
    let c = entry("c", "schedule: { after: [b] }");
    let mut scheduler = Scheduler::new(vec![a, b, c], at("2026-03-15T01:00:00Z"));

    scheduler.due(at("2026-03-15T02:00:00Z"));
    assert_eq!(names(&scheduler.finished("a", true)), vec!["b"]);
    assert_eq!(names(&scheduler.finished("b", true)), vec!["c"]);
    assert!(scheduler.finished("c", true).is_empty());
}

// --- concurrencia -----------------------------------------------------------

#[test]
fn por_defecto_se_salta_el_disparo_si_sigue_corriendo() {
    let lento = entry("lento", "schedule: { cron: \"* * * * *\" }");
    let mut scheduler = Scheduler::new(vec![lento], at("2026-03-15T00:00:00Z"));

    assert_eq!(scheduler.due(at("2026-03-15T00:01:00Z")).len(), 1);
    // Sigue corriendo: el siguiente minuto no arranca nada.
    assert!(scheduler.due(at("2026-03-15T00:02:00Z")).is_empty());
    assert_eq!(scheduler.skipped(), 1);

    // Al terminar, el siguiente turno sí.
    scheduler.finished("lento", true);
    assert_eq!(scheduler.due(at("2026-03-15T00:03:00Z")).len(), 1);
}

#[test]
fn con_queue_el_disparo_perdido_espera_turno() {
    let lento = entry(
        "lento",
        "schedule: { cron: \"* * * * *\", concurrency: queue }",
    );
    let mut scheduler = Scheduler::new(vec![lento], at("2026-03-15T00:00:00Z"));

    assert_eq!(scheduler.due(at("2026-03-15T00:01:00Z")).len(), 1);
    // Se encola, no arranca.
    assert!(scheduler.due(at("2026-03-15T00:02:00Z")).is_empty());

    // Al liberarse, sale el que esperaba.
    scheduler.finished("lento", true);
    let launches = scheduler.due(at("2026-03-15T00:02:30Z"));
    assert_eq!(launches.len(), 1);
    assert_eq!(launches[0].reason, Reason::Queued);
}

#[test]
fn la_cola_guarda_uno_como_mucho() {
    // Un atasco de una hora no puede convertirse en sesenta ejecuciones
    // seguidas en cuanto se libere.
    let lento = entry(
        "lento",
        "schedule: { cron: \"* * * * *\", concurrency: queue }",
    );
    let mut scheduler = Scheduler::new(vec![lento], at("2026-03-15T00:00:00Z"));

    scheduler.due(at("2026-03-15T00:01:00Z"));
    for minute in 2..10 {
        scheduler.due(at(&format!("2026-03-15T00:0{minute}:00Z")));
    }
    scheduler.finished("lento", true);

    let launches = scheduler.due(at("2026-03-15T00:10:00Z"));
    assert_eq!(launches.len(), 1, "sólo debía quedar uno esperando");
}

#[test]
fn con_allow_arrancan_a_la_vez() {
    let paralelo = entry(
        "paralelo",
        "schedule: { cron: \"* * * * *\", concurrency: allow }",
    );
    let mut scheduler = Scheduler::new(vec![paralelo], at("2026-03-15T00:00:00Z"));

    assert_eq!(scheduler.due(at("2026-03-15T00:01:00Z")).len(), 1);
    assert_eq!(scheduler.due(at("2026-03-15T00:02:00Z")).len(), 1);
    assert_eq!(scheduler.skipped(), 0);
}

#[test]
fn un_pipeline_desactivado_no_arranca_nunca() {
    let apagado = entry(
        "apagado",
        "schedule: { cron: \"* * * * *\", enabled: false }",
    );
    let disparador = entry("disparador", "schedule: { cron: \"0 2 * * *\" }");
    let encadenado = entry(
        "encadenado",
        "schedule: { after: [disparador], enabled: false }",
    );
    let mut scheduler = Scheduler::new(
        vec![apagado, disparador, encadenado],
        at("2026-03-15T01:00:00Z"),
    );

    let launches = scheduler.due(at("2026-03-15T02:00:00Z"));
    assert_eq!(names(&launches), vec!["disparador"]);
    assert!(scheduler.finished("disparador", true).is_empty());
}

#[test]
fn sin_schedule_el_pipeline_solo_corre_a_mano() {
    let manual = entry("manual", "");
    assert!(!manual.is_triggered());
    assert_eq!(manual.describe_trigger(), "a mano");

    let mut scheduler = Scheduler::new(vec![manual], at("2026-03-15T00:00:00Z"));
    assert!(scheduler.due(at("2026-03-16T00:00:00Z")).is_empty());
    assert!(scheduler.next_wakeup().is_none());
}

#[test]
fn el_proximo_despertar_es_el_disparo_mas_cercano() {
    let pronto = entry("pronto", "schedule: { cron: \"0 1 * * *\" }");
    let tarde = entry("tarde", "schedule: { cron: \"0 5 * * *\" }");
    let scheduler = Scheduler::new(vec![tarde, pronto], at("2026-03-15T00:00:00Z"));

    assert_eq!(
        scheduler.next_wakeup(),
        Some(Utc.with_ymd_and_hms(2026, 3, 15, 1, 0, 0).unwrap())
    );
}

// --- descubrimiento ---------------------------------------------------------

fn write(dir: &TempDir, name: &str, body: &str) {
    std::fs::write(dir.path().join(name), body).expect("escribir");
}

fn pipeline_yaml(name: &str, schedule: &str) -> String {
    format!(
        r#"
name: {name}
{schedule}
nodes:
  - {{ id: g, type: source, connector: generator, config: {{ rows: 1 }} }}
  - {{ id: d, type: sink, connector: "null" }}
edges:
  - {{ from: g, to: d }}
"#
    )
}

#[test]
fn descubre_los_pipelines_del_directorio() {
    let dir = TempDir::new().expect("tempdir");
    write(&dir, "b.yaml", &pipeline_yaml("beta", ""));
    write(
        &dir,
        "a.yml",
        &pipeline_yaml("alfa", "schedule: { cron: \"0 2 * * *\" }"),
    );
    write(&dir, "notas.txt", "esto no es un pipeline");

    let found = discover(dir.path()).expect("descubrir");
    let names: Vec<&str> = found.entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, vec!["alfa", "beta"], "en orden de fichero");
    assert!(found.entries[0].is_triggered());
    assert!(!found.entries[1].is_triggered());
    assert!(found.broken.is_empty());
    assert!(found.problems.is_empty());
}

#[test]
fn un_fichero_roto_no_esconde_a_los_demas() {
    // Bastaba un pipeline con un secreto sin definir para que el escaneo
    // entero fallara y la ventana apareciera vacía, como si no hubiera nada.
    let dir = TempDir::new().expect("tempdir");
    write(&dir, "a.yaml", &pipeline_yaml("sano", ""));
    write(&dir, "b.yaml", "esto: no es un pipeline\n");
    write(
        &dir,
        "c.yaml",
        &pipeline_yaml("otro-sano", "schedule: { cron: \"0 2 * * *\" }"),
    );

    let found = discover(dir.path()).expect("descubrir");
    let names: Vec<&str> = found.entries.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, vec!["sano", "otro-sano"]);
    assert_eq!(found.broken.len(), 1);
    assert!(found.broken[0].path.ends_with("b.yaml"));
    assert!(!found.broken[0].error.is_empty());
}

#[test]
fn un_cron_invalido_deja_el_fichero_marcado_pero_no_rompe_el_resto() {
    let dir = TempDir::new().expect("tempdir");
    write(&dir, "a.yaml", &pipeline_yaml("sano", ""));
    write(
        &dir,
        "b.yaml",
        &pipeline_yaml("malo", "schedule: { cron: \"esto no es cron\" }"),
    );

    let found = discover(dir.path()).expect("descubrir");
    assert_eq!(found.entries.len(), 1);
    assert_eq!(found.entries[0].name, "sano");
    assert_eq!(found.broken.len(), 1);
}

#[test]
fn con_nombres_repetidos_se_queda_el_primero_y_se_avisa() {
    let dir = TempDir::new().expect("tempdir");
    write(&dir, "uno.yaml", &pipeline_yaml("repetido", ""));
    write(&dir, "dos.yaml", &pipeline_yaml("repetido", ""));

    let found = discover(dir.path()).expect("descubrir");
    assert_eq!(found.entries.len(), 1, "no puede haber dos con el mismo id");
    assert_eq!(found.problems.len(), 1);
    assert!(
        found.problems[0].contains("repetido"),
        "{:?}",
        found.problems
    );
}

#[test]
fn un_after_a_un_pipeline_inexistente_se_avisa() {
    // Callarlo dejaría un pipeline que no arranca nunca sin que nadie sepa
    // por qué; abortar el escaneo escondería los que sí funcionan.
    let dir = TempDir::new().expect("tempdir");
    write(&dir, "a.yaml", &pipeline_yaml("existe", ""));
    write(
        &dir,
        "b.yaml",
        &pipeline_yaml("huerfano", "schedule: { after: [fantasma] }"),
    );

    let found = discover(dir.path()).expect("descubrir");
    assert_eq!(found.entries.len(), 2);
    assert_eq!(found.problems.len(), 1);
    assert!(
        found.problems[0].contains("fantasma"),
        "{:?}",
        found.problems
    );
}
