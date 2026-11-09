//! El bloque `alerts` de un pipeline: cómo se escribe y qué se rechaza.

use std::time::Duration;

use orch_core::{Dag, PipelineSpec, Plazo};

fn con_alertas(bloque: &str) -> Result<PipelineSpec, String> {
    PipelineSpec::from_yaml_str(
        "t.yaml",
        &format!("name: p\nnodes: [{{ id: a, type: source, connector: x }}]\nalerts:\n{bloque}"),
    )
    .map_err(|e| e.to_string())
}

#[test]
fn un_plazo_se_escribe_como_se_dice() {
    for (texto, segundos) in [("90s", 90), ("10m", 600), ("1h", 3_600), ("1h30m", 5_400)] {
        let plazo: Plazo = texto.parse().unwrap_or_else(|e| panic!("{texto}: {e}"));
        assert_eq!(plazo.0, Duration::from_secs(segundos));
        // Al escribirlo se normaliza (`90s` sale `1m30s`), pero vuelve igual.
        assert_eq!(plazo.to_string().parse::<Plazo>(), Ok(plazo));
    }
    assert_eq!(Plazo(Duration::from_secs(5_400)).to_string(), "1h30m");
    assert_eq!(Plazo(Duration::from_secs(90)).to_string(), "1m30s");
}

#[test]
fn un_plazo_ambiguo_o_vacio_se_rechaza() {
    // `10` a secas: ¿segundos, minutos? Mejor preguntarlo que adivinarlo.
    for texto in ["10", "diez", "0s", "5x", ""] {
        let error = texto.parse::<Plazo>().unwrap_err();
        assert!(error.contains("no es un plazo"), "{texto}: {error}");
    }
}

#[test]
fn el_bloque_completo_se_lee() {
    let spec = con_alertas(
        "  stalled_after: 10m\n  slower_than: 2\n  webhook: https://ejemplo.test/hook\n",
    )
    .unwrap();
    let alertas = spec.alerts.unwrap();
    // Sin decirlo, se avisa de los fallos: es el caso que nadie quiere perderse.
    assert!(alertas.on_failure);
    assert_eq!(alertas.stalled_after, Some(Plazo(Duration::from_secs(600))));
    assert_eq!(alertas.slower_than, Some(2.0));
}

#[test]
fn una_clave_mal_escrita_no_pasa_en_silencio() {
    let error = con_alertas("  stalled: 10m\n").unwrap_err();
    assert!(error.contains("stalled"), "{error}");
}

#[test]
fn un_plazo_mal_escrito_se_dice_al_cargar() {
    let error = con_alertas("  stalled_after: 10\n").unwrap_err();
    assert!(error.contains("no es un plazo"), "{error}");
}

#[test]
fn slower_than_tiene_que_ser_mas_que_una_vez_lo_normal() {
    let spec = con_alertas("  slower_than: 1\n").unwrap();
    let error = Dag::build(spec).unwrap_err().to_string();
    assert!(error.contains("slower_than"), "{error}");
}

#[test]
fn el_webhook_admite_secretos_del_entorno() {
    // Un nombre propio de este test: el entorno lo comparten todos.
    std::env::set_var(
        "ORCH_TEST_ALERTAS_WEBHOOK",
        "https://hooks.ejemplo.test/T0/abc",
    );
    let spec = con_alertas("  webhook: \"${env:ORCH_TEST_ALERTAS_WEBHOOK}\"\n").unwrap();
    assert_eq!(
        spec.alerts.unwrap().webhook.as_deref(),
        Some("https://hooks.ejemplo.test/T0/abc")
    );
}
