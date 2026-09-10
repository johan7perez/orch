//! Expresiones cron.
//!
//! El crate `cron` espera seis campos (con segundos al principio) y numera
//! los días de la semana del 1 al 7 empezando en domingo. Todo el mundo
//! escribe cron de cinco campos con los días del 0 al 6, también empezando
//! en domingo. Aquí se traduce de lo segundo a lo primero, porque un
//! `0 2 * * 1-5` que en vez de lunes a viernes dispare de domingo a jueves
//! es exactamente el tipo de fallo que nadie nota hasta que es tarde.

use std::str::FromStr;

use orch_core::{OrchError, Result};

/// Convierte una expresión de usuario en la que espera el crate `cron`.
pub fn parse(pipeline: &str, expression: &str) -> Result<cron::Schedule> {
    let normalized = normalize(pipeline, expression)?;
    cron::Schedule::from_str(&normalized).map_err(|e| {
        OrchError::Validation(format!(
            "`{pipeline}`: la expresión cron `{expression}` no es válida: {e}"
        ))
    })
}

fn normalize(pipeline: &str, expression: &str) -> Result<String> {
    let fields: Vec<&str> = expression.split_whitespace().collect();
    match fields.len() {
        // Cinco campos: el cron de toda la vida. Se le añaden los segundos
        // y se traduce el día de la semana.
        5 => Ok(format!(
            "0 {} {} {} {} {}",
            fields[0],
            fields[1],
            fields[2],
            fields[3],
            translate_weekday(pipeline, fields[4])?
        )),
        // Seis o siete: quien los escribe ya conoce el formato del crate y
        // no se le toca nada.
        6 | 7 => Ok(expression.to_string()),
        other => Err(OrchError::Validation(format!(
            "`{pipeline}`: la expresión cron `{expression}` tiene {other} campos; \
             se esperan 5 (minuto hora día mes día-semana) o 6 con segundos"
        ))),
    }
}

/// Pasa un día de la semana de la numeración de Unix (0 = domingo) a la del
/// crate (1 = domingo).
fn translate_weekday(pipeline: &str, field: &str) -> Result<String> {
    field
        .split(',')
        .map(|item| translate_item(pipeline, item))
        .collect::<Result<Vec<_>>>()
        .map(|items| items.join(","))
}

fn translate_item(pipeline: &str, item: &str) -> Result<String> {
    // `a-b/n` o `*/n`: el paso no se toca, sólo el rango.
    if let Some((range, step)) = item.split_once('/') {
        return Ok(format!("{}/{step}", translate_range(pipeline, range)?));
    }
    translate_range(pipeline, item)
}

fn translate_range(pipeline: &str, range: &str) -> Result<String> {
    if range == "*" {
        return Ok("*".to_string());
    }
    if let Some((from, to)) = range.split_once('-') {
        return Ok(format!(
            "{}-{}",
            shift(pipeline, from)?,
            shift(pipeline, to)?
        ));
    }
    shift(pipeline, range)
}

fn shift(pipeline: &str, value: &str) -> Result<String> {
    let value = value.trim();
    // Los nombres (SUN, MON…) significan lo mismo en las dos numeraciones.
    let Ok(day) = value.parse::<u32>() else {
        return Ok(value.to_string());
    };
    match day {
        // Unix admite tanto 0 como 7 para domingo.
        0 | 7 => Ok("1".to_string()),
        1..=6 => Ok((day + 1).to_string()),
        other => Err(OrchError::Validation(format!(
            "`{pipeline}`: `{other}` no es un día de la semana; se esperan 0-7 \
             (0 y 7 son domingo) o nombres como MON"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(expression: &str) -> String {
        normalize("p", expression).expect("expresión válida")
    }

    #[test]
    fn cinco_campos_se_completan_con_segundos() {
        assert_eq!(norm("30 2 * * *"), "0 30 2 * * *");
    }

    #[test]
    fn el_dia_de_la_semana_se_desplaza() {
        // Lunes a viernes en Unix es 1-5; en el crate, 2-6.
        assert_eq!(norm("0 2 * * 1-5"), "0 0 2 * * 2-6");
        // Domingo es 0 o 7 en Unix; 1 en el crate.
        assert_eq!(norm("0 2 * * 0"), "0 0 2 * * 1");
        assert_eq!(norm("0 2 * * 7"), "0 0 2 * * 1");
        // Sábado y domingo.
        assert_eq!(norm("0 2 * * 6,0"), "0 0 2 * * 7,1");
    }

    #[test]
    fn los_nombres_pasan_tal_cual() {
        assert_eq!(norm("0 2 * * MON-FRI"), "0 0 2 * * MON-FRI");
    }

    #[test]
    fn los_pasos_se_conservan() {
        assert_eq!(norm("0 2 * * */2"), "0 0 2 * * */2");
    }

    #[test]
    fn seis_campos_no_se_tocan() {
        assert_eq!(norm("15 30 2 * * 2"), "15 30 2 * * 2");
    }

    #[test]
    fn un_numero_de_campos_raro_se_rechaza() {
        let err = normalize("p", "0 2 *").expect_err("faltan campos");
        assert!(err.to_string().contains("3 campos"), "{err}");
    }

    #[test]
    fn un_dia_imposible_se_rechaza() {
        let err = normalize("p", "0 2 * * 9").expect_err("no existe el día 9");
        assert!(err.to_string().contains('9'), "{err}");
    }
}
