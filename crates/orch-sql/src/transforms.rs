//! Transformaciones SQL.
//!
//! `filter`, `derive` y `aggregate` no son motores aparte: generan la query
//! equivalente y la ejecutan por el mismo camino que `sql`. Un solo camino de
//! ejecución que mantener, y las tres heredan gratis el optimizador de
//! DataFusion.
//!
//! Los dos campos comunes (`table` y `memory_limit_mb`) se repiten en cada
//! config en vez de compartirse con `#[serde(flatten)]`: serde no admite
//! `flatten` junto a `deny_unknown_fields`, y detectar un campo mal escrito en
//! el YAML durante `validate` vale más que ahorrar cuatro líneas.

use std::collections::BTreeMap;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use orch_core::{
    parse_config, Input, InputSchemas, NodeContext, OrchError, Output, Result, Transform,
};
use serde::Deserialize;

use crate::engine::{self, Plan};

fn default_table() -> String {
    "input".to_string()
}

/// Escapa un identificador para incrustarlo en SQL.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Transformación ya reducida a una query.
pub struct SqlTransform {
    op: &'static str,
    /// `true` para las operaciones que se escriben sobre una sola tabla.
    /// `sql` es la excepción: admite varias entradas y por eso puede unir.
    single_input: bool,
    plan: Plan,
}

impl SqlTransform {
    fn new(
        op: &'static str,
        single_input: bool,
        node: &str,
        table: String,
        memory_limit_mb: Option<usize>,
        query: String,
    ) -> Result<Self> {
        // Detecta un `SELCT` en `validate`, sin esperar a la ejecución.
        engine::check_syntax(node, &query)?;
        Ok(Self {
            op,
            single_input,
            plan: Plan::new(node, table, query, memory_limit_mb)?,
        })
    }

    /// La query final. Es lo que hace verificable la generación de SQL.
    pub fn query(&self) -> &str {
        &self.plan.query
    }
}

#[async_trait]
impl Transform for SqlTransform {
    fn op(&self) -> &str {
        self.op
    }

    async fn plan(&self, inputs: &InputSchemas) -> Result<Option<SchemaRef>> {
        if self.single_input {
            inputs.require_single(&self.plan.node, self.op)?;
        } else if inputs.is_empty() {
            return Err(OrchError::Validation(format!(
                "`{}`: `sql` necesita al menos una entrada",
                self.plan.node
            )));
        }
        self.plan.plan_schema(inputs).await
    }

    async fn apply(&self, ctx: &NodeContext, input: &mut Input, output: &Output) -> Result<()> {
        tracing::debug!(query = %self.plan.query, "ejecutando query");
        engine::execute(&self.plan, ctx, input, output).await
    }
}

// --- sql --------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlConfig {
    pub query: String,
    /// Nombre con el que la entrada queda registrada como tabla. En SQL los
    /// identificadores sin comillas se normalizan a minúsculas, así que
    /// conviene usar un nombre en minúsculas.
    #[serde(default = "default_table")]
    pub table: String,
    /// Tope de memoria del plan. Las operaciones que rompen el streaming
    /// (`GROUP BY`, `ORDER BY`, joins) acumulan datos; sin tope, un agregado
    /// sobre un dataset enorme puede agotar la RAM.
    #[serde(default)]
    pub memory_limit_mb: Option<usize>,
}

pub fn build_sql(node: &str, config: &serde_json::Value) -> Result<SqlTransform> {
    let config: SqlConfig = parse_config(node, config)?;
    if config.query.trim().is_empty() {
        return Err(OrchError::config(node, "`query` no puede estar vacía"));
    }
    SqlTransform::new(
        "sql",
        false,
        node,
        config.table,
        config.memory_limit_mb,
        config.query,
    )
}

// --- filter -----------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilterConfig {
    /// Predicado SQL, sin la palabra `WHERE`.
    #[serde(rename = "where")]
    pub predicate: String,
    #[serde(default = "default_table")]
    pub table: String,
    #[serde(default)]
    pub memory_limit_mb: Option<usize>,
}

pub fn build_filter(node: &str, config: &serde_json::Value) -> Result<SqlTransform> {
    let config: FilterConfig = parse_config(node, config)?;
    if config.predicate.trim().is_empty() {
        return Err(OrchError::config(node, "`where` no puede estar vacío"));
    }
    let query = format!(
        "SELECT * FROM {} WHERE ({})",
        quote_ident(&config.table),
        config.predicate
    );
    SqlTransform::new(
        "filter",
        true,
        node,
        config.table,
        config.memory_limit_mb,
        query,
    )
}

// --- derive -----------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeriveConfig {
    /// Mapa `nombre_nuevo: expresión SQL`. Se añaden a las columnas existentes.
    pub columns: BTreeMap<String, String>,
    #[serde(default = "default_table")]
    pub table: String,
    #[serde(default)]
    pub memory_limit_mb: Option<usize>,
}

pub fn build_derive(node: &str, config: &serde_json::Value) -> Result<SqlTransform> {
    let config: DeriveConfig = parse_config(node, config)?;
    if config.columns.is_empty() {
        return Err(OrchError::config(node, "`columns` no puede estar vacío"));
    }
    let derived = config
        .columns
        .iter()
        .map(|(alias, expr)| format!("({}) AS {}", expr, quote_ident(alias)))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!("SELECT *, {derived} FROM {}", quote_ident(&config.table));
    SqlTransform::new(
        "derive",
        true,
        node,
        config.table,
        config.memory_limit_mb,
        query,
    )
}

// --- aggregate --------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateConfig {
    /// Columnas por las que agrupar. Vacío = una sola fila de totales.
    #[serde(default)]
    pub group_by: Vec<String>,
    /// Mapa `alias: expresión de agregación` (`sum(spend)`, `count(*)`, …).
    pub aggregates: BTreeMap<String, String>,
    #[serde(default = "default_table")]
    pub table: String,
    #[serde(default)]
    pub memory_limit_mb: Option<usize>,
}

pub fn build_aggregate(node: &str, config: &serde_json::Value) -> Result<SqlTransform> {
    let config: AggregateConfig = parse_config(node, config)?;
    if config.aggregates.is_empty() {
        return Err(OrchError::config(node, "`aggregates` no puede estar vacío"));
    }

    let mut projection: Vec<String> = config.group_by.iter().map(|c| quote_ident(c)).collect();
    projection.extend(
        config
            .aggregates
            .iter()
            .map(|(alias, expr)| format!("({}) AS {}", expr, quote_ident(alias))),
    );

    let mut query = format!(
        "SELECT {} FROM {}",
        projection.join(", "),
        quote_ident(&config.table)
    );
    if !config.group_by.is_empty() {
        let groups: Vec<String> = config.group_by.iter().map(|c| quote_ident(c)).collect();
        query.push_str(" GROUP BY ");
        query.push_str(&groups.join(", "));
    }

    SqlTransform::new(
        "aggregate",
        true,
        node,
        config.table,
        config.memory_limit_mb,
        query,
    )
}
