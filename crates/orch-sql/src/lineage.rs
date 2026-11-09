//! Lineage de una query: de qué columnas de entrada sale cada columna del
//! resultado.
//!
//! Se recorre el plan lógico **sin optimizar** que devuelve el planificador
//! de SQL. Es el más fiel a lo que se escribió —el optimizador funde
//! proyecciones, empuja filtros y reescribe expresiones— y en DataFusion 55
//! ya trae el `*` expandido, así que cada columna de cada nodo del plan tiene
//! su expresión.
//!
//! El recorrido va de las hojas (las tablas de entrada) hacia arriba: cada
//! nodo del plan dice, para cada columna que produce, de qué columnas de
//! tabla sale, apoyándose en lo que dijeron sus entradas. Lo que no se sabe
//! seguir (un `UNNEST`, una consulta recursiva) queda como incierto en vez de
//! inventarse.

use std::collections::BTreeSet;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema};
use datafusion::logical_expr::{Distinct, Expr, JoinType, LogicalPlan};
use orch_core::Derivation;

/// Una columna de una tabla de entrada: `(tabla, columna)`.
pub(crate) type ColumnaDeTabla = (String, String);

/// De dónde sale una columna de un nodo del plan.
#[derive(Debug, Clone)]
pub(crate) struct Rastro {
    pub columnas: BTreeSet<ColumnaDeTabla>,
    pub derivacion: Derivation,
}

impl Rastro {
    fn de_tabla(tabla: &str, columna: &str) -> Self {
        Self {
            columnas: BTreeSet::from([(tabla.to_string(), columna.to_string())]),
            derivacion: Derivation::Copy,
        }
    }

    /// Un valor que no lee ninguna columna: un literal, `VALUES`.
    fn constante() -> Self {
        Self {
            columnas: BTreeSet::new(),
            derivacion: Derivation::Computed,
        }
    }

    fn incierto() -> Self {
        Self {
            columnas: BTreeSet::new(),
            derivacion: Derivation::Unknown,
        }
    }

    fn sumar(&mut self, otro: &Rastro) {
        self.columnas.extend(otro.columnas.iter().cloned());
        self.derivacion = self.derivacion.then(otro.derivacion);
    }
}

/// Lo que se saca de recorrer una query entera.
#[derive(Debug, Default)]
pub(crate) struct LineageDeQuery {
    /// Una por columna del resultado, en orden.
    pub columnas: Vec<Rastro>,
    /// Columnas que se leen para filtrar, unir, agrupar u ordenar.
    pub indirectas: BTreeSet<ColumnaDeTabla>,
}

pub(crate) fn de_plan(plan: &LogicalPlan) -> LineageDeQuery {
    let mut rastreador = Rastreador::default();
    let columnas = rastreador.rastrear(plan);
    LineageDeQuery {
        columnas,
        indirectas: rastreador.indirectas,
    }
}

#[derive(Default)]
struct Rastreador {
    indirectas: BTreeSet<ColumnaDeTabla>,
}

impl Rastreador {
    /// Una entrada por columna del esquema de `plan`, siempre.
    fn rastrear(&mut self, plan: &LogicalPlan) -> Vec<Rastro> {
        let campos = plan.schema().fields().len();
        let rastro = self.nodo(plan);
        // Si algo no cuadra con el esquema, mejor decir que no se sabe que
        // asignar orígenes a la columna equivocada.
        if rastro.len() == campos {
            rastro
        } else {
            vec![Rastro::incierto(); campos]
        }
    }

    fn nodo(&mut self, plan: &LogicalPlan) -> Vec<Rastro> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                let tabla = scan.table_name.table();
                scan.projected_schema
                    .fields()
                    .iter()
                    .map(|f| Rastro::de_tabla(tabla, f.name()))
                    .collect()
            }

            LogicalPlan::Projection(p) => {
                let entrada = self.rastrear(&p.input);
                p.expr
                    .iter()
                    .map(|e| self.expresion(e, p.input.schema(), &entrada))
                    .collect()
            }

            LogicalPlan::Filter(f) => {
                let entrada = self.rastrear(&f.input);
                self.indirecta(&f.predicate, f.input.schema(), &entrada);
                entrada
            }

            LogicalPlan::Sort(s) => {
                let entrada = self.rastrear(&s.input);
                for orden in &s.expr {
                    self.indirecta(&orden.expr, s.input.schema(), &entrada);
                }
                entrada
            }

            LogicalPlan::Aggregate(a) => {
                let entrada = self.rastrear(&a.input);
                let esquema = a.input.schema();
                // Agrupar por una columna decide qué filas se juntan aunque
                // la columna no salga en el resultado.
                for grupo in &a.group_expr {
                    self.indirecta(grupo, esquema, &entrada);
                }
                let mut salida: Vec<Rastro> = a
                    .group_expr
                    .iter()
                    .chain(&a.aggr_expr)
                    .map(|e| self.expresion(e, esquema, &entrada))
                    .collect();
                // Con GROUPING SETS el plan añade una columna interna con el
                // id del grupo: no sale de ninguna columna.
                salida.resize(a.schema.fields().len(), Rastro::constante());
                salida
            }

            LogicalPlan::Window(w) => {
                let mut salida = self.rastrear(&w.input);
                let entrada = salida.clone();
                for e in &w.window_expr {
                    salida.push(self.expresion(e, w.input.schema(), &entrada));
                }
                salida
            }

            LogicalPlan::Join(j) => {
                let izquierda = self.rastrear(&j.left);
                let derecha = self.rastrear(&j.right);
                // Las claves y el filtro del join citan columnas de los dos
                // lados: cada una se busca en el suyo.
                let lados = [
                    (j.left.schema().as_ref(), izquierda.as_slice()),
                    (j.right.schema().as_ref(), derecha.as_slice()),
                ];
                for (a, b) in &j.on {
                    self.indirecta_en(a, &lados);
                    self.indirecta_en(b, &lados);
                }
                if let Some(filtro) = &j.filter {
                    self.indirecta_en(filtro, &lados);
                }
                match j.join_type {
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                        [izquierda, derecha].concat()
                    }
                    JoinType::LeftSemi | JoinType::LeftAnti => izquierda,
                    JoinType::RightSemi | JoinType::RightAnti => derecha,
                    // La marca dice si hubo pareja: no copia ningún valor.
                    JoinType::LeftMark => [izquierda, vec![Rastro::constante()]].concat(),
                    JoinType::RightMark => [derecha, vec![Rastro::constante()]].concat(),
                }
            }

            LogicalPlan::Union(u) => {
                let mut salida: Option<Vec<Rastro>> = None;
                for entrada in &u.inputs {
                    let rastro = self.rastrear(entrada);
                    match salida.as_mut() {
                        None => salida = Some(rastro),
                        Some(acumulado) => {
                            for (a, r) in acumulado.iter_mut().zip(&rastro) {
                                a.sumar(r);
                            }
                        }
                    }
                }
                salida.unwrap_or_default()
            }

            LogicalPlan::Distinct(Distinct::All(entrada)) => self.rastrear(entrada),
            LogicalPlan::Distinct(Distinct::On(d)) => {
                let entrada = self.rastrear(&d.input);
                let esquema = d.input.schema();
                for e in &d.on_expr {
                    self.indirecta(e, esquema, &entrada);
                }
                d.select_expr
                    .iter()
                    .map(|e| self.expresion(e, esquema, &entrada))
                    .collect()
            }

            LogicalPlan::Limit(l) => self.rastrear(&l.input),
            LogicalPlan::Repartition(r) => self.rastrear(&r.input),
            LogicalPlan::SubqueryAlias(s) => self.rastrear(&s.input),
            LogicalPlan::Subquery(s) => self.rastrear(&s.subquery),

            LogicalPlan::Values(_) | LogicalPlan::EmptyRelation(_) => {
                vec![Rastro::constante(); plan.schema().fields().len()]
            }

            _ => vec![Rastro::incierto(); plan.schema().fields().len()],
        }
    }

    /// El rastro de una expresión evaluada sobre `esquema`.
    fn expresion(&mut self, expr: &Expr, esquema: &DFSchema, entrada: &[Rastro]) -> Rastro {
        let mut rastro = Rastro {
            columnas: BTreeSet::new(),
            derivacion: paso(expr),
        };
        for columna in expr.column_refs() {
            match buscar(columna, esquema, entrada) {
                Some(origen) => rastro.sumar(origen),
                None => rastro.derivacion = Derivation::Unknown,
            }
        }
        // Una subconsulta escalar pone en la salida el valor de su única
        // columna; las de `EXISTS` o `IN` sólo deciden, como un filtro.
        let mut subconsultas = Vec::new();
        let _ = expr.apply(|e| {
            if let Expr::ScalarSubquery(sub) = e {
                subconsultas.push(sub.subquery.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        });
        for sub in subconsultas {
            for origen in self.rastrear(&sub) {
                rastro.sumar(&origen);
            }
        }
        rastro
    }

    /// Anota como indirectas las columnas que lee `expr`.
    fn indirecta(&mut self, expr: &Expr, esquema: &DFSchema, entrada: &[Rastro]) {
        self.indirecta_en(expr, &[(esquema, entrada)]);
    }

    /// Igual, buscando cada columna en el primer lado que la tenga.
    fn indirecta_en(&mut self, expr: &Expr, lados: &[(&DFSchema, &[Rastro])]) {
        for columna in expr.column_refs() {
            let origen = lados
                .iter()
                .find_map(|(esquema, entrada)| buscar(columna, esquema, entrada));
            if let Some(origen) = origen {
                self.indirectas.extend(origen.columnas.iter().cloned());
            }
        }
    }
}

fn buscar<'a>(columna: &Column, esquema: &DFSchema, entrada: &'a [Rastro]) -> Option<&'a Rastro> {
    esquema
        .index_of_column(columna)
        .ok()
        .and_then(|i| entrada.get(i))
}

/// Qué le hace al valor este paso, sin mirar lo que traían sus entradas.
fn paso(expr: &Expr) -> Derivation {
    let mut e = expr;
    while let Expr::Alias(alias) = e {
        e = &alias.expr;
    }
    if matches!(e, Expr::Column(_)) {
        return Derivation::Copy;
    }
    let mut agrega = false;
    let _ = e.apply(|n| {
        if matches!(n, Expr::AggregateFunction(_) | Expr::WindowFunction(_)) {
            agrega = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if agrega {
        Derivation::Aggregated
    } else {
        Derivation::Computed
    }
}
