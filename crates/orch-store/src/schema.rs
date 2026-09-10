//! Esquema del almacén.
//!
//! Las migraciones son una lista ordenada de sentencias: cada arranque
//! aplica las que falten y anota hasta dónde llegó. Basta para un fichero
//! local de un solo escritor, y evita tener que adivinar el estado del
//! esquema por introspección.

pub const MIGRATIONS: &[&str] = &[
    // 1 — tablas base.
    //
    // Las marcas de tiempo se guardan como microsegundos desde epoch, que es
    // un entero que cualquier cliente sabe enlazar. Las vistas de abajo las
    // convierten en `TIMESTAMP` de verdad, así que las consultas analíticas
    // (`GROUP BY date_trunc(...)`) siguen siendo naturales.
    r#"
    CREATE TABLE IF NOT EXISTS runs (
        run_id         VARCHAR PRIMARY KEY,
        pipeline       VARCHAR NOT NULL,
        started_at_us  BIGINT  NOT NULL,
        finished_at_us BIGINT,
        status         VARCHAR NOT NULL,
        elapsed_ms     BIGINT,
        nodes          INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS node_runs (
        run_id      VARCHAR NOT NULL,
        node        VARCHAR NOT NULL,
        position    INTEGER NOT NULL,
        kind        VARCHAR NOT NULL,
        component   VARCHAR NOT NULL,
        status      VARCHAR NOT NULL,
        attempts    INTEGER NOT NULL,
        rows_in     BIGINT  NOT NULL,
        batches_in  BIGINT  NOT NULL,
        bytes_in    BIGINT  NOT NULL,
        stalled_in_ms  BIGINT NOT NULL,
        rows_out    BIGINT  NOT NULL,
        batches_out BIGINT  NOT NULL,
        bytes_out   BIGINT  NOT NULL,
        stalled_out_ms BIGINT NOT NULL,
        elapsed_ms  BIGINT  NOT NULL,
        error       VARCHAR,
        PRIMARY KEY (run_id, node)
    );

    CREATE TABLE IF NOT EXISTS events (
        run_id  VARCHAR NOT NULL,
        seq     BIGINT  NOT NULL,
        at_us   BIGINT  NOT NULL,
        kind    VARCHAR NOT NULL,
        node    VARCHAR,
        detail  VARCHAR,
        payload VARCHAR NOT NULL
    );
    "#,
    // 2 — vistas con las marcas de tiempo ya convertidas.
    r#"
    CREATE OR REPLACE VIEW run_history AS
    SELECT run_id, pipeline,
           make_timestamp(started_at_us)  AS started_at,
           make_timestamp(finished_at_us) AS finished_at,
           status, elapsed_ms, nodes
    FROM runs;

    CREATE OR REPLACE VIEW event_log AS
    SELECT run_id, seq, make_timestamp(at_us) AS at, kind, node, detail, payload
    FROM events;
    "#,
    // 3 — una vista que ya trae calculado lo que se mira siempre.
    r#"
    CREATE OR REPLACE VIEW node_throughput AS
    SELECT
        run_id,
        node,
        kind,
        component,
        status,
        elapsed_ms,
        CASE WHEN kind = 'sink' THEN rows_in ELSE rows_out END AS rows_moved,
        CASE
            WHEN elapsed_ms > 0
            THEN (CASE WHEN kind = 'sink' THEN rows_in ELSE rows_out END) * 1000.0 / elapsed_ms
        END AS rows_per_second,
        CASE
            WHEN elapsed_ms > 0
            THEN (CASE WHEN kind = 'sink' THEN bytes_in ELSE bytes_out END) * 1000.0 / elapsed_ms
        END AS bytes_per_second,
        stalled_in_ms,
        stalled_out_ms,
        -- Tiempo trabajando de verdad, sin esperar a ningún vecino. En
        -- absoluto y no en porcentaje: el que marca el ritmo del pipeline es
        -- el que más tiempo pasa ocupado, no el que tiene mejor proporción.
        -- Un nodo que vive 3 ms y no espera nada da 100% y no es el cuello
        -- de botella de nada.
        greatest(elapsed_ms - stalled_in_ms - stalled_out_ms, 0) AS busy_ms,
        CASE
            WHEN elapsed_ms > 0
            THEN 100.0 * greatest(elapsed_ms - stalled_in_ms - stalled_out_ms, 0) / elapsed_ms
        END AS busy_pct
    FROM node_runs;
    "#,
];

/// Tabla donde se anota qué migraciones se aplicaron.
pub const VERSION_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS schema_version (
    version    INTEGER   PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL
);
"#;
