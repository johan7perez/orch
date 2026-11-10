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
    // 4 — cómo avanzó cada nodo durante la ejecución.
    //
    // Una muestra por segundo, contadores acumulados desde el arranque de la
    // ejecución (`at_ms`): las filas por segundo de un tramo salen de dos
    // seguidas. Es lo que dibuja la curva de una ejecución pasada y contra lo
    // que se compara una nueva para saber si va más lenta de lo normal.
    r#"
    CREATE TABLE IF NOT EXISTS node_progress (
        run_id         VARCHAR NOT NULL,
        node           VARCHAR NOT NULL,
        at_ms          BIGINT  NOT NULL,
        rows_in        BIGINT  NOT NULL,
        rows_out       BIGINT  NOT NULL,
        bytes_in       BIGINT  NOT NULL,
        bytes_out      BIGINT  NOT NULL,
        stalled_in_ms  BIGINT  NOT NULL,
        stalled_out_ms BIGINT  NOT NULL
    );
    "#,
    // 5 — las alertas que se dieron: que se avisó, de qué y cuándo.
    r#"
    CREATE TABLE IF NOT EXISTS alerts (
        run_id   VARCHAR NOT NULL,
        pipeline VARCHAR NOT NULL,
        at_us    BIGINT  NOT NULL,
        kind     VARCHAR NOT NULL,
        node     VARCHAR,
        message  VARCHAR NOT NULL
    );
    "#,
    // 6 — qué versión del pipeline corrió en cada ejecución.
    //
    // El texto se guarda una vez por versión, no por ejecución: un pipeline
    // que corre cada hora sin cambiar ocupa lo mismo que uno que corrió una
    // vez. Con git, además, el último commit que tocó el fichero.
    r#"
    CREATE TABLE IF NOT EXISTS pipeline_versions (
        hash VARCHAR PRIMARY KEY,
        text VARCHAR NOT NULL
    );

    CREATE TABLE IF NOT EXISTS run_versions (
        run_id      VARCHAR PRIMARY KEY,
        path        VARCHAR NOT NULL,
        hash        VARCHAR NOT NULL,
        in_git      BOOLEAN NOT NULL,
        git_commit  VARCHAR,
        git_subject VARCHAR,
        modified    BOOLEAN NOT NULL
    );

    CREATE OR REPLACE VIEW run_history AS
    SELECT r.run_id, r.pipeline,
           make_timestamp(r.started_at_us)  AS started_at,
           make_timestamp(r.finished_at_us) AS finished_at,
           r.status, r.elapsed_ms, r.nodes,
           v.hash AS pipeline_hash, v.git_commit, v.modified
    FROM runs r LEFT JOIN run_versions v USING (run_id);
    "#,
];

/// Tabla donde se anota qué migraciones se aplicaron.
pub const VERSION_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS schema_version (
    version    INTEGER   PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL
);
"#;
