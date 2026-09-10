# Plan de trabajo por fases

Desglose ejecutable de la hoja de ruta de [doc.md](../doc.md). Cada sub-fase
termina en algo que se puede ejecutar y medir; ninguna es un refactor a ciegas.

Leyenda: ✅ hecho · 🚧 en curso · ⬜ pendiente

---

## Fase 0 — Core mínimo (CLI)

### 0.1 Motor y vertical slice 🚧

- ✅ Workspace de crates (`orch-core`, `orch-connectors`, `orch-cli`).
- ✅ Modelo declarativo de pipeline en YAML con versión de formato.
- ✅ Validación del DAG: ids duplicados, aristas huérfanas, ciclos (incluidos
  los que introducen las barreras `after`), cableado incoherente por tipo de nodo.
- ✅ Ejecutor asíncrono de dataflow por streaming sobre Tokio, con
  contrapresión por arista.
- ✅ Reintentos con backoff exponencial, restringidos a nodos sin datos en vuelo.
- ✅ Propagación de fallos: un consumidor nunca da por bueno un flujo truncado.
- ✅ Parada temprana limpia (`limit` corta y el origen deja de producir).
- ✅ Traits `Source` / `Transform` / `Sink` + `Registry` sin acoplamiento al core.
- ✅ Eventos de ejecución en canal `broadcast` (telemetría que nunca frena los datos).
- ✅ Conectores CSV (origen y destino), `generator`, `null`.
- ✅ Transformaciones `select`, `rename`, `limit`.
- ✅ CLI `validate` / `graph` / `run` / `connectors`, con informe en texto y JSON.
- ✅ Tests de validación, semántica de ejecución y pipelines de extremo a extremo
  (26 tests, todos en verde).
- ✅ Build verificado con `stable-x86_64-pc-windows-msvc` 1.98.1.
- ✅ `cargo clippy --all-targets -D warnings` limpio y `cargo fmt` aplicado.
- ✅ Línea base de throughput: 10 M filas en 56 ms (~180 M filas/s) con
  generador → sink nulo en release.

### 0.2 Motor de transformaciones (DataFusion) ✅

- ✅ Crate `orch-sql` con DataFusion 55 (alineado con `arrow` 59, una sola
  versión de Arrow en el workspace).
- ✅ `StreamingTable` + `PartitionStream` sobre el flujo del nodo: DataFusion
  tira de nuestros batches en vez de exigir el dataset materializado.
- ✅ Transformación `sql` con SQL libre.
- ✅ `filter`, `derive` y `aggregate`, que bajan a SQL y comparten el mismo
  camino de ejecución.
- ✅ `memory_limit_mb` por nodo para las operaciones que rompen el streaming.
- ✅ Parada temprana correcta: un `LIMIT` corta la alimentación sin que el
  origen lo reporte como fallo.
- ✅ Validación de sintaxis SQL y de campos desconocidos en `orch validate`.
- ✅ `SessionContext` construido al preparar, no al ejecutar. En debug esto
  llevó `sql_resumen` de 730 ms a 78 ms; en release el catálogo de funciones
  cuesta <1 ms por nodo, así que el ahorro medido es de ~3 ms sobre 4 nodos.
- ✅ Coste del camino SQL medido: 10 M de filas por un `filter` de DataFusion
  en 65 ms, frente a 54 ms de la línea base sin nodo SQL (~1 ns/fila). El
  filtro no materializa nada.
- ✅ **Punto de integración decidido: transformación aislada.** Medido el
  coste de no fusionar, comparando tres nodos SQL encadenados (`filter` →
  `derive` → `aggregate`) contra una sola query equivalente sobre 10 M de
  filas: **86 ms encadenado, 90 ms fusionado**. No hay diferencia — si acaso
  la cadena va marginalmente mejor. Cada nodo es una tarea de Tokio, así que
  las etapas se solapan (`generar` termina en 73 ms y `agregar` en 86) y los
  saltos de canal son clones de `Arc`. Fusionar transformaciones no compra
  nada, y la opción aislada mantiene los conectores independientes de
  DataFusion. Un test comprueba que ambas formas dan el mismo resultado, para
  que la comparación signifique algo.
  - Salvedad: esto mide la fusión **entre transformaciones**, no el empuje de
    filtros y proyecciones **hasta el conector**. Eso sí puede valer mucho
    (leer menos columnas de un Parquet, mandar el `WHERE` a Postgres) y se
    consigue con pushdown por conector, sin planificador global. Entra en 0.3.
- ✅ **Propagación estática de esquemas.** Los orígenes declaran su esquema
  sin leer datos (`csv` infiere de la cabecera, `generator` lo conoce) y cada
  transformación calcula el suyo en `prepare`. Un origen que no puede saberlo
  devuelve `None` y la cadena se corta sin invalidar el pipeline: `validate`
  no puede exigir que las fuentes existan. Ahora se detectan en `validate`
  las columnas inexistentes, el fan-in con esquemas incompatibles y un
  `filter` con dos entradas. Y con esquema conocido hay plan aunque no llegue
  ni un lote, así que un `COUNT(*)` sobre cero filas devuelve una fila con 0.
- ✅ **Joins entre ramas del DAG.** Cada arista entrante es un puerto con
  nombre (por defecto el id del nodo de origen, o `port:` en la arista), y un
  nodo `sql` registra cada puerto como una tabla. El join se escribe sin
  sintaxis nueva. `filter`, `derive` y `aggregate` siguen exigiendo una sola
  entrada y lo dicen en `validate`.
- ⬜ Un nodo `sql` con varias entradas necesita que todas declaren esquema:
  espiar varios puertos en serie podría bloquear el pipeline si comparten un
  origen aguas arriba. Se resolvería espiándolos en paralelo.

### 0.3 Conectores restantes de la Fase 0 🚧

Los tests de PostgreSQL necesitan un servidor. Si no hay ninguno accesible se
saltan con un aviso en vez de fallar, para que el repositorio siga siendo
comprobable sin instalarlo. El DSN se toma de `ORCH_TEST_PG_DSN`.

- ✅ **Parquet** origen y destino. El esquema sale del pie del fichero, con
  los tipos reales en vez de inferidos, así que `validate` lo conoce siempre.
  `columns:` empuja la proyección al lector: las columnas que no se piden ni
  se descomprimen. El destino escribe un fichero legible aunque no llegue
  ninguna fila, porque el esquema viene propagado desde `prepare`.
- ✅ **Secretos fuera del YAML**: `${env:NOMBRE}` en cualquier cadena de la
  config, resuelto al cargar para que una variable ausente se note en
  `validate`. Un esquema desconocido es un error, no un literal.

- ✅ **Postgres** origen y destino, en el crate `orch-postgres`. Lectura por
  cursor dentro de una transacción, `fetch_size` filas por vuelta, con las
  filas acumuladas entre vueltas para que los lotes de Arrow salgan enteros.
  Escritura con `COPY ... FORMAT binary` dentro de una transacción: es el
  único destino del proyecto que **sí es transaccional**, y si el pipeline
  falla a medias la tabla queda como estaba. `truncate: true` sustituye la
  tabla entera de forma atómica.
  - Binario y no CSV por corrección, no por velocidad: en `COPY ... FORMAT
    csv` una cadena vacía sin comillas significa NULL, y el escritor CSV de
    Arrow emite lo mismo para un NULL que para un `""`. Cualquier columna de
    texto nullable se corrompería en silencio.
  - El esquema sale de preparar la sentencia, que no ejecuta nada, así que
    `validate` conoce los tipos exactos del servidor sin leer datos.
  - Tipos cubiertos por un test de ida y vuelta contra una base real: bool,
    int2/4/8, float4/8, text, date, timestamp, timestamptz, uuid, jsonb,
    bytea, más sus NULL. `numeric` todavía no; el error dice que se convierta
    con `::text`.
  - Medido con PostgreSQL 17 en la misma máquina, 1 M de filas de tres
    columnas: carga en 1,15 s (~870 K filas/s), lectura en 691 ms
    (~1,45 M filas/s).
- ⬜ **TLS para Postgres.** Hoy la conexión es sin cifrar: un servidor que
  exija SSL la rechaza con un error claro, pero eso deja fuera a casi
  cualquier Postgres gestionado.
- ⬜ Soporte de `numeric` sin pasar por texto.
- ⬜ **Pushdown por conector**: que un `filter` o un `select` inmediatamente
  posterior a un origen se traduzca en leer menos. Es donde de verdad está la
  ganancia que un planificador global habría dado (ver 0.2), y se consigue
  sin acoplar el motor a DataFusion.
- ✅ **REST** origen y destino, en el crate `orch-rest`. Paginación por
  número de página, por offset y por cursor, con `max_pages` como freno.
  Reintentos sólo en códigos transitorios (408, 429, 5xx): un 401 no mejora
  repitiéndolo. Se respeta `Retry-After`. Límite de tasa configurable. El
  destino trocea los lotes en peticiones de N filas. Las cabeceras nunca se
  registran en los logs.
  - El esquema hay que declararlo para que llegue a `validate`: llamar a una
    API durante la validación tendría efectos secundarios. Sin declararlo se
    deduce de la primera página.
- ⬜ Almacén de secretos del sistema operativo (`${keyring:...}`), además de
  las variables de entorno.

### 0.4 Persistencia y observabilidad ⬜

- ⬜ DuckDB embebido: esquema de `runs`, `node_runs`, `events`, `metrics`.
- ⬜ Escritor de eventos asíncrono suscrito al canal `broadcast`, con lotes y
  sin bloquear la ejecución.
- ⬜ `orch runs` / `orch logs <run_id>` en la CLI.
- ⬜ Métricas de throughput por nodo (filas/s, bytes/s) y de contrapresión
  (tiempo bloqueado por arista) — es lo que dirá dónde está el cuello de botella.

### 0.5 Programación ⬜

- ⬜ Triggers cron y por dependencia entre pipelines.
- ⬜ Modo `orch daemon`: proceso residente que evalúa triggers.
- ⬜ Control de concurrencia entre ejecuciones del mismo pipeline.

---

## Fase 1 — Shell de escritorio ⬜

- ⬜ Proyecto Tauri con `orch-core` como biblioteca en el proceso backend.
- ⬜ Puente de eventos: el canal `broadcast` del ejecutor → WebSocket/IPC → frontend.
- ⬜ Vista de ejecuciones (lista, detalle, logs en vivo).
- ⬜ Fundamentos de HIG desde el primer prototipo: tipografía del sistema,
  modo claro/oscuro automático, jerarquía visual, animaciones con propósito.
- ⬜ Verificar que la UI nunca bloquea: todo el trabajo pesado sigue en Rust.

## Fase 2 — Diseñador visual + SDK de conectores ⬜

- ⬜ Canvas drag-and-drop (React Flow / Svelte Flow) que produce y consume el
  mismo YAML de la Fase 0.
- ⬜ Virtualización del canvas (sólo nodos visibles) para pipelines grandes.
- ⬜ Inspector contextual por nodo, generado a partir del esquema de config
  del conector.
- ⬜ Arquitectura de plugins WASM y SDK de conectores.
- ⬜ **Medir el overhead de WASM frente a los conectores nativos** antes de
  comprometerse; si es alto, reservarlo para conectores de baja frecuencia.
- ⬜ Implementación completa de los patrones Apple/HIG.

## Fase 3 — Monitoreo avanzado ⬜

- ⬜ Lineage de datos a nivel de columna.
- ⬜ Dashboard de throughput por nodo en tiempo real.
- ⬜ Alertas (fallos, degradación de throughput, ejecuciones colgadas).

## Fase 4 — Escalabilidad ⬜

- ⬜ Versionado de pipelines con git.
- ⬜ Modo distribuido opcional: reutilizar los traits de conector sobre un
  transporte remoto (Arrow Flight es el candidato natural).

---

## Deuda técnica anotada

Cada punto está documentado en el código donde aplica y listado en el README:

- Sinks no transaccionales (un fallo a media escritura deja salida parcial).
- Fan-in por concatenación, no intercalado.
- Las ramas independientes no se cancelan cuando otra falla.
- `serde_yaml` está sin mantenimiento; migrar cuando haya un sucesor claro.
