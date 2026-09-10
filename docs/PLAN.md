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

### 0.2 Motor de transformaciones (DataFusion) 🚧

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
- ⬜ **Decidir el punto de integración.** Implementada la opción "transformación
  aislada". Falta prototipar y medir la alternativa: DataFusion planificando
  sub-grafos completos, con los conectores expuestos como `TableProvider`.
  Ganaría empuje de filtros hasta el origen; costaría atar el motor a su
  modelo de ejecución.
- ⬜ **Propagación estática de esquemas hasta `validate`.** Hoy el esquema se
  descubre del primer lote, así que una columna inexistente falla en
  ejecución y no en `validate`, y una entrada vacía no produce plan (un
  `COUNT(*)` sobre cero filas devuelve vacío en vez de una fila con 0).
  Requiere que los orígenes declaren su esquema sin leer datos.
- ⬜ Joins entre dos ramas del DAG: hoy la tabla de entrada es de una sola
  pasada y un nodo SQL sólo ve un flujo (el fan-in concatena).

### 0.3 Conectores restantes de la Fase 0 ⬜

- ⬜ **Postgres** origen y destino (`tokio-postgres`): lectura por cursor en
  streaming, escritura con `COPY BINARY` — nunca `INSERT` fila a fila.
- ⬜ **REST** origen y destino: paginación, límite de tasa, reintentos por
  código de estado.
- ⬜ **Parquet** origen y destino: es el formato donde Arrow rinde mejor y el
  que hace comparables los benchmarks con otras herramientas.
- ⬜ Gestión de credenciales fuera del YAML (variables de entorno / almacén
  del sistema operativo).

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
