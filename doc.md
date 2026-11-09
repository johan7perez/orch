# Orch — Especificación del proyecto

## Descripción general

**Orch** es una plataforma de orquestación y transporte de datos (tipo Azure Data Factory / SSIS) que corre como **aplicación de escritorio local**, combinando lo mejor de las herramientas ETL/ELT actuales en una sola solución de alto rendimiento.

## Objetivos principales

1. **Orquestación rápida**: programar, encadenar y ejecutar pipelines de datos con baja latencia y alta concurrencia.
2. **Transporte de datos de alto rendimiento**: mover grandes volúmenes de datos entre orígenes y destinos (bases de datos, APIs, archivos, streaming) con el menor overhead posible.
3. **Monitoreo detallado**: observabilidad en tiempo real de cada pipeline (logs, métricas, estado de tareas, throughput, errores, lineage de datos).

## Requisitos funcionales (paridad+ con ETL/ELT actuales)

- Diseño visual de pipelines (drag-and-drop) + soporte para definirlos como código.
- Conectores para bases de datos relacionales, NoSQL, APIs REST, archivos (CSV, Parquet, JSON) y colas de mensajes.
- Transformaciones de datos (mapping, filtrado, agregaciones, validaciones).
- Programación de tareas (cron, triggers por evento, dependencias entre pipelines).
- Reintentos automáticos, manejo de errores y alertas.
- Versionado de pipelines y control de cambios.

## Requisitos no funcionales

- Debe ser la opción **más rápida** disponible: minimizar latencia de orquestación y maximizar throughput de transferencia de datos.
- Debe correr eficientemente en una máquina local (uso eficiente de CPU/memoria, sin depender de infraestructura cloud).
- Escalable en el futuro a modo distribuido/cluster si es necesario.

## Requisitos de diseño de interfaz (Apple Human Interface Guidelines)

La interfaz de escritorio de Orch debe seguir los **patrones de diseño de Apple (HIG)**, no como una capa estética superficial sino como principio rector de la experiencia:

- **Claridad**: tipografía legible (SF Pro o equivalente del sistema), jerarquía visual clara, iconografía consistente (estilo SF Symbols: lineal, monocromático, escalable).
- **Deferencia**: el contenido (los pipelines, los datos, las métricas) es el protagonista — el chrome de la UI (barras, controles) debe ser minimalista y no competir visualmente con la información.
- **Profundidad**: uso sutil de capas, materiales translúcidos (vibrancy/blur) y sombras suaves para comunicar jerarquía sin recargar la interfaz.
- **Modo claro/oscuro nativo**: soporte completo de dark mode, siguiendo automáticamente la preferencia del sistema operativo.
- **Controles nativos**: usar los patrones de interacción estándar del OS (paneles laterales, barras de herramientas, inspectores contextuales, menús contextuales) en vez de reinventar componentes.
- **Movimiento con propósito**: animaciones cortas, con curvas de easing suaves (tipo `ease-in-out`), usadas para comunicar estado (carga, transición, error) y no como decoración.
- **Consistencia entre plataformas**: si en el futuro Orch se porta a macOS/Windows/Linux, mantener el mismo lenguaje visual, respetando las convenciones nativas de cada OS donde sea posible.

Esto aplica directamente al stack de UI (Tauri + React/Svelte Flow): el diseño visual del pipeline, el dashboard de monitoreo y los paneles de configuración deben construirse respetando estos principios desde el primer prototipo, no como un ajuste posterior.

## Stack tecnológico

| Capa | Tecnología | Justificación |
|---|---|---|
| Motor core / orquestador | Rust + Tokio (async runtime) | Sin garbage collector, control fino de memoria, concurrencia masiva con bajísimo overhead. |
| Motor de transporte de datos | Apache Arrow (arrow-rs) | Formato columnar en memoria, zero-copy entre conectores, evita serialización/deserialización redundante. |
| Motor de transformaciones/SQL | Apache DataFusion | Motor de queries embebible sobre Arrow, en Rust, con optimizador de queries incluido. |
| Almacenamiento de metadatos/logs/monitoreo | DuckDB (embebido) | Analítico, columnar, corre local sin servidor, ideal para consultas rápidas de métricas. |
| UI de escritorio | Tauri (Rust) + React/Svelte + React Flow (o Svelte Flow) | Ligero comparado con Electron, usa el motor nativo del OS, permite diseñador visual drag-and-drop. |
| Conectores/plugins | SDK nativo en Rust (core) + WASM para plugins de terceros | Rendimiento máximo en conectores críticos, extensibilidad segura (sandboxed) para el ecosistema. |
| Paralelismo CPU-bound | Rayon | Paralelización de transformaciones en máquinas multinúcleo. |
| Telemetría interna | Tracing (crate de Rust) + push a DuckDB + WebSocket al frontend | Monitoreo en tiempo real sin polling costoso. |

## Arquitectura de alto nivel

- **UI (diseñador visual + dashboard)**: se comunica con el orquestador vía IPC nativo de Tauri.
- **Orquestador (Rust + Tokio)**: ejecuta el DAG, maneja triggers, dependencias y reintentos.
- **Motor de datos (Arrow + DataFusion)**: mueve y transforma datos directamente en formato columnar, sin serialización intermedia.
- **Conectores (nativos + plugins WASM)**: interfaz con orígenes/destinos externos (Postgres, MySQL, S3, Parquet, Kafka, REST).
- **DuckDB embebido**: almacena metadatos, logs y métricas de ejecución, consultado por el dashboard en tiempo real.

## Hoja de ruta

1. **Fase 0 — Core mínimo (CLI)**: motor Rust con ejecución de DAG básica, 2-3 conectores (Postgres, CSV, REST), sin UI.
2. **Fase 1 — Shell de escritorio**: Tauri + UI mínima para ver ejecuciones y logs.
3. **Fase 2 — Diseñador visual + SDK de conectores**: drag-and-drop con React Flow / Svelte Flow, arquitectura de plugins WASM, primera implementación completa de los patrones de diseño Apple/HIG.
4. **Fase 3 — Monitoreo avanzado**: lineage de datos, métricas de throughput por nodo, alertas.
5. **Fase 4 — Escalabilidad**: modo distribuido/cluster opcional, versionado de pipelines con git.

## Riesgos técnicos

- **Datasets enormes en memoria**: mitigarlo procesando en streaming por record batches de Arrow, nunca cargando todo el dataset de una vez.
- **Overhead de plugins WASM**: medir el costo real vs. conectores nativos; reservar WASM solo para conectores de baja frecuencia si el overhead es alto.
- **UI bloqueada por pipelines pesados**: todo el trabajo pesado corre en el backend Rust vía async; la UI solo recibe eventos, nunca bloquea el hilo principal.
- **Diseñador visual con pipelines grandes**: requiere virtualización del canvas (renderizar solo nodos visibles) para no degradar el rendimiento con cientos de nodos.

## Lo decidido al implementar

Esta especificación es el punto de partida y se deja como estaba. Lo que dejaba
abierto se fue concretando así; el detalle de cada fase está en
[docs/PLAN.md](docs/PLAN.md) y el de cada decisión, en el [README](README.md).

- **Plugins: modelo de componentes de WebAssembly, sobre wasmtime.** El
  contrato está en WIT, los lotes cruzan como IPC de Arrow, y hay un SDK en
  Rust. De momento, sólo transformaciones.
- **UI: React + React Flow.** De las dos opciones del stack. El lienzo dibuja
  y edita el mismo YAML que se ejecuta: no hay un formato aparte para el
  diseñador, y editar desde el lienzo toca sólo las líneas del cambio, con
  los comentarios del fichero intactos.
- **Telemetría sin WebSocket.** El motor corre dentro del proceso de la
  aplicación, así que los eventos van del canal `broadcast` del ejecutor a la
  ventana por los eventos de Tauri. El mismo canal alimenta el visor de la
  CLI y el historial en DuckDB.
- **Sin Rayon, de momento.** El paralelismo sale de que todos los nodos
  corren a la vez como tareas de Tokio, con contrapresión por arista, y de
  que DataFusion paraleliza dentro de cada nodo SQL. Ningún perfil ha pedido
  más.
- **Pushdown por conector en vez de un planificador global.** Un `filter` o
  un `select` pegado a un origen que sabe resolverlo (PostgreSQL, Parquet) se
  absorbe en él, sin acoplar el motor a DataFusion.
- **Programación en el propio fichero.** El bloque `schedule` vive con el
  pipeline, y un demonio vigila la carpeta.

Y en qué quedaron los riesgos:

- **Datasets enormes:** mitigado. Todo va en streaming por lotes de Arrow y la
  memoria queda acotada por `batch_size × channel_capacity × aristas`; sólo
  `aggregate` y `ORDER BY` acumulan, con un tope de memoria configurable.
- **UI bloqueada:** mitigado. La ventana sólo recibe eventos; los lotes nunca
  salen de Rust.
- **Diseñador con pipelines grandes:** medido con 500 nodos (Fase 2.4). La
  virtualización sola no basta: con el grafo entero a la vista, cada nodo es
  un elemento que se repinta en cada fotograma. Un pipeline grande se abre a
  un tamaño legible, con unas decenas de nodos a la vista, y así va fluido; la
  vista general de cientos de nodos sigue siendo lenta, y el paso siguiente
  sería pintarla en un `<canvas>`.
- **Overhead de plugins WASM:** medido en la Fase 2.5 con la misma
  transformación compilada de las dos formas. Cruzar la frontera cuesta ~30 ns
  por fila y el código dentro del sandbox va unas dos veces más lento. Se
  aplicó lo que preveía este riesgo: los conectores críticos son nativos, y
  los plugins, para lo que Orch no trae.
- **Seguridad de los plugins:** cada instancia corre sin ficheros, red ni
  entorno, con tope de memoria y de tiempo por llamada; un test lo comprueba
  con un plugin que intenta salirse.
