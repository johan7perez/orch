# Plan de trabajo por fases

Desglose ejecutable de la hoja de ruta de [doc.md](../doc.md). Cada sub-fase
termina en algo que se puede ejecutar y medir; ninguna es un refactor a ciegas.

Leyenda: ✅ hecho · 🚧 en curso · ⬜ pendiente

---

## Fase 0 — Core mínimo (CLI)

### 0.1 Motor y vertical slice ✅

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

### 0.3 Conectores restantes de la Fase 0 ✅

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
- ✅ **TLS para Postgres.** El modo sale del `sslmode` del DSN, como en
  libpq. La verificación del certificado depende del modo: con `prefer` (el
  defecto) no se verifica —es cifrado oportunista y exigir más rompería
  cualquier servidor con certificado propio sin que nadie pidiera
  garantías—; con `require` sí. **Ahí se diverge de libpq a propósito**:
  allí `require` cifra sin comprobar nada, lo que da una falsa sensación de
  seguridad. `tls.verify` y `tls.root_cert` fuerzan cualquiera de los dos
  comportamientos. Probado contra un servidor con SSL y certificado
  autofirmado, en los tres modos.
- ✅ **`numeric`**, transportado como su texto exacto. Convertirlo a
  `Decimal128` de Arrow obligaría a fijar una escala y redondear en silencio
  lo que no encajara, que en datos de dinero es inaceptable. El texto da la
  vuelta sin perder nada; hay test con 20 dígitos y seis decimales. Por
  encima de 28 dígitos significativos el error dice que se use `round()`.
- ✅ **Pushdown por conector.** Un `filter` o un `select` pegado a un origen
  se absorbe en su config y el nodo desaparece. Es la ganancia que un
  planificador global habría dado, conseguida sin acoplar el motor a
  DataFusion: la reescritura es un paso sobre el `PipelineSpec`, antes de
  construir el DAG, y el ejecutor no se entera.
  - `postgres` absorbe ambos, con la forma `table`; el `WHERE` lo resuelve el
    servidor y por la red viaja sólo el resultado. Dos filtros encadenados se
    unen con `AND`. Con `query` propia no se toca nada: envolverla en una
    subconsulta cambiaría cómo la planifica PostgreSQL.
  - `parquet` absorbe el `select`: las columnas que no se piden ni se
    descomprimen. Para que fuera exacto hubo que hacer que `columns` respete
    el orden pedido y no el del fichero.
  - Sólo se empuja si el origen tiene **un único consumidor**: con más,
    recortarle columnas o filas cambiaría lo que ven los demás. Tampoco si el
    nodo tiene una barrera `after` o si alguien depende de él.
  - `orch graph` y `orch validate` dicen qué se empujó: el pipeline que se
    ejecuta ya no es el que está escrito.
  - Medido sobre una tabla de 1 M de filas con un filtro que deja 1 000:
    **1,16 s → 127 ms**, y de 1 000 000 de filas por la red a 1 000. Nueve
    veces, frente al cero que dio fusionar transformaciones en 0.2. Ahí
    estaba la ganancia.
- ✅ **REST** origen y destino, en el crate `orch-rest`. Paginación por
  número de página, por offset y por cursor, con `max_pages` como freno.
  Reintentos sólo en códigos transitorios (408, 429, 5xx): un 401 no mejora
  repitiéndolo. Se respeta `Retry-After`. Límite de tasa configurable. El
  destino trocea los lotes en peticiones de N filas. Las cabeceras nunca se
  registran en los logs.
  - El esquema hay que declararlo para que llegue a `validate`: llamar a una
    API durante la validación tendría efectos secundarios. Sin declararlo se
    deduce de la primera página.
- ✅ Almacén de secretos del sistema operativo: `${keyring:servicio/usuario}`
  además de `${env:...}`. Credential Manager en Windows, Llavero en macOS,
  Secret Service en Linux.

### 0.4 Persistencia y observabilidad ✅

- ✅ DuckDB embebido en el crate `orch-store`: tablas `runs`, `node_runs` y
  `events`, más una vista `node_throughput` con las métricas ya calculadas.
  Las migraciones son una lista ordenada y se anota hasta dónde se llegó.
- ✅ Escritor de eventos en su propia tarea, suscrito al canal `broadcast`,
  que vuelca en lotes de 256 o cada 200 ms. Nunca frena la ejecución: si se
  retrasa, el canal descarta eventos y él lo registra. Las escrituras van a
  un hilo de bloqueo para no ocupar uno del runtime.
- ✅ Métricas de contrapresión por nodo. `Output::send` y `InputPort::recv`
  lo intentan primero sin esperar: cuando hay hueco —el caso normal— no se
  lee el reloj ni una vez, así que medir sólo cuesta cuando de verdad hay
  espera. En la salida es contrapresión (el consumidor no da abasto) y en la
  entrada es hambre (el productor no trae datos).
- ✅ `orch runs` y `orch logs <run_id>`, aceptando un prefijo del
  identificador. El informe señala el nodo con mayor porcentaje de
  ocupación: **es el cuello de botella**, el que marca el ritmo mientras el
  resto le espera.
- ✅ Retención: `orch prune --keep-days N`, y el demonio poda al arrancar si
  se le da `--keep-days`.

### 0.5 Programación ✅

- ✅ Bloque `schedule` en el propio pipeline: el cuándo vive con el qué, en
  un solo fichero. El core sólo guarda la forma; interpretarla es cosa del
  crate `orch-schedule`, así que el motor no arrastra dependencias de cron.
- ✅ Cron de cinco campos, el de toda la vida, **traducido** al de seis que
  espera el crate `cron`. Incluye la numeración del día de la semana: allí
  1 es domingo y en Unix es 0, así que un `1-5` sin traducir dispararía de
  domingo a jueves en vez de lunes a viernes. Hay test con fechas reales.
- ✅ Zonas horarias IANA. Sin indicar, UTC: es lo único que no cambia dos
  veces al año.
- ✅ Encadenamiento entre pipelines (`after`), sólo tras un éxito —encadenar
  tras un fallo propagaría datos a medias— y en cascada.
- ✅ Control de concurrencia: `skip` (por defecto), `queue` (uno como mucho,
  para que un atasco de una hora no se convierta en sesenta ejecuciones
  seguidas) y `allow`.
- ✅ Una parada larga no provoca una avalancha: al volver, el próximo
  disparo se recalcula desde ahora y no desde el que se perdió.
- ✅ `orch daemon --dir <directorio>`: valida todos los pipelines al
  arrancar —descubrir a las 3 de la mañana que uno no compila no sirve de
  nada—, rechaza nombres duplicados y `after` a pipelines inexistentes, y al
  recibir Ctrl-C deja de disparar pero espera a lo que esté en vuelo.
- ✅ El planificador es puro: se le pregunta qué toca a una hora dada y
  responde. Todas sus reglas se prueban con fechas escritas a mano, sin
  esperas reales.
- ⬜ Reload en caliente: hoy el demonio lee el directorio al arrancar y hay
  que reiniciarlo para recoger un pipeline nuevo.

---

## Fase 1 — Shell de escritorio ✅

- ✅ Aplicación Tauri 2 (`orch-app`) con `orch-core` **dentro del proceso**:
  no hay servidor local ni demonio aparte, así que no hay puerto que
  asegurar ni un segundo proceso que se quede colgado.
- ✅ Puente de eventos: el canal `broadcast` del ejecutor se reemite como
  evento `orch://event` de Tauri. El frontend no sondea; recibe.
- ✅ Comandos: `workspace`, `set_directory`, `recent_runs`, `run_detail`,
  `start_run`, `catalog`.
- ✅ Vista de pipelines con ejecución en vivo: estado por nodo y registro de
  eventos según ocurren, reintentos incluidos.
- ✅ Vista de ejecuciones: historial desde DuckDB, métricas por nodo
  (filas, filas/s, tiempo, ocupación) y el nodo que marca el ritmo.
- ✅ Selector de carpeta de pipelines, con la elegida recordada en el estado
  de la aplicación.
- ✅ Fundamentos de HIG desde el primer prototipo: tipografía del sistema,
  escala tipográfica corta, modo claro/oscuro siguiendo al sistema sin
  interruptor, jerarquía por peso y color, y movimiento con propósito
  (el latido de un nodo en curso) que respeta `prefers-reduced-motion`.
- ✅ Un pipeline roto ya no esconde a los demás: el descubrimiento devuelve
  los válidos, los rotos con su error y los problemas de conjunto (nombres
  duplicados, `after` colgando). Antes, un `postgres.yaml` sin `ORCH_PG_DSN`
  dejaba la lista entera vacía. Afecta igual al `orch daemon`.
- ✅ Las rutas relativas de un pipeline se resuelven **contra la carpeta del
  fichero YAML**, no contra el directorio de trabajo del proceso. Una
  aplicación de escritorio arranca desde donde el sistema quiera, así que
  una carpeta de pipelines tiene que ser portable.
- ✅ La UI nunca bloquea: cada ejecución se lanza en su propia tarea de Tokio
  y el frontend sólo pinta eventos.
- ✅ Verificado conduciendo la ventana real (clic en un pipeline → *Ejecutar*
  → eventos en vivo → cierre), no sólo con tests.
- ⬜ Iconos de la aplicación provisionales, generados a mano.
- ⬜ Probado sólo en Windows; falta pasar por macOS y Linux.

## Fase 2 — Diseñador visual + SDK de conectores 🚧

### 2.1 Lienzo del pipeline (lectura) ✅

- ✅ Comando `pipeline_graph`: nodos, aristas y puertos del pipeline, más el
  motivo si no valida. **Devuelve el grafo aunque el DAG no compile** — un
  diseñador que sólo funciona con pipelines correctos no sirve justo cuando
  hace falta, que es para arreglar el que está roto.
- ✅ `PipelineSpec::from_path_as_written`: carga sin expandir secretos ni
  resolver rutas. La diferencia no es cosmética: cargando como para ejecutar,
  el inspector enseñaría la contraseña de la base de datos en pantalla y
  acabaría en la primera captura que alguien pegue en un chat. Lo que se ve
  es `${env:PG_DSN}`, que es lo que el fichero dice.
- ✅ Lienzo con React Flow y disposición automática por capas: la capa de un
  nodo es el camino **más largo** desde un origen, para que ninguno quede a
  la izquierda de algo que lo alimenta. Aguanta ciclos —el lienzo también
  sirve para arreglarlos— colocando al final lo que quedó sin ordenar.
- ✅ El lienzo enseña el fichero, no el plan reescrito: no se aplica pushdown.
  Ver un nodo desaparecer porque el motor lo absorbió, justo mientras lo
  editas, sería desconcertante.
- ✅ Las barreras `after` se dibujan punteadas: ordenan, no transportan.
- ✅ Inspector del nodo elegido con su config tal y como está escrita, y
  aviso si el conector no está registrado (`Registry::has`), en vez de
  dibujarlo como bueno y fallar sólo al ejecutar.
- ✅ Control segmentado Ejecución/Diseño en la barra, no una pestaña más en
  el lateral: el lateral es *qué* estás navegando y esto son dos vistas del
  *mismo* pipeline. La barra y el botón de ejecutar son comunes a las dos.

### 2.2 Esquemas de config e inspector generado ⬜

- ⬜ Esquema JSON por conector y transformación, derivado del propio struct
  de config, para no mantener dos definiciones.
- ⬜ Inspector como formulario generado a partir del esquema, con los tipos y
  los valores por defecto reales.
- ⬜ El catálogo pasa a llevar el esquema, no sólo el nombre.

### 2.3 Edición y escritura del YAML ⬜

- ⬜ Drag-and-drop: crear nodos desde el catálogo, conectar y desconectar.
- ⬜ **Escritura que preserve comentarios y formato.** Serializar el spec
  entero destruiría los comentarios del fichero, y un pipeline es código que
  la gente edita a mano. Hay que editar el texto quirúrgicamente y tocar sólo
  lo que cambió.
- ⬜ Posiciones de los nodos: hoy se calculan; si se guardan, tienen que ir
  donde no estorben a quien edita el YAML a mano.

### 2.4 Virtualización del lienzo ⬜

- ⬜ Renderizar sólo los nodos visibles, para que cientos de nodos no
  degraden el lienzo.
- ⬜ Medirlo con un pipeline generado de varios cientos de nodos antes y
  después.

### 2.5 Plugins WASM y SDK de conectores ⬜

- ⬜ Arquitectura de plugins WASM y SDK.
- ⬜ **Medir el overhead frente a los conectores nativos** antes de
  comprometerse; si es alto, reservarlo para conectores de baja frecuencia.

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
