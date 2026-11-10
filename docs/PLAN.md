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
- ✅ Reload en caliente (Fase 5.3).

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
- ✅ Iconos de la aplicación con la marca, generados con `tauri icon` desde
  `ui/src/assets/marca`. En Linux el de la ventana es el primer PNG de
  `bundle.icon`, y GTK descarta en silencio uno de 512 px: va primero el de 256.
- ✅ Probado en Windows y en Linux (Linux Mint 22.3, X11): build completa,
  tests —los de PostgreSQL contra un servidor real— y la app arrancada.
- ⬜ Falta pasar por macOS.

## Fase 2 — Diseñador visual + SDK de conectores ✅

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

### 2.2 Esquemas de config e inspector generado ✅

- ✅ **El esquema sale del mismo struct que deserializa la config.**
  `Registry::register_*` toma ahora el tipo de config (inferido de la
  anotación del cierre) y de él saca las dos cosas: el parseo y el esquema
  JSON. No hay dos definiciones, así que no pueden desincronizarse.
- ✅ Efecto secundario: los conectores dejaron de repetir
  `parse_config(node, config)?` en los 17 puntos de registro; ahora lo hace
  el registro una sola vez.
- ✅ Las descripciones del esquema salen de los comentarios `///` que ya
  tenían los campos, así que no hay documentación aparte que envejezca.
- ✅ Un nodo sin `config:` ya no falla. Llegaba como `null` y serde lo
  rechazaba donde esperaba un struct; ahora se trata como `{}`, y un
  conector con todos los campos opcionales se puede escribir sin esa línea.
- ✅ `NoConfig` para los componentes que no llevan configuración. No es lo
  mismo que ignorar el bloque: al llevar `deny_unknown_fields`, un `config:`
  con algo dentro se rechaza en vez de aceptarse en silencio.
- ✅ El catálogo lleva el esquema, no sólo el nombre.
- ✅ Inspector generado: enseña **todos** los campos del componente, no sólo
  los escritos. Los ausentes salen en segundo plano con su valor por
  defecto, porque saber qué se puede poner vale tanto como ver qué hay.
  Marca en rojo un obligatorio que falta y una clave que el esquema no
  conoce —con `deny_unknown_fields` las dos revientan al ejecutar, y verlo
  en el inspector ahorra el viaje—.
- ✅ `orch connectors <nombre>`: la misma información para quien escribe el
  YAML a mano.

### 2.3 Edición y escritura del YAML ✅

- ✅ Drag-and-drop: crear nodos desde el catálogo (arrastrando o con un clic),
  conectar y desconectar, quitar con Supr. Lo que el motor rechazaría —un
  sink como origen de datos, una arista repetida— el lienzo ni lo ofrece.
- ✅ **Escritura que preserva comentarios y formato.** Crate `orch-edit`
  sobre `yaml-edit`, un árbol sintáctico sin pérdidas: cada cambio toca sólo
  sus líneas e imita el estilo del fichero (línea en blanco entre nodos,
  aristas `{ from, to }` en una línea, `after` junto al componente). Quitar un
  nodo se lleva el comentario que lo describía.
- ✅ **Cada cambio se comprueba antes de escribirse**: se aplica al modelo y
  al texto, y el texto se relee con el parser del motor; si no da
  exactamente el modelo esperado, no se escribe. Se conservan BOM y CRLF.
- ✅ Concurrencia optimista: el grafo lleva la huella del texto que se
  dibujó, y un cambio sobre un fichero que cambió por debajo se rechaza en vez
  de pisarlo. Escritura atómica (temporal + renombrado).
- ✅ Inspector editable: un control por tipo del esquema (texto, número,
  booleano, opciones, lista, mapa `clave: valor`, JSON para estructuras),
  renombrar con clic en el id y esperas `after`. Renombrar el origen de un
  `sql` fija `port:` con el nombre antiguo, como hace el pushdown, para que la
  query no se rompa.
- ✅ Tests: resultado exacto de cada operación, más un barrido de todas las
  operaciones sobre cada ejemplo comprobando que el diff toca sólo lo suyo.
  Verificado conduciendo la ventana real: una docena de cambios de ida y
  vuelta dejan el fichero idéntico byte a byte.
- ✅ Posiciones de los nodos, en un bloque `layout` al final del propio
  fichero —el pipeline sigue siendo un único fichero, como con `schedule`—,
  con un comentario que dice qué es y que se puede borrar. Sólo existe si
  alguien movió un nodo; al mover el primero se fijan todos, para que los
  demás no se recoloquen alrededor. Lectura tolerante: una entrada mal
  escrita se ignora. Quitar o renombrar un nodo arrastra su posición, y un
  botón de los controles las borra todas.
- ✅ Con posiciones guardadas, un componente aparece donde se suelta, y si el
  inspector que se abre lo tapa, el lienzo se desplaza lo justo para que
  asome.

### 2.4 Virtualización del lienzo ✅

- ✅ Pipeline generado de 500 nodos y 450 aristas
  (`cargo run -p orch-edit --example grande`), que además cronometra cada
  operación del editor sobre él.
- ✅ Medidor en la propia ventana (`VITE_ORCH_MEDIR=1`): nodos y aristas en el
  DOM, tiempo de construir el grafo y de cada render de React, y fotogramas de
  los dos últimos segundos. Las interacciones se repitieron idénticas antes y
  después (desplazar, zoom con la rueda, elegir un nodo).
- ✅ **Lo primero que dijo la medición: la virtualización sola no basta.**
  Con el grafo entero a la vista, quitar sombras, texto, aristas y fondo no
  cambiaba nada: en WebKitGTK cada nodo visible es un elemento que se
  repinta en cada fotograma, y con unos 300 a la vista eso son 30 ms
  aunque sean cajas vacías. Lo que funciona es tener menos nodos a la vista.
- ✅ Sólo se montan los nodos y aristas visibles (`onlyRenderVisibleElements`).
- ✅ Un pipeline que no cabe entero a un tamaño legible (zoom 0,6) se abre por
  el principio, no en una vista general que ni se lee ni va fluida.
- ✅ Elegir un nodo cambia dos objetos, no quinientos; y después de editar,
  React Flow sólo vuelve a procesar los nodos y aristas que cambiaron.
- ✅ Disposición automática y montaje del grafo lineales en nodos y aristas.
- ✅ Dos fallos que salieron al medir: un nodo recién soltado se ocultaba con
  sus aristas (llegaba a React Flow sin medidas; ahora el tamaño, que es
  fijo, va declarado), y arrastrar con Espacio pulsado dejaba un nodo
  desplazado en pantalla sin guardar nada (desactivado).

Con 500 nodos, en esta máquina (Linux Mint, WebKitGTK, React en modo
desarrollo; en release y con WebView2 por GPU debería ir mejor, no está
medido):

| | Antes | Después |
|---|---|---|
| Al abrir | vista general, 500 nodos en el DOM | legible por los orígenes, ~70 |
| Desplazar la vista con que se abre | p95 146 ms, 15 de 36 fotogramas lentos | p95 ~40 ms |
| Desplazar con zoom de trabajo | p95 50 ms, 19 de 74 lentos | p95 28 ms, 3 de 107 |
| Zoom con la rueda | máx 150 ms | máx ~100 ms |
| Elegir un nodo | fotograma de 380 ms | 126 ms |
| Construir el grafo | 5 ms | 2 ms |
| Vista general pedida a mano | p95 146 ms | igual: es el límite del DOM |

El editor no era el cuello de botella: sobre el mismo pipeline cada cambio
tarda 17–64 ms en release (150–540 ms en debug).

- ⬜ Si la vista general tiene que ir fluida con cientos de nodos, el paso
  siguiente es pintarla en un `<canvas>` por debajo de cierto zoom en vez de
  con elementos del DOM.

### 2.5 Plugins WASM y SDK de conectores ✅

- ✅ **Primero, medir** (`cargo run --release -p orch-wasm --example medir`):
  la misma transformación compilada nativa y como plugin, sobre los mismos
  lotes. Cruzar la frontera cuesta ~30 ns por fila en lotes de 8 192, y el
  código dentro del sandbox va unas dos veces más lento que el nativo:
  pasar a mayúsculas una columna, ~35 ns por fila nativo y ~110 como plugin.
  Junto a cualquier E/S real es poco; para el núcleo en memoria (5 ns por
  fila), no. **Decisión:** los conectores y transformaciones de Orch siguen
  siendo nativos, y los plugins son el mecanismo para lo que Orch no trae.
- ✅ Contrato en WIT (`wit/orch-plugin.wit`), con el modelo de componentes de
  WebAssembly: un plugin se puede escribir en cualquier lenguaje que compile
  a un componente. Los lotes cruzan como IPC de Arrow, la copia que conserva
  tipos y nulos sin inventar un formato.
- ✅ SDK en Rust (`orch-plugin`): un trait, `Transformacion`, y una macro,
  `exportar!`. La config se declara con un struct y su esquema —con las
  descripciones de los `///`— llega solo al inspector, como en un conector
  nativo. Fuera de WASM el mismo crate es una biblioteca nativa, que es como
  se mide la frontera.
- ✅ Host (`orch-wasm`, sobre wasmtime): una instancia por nodo, sin ficheros,
  red ni entorno; 512 MB de memoria y 30 s por llamada; las llamadas, fuera
  de los hilos del motor; los lotes, en trozos de 8 192 filas, el tamaño que
  mejor sale (con 65 536 el lote serializado no cabe en la caché y cada fila
  cuesta casi el doble).
- ✅ Caché de compilación: compilar un plugin de 2,4 MB cuesta ~1 s la primera
  vez y ~40 ms los arranques siguientes.
- ✅ Los plugins viven en `plugins/` dentro de la carpeta de pipelines, para
  que sea portable, o donde diga `ORCH_PLUGINS`. Los cargan la CLI, el
  demonio y la aplicación, que los enseña en su propio grupo de la paleta.
  Un plugin no puede llamarse como un componente que ya existe.
- ✅ Tests con plugins de verdad: de extremo a extremo, errores atribuidos a
  su nodo, y un plugin (`fisgon`) que intenta leer un fichero que existe,
  conectarse a un puerto que escucha y ver una variable que está, y no
  puede. Fuera del sandbox las tres sondas funcionan, así que la prueba vale.
- ✅ La versión mínima de Rust sube a 1.97, la que exige wasmtime 50. Para un
  sandbox, ir con la versión al día pesa más que soportar compiladores viejos.
- ⬜ Orígenes y destinos como plugins: necesitan red o disco, y antes hay que
  decidir cómo se conceden esos permisos (por pipeline, por plugin).
- ⬜ Plugins más pequeños (hoy ~2,4 MB, casi todo Arrow IPC y serde) y probar
  el contrato desde otro lenguaje.

## Fase 3 — Monitoreo avanzado ✅

### 3.1 Throughput por nodo en tiempo real ✅

- ✅ El ejecutor publica `Progress` cada 250 ms: un evento por ejecución con
  los contadores acumulados de todos los nodos. Lee los mismos atómicos que
  los nodos ya incrementan, a través de una sonda de sólo lectura (`Probe`):
  no toca el camino de los datos.
- ✅ No publica si nadie escucha ni si nada se movió desde la última vez: un
  nodo esperando no llena el canal de repeticiones. Y se para antes del
  `RunFinished`, así que ningún progreso llega después del final.
- ✅ **Medido antes y después** (`cargo run --release -p orch-connectors
  --example motor`, 1 000 M de filas del generador al sink nulo con un oyente
  enganchado): 1 566–1 624 ms sin muestreo, 1 540–1 580 ms con él. Nada que
  se note.
- ✅ El historial guarda una muestra por segundo en `node_progress`, aparte
  del registro de eventos, y la poda la limpia con lo demás. Es lo que dibuja
  la curva de una ejecución pasada y contra lo que se comparará una nueva
  para saber si va más lenta de lo normal (3.2).
- ✅ `orch run --follow`: una línea cada dos segundos con lo que se mueve y a
  qué ritmo.
- ✅ Aplicación: filas, filas por segundo y curva del último minuto por nodo
  en vivo; en el detalle de una ejecución, la curva entera con el valor de
  cada instante al pasar el puntero. Una curva por fila, de un solo color y
  con el eje de tiempo compartido: múltiplos pequeños en vez de una gráfica
  con veinte colores.
- ✅ Tests con el tiempo de Tokio simulado: el progreso se publica mientras
  corre, nunca retrocede, no llega después del final y no se repite si nada
  se mueve; y el historial guarda uno por segundo sin ensuciar el registro.

### 3.2 Alertas ✅

- ✅ Bloque `alerts` en el propio pipeline, como `schedule`: `on_failure`
  (por defecto, sí), `stalled_after` con plazos legibles (`90s`, `10m`,
  `1h30m`; un `10` a secas se rechaza por ambiguo), `slower_than` (mayor que
  1) y `webhook`, que admite `${env:...}`. Claves mal escritas, rechazadas.
- ✅ Crate `orch-alert`: un vigilante que es un oyente más del canal de
  eventos. Avisa de un **fallo** (con el nodo y su error, sin repetir el
  nombre que el error ya trae), de una ejecución **colgada** (nada se mueve en
  `stalled_after`: el motor sólo publica progreso cuando algo se mueve, así
  que se vigila el silencio) y de una **lenta** (más de `slower_than` veces la
  mediana de las últimas diez correctas, en cuanto pasa y sin esperar al
  final). Cada una, una sola vez por ejecución.
- ✅ Entrega: a quien vigila (terminal, demonio, ventana), al historial (tabla
  `alerts`, que la poda limpia) y al webhook, con `text` para Slack y Teams.
  Un webhook caído se cuenta y no impide el resto.
- ✅ El demonio avisa también de un pipeline que **no llega a arrancar**: el
  bloque `alerts` se lee del YAML en bruto, así que sirve aunque un nodo mal
  escrito impida cargar el resto. Salió al escribir su test: leyendo el
  pipeline entero, ese caso perdía justo el webhook.
- ✅ Tests con el reloj simulado de Tokio (diez minutos de cuelgue en
  milisegundos), mediana contra un historial de verdad, y entrega contra un
  receptor HTTP local. Probado de punta a punta con la CLI: fallo, cuelgue
  que avisa mientras sigue en marcha, y los tres llegando al webhook.
- ✅ Aplicación: las alertas salen encima de la tabla de la ejecución en
  curso según llegan, y en el detalle de una ejecución pasada desde el
  historial. Un fallo o un cuelgue, en rojo; ir lenta, en el color de aviso;
  siempre con el texto, nunca sólo el color.
- ✅ Notificación del sistema en la aplicación (Fase 6.2).

### 3.3 Lineage de datos a nivel de columna ✅

- ✅ Para cada columna de salida, de qué columnas de origen viene, y para
  cada columna de un origen, a qué salidas llega. Sin leer datos: va por el
  mismo camino que los esquemas de `validate`. Cada transformación declara un
  solo salto (método `lineage` del trait `Transform`, opcional como `plan`) y
  el ejecutor los encadena en orden topológico (`Executor::lineage`).
- ✅ Cómo llega el valor: copiado (también renombrado), calculado fila a fila
  o agregado. Encadenar pasos se queda con el más fuerte.
- ✅ **Directo e indirecto.** Las columnas que sólo deciden qué filas pasan o
  en qué orden (`where`, claves de join, `GROUP BY`, `ORDER BY`) se cuentan
  aparte. Salió al pensar en el análisis de impacto: la clave de un join no
  llega a ninguna columna de salida, pero quitarla rompe el pipeline. Una
  columna puede ser las dos cosas a la vez (`SELECT * … WHERE status = …`).
- ✅ SQL sobre el plan lógico **sin optimizar** de DataFusion, el más fiel a
  lo escrito, que en la versión 55 ya trae el `*` expandido: proyecciones,
  filtros, agregados, joins de todos los tipos, `UNION`, CTE, ventanas,
  `DISTINCT ON` y subconsultas escalares. Lo que no sabe seguir (`UNNEST`,
  consultas recursivas) sale como `incierta`, sin inventar orígenes.
- ✅ Donde no se puede saber, lo dice: un nodo sin esquema declarado (un
  origen cuyo fichero aún no existe, un plugin) corta la cadena y todo lo de
  detrás indica en qué nodo se cortó.
- ✅ `orch lineage <pipeline>` (lo que escribe cada salida) y `--node <id>`
  (desde un origen: adónde llega cada columna y dónde se usa), también en
  JSON. Sin pushdown: habla de los nodos tal y como están escritos.
- ✅ Aplicación: sección *Columnas* en el inspector, con tipo y origen de
  cada columna, y en un origen su impacto. Cada referencia salta a ese nodo en
  el lienzo. Se pide al abrir el inspector, una vez por versión del fichero:
  los orígenes miran su esquema y eso no puede frenar el dibujo del lienzo.
- ✅ Tests de punta a punta con los pipelines de ejemplo (`join.yaml`,
  `sql_resumen.yaml`), copias y renombres, fan-in, SQL variado, y los dos
  cortes de cadena: un origen sin esquema y una transformación que no declara
  su lineage.
- ✅ Lineage entre pipelines (Fase 6.3).

## Fase 4 — Escalabilidad ✅

### 4.1 Cada ejecución sabe qué versión corrió ✅

- ✅ Al arrancar se lee el YAML una sola vez y de ese texto salen lo que se
  ejecuta y lo que se guarda (`Snapshot`), así que «la versión que corrió» es
  exactamente lo que corrió. El historial guarda el texto una vez por versión
  (`pipeline_versions`) y, por ejecución, su huella, el último commit que tocó
  el fichero y si tenía cambios sin guardar (`run_versions`). La huella es la
  misma con la que el lienzo detecta cambios por debajo. Sin git, la huella y
  el texto bastan.
- ✅ Crate `orch-version` sobre el `git` instalado, no una librería: respeta
  autor, firma y hooks del usuario, y no añade otra dependencia en C. Cuesta
  unos 11 ms al arrancar cada ejecución (cuatro procesos). Nunca ejecuta el
  `core.fsmonitor` de un repositorio, ni pregunta nada por terminal.
- ✅ Diff de pipeline, no de líneas: campos hasta la hoja (`config.where`,
  `schedule.cron`), nodos añadidos, quitados y renombrados (un renombrado no
  arrastra sus aristas), aristas, y el lienzo aparte. Lo que no cambia el
  significado no aparece. Las queries de varias líneas, línea a línea.
- ✅ `orch diff <pipeline> [desde] [hasta]` con commits, la copia de trabajo o
  `run:<id>`; también en JSON. Rechaza comparar con una ejecución de otro
  pipeline.
- ✅ `orch history`: los commits del fichero (siguiéndolo en los renombrados)
  con sus ejecuciones. Una ejecución se atribuye al commit **cuyo texto
  corrió**, no al que había al ejecutar: salió al probarlo, cuando una
  ejecución con cambios que luego se guardaron tal cual aparecía como «nunca
  guardada». Fuera de git, las versiones que han corrido por su huella.
- ✅ `orch runs` y `orch logs` dicen la versión; `run_history` la trae.
- ✅ Tests contra repositorios temporales de verdad (con identidad y sin
  hooks ni firma propios, para no depender de la configuración de nadie):
  guardar, cambiar, descartar, renombrar, versiones que no tocaron el fichero;
  y del diff y del historial guardado.

### 4.2 Versiones desde la aplicación ✅

- ✅ Vista *Versiones* junto a *Ejecución* y *Diseño*: lo que hay sin
  guardar como diff de pipeline, el historial de commits con las ejecuciones
  de cada uno y, al elegir uno, qué cambió en él o qué ha cambiado desde él.
  Fuera de git, las versiones que han corrido según el historial, que se
  comparan y recuperan igual.
- ✅ Guardar versión (commit sólo de ese fichero, sin tocar lo que el usuario
  tuviera preparado), descartar cambios (con confirmación: es lo único que
  pierde trabajo) y recuperar una versión anterior, que vuelve al fichero
  como cambio sin guardar en vez de reescribir el historial. Descartar y
  recuperar llevan la revisión del texto que se estaba viendo: si el fichero
  cambió por debajo, no se hace nada.
- ✅ El detalle de una ejecución dice qué versión corrió y, a petición, qué ha
  cambiado desde entonces.
- ✅ La lógica de cargar versiones y montar el historial está en
  `orch_store::versions`, compartida con la CLI, con tests contra un
  repositorio y un historial reales. El diff de la ventana se comprobó
  pintándolo con los diffs reales de `orch diff`.
- ✅ Pasado por la ventana de verdad, sobre un repositorio de prueba: estado
  al día, cambios sin guardar con su diff, guardar con mensaje (el commit
  aparece en git), cancelar y confirmar el descarte, recuperar una versión
  antigua y el detalle de una ejecución con lo que ha cambiado desde
  entonces. Salió un fallo del historial: una ejecución con cambios que luego
  se guardaron tal cual contaba en dos sitios (el historial guarda también
  cómo estaba git al ejecutar), y ahora se juntan por el texto. De paso, la
  sección *Columnas* del inspector (3.3), también vista en la ventana,
  perdía el margen de su título.

### 4.3 Modo distribuido opcional ✅

Reutilizar los traits de conector sobre un transporte remoto (Arrow Flight es
el candidato natural). Medir antes de decidir, por pasos:

- ✅ **Medir el transporte** (`cargo run --release -p orch-flight --example
  transporte`, 20 M filas, lotes de 65 536, tres pasadas): en memoria 41–48
  ms; sólo codificar y decodificar IPC, 61–67 ms (unos 5 GB/s); Flight por la
  red local, 432–461 ms, unos 700 MB/s (5,6 Gbit/s). Lo caro es gRPC, no la
  serialización. Con un origen que trabaja de verdad (texto), la arista
  remota añade un 25 % (1,59–1,65 s frente a 1,96–2,00 s). En una red de 1
  Gbit el límite es el cable.
- ✅ **Aristas remotas como conectores** (crate `orch-flight`): el sink
  `flight` sirve su flujo a un consumidor y el origen `flight` lo lee de
  cualquier servidor Flight. Contrapresión de HTTP/2 como canal acotado; un
  fallo a mitad del que sirve llega como fallo al que lee (con su motivo), y
  un consumidor que deja de leer antes del final hace fallar al que sirve
  (desde allí no se distingue un `limit` de una caída). Cada uno arranca
  cuando quiere: el que lee insiste, el que sirve espera, con plazos.
  Escuchar fuera de la máquina exige `token`, comparado en tiempo constante.
  Probado con dos ejecutores independientes por la red local y con dos
  procesos `orch run`.
- ✅ **Coordinador y workers** (`orch_flight::cluster`): `worker: <nombre>`
  en un nodo, y un fichero de clúster aparte con cómo llegar a cada worker
  y con qué certificados. El coordinador (`orch run --cluster`) parte el DAG
  donde una arista cruza de máquina —una reescritura del `PipelineSpec`,
  como el pushdown—: `u→b` envía en la máquina de `u` y `u@b` recibe en `b`,
  con el puerto `u` para que una query siga leyendo la tabla `u`. Cada
  `orch worker` sirve las aristas que salen de él por su propio puerto, uno
  por worker y no uno por arista, y ejecuta su parte tal y como está escrita
  (sus secretos, sus rutas). Una sola ejecución: un `run_id`, los eventos de
  todas las máquinas juntos (el progreso de todas en un evento), y un informe
  con las aristas como `edge`, que no cuentan como filas escritas.
- ✅ Un fallo de un nodo viaja por las aristas como en una sola máquina; una
  parte que no puede correr (worker que no responde, conector que no tiene,
  conexión perdida) cancela las demás: con un worker caído, `orch run` falla
  en 0,3 s diciendo cuál, en vez de esperar el plazo de las aristas. TLS
  mutuo obligatorio en todo el clúster. Un `orch run` sin `--cluster` se
  niega a ejecutar nodos con `worker`.
- ✅ Salió al probarlo un fallo del motor: cancelar una ejecución soltando su
  future dejaba viva la tarea que muestrea el progreso, con el canal de
  eventos abierto para siempre (y colgado a quien lo escuchaba). Ahora se
  aborta al soltarla; test propio.
- ✅ Tests con workers de verdad (servicios Flight con TLS mutuo en puertos
  de esta máquina): worker → worker → aquí, aquí → worker → aquí, fallo
  dentro de un worker, worker que no responde, worker sin un conector,
  coordinador de otra CA; y de la división. Probado también con tres
  procesos (`orch worker` ×2 y `orch run --cluster`) y certificados hechos
  con `openssl`.
- ✅ Repartir desde el demonio y la aplicación (Fase 6.1).
- ⬜ Esperas `after` entre nodos de máquinas distintas.
- ✅ **Flight sobre TLS**: bloque `tls` en los dos extremos (PEM), con TLS
  mutuo (`client_ca`) como alternativa al token para escuchar fuera. Sobre
  rustls y aws-lc-rs, los que ya usaba `reqwest`: no se compila otra
  criptografía. Un certificado que no vale falla al momento, sin agotar el
  plazo; uno que no está, en `validate`. Las rutas de certificados dentro de
  `tls` cuelgan ahora de la carpeta del pipeline, como `path`, lo que arregla
  de paso el `root_cert` de PostgreSQL, que se resolvía contra el directorio
  de trabajo. Tests con certificados generados en cada test (rcgen): TLS
  mutuo, cliente sin certificado, cliente y servidor de otra CA, `https` sin
  CA propia.

## Fase 5 — Solidez ✅

Lo que tropezaría a cualquiera que use Orch a diario, antes que funciones
nuevas.

### 5.1 Un historial para todos los procesos ✅

- ✅ DuckDB sólo deja un proceso con el fichero abierto, y cada proceso lo
  retenía toda su vida: con la aplicación abierta no arrancaba un `orch run`
  sobre el mismo historial. Ahora se abre al usarlo y se suelta tras 300 ms
  ocioso; quien lo encuentra cogido reintenta hasta 15 s.
- ✅ Medido antes de decidir: abrir, escribir un lote y cerrar cuesta unos
  89 ms en debug, frente a 8,5 ms con la conexión abierta (casi todo, el
  checkpoint al cerrar). Por eso no se abre en cada operación: una ráfaga
  reutiliza la conexión, y el escritor de eventos vuelca cada segundo en vez
  de cada 200 ms, dejando hueco entre volcados. Lo que se ve en vivo no
  cambia: llega por el canal de eventos.
- ✅ Tests con dos procesos de verdad (el test se relanza a sí mismo): dos
  escribiendo a la vez con la cadencia del escritor, y un proceso ocioso que
  no retiene el fichero. Probado también con dos `orch run` simultáneos.

### 5.2 Sinks de fichero atómicos ✅

- ✅ CSV y Parquet escriben en un temporal junto al destino (misma carpeta,
  así que renombrar es atómico) y lo ponen en su sitio sólo si todo fue bien,
  con el contenido en disco antes del renombrado. Si el pipeline falla a
  mitad, el destino se queda como estaba —el resultado anterior, o ninguno—
  y no quedan temporales.
- ✅ Quien confirma es el nodo, no el escritor: con un fallo aguas arriba, el
  escritor termina limpio con lo que le llegó, y eso no es un resultado. Era
  justo el caso que dejaba un CSV cortado con cara de bueno.
- ✅ Tests con un origen que falla a mitad (con y sin resultado anterior, CSV
  y Parquet) y con uno que termina bien sustituyendo al anterior.

### 5.3 El demonio ve los cambios de su carpeta ✅

- ✅ Cada dos segundos compara el contenido de cada pipeline de su carpeta
  (la huella del texto, no la fecha: tocar sin cambiar no cuenta) y, si algo
  cambió, la relee y valida sólo lo que cambió. Por sondeo y no por avisos
  del sistema: no depende de que la carpeta los dé (una de red, por ejemplo).
- ✅ El planificador recibe los pipelines nuevos con `replace`, puro como el
  resto: lo que no cambió de disparador conserva su próximo disparo y su
  turno en cola; lo que cambió se recalcula desde ahora; lo que desaparece
  deja de dispararse, y lo que corría termina con su versión. Un pipeline
  que se rompe a medio editar deja de dispararse con un aviso y vuelve al
  arreglarse.
- ✅ El demonio ya no se niega a arrancar con la carpeta vacía o sin nada
  válido: con recarga, esperar a que aparezca algo tiene sentido.
- ✅ Tests del planificador con fechas escritas a mano (sin cambios, cron
  cambiado, nuevo y quitado, encadenamiento nuevo, cola de uno desactivado,
  quitar uno que corre) y probado con el demonio de verdad: añadir, romper,
  arreglar, borrar, desactivar, cambiar contenido y tocar sin cambiar.

## Fase 6 — Completar lo nuevo ✅

### 6.1 Pipelines repartidos desde el demonio y la aplicación ✅

- ✅ `Motor` compartido por `orch run`, el demonio y la aplicación: decide
  por el pipeline —con algún `worker:`, el coordinador; sin ninguno, el motor
  de siempre— y un pipeline repartido sin clúster se rechaza diciendo qué
  falta, en vez de correr en la máquina equivocada.
- ✅ Un coordinador para varias ejecuciones a la vez: cada una publica en su
  propio canal (las alertas de una no saltan con la otra), y el servicio que
  sirve las aristas que salen de aquí se abre una vez y vive con el
  coordinador, en vez de pelearse dos ejecuciones por el mismo puerto.
- ✅ `orch daemon --cluster` (o `ORCH_CLUSTER`) valida y dispara los
  repartidos; `orch validate --cluster` comprueba lo de aquí entero y que el
  pipeline se puede partir, sin necesitar los workers encendidos. La
  aplicación toma el clúster de `ORCH_CLUSTER`; sin él, la lista marca los
  repartidos con lo que falta.
- ✅ Tests: dos ejecuciones a la vez con el mismo coordinador (y su servidor
  de aristas compartido) sin mezclar eventos, y la decisión del `Motor`.
  Probado con el demonio de verdad: dos workers y un pipeline repartido con
  cron cada minuto, que se disparó a su hora y quedó en el historial.
- ⬜ Pasarlo por la ventana: el camino de la aplicación es el mismo `Motor`
  que el demonio, pero no se ha ejecutado desde la ventana con un clúster.

### 6.2 Alertas como notificación del sistema ✅

- ✅ La aplicación avisa con una notificación del sistema cuando salta una
  alerta y la ventana no está al frente (detrás de otra, minimizada); con la
  ventana delante no, porque el aviso ya sale en ella. El título dice qué
  pasó y en qué pipeline; el cuerpo, el detalle.
- ✅ Sale desde Rust, donde nace la alerta, con el plugin de notificaciones
  de Tauri: llega aunque la interfaz no esté pintando nada, y en un hilo
  aparte, porque en Linux espera la respuesta de D-Bus.
- ✅ Probado en la ventana con un pipeline que se cuelga: minimizada, llegan
  por D-Bus la notificación del cuelgue a los 3 s y la del fallo después; al
  frente, ninguna, y el aviso sale dentro. De paso quedó visto en la ventana
  el aviso de la 3.2, que no se había podido comprobar.
- ⬜ La notificación se presenta con el nombre del ejecutable (`orch-app`):
  el plugin no deja elegirlo. Ver qué muestra con la aplicación instalada.

### 6.3 Lineage entre pipelines ✅

- ✅ Un origen que lee lo que escribe un sink hereda las columnas y los
  orígenes de ese sink: del mismo pipeline (escribir un Parquet y releerlo,
  con `after`) o de otro de la misma carpeta. El vínculo es la ruta (`path`),
  normalizada sin tocar el disco: `./out/x.csv` y `out/x.csv` son el mismo.
- ✅ Aunque el fichero aún no exista: el origen toma prestado el esquema que
  escribirá el sink, recortado a sus `columns` (la convención del pushdown).
  **Sólo para el lineage**: al validar y ejecutar manda lo que hay en disco,
  porque un esquema prestado equivocado haría planificar contra columnas que
  no llegan. Un test lo comprueba.
- ✅ Una carpeta se calcula en orden —primero quien escribe, luego quien
  lee— y un ciclo (dos que se leen mutuamente) se calcula sin enlazar en vez
  de dar vueltas. Lo de otro pipeline sale con su nombre (`filtra-clientes:
  leer.spend`), y el impacto de una columna llega a las salidas de los
  pipelines que la leen después.
- ✅ `orch lineage` y el inspector de la aplicación miran la carpeta del
  pipeline; en el inspector, un origen de otro pipeline se nombra pero no es
  un botón, porque no está en ese lienzo. `examples/pipelines/parquet.yaml`,
  que antes se cortaba en `releer`, ahora llega hasta el CSV.
- ✅ Tests: el caso de `parquet.yaml`, dos pipelines pasados en orden inverso,
  un ciclo, y que validar no usa el esquema prestado.
- ⬜ Seguir también tablas (PostgreSQL), no sólo ficheros.

---

## Deuda técnica anotada

Cada punto está documentado en el código donde aplica y, si se nota al usar
Orch, listado en las limitaciones del README:

- Fan-in por concatenación, no intercalado.
- Las ramas independientes no se cancelan cuando otra falla.
- Un nodo `sql` con varias entradas necesita que todas declaren esquema.
- El lienzo no tiene deshacer: cada cambio se escribe al momento.
- La vista general del lienzo con cientos de nodos a la vista es lenta: es el
  límite de pintarla con elementos del DOM.
- `serde_yaml` está sin mantenimiento; migrar cuando haya un sucesor claro.
  El editor del lienzo ya no depende de él para escribir (usa `yaml-edit`),
  sólo para leer.
