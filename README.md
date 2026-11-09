# Orch

Plataforma local de orquestación y transporte de datos. La especificación
completa está en [doc.md](doc.md); el desglose por fases, en
[docs/PLAN.md](docs/PLAN.md).

**Estado: Fases 0, 1 y 2 completas.** Motor de DAG con ejecución en
streaming, transformaciones SQL sobre DataFusion, conectores
CSV/Parquet/PostgreSQL/REST, historial en DuckDB y demonio con disparadores
cron, más la aplicación de escritorio (Tauri) con el lienzo del pipeline, que
edita el YAML tocando sólo lo que cambia y aguanta pipelines de cientos de
nodos. Las transformaciones de terceros llegan como plugins de WebAssembly,
aislados y con un SDK en Rust. Lo siguiente es el monitoreo avanzado
(Fase 3).

---

## Requisitos

Rust estable (1.97 o superior: lo exige wasmtime, el motor de los plugins), y
Node.js 20.19 o superior para la interfaz de la aplicación de escritorio (lo
exige Vite 7). Para compilar plugins hace falta además el target de
WebAssembly: `rustup target add wasm32-wasip2`.

### Windows

Con el toolchain **MSVC**:

```powershell
# Build Tools de Visual Studio (requiere elevación; ~4 GB)
winget install --id Microsoft.VisualStudio.2022.BuildTools -e `
  --override "--quiet --wait --norestart --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"

winget install --id Rustlang.Rustup -e
rustup default stable-x86_64-pc-windows-msvc
```

Cierra y reabre la terminal para que `cargo` entre en el `PATH`.

> **El toolchain GNU no sirve.** El mingw autocontenido de rustup incluye
> `dlltool.exe` pero no el ensamblador `as.exe`, así que falla al generar las
> import libraries de `raw-dylib` que necesita `getrandom`. El toolchain
> `gnullvm` tampoco: espera un `x86_64-w64-mingw32-clang` externo. MSVC es
> además lo que exigen Tauri y DuckDB.

### Linux

Probado en Linux Mint 22.3 (base Ubuntu 24.04). Rust con rustup, sin
privilegios:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Y las bibliotecas del sistema: WebKitGTK y compañía para la ventana de Tauri,
D-Bus (lo enlaza `tao`) y OpenSSL (`native-tls`, en el conector PostgreSQL,
que arrastran tanto la CLI como la aplicación):

```bash
sudo apt install -y build-essential libwebkit2gtk-4.1-dev libgtk-3-dev \
  libsoup-3.0-dev libayatana-appindicator3-dev librsvg2-dev libssl-dev \
  libxdo-dev libdbus-1-dev
```

El llavero (`${keyring:...}`) habla con Secret Service por `zbus`, en Rust
puro, así que no necesita paquetes. Sí necesita que la sesión tenga un
llavero por defecto: si falla con `Object does not exist at path
…/collection/login`, créalo en *Contraseñas y claves* (Seahorse).

## Compilar y probar

```sh
cargo build --workspace
cargo test  --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

> **Con 8 GB de RAM, compila con `-j 2`** (o `CARGO_BUILD_JOBS=2`, que vale
> también para `tauri dev`). Varios `rustc` en paralelo sobre DataFusion
> agotan la memoria: en Windows el compilador muere con
> `STATUS_STACK_BUFFER_OVERRUN` y en Linux lo mata el sistema, y en los dos
> casos quedan artefactos a medio escribir en `target/debug/deps` que luego
> dan `E0786`/`E0460`. Si ya ha pasado, hay que limpiar los crates afectados
> (`cargo clean -p <crate>`) y recompilar. La primera build completa tarda:
> DuckDB se compila desde fuente.

Los tests de PostgreSQL y el del llavero **se saltan solos** si no hay
servidor (`ORCH_TEST_PG_DSN`) o almacén de credenciales, y entonces cuentan
como aprobados. Los de plugins, igual si no están compilados a WASM: hay que
hacerlo antes, porque compilar desde un test bloquearía el mismo `target`
que está usando `cargo test`. El aviso sólo se ve con `-- --nocapture`.

```sh
cargo build --release --target wasm32-wasip2 \
  -p orch-plugin-identidad -p orch-plugin-mayusculas -p orch-plugin-fisgon
```

Para medir el lienzo con un pipeline grande:

```sh
# Genera uno de 500 nodos y mide cada operación del editor sobre él.
cargo run --release -p orch-edit --example grande -- 500 /tmp/grande/grande.yaml

# La app con el medidor: nodos en el DOM, tiempos de render y de fotograma.
VITE_ORCH_MEDIR=1 ORCH_PIPELINES=/tmp/grande npx --prefix ui tauri dev
```

## Uso

```sh
# Qué conectores y transformaciones hay disponibles
cargo run -p orch-cli -- connectors

# Comprueba el DAG y la config de cada nodo sin ejecutar nada
cargo run -p orch-cli -- validate examples/pipelines/csv_to_csv.yaml

# Estructura del pipeline
cargo run -p orch-cli -- graph examples/pipelines/csv_to_csv.yaml

# Ejecutar, con eventos en vivo
cargo run -p orch-cli -- run examples/pipelines/csv_to_csv.yaml --follow

# Historial de ejecuciones, y el detalle de una (basta el prefijo del id)
cargo run -p orch-cli -- runs
cargo run -p orch-cli -- logs 2d98

# Demonio: vigila un directorio y dispara lo que toque
cargo run -p orch-cli -- daemon --dir examples/pipelines --keep-days 30

# Informe en JSON, para encadenar con otras herramientas
cargo run -p orch-cli -- run examples/pipelines/csv_to_csv.yaml --format json

# Aplicación de escritorio (la primera vez, `npm --prefix ui install`)
npx --prefix ui tauri dev
```

Medir el motor aislado de disco y red (usa `--release`, la diferencia es de
un orden de magnitud):

```sh
cargo run --release -p orch-cli -- run examples/pipelines/benchmark.yaml
```

Líneas base actuales (10 M de filas, `batch_size: 65536`, sin tocar disco):

| Pipeline | Tiempo | Throughput |
|---|---|---|
| `benchmark.yaml` — generador → null | 54 ms | ~185 M filas/s |
| `benchmark_sql.yaml` — generador → `filter` → null | 65 ms | ~155 M filas/s de entrada |

Es el techo del orquestador sin I/O; cualquier cambio en el ejecutor debería
compararse contra estas cifras. El nodo de DataFusion añade ~11 ms sobre 10 M
de filas (~1 ns/fila) y no materializa nada.

Con PostgreSQL 17 en la misma máquina, un millón de filas de tres columnas:

| Operación | Tiempo | Throughput |
|---|---|---|
| Carga con `COPY BINARY` | 1,15 s | ~870 K filas/s |
| Lectura por cursor | 691 ms | ~1,45 M filas/s |

Y lo que gana el pushdown sobre esa misma tabla, con un `filter` que deja
1 000 de las 1 000 000 de filas y un `select` de dos columnas:

| | Filas leídas | Tiempo |
|---|---|---|
| Filtrando en Orch | 1 000 000 | 1,16 s |
| Empujado a PostgreSQL | 1 000 | **127 ms** |

Tamaño en disco del mismo millón de filas del generador:

| Formato | Tamaño |
|---|---|
| CSV | 25,8 MB |
| Parquet (snappy) | 13,3 MB |
| Parquet (zstd) | **3,2 MB** |

Logs detallados con `ORCH_LOG`, que admite filtros por módulo:
`ORCH_LOG=orch_core=debug,orch_connectors=debug` en bash, o
`$env:ORCH_LOG = "orch_core=debug,orch_connectors=debug"` en PowerShell. Vale
igual para la CLI que para la aplicación.

`orch run` devuelve código de salida 0 sólo si todos los nodos terminaron
correctamente.

## Aplicación de escritorio

El motor corre **dentro del mismo proceso**, como biblioteca: no hay
servidor, ni puerto, ni serialización de los datos. Lo único que cruza al
webview son eventos y métricas — los lotes de Arrow no salen de Rust nunca.
Por eso la ventana sigue fluida mientras el motor mueve millones de filas.

El puente de eventos es otro suscriptor del mismo canal `broadcast` que ya
alimentaba al visor de la CLI y al escritor de DuckDB: la UI no necesitó
nada nuevo del motor, sólo escuchar donde ya se estaba emitiendo.

La interfaz sigue las HIG de Apple como principio y no como maquillaje:
tipografía del sistema, una escala tipográfica de cuatro tamaños, jerarquía
por peso y color antes que por adornos, modo claro/oscuro siguiendo al
sistema sin interruptor propio, y movimiento sólo donde comunica algo —el
latido de un nodo en curso, la entrada de un evento nuevo—, respetando
`prefers-reduced-motion`.

Con `tauri dev` el proceso arranca en `crates/orch-app`, así que no encuentra
solo `examples/pipelines`. La carpeta se le indica con `ORCH_PIPELINES` (ruta
absoluta), o se elige desde el pie del lateral. `ORCH_STORE` apunta al
historial; si es el mismo `orch.duckdb` que usa la CLI, la aplicación ve
también sus ejecuciones:

```bash
ORCH_PIPELINES=$PWD/examples/pipelines ORCH_STORE=$PWD/orch.duckdb \
  npx --prefix ui tauri dev
```

La marca vive en `ui/src/assets/marca`. El logo de la interfaz
(`ui/src/Marca.tsx`) copia sus trazos para seguir al modo claro/oscuro, y el
azul de la marca es el acento. Los iconos de `crates/orch-app/icons` salen de
`orch-icono-app.png` con `npx --prefix ui tauri icon`; de lo que genera, sólo
se guardan los que lista `tauri.conf.json`. El primer PNG de esa lista es el
icono de la ventana en Linux, y va primero el de 256 px a propósito: con el de
512 px, GTK lo descarta sin avisar (no cabe en una petición de X11) y la
ventana se queda sin icono en la barra de tareas.

### El lienzo

Cada pipeline se puede ver como el grafo que es, con el control segmentado
**Ejecución / Diseño** de la barra. Los nodos se colocan solos en capas de
izquierda a derecha, siguiendo el flujo de los datos; las barreras `after`
van punteadas porque ordenan pero no transportan; y al elegir un nodo, el
inspector enseña su configuración.

Tres decisiones que conviene conocer:

- **El lienzo dibuja el fichero, no el plan.** No se aplica pushdown: ver
  cómo un `filter` desaparece porque el motor lo absorbió dentro del origen,
  justo mientras lo estás editando, sería desconcertante.
- **El grafo se dibuja aunque no valide.** Un diseñador que sólo abre
  pipelines correctos falla justo cuando hace falta, que es para arreglar el
  que está roto. El motivo del fallo se enseña encima, y el conector que no
  existe se marca en el nodo.
- **Un pipeline grande se abre a un tamaño legible, por el principio.** Si
  no cabe entero con el texto de los nodos legible, se abre arriba a la
  izquierda, donde están los orígenes, en vez de en una vista general
  ilegible. Esa vista general sigue en el botón de encuadre, pero es la más
  cara de pintar: cada nodo a la vista es un elemento que se repinta en cada
  fotograma.

El inspector carga el pipeline **sin expandir los secretos**: lo que se ve es
`${env:PG_DSN}` y no su valor. Por eso un pipeline cuyo secreto no está
definido en esta máquina se puede abrir y editar igual, aunque no se pueda
ejecutar.

### Editar desde el lienzo

- **Añadir:** arrastra un componente de la paleta al lienzo, o haz clic en
  él. El nodo nuevo queda elegido y el inspector marca lo obligatorio que
  falta.
- **Conectar:** arrastra del conector de salida de un nodo al de entrada de
  otro. Lo que el motor rechazaría (un sink como origen, una arista repetida)
  el lienzo ni lo ofrece.
- **Quitar:** elige un nodo o una arista y pulsa Supr, o usa *Eliminar* en el
  inspector. Un nodo se va con sus aristas y con las esperas `after` que lo
  nombraban.
- **Configurar:** cada campo del inspector se guarda al salir de él o con
  Intro, sin botón de guardar. Vaciarlo, o pulsar ↺, lo quita del fichero y
  vuelve a su valor por defecto. Las listas se escriben separadas por comas
  y los mapas, una línea `clave: valor` por entrada.
- **Renombrar:** clic en el id, arriba del inspector.
- **Colocar:** arrastra los nodos. Al mover el primero se guardan las
  posiciones de todos —si sólo se fijara ese, los demás se seguirían
  recolocando solos a su alrededor—, y desde entonces un componente nuevo
  aparece donde lo sueltas. El botón de cuadrícula de los controles borra las
  posiciones y el lienzo vuelve a colocarlos solo.

Las posiciones van en un bloque `layout` al final del fichero, lejos de lo que
se edita a mano y con un comentario que explica qué es:

```yaml
# Dónde dibuja cada nodo el lienzo de Orch. El motor no lo usa; si se borra,
# el lienzo vuelve a colocarlos solo.
layout:
  leer_clientes: [0, 0]
  pagados: [244, 261]
```

El motor lo ignora, y su lectura es tolerante: una entrada mal escrita se
descarta, porque algo cosmético nunca puede impedir ejecutar un pipeline.

Lo que garantiza:

- **El YAML se edita, no se reescribe.** Un pipeline es código que la gente
  escribe a mano, con comentarios que explican el porqué y SQL formateado con
  cuidado. El editor ([orch-edit](crates/orch-edit)) trabaja sobre un árbol
  sintáctico sin pérdidas y toca sólo las líneas del cambio, imitando el
  estilo del fichero: la línea en blanco entre nodos, las aristas
  `{ from: a, to: b }` en una línea, `after` junto al componente. Al quitar
  un nodo se va también el comentario que lo describía.
- **Cada cambio se comprueba antes de escribirlo.** Se aplica al modelo y al
  texto, y el texto resultante se relee con el mismo parser que usa el motor:
  si no dice exactamente lo esperado, no se escribe nada. Un editor que a
  veces dice «esto no lo sé hacer» es aceptable; uno que a veces cambia algo
  que no le pediste, no.
- **No pisa cambios ajenos.** Si el fichero cambió fuera de Orch desde que se
  abrió (otro editor, un `git checkout`), el cambio se rechaza y el lienzo
  recarga lo que hay en disco. La escritura es atómica: un corte a mitad
  nunca deja el pipeline a medio escribir.
- **Renombrar no rompe un `sql`.** El puerto de una arista se llama, por
  defecto, como su nodo de origen, y un `sql` lo usa como nombre de tabla.
  Si el destino es un `sql`, renombrar el origen fija `port:` con el nombre
  antiguo para que la query siga funcionando.
- **Los ids nuevos son minúsculas, números y `_`**, por ese mismo motivo: un
  guion obligaría a entrecomillar la tabla, y DataFusion pasa a minúsculas lo
  que no va entre comillas.

## Formato de pipeline

```yaml
version: 1
name: mi-pipeline

settings:
  batch_size: 8192          # filas por lote de Arrow
  channel_capacity: 4       # lotes en vuelo por arista (contrapresión)

nodes:
  - id: leer
    type: source            # source | transform | sink
    connector: csv          # `op:` en vez de `connector:` para transform
    config: { path: datos.csv }
    after: []               # dependencias de orden puro, sin flujo de datos
    retry: { max_attempts: 3, backoff_ms: 250, backoff_multiplier: 2.0 }

  - id: escribir
    type: sink
    connector: csv
    config: { path: salida.csv }

edges:
  - { from: leer, to: escribir }
  # `port:` da nombre a la entrada; por defecto es el id del nodo de origen.
  # Sólo importa en un nodo `sql`, que registra cada puerto como una tabla.
  - { from: otra_rama, to: unir, port: pedidos }
```

Las rutas de `path` se resuelven **contra la carpeta del fichero YAML**, no
contra el directorio de trabajo. Una carpeta de pipelines es así portable: se
comporta igual lanzada con la CLI desde la raíz del repositorio que desde la
aplicación de escritorio, que arranca desde donde el sistema quiera. Una ruta
absoluta se deja tal cual.

Si alguien movió nodos en el lienzo, al final del fichero hay además un
bloque `layout` con sus posiciones. El motor lo ignora; ver
[Editar desde el lienzo](#editar-desde-el-lienzo).

### Programación

El *cuándo* vive con el *qué*, en el mismo fichero. Sin bloque `schedule`, un
pipeline sólo corre a mano:

```yaml
schedule:
  cron: "30 2 * * 1-5"              # cinco campos, como en Unix
  timezone: America/Santo_Domingo   # sin esto, UTC
  after: [otro-pipeline]            # o tras el éxito de otro
  concurrency: skip                 # skip | queue | allow
  enabled: true
```

El cron es el de cinco campos que todo el mundo escribe, **traducido** al de
seis que usa la librería por debajo — incluida la numeración del día de la
semana, que allí empieza en 1 y en Unix en 0. Sin esa traducción, un `1-5`
dispararía de domingo a jueves en vez de lunes a viernes.

`orch daemon --dir <directorio>` valida todos los pipelines al arrancar
(descubrir a las 3 de la mañana que uno no compila no sirve de nada), rechaza
nombres duplicados y `after` a pipelines que no existen, y con Ctrl-C deja de
disparar pero espera a lo que esté en vuelo.

Por defecto, si toca arrancar y la ejecución anterior sigue viva, el disparo
se salta: un pipeline que tarda más que su intervalo no debe ir acumulando
copias de sí mismo. Con `queue` se guarda uno —sólo uno, para que un atasco
de una hora no se convierta en sesenta ejecuciones seguidas— y con `allow`
arrancan a la vez.

### Joins

Un nodo `sql` ve cada entrada como una tabla con el nombre de su puerto, así
que unir dos ramas no necesita sintaxis nueva:

```yaml
nodes:
  - { id: clientes, type: source, connector: csv, config: { path: clientes.csv } }
  - { id: pedidos,  type: source, connector: csv, config: { path: pedidos.csv } }
  - id: cruzar
    type: transform
    op: sql
    config:
      query: "SELECT c.nombre, sum(p.importe) AS total
              FROM clientes c JOIN pedidos p ON c.id = p.cliente_id
              GROUP BY c.nombre"
edges:
  - { from: clientes, to: cruzar }
  - { from: pedidos, to: cruzar }
```

Cuando el nodo tiene una sola entrada, se registra además como `input`, que es
lo que usan `filter`, `derive` y `aggregate`. Esas tres exigen exactamente una
entrada y lo dicen en `validate`; para combinar varias hay que usar `sql`.
Ejemplo completo en [examples/pipelines/join.yaml](examples/pipelines/join.yaml).

### Componentes disponibles

Esta tabla es un resumen. La lista exacta y siempre al día la da el propio
binario, porque sale del mismo struct que deserializa la config:

```sh
orch connectors           # todos los nombres
orch connectors csv       # qué config acepta, con tipos y valores por defecto
```

```
source `csv`
  path        texto      obligatorio
  has_header  booleano   por defecto: true
  delimiter   texto      por defecto: ","
  infer_rows  entero     por defecto: 1000
              Filas que se leen para inferir el esquema. `0` = escanear el
              fichero completo (más lento, pero seguro con columnas cuyo
              tipo sólo se distingue al final).
```

Lo mismo alimenta al inspector del lienzo, que además enseña los campos que
**no** están escritos, con el valor que tomarían.

| Tipo | Nombre | Config |
|---|---|---|
| source | `csv` | `path`, `has_header`, `delimiter`, `infer_rows` (0 = fichero entero), `batch_size` |
| source | `parquet` | `path`, `columns` (proyección empujada al fichero), `batch_size` |
| source | `postgres` | `dsn`, y `query` o bien `table`/`columns`/`where`; `fetch_size` |
| source | `rest` | `url`, `headers`, `query`, `records_path`, `pagination`, `schema`, `retry`, `rate_limit_per_second` |
| source | `generator` | `rows`, `with_text`, `batch_size` — datos sintéticos deterministas |
| transform | `select` | `columns: [..]` — proyecta y reordena |
| transform | `rename` | `columns: { viejo: nuevo }` |
| transform | `limit` | `rows` — corta y detiene la lectura aguas arriba |
| transform | `filter` | `where` — predicado SQL |
| transform | `derive` | `columns: { nuevo: expresión }` |
| transform | `aggregate` | `group_by: [..]`, `aggregates: { alias: "sum(x)" }` |
| transform | `sql` | `query` — SQL libre sobre la entrada |
| sink | `csv` | `path`, `has_header`, `delimiter`, `create_dirs` |
| sink | `parquet` | `path`, `compression` (`snappy`/`zstd`/`gzip`/`lz4`/`none`), `row_group_size`, `create_dirs` |
| sink | `postgres` | `dsn`, `table`, `columns`, `truncate` — carga con `COPY BINARY` en una transacción |
| sink | `rest` | `url`, `method`, `headers`, `rows_per_request`, `body` (`json_array`/`ndjson`), `wrap_in`, `retry` |
| sink | `null` | descarta; para dry-runs y benchmarks |

El conector REST reintenta sólo los códigos transitorios (408, 429, 5xx) y
respeta `Retry-After`: un 401 no mejora repitiéndolo. Pagina por número de
página, por offset o por cursor, y `max_pages` frena una API que nunca dice
que se acabó. Las cabeceras no aparecen en los logs, porque llevan tokens.

### Secretos

Una contraseña no debe vivir en el YAML, que se versiona y se comparte. En su
lugar se escribe una referencia, que se resuelve **al cargar el pipeline** —
así una variable que falta se nota en `orch validate` y no a mitad de una
ejecución:

```yaml
config:
  dsn: "postgres://app:${env:PGPASSWORD}@localhost/ventas"
```

Dos orígenes disponibles:

| Referencia | De dónde sale |
|---|---|
| `${env:NOMBRE}` | Variable de entorno |
| `${keyring:servicio/usuario}` | Almacén del sistema: Credential Manager, Llavero o Secret Service |

Un esquema mal escrito (`${ENV:X}`) es un error, no un literal: si pasara tal
cual a una cadena de conexión, el fallo sería incomprensible. Un `${...}` sin
esquema (`${HOME}`) sí se deja literal, por si lo interpreta el destino.

### TLS en PostgreSQL

El modo sale del `sslmode` del DSN, como en libpq. Lo que cambia es cuándo se
**verifica** el certificado:

| `sslmode` | Cifra | Verifica por defecto |
|---|---|---|
| `disable` | no | — |
| `prefer` (defecto) | si el servidor puede | **no** |
| `require` | siempre | **sí** |

Con `prefer` no se verifica porque es cifrado oportunista, igual que en libpq.
Con `require` sí, y **ahí se diverge de libpq a propósito**: allí `require`
cifra sin comprobar nada, lo que protege del espionaje pasivo pero no de un
intermediario. Si alguien pide TLS explícitamente, que sirva de algo.

```yaml
config:
  dsn: "host=db.interno user=app password=${env:PGPASSWORD} sslmode=require"
  tls:
    root_cert: /etc/orch/ca.pem   # para un certificado propio
    # verify: false               # o desactivarlo, a sabiendas
```

## Plugins WASM

Lo que Orch no trae se puede añadir como plugin: una transformación escrita en
Rust (o en cualquier lenguaje que compile a un componente de WebAssembly) que
se usa en el YAML como cualquier otra, con `op:`. Aparece en la paleta del
lienzo, en su propio grupo; el inspector genera el formulario de su config a
partir del esquema que declara el propio plugin; y `orch connectors` la lista
marcada como `(plugin)`.

Los plugins viven en la subcarpeta `plugins/` de la carpeta de pipelines, para
que una carpeta sea portable con todo lo que necesita, igual que sus rutas se
resuelven contra ella. `--plugins` u `ORCH_PLUGINS` apuntan a otra.

Escribir uno con el SDK ([`orch-plugin`](crates/orch-plugin)):

```rust
use orch_plugin::{RecordBatch, Transformacion};

#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct Config {
    /// Columna de texto que se pasa a mayúsculas.
    columna: String,
}

struct Mayusculas { config: Config }

impl Transformacion for Mayusculas {
    const NOMBRE: &'static str = "mayusculas";
    type Config = Config;

    fn nueva(config: Config) -> Result<Self, String> { Ok(Self { config }) }

    fn procesar(&mut self, lote: RecordBatch) -> Result<Vec<RecordBatch>, String> {
        // ...
    }
}

orch_plugin::exportar!(Mayusculas);
```

```sh
cargo build --release --target wasm32-wasip2 -p orch-plugin-mayusculas
cp target/wasm32-wasip2/release/orch_plugin_mayusculas.wasm mis-pipelines/plugins/
```

El ejemplo completo y otros dos están en [plugins/](plugins). El contrato,
para escribirlos en otro lenguaje, es [wit/orch-plugin.wit](wit/orch-plugin.wit);
de momento sólo está probado desde Rust.

Lo que garantiza:

- **Aislamiento.** Cada nodo que usa un plugin tiene su propia instancia, sin
  ficheros, sin red y sin variables de entorno. Hay un test que lo comprueba
  con un plugin que lo intenta, contra un fichero que existe, un puerto que
  escucha y una variable que está.
- **Límites.** 512 MB de memoria por instancia y 30 s por llamada: un plugin
  colgado hace fallar su nodo, no deja el pipeline esperando. Sus llamadas
  corren fuera de los hilos del motor, así que tampoco frena a los demás
  nodos.
- **Nada se suplanta.** Un plugin que se llame como un componente que ya
  existe no se carga: un `sql` de terceros que sustituyera en silencio al de
  verdad sería un agujero, no una extensión.
- **Errores en su sitio.** Una config que el plugin rechaza es un error de
  config del nodo, y un fallo al procesar es un fallo del nodo, igual que con
  uno nativo.

Lo que cuesta, medido con la misma transformación compilada de las dos formas
(`cargo run --release -p orch-wasm --example medir`):

| | Nativo | Plugin |
|---|---|---|
| Cruzar la frontera (plugin que no hace nada) | — | ~30 ns/fila |
| Pasar a mayúsculas una columna de texto | ~35 ns/fila | ~110 ns/fila (×3) |
| Compilar el plugin (2,4 MB), la primera vez | — | ~1 s |
| Cargarlo en los arranques siguientes (caché) | — | ~40 ms |

Junto a cualquier E/S real —un CSV, PostgreSQL a ~700 ns por fila, una API—
es poco. Para el núcleo en memoria, que mueve una fila en 5 ns, no: por eso
los conectores y transformaciones de Orch son nativos, y los plugins son para
lo que Orch no trae. Los lotes cruzan en trozos de 8 192 filas, el tamaño que
mejor sale: por encima, el lote serializado deja de caber en la caché del
procesador y cada fila cuesta casi el doble.

## Arquitectura

```
crates/
  orch-core/         modelo de pipeline, validación del DAG, ejecutor, traits de conector
  orch-store/        historial de ejecuciones y métricas, en DuckDB
  orch-schedule/     disparadores cron y encadenamiento entre pipelines
  orch-edit/         edición del YAML desde el lienzo, tocando sólo lo que cambia
  orch-wasm/         carga y ejecución de plugins WASM, aislados
  orch-plugin/       SDK para escribir plugins en Rust
  orch-connectors/   implementaciones nativas (CSV, generador, null, transformaciones)
  orch-sql/          transformaciones con expresiones, sobre DataFusion
  orch-rest/         conector HTTP: paginación, límite de tasa, reintentos
  orch-postgres/     conector PostgreSQL: cursor de lectura, COPY BINARY de escritura
  orch-cli/          binario `orch`
  orch-app/          aplicación de escritorio (Tauri)
ui/                  frontend de la aplicación (React + Vite)
plugins/             plugins de ejemplo
wit/                 el contrato entre Orch y sus plugins
```

El core no conoce ninguna implementación concreta: recibe un `Registry` con lo
que haya disponible. Por eso el mismo motor sirve a la CLI, al demonio y a la
aplicación de escritorio sin cambios, y un plugin WASM se registra igual que
un conector nativo.

### Decisiones que conviene conocer

**Dataflow por streaming, no planificación por capas.** Todos los nodos
arrancan a la vez; cada arista es un canal acotado de `RecordBatch`. El sink
escribe mientras el source todavía lee, y la memoria queda acotada por
`batch_size × channel_capacity × nº de aristas`, no por el tamaño del dataset.

**No hay un "máximo de nodos en paralelo".** Sería un semáforo capaz de
bloquear el pipeline entero: un productor con permiso esperando a un
consumidor que nunca obtiene el suyo. La palanca real es `channel_capacity`.

**El fan-out no copia datos.** Los `RecordBatch` de Arrow son `Arc` por
dentro; repartir el mismo flujo a N destinos clona punteros.

**Un fallo aguas arriba nunca se reporta como éxito.** Cuando una entrada se
cierra porque su nodo falló, el consumidor recibe un error, no un fin de flujo
limpio — de lo contrario un sink escribiría un resultado truncado y lo daría
por bueno. Esos nodos aparecen como `omitido` en el informe.

**Cortar antes de tiempo es legítimo.** Un `limit` que alcanza su cuota cierra
su entrada; el origen lo detecta y deja de producir en vez de fallar.

**Los reintentos sólo ocurren si no hay datos en vuelo.** Repetir un nodo que
ya consumió o emitió lotes duplicaría o perdería filas. Si el nodo ya se movió,
el fallo es definitivo y el informe lo dice.

**DataFusion entra como transformación aislada, no como planificador global.**
Cada nodo SQL registra sus entradas como `StreamingTable` y DataFusion tira de
los batches: `filter` y `derive` no acumulan nada. La decisión está medida:
tres nodos SQL encadenados tardan lo mismo que una sola query equivalente (86
ms frente a 90 ms sobre 10 M de filas), porque cada nodo es una tarea de Tokio
y las etapas se solapan. Fusionar transformaciones no compra nada, y mantener
los conectores independientes de DataFusion sí vale. Lo que un planificador
global sí daría —empujar filtros hasta el origen— se consigue con pushdown por
conector, en la Fase 0.3.

**El trabajo se empuja hasta el origen cuando el conector sabe hacerlo.** Un
`filter` o un `select` pegado a un origen se absorbe en su config y el nodo
desaparece: PostgreSQL resuelve el `WHERE` y por la red viaja sólo el
resultado; Parquet ni descomprime las columnas que no se piden. Es la
ganancia que un planificador global habría dado, conseguida sin acoplar el
motor a DataFusion — la reescritura es un paso sobre el pipeline, antes de
construir el DAG.

Sólo se empuja si el origen tiene **un único consumidor**: con más,
recortarle columnas o filas cambiaría lo que ven los demás. `orch graph` y
`orch validate` dicen qué se empujó, porque el pipeline que se ejecuta ya no
es el que está escrito:

```
empujado hasta el origen:
  filter de `filtrar` → `leer`
  select de `recortar` → `leer`
```

**Cada ejecución queda guardada, y el informe dice dónde está el cuello de
botella.** `Output::send` y `InputPort::recv` intentan primero sin esperar:
cuando hay hueco —el caso normal— no se lee el reloj ni una vez, así que
medir sólo cuesta cuando de verdad hay espera. En la salida esa espera es
contrapresión (el consumidor no da abasto) y en la entrada es hambre (el
productor no trae datos). El nodo que más tiempo pasa **sin** esperar a nadie
es el que marca el ritmo:

```
  nodo      estado            filas         filas/s      tiempo     ocupado
  generar   correcto      3 000 000       7 352 941      408 ms   7 ms (2%)
  escribir  correcto      3 000 000       7 142 857      420 ms  420 ms (100%) ←

  ← `escribir` es el que marca el ritmo: el resto le espera
```

El generador pasó 401 de sus 408 ms bloqueado esperando al CSV. Se mira en
absoluto y no en porcentaje: un nodo que vive 3 ms sin esperar da 100% y no
es el cuello de botella de nada.

El historial vive en un fichero DuckDB (`orch.duckdb`, o `--store`), así que
se puede consultar directamente:

```sql
SELECT pipeline, avg(elapsed_ms) FROM run_history
WHERE started_at > now() - INTERVAL 7 DAY GROUP BY 1;
```

**Los esquemas se propagan en `validate`.** Los orígenes declaran qué columnas
producen sin leer datos y cada transformación calcula su salida, así que una
columna mal escrita, un fan-in con esquemas incompatibles o un `filter` con dos
entradas se detectan antes de tocar ninguna fuente. Un origen que aún no puede
saberlo (un CSV que generará un paso anterior) devuelve "desconocido" y la
cadena se corta sin invalidar el pipeline.

**El `SessionContext` de cada nodo SQL se construye al preparar el pipeline,
no al ejecutarlo**, para que un pipeline preparado una vez y ejecutado muchas
(el caso del demonio y de la aplicación) no lo repita. En release el
ahorro es pequeño —montar el catálogo de funciones de DataFusion cuesta menos
de 1 ms por nodo—, pero en debug son cientos de milisegundos por nodo, así que
también hace usable el ciclo de desarrollo.

## Limitaciones conocidas

- **Los sinks de fichero no son transaccionales.** Si el pipeline falla a
  medias, el CSV de salida queda con las filas escritas hasta ese punto. El
  destino de PostgreSQL sí lo es: carga dentro de una transacción y, si algo
  falla, la tabla queda como estaba.
- **`numeric` de PostgreSQL viaja como texto exacto.** Convertirlo a
  `Decimal128` obligaría a fijar una escala y redondear en silencio lo que no
  encajara, inaceptable en datos de dinero. Por encima de 28 dígitos
  significativos falla y pide un `round()`.
- **Fan-in por concatenación**, no intercalado: las entradas se drenan en el
  orden en que se declararon las aristas.
- **Las ramas independientes no se cancelan** cuando otra falla: terminan su
  trabajo y el run se marca como fallido al final.
- **Cada entrada es un flujo de una sola pasada.** Si un plan intenta escanear
  la misma tabla dos veces (un self-join), falla con un mensaje explícito en
  vez de devolver vacío en silencio.
- **Un nodo `sql` con varias entradas necesita que todas declaren esquema.**
  Espiar varios puertos en serie podría bloquear el pipeline si comparten un
  origen aguas arriba; espiarlos en paralelo está pendiente.
- **`aggregate` y `ORDER BY` rompen el streaming**: acumulan estado en
  memoria. Usa `memory_limit_mb` en esos nodos.
- **No hay deshacer en el lienzo.** Cada cambio se escribe al momento en el
  fichero, así que lo que haya en git es la red de seguridad.
- **Los plugins sólo pueden ser transformaciones.** Un origen o un destino
  necesitan red o disco, y darles ese acceso a un plugin pide decidir antes
  cómo se conceden los permisos.
