# Orch

Plataforma local de orquestación y transporte de datos. La especificación
completa está en [doc.md](doc.md); el desglose por fases, en
[docs/PLAN.md](docs/PLAN.md).

**Estado: Fase 0 completa.** Motor de DAG con ejecución en streaming,
transformaciones SQL sobre DataFusion, conectores CSV/Parquet/PostgreSQL/REST,
historial en DuckDB y demonio con disparadores cron. Sin UI todavía: eso es
la Fase 1.

---

## Requisitos

Rust estable (1.85 o superior) con el toolchain **MSVC**. En Windows:

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
> además lo que exigirán Tauri (Fase 1) y DuckDB (Fase 0.4).

## Compilar y probar

```powershell
cargo build --workspace
cargo test  --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

> **Con 8 GB de RAM, compila con `-j 2`.** Varios `rustc` en paralelo sobre
> DataFusion agotan la memoria y el compilador muere con
> `STATUS_STACK_BUFFER_OVERRUN`, dejando artefactos a medio escribir en
> `target/debug/deps` que luego dan `E0786`/`E0460`. Si ya ha pasado, hay que
> limpiar los crates afectados (`cargo clean -p <crate>`) y recompilar.

## Uso

```powershell
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
npx --prefix ui tauri dev --config ../crates/orch-app/tauri.conf.json
```

Medir el motor aislado de disco y red (usa `--release`, la diferencia es de
un orden de magnitud):

```powershell
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

Logs detallados: `$env:ORCH_LOG = "orch_core=debug,orch_connectors=debug"`.

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

### El lienzo

Cada pipeline se puede ver como el grafo que es, con el control segmentado
**Ejecución / Diseño** de la barra. Los nodos se colocan solos en capas de
izquierda a derecha, siguiendo el flujo de los datos; las barreras `after`
van punteadas porque ordenan pero no transportan; y al elegir un nodo, el
inspector enseña su configuración.

Dos decisiones que conviene conocer:

- **El lienzo dibuja el fichero, no el plan.** No se aplica pushdown: ver
  cómo un `filter` desaparece porque el motor lo absorbió dentro del origen,
  justo mientras lo estás editando, sería desconcertante.
- **El grafo se dibuja aunque no valide.** Un diseñador que sólo abre
  pipelines correctos falla justo cuando hace falta, que es para arreglar el
  que está roto. El motivo del fallo se enseña encima, y el conector que no
  existe se marca en el nodo.

El inspector carga el pipeline **sin expandir los secretos**: lo que se ve es
`${env:PG_DSN}` y no su valor. Por eso un pipeline cuyo secreto no está
definido en esta máquina se puede abrir y editar igual, aunque no se pueda
ejecutar.

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

## Arquitectura

```
crates/
  orch-core/         modelo de pipeline, validación del DAG, ejecutor, traits de conector
  orch-store/        historial de ejecuciones y métricas, en DuckDB
  orch-schedule/     disparadores cron y encadenamiento entre pipelines
  orch-connectors/   implementaciones nativas (CSV, generador, null, transformaciones)
  orch-sql/          transformaciones con expresiones, sobre DataFusion
  orch-rest/         conector HTTP: paginación, límite de tasa, reintentos
  orch-postgres/     conector PostgreSQL: cursor de lectura, COPY BINARY de escritura
  orch-cli/          binario `orch`
  orch-app/          aplicación de escritorio (Tauri)
ui/                  frontend de la aplicación (React + Vite)
```

El core no conoce ninguna implementación concreta: recibe un `Registry` con lo
que haya disponible. El mismo motor servirá al backend de Tauri (Fase 1) y a
los plugins WASM (Fase 2) sin cambios.

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
(el caso del demonio de la Fase 0.5 y de la UI) no lo repita. En release el
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
