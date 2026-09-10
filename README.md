# Orch

Plataforma local de orquestación y transporte de datos. La especificación
completa está en [doc.md](doc.md); el desglose por fases, en
[docs/PLAN.md](docs/PLAN.md).

**Estado: Fase 0 en curso.** Motor de DAG, ejecutor asíncrono y CLI
funcionando sobre conectores CSV y sintéticos. Sin UI, sin Postgres, sin REST
y sin DuckDB todavía.

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

# Informe en JSON, para encadenar con otras herramientas
cargo run -p orch-cli -- run examples/pipelines/csv_to_csv.yaml --format json
```

Medir el motor aislado de disco y red (usa `--release`, la diferencia es de
un orden de magnitud):

```powershell
cargo run --release -p orch-cli -- run examples/pipelines/benchmark.yaml
```

Línea base actual (10 M de filas, generador → sink nulo, `batch_size: 65536`):
**56 ms, ~180 M filas/s**. Es el techo del orquestador sin I/O; cualquier
cambio en el ejecutor debería compararse contra esta cifra.

Logs detallados: `$env:ORCH_LOG = "orch_core=debug,orch_connectors=debug"`.

`orch run` devuelve código de salida 0 sólo si todos los nodos terminaron
correctamente.

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
```

### Componentes disponibles

| Tipo | Nombre | Config |
|---|---|---|
| source | `csv` | `path`, `has_header`, `delimiter`, `infer_rows` (0 = fichero entero), `batch_size` |
| source | `generator` | `rows`, `with_text`, `batch_size` — datos sintéticos deterministas |
| transform | `select` | `columns: [..]` — proyecta y reordena |
| transform | `rename` | `columns: { viejo: nuevo }` |
| transform | `limit` | `rows` — corta y detiene la lectura aguas arriba |
| sink | `csv` | `path`, `has_header`, `delimiter`, `create_dirs` |
| sink | `null` | descarta; para dry-runs y benchmarks |

## Arquitectura

```
crates/
  orch-core/         modelo de pipeline, validación del DAG, ejecutor, traits de conector
  orch-connectors/   implementaciones nativas (CSV, generador, null, transformaciones)
  orch-cli/          binario `orch`
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

## Limitaciones conocidas de la Fase 0

- **Los sinks no son transaccionales.** Si el pipeline falla a medias, el CSV
  de salida queda con las filas escritas hasta ese punto.
- **Fan-in por concatenación**, no intercalado: las entradas se drenan en el
  orden en que se declararon las aristas.
- **Sin persistencia.** Métricas y logs viven en memoria y se pierden al
  terminar el proceso; DuckDB entra en la Fase 0.4.
- **Transformaciones sin expresiones.** Filtros, agregaciones y SQL llegan con
  DataFusion en la Fase 0.2.
- **Las ramas independientes no se cancelan** cuando otra falla: terminan su
  trabajo y el run se marca como fallido al final.
