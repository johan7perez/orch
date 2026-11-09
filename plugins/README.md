# Plugins de ejemplo

Transformaciones escritas con el SDK [`orch-plugin`](../crates/orch-plugin) y
compiladas a WebAssembly. Cómo funcionan los plugins, qué pueden hacer y
cuánto cuestan está en el [README principal](../README.md#plugins-wasm).

| Plugin | Qué hace |
|---|---|
| [`mayusculas`](mayusculas) | Pasa a mayúsculas una columna de texto. El ejemplo de plugin con config. |
| [`identidad`](identidad) | Devuelve cada lote tal cual. Sirve para medir lo que cuesta la frontera. |
| [`fisgon`](fisgon) | Intenta leer ficheros, conectarse y ver el entorno. Los tests comprueban que no puede. |

Compilarlos:

```sh
rustup target add wasm32-wasip2
cargo build --release --target wasm32-wasip2 -p orch-plugin-mayusculas
```

El componente queda en
`target/wasm32-wasip2/release/orch_plugin_mayusculas.wasm`. Para usarlo, se
copia a la carpeta `plugins/` de la carpeta de pipelines (o se apunta
`ORCH_PLUGINS` a donde esté) y se escribe en el YAML como cualquier otra
transformación:

```yaml
- id: nombres
  type: transform
  op: mayusculas
  config:
    columna: name
```

Para escribir uno nuevo, el punto de partida es
[`mayusculas/src/lib.rs`](mayusculas/src/lib.rs): un `Cargo.toml` con
`crate-type = ["cdylib", "rlib"]` que depende de `orch-plugin`, un tipo que
implementa `Transformacion` y la línea `orch_plugin::exportar!(Tipo)`.
