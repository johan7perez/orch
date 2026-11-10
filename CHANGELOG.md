# Cambios

Lo que cambia en cada versión publicada, de la más nueva a la más vieja. Al
publicar una, su sección son las notas del release: el workflow `Publicar`
busca la que se titula con el número de versión (`## 0.1.0 — 2026-11-20`) y
falla si no está. Lo que aún no se ha publicado va en «Sin publicar»; cómo
se publica, en [docs/PUBLICAR.md](docs/PUBLICAR.md).

## Sin publicar

Primera versión con instaladores.

- **Pipelines como YAML o en un lienzo.** Orígenes CSV, Parquet, PostgreSQL y
  REST; transformaciones con SQL sobre DataFusion (filtros, columnas
  calculadas, agregaciones, joins); destinos CSV, Parquet y PostgreSQL. Todo
  en streaming sobre Arrow, con reintentos por nodo.
- **Programación.** Disparadores cron y encadenamiento entre pipelines, con
  `orch daemon`, que ve los cambios de su carpeta sin reiniciarse.
- **Aplicación de escritorio.** Lienzo editable que escribe el YAML tocando
  sólo lo que cambia, ejecución en vivo con el ritmo de cada nodo, historial
  de ejecuciones, alertas (también como notificación del sistema), lineage por
  columna —también entre pipelines de la misma carpeta— y versiones del
  pipeline en git.
- **Plugins de WebAssembly**, aislados, con un SDK en Rust.
- **Modo distribuido opcional.** Aristas remotas sobre Arrow Flight con TLS
  mutuo, y un pipeline repartido entre varias máquinas con un coordinador y
  workers.
- **Un historial para todos.** La CLI, el demonio y la aplicación comparten
  el mismo, en la carpeta de datos del usuario. Antes era `orch.duckdb` en el
  directorio de trabajo: quien quiera seguir con ese fichero, lo indica con
  `--store` u `ORCH_STORE`, o lo mueve a la carpeta nueva.
