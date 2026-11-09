/**
 * Las columnas del nodo elegido y de dónde sale cada una: el lineage.
 *
 * En un nodo que transforma o escribe se mira hacia atrás: de qué columnas de
 * qué orígenes sale cada columna. En un origen se mira hacia delante, que es
 * lo que se pregunta antes de tocarlo: a qué salidas llega cada columna y qué
 * nodos la usan sin pasarla a la salida (la clave de un join no llega a
 * ninguna columna, pero quitarla lo rompe).
 *
 * Cada referencia a otro nodo es un botón que lo elige en el lienzo: seguir
 * el rastro es ir saltando.
 */
import type { ColumnLineage, Derivacion, NodeColumn, NodeLineage } from "../api";

/** Lo que el lienzo sabe del lineage del pipeline en este momento. */
export interface Linaje {
  /** Lo último que se calculó, mientras llega lo nuevo. */
  nodos: NodeLineage[] | null;
  error: string | null;
  cargando: boolean;
}

const COMO: Record<Exclude<Derivacion, "copy">, { texto: string; ayuda: string }> = {
  computed: { texto: "calculada", ayuda: "Se calcula fila a fila a partir de sus orígenes." },
  aggregated: { texto: "agregada", ayuda: "Resume varias filas: una suma, un recuento, una ventana." },
  unknown: { texto: "incierta", ayuda: "Algún paso del camino no se pudo seguir." },
};

interface Props {
  nodo: string;
  linaje: Linaje;
  onIrA: (nodo: string) => void;
}

export function Columnas({ nodo, linaje, onIrA }: Props) {
  const todos = linaje.nodos ?? [];
  const propio = todos.find((n) => n.node === nodo);
  const columnas = propio?.columns ?? null;

  return (
    <section aria-busy={linaje.cargando}>
      <p className="seccion">
        Columnas
        {columnas ? <span className="seccion__cuenta"> · {columnas.length}</span> : null}
        {linaje.cargando && propio ? <span className="seccion__cuenta"> · calculando…</span> : null}
      </p>

      {linaje.error ? (
        <p className="campo__ayuda">No se pueden calcular: {linaje.error}</p>
      ) : !propio ? (
        <p className="campo__ayuda">{linaje.cargando ? "Calculando…" : "Sin datos."}</p>
      ) : columnas === null ? (
        <Desconocido nodo={propio} todos={todos} onIrA={onIrA} />
      ) : (
        <>
          <ul className="columnas">
            {columnas.map((columna, i) => (
              <Columna
                // Una query puede devolver dos columnas con el mismo nombre.
                key={`${i}:${columna.column}`}
                nodo={propio}
                columna={columna}
                todos={todos}
                onIrA={onIrA}
              />
            ))}
          </ul>
          {propio.kind === "source" ? (
            <SalidasInciertas todos={todos} onIrA={onIrA} />
          ) : (
            <Indirectas nodo={propio} onIrA={onIrA} />
          )}
        </>
      )}
    </section>
  );
}

function Columna({
  nodo,
  columna,
  todos,
  onIrA,
}: {
  nodo: NodeLineage;
  columna: ColumnLineage;
  todos: NodeLineage[];
  onIrA: (nodo: string) => void;
}) {
  const como = columna.derivation === "copy" ? null : COMO[columna.derivation];

  return (
    <li className="columna">
      <div className="columna__cabecera">
        <span className="columna__nombre">{columna.column}</span>
        {como ? (
          <span
            className={`columna__como${columna.derivation === "unknown" ? " columna__como--incierta" : ""}`}
            title={como.ayuda}
          >
            {como.texto}
          </span>
        ) : null}
        <span className="columna__tipo">{columna.data_type}</span>
      </div>
      {nodo.kind === "source" ? (
        <Destinos origen={{ node: nodo.node, column: columna.column }} todos={todos} onIrA={onIrA} />
      ) : columna.origins.length === 0 ? (
        <p className="columna__nada">no lee ninguna columna</p>
      ) : (
        <Referencias flecha="←" etiqueta="Sale de" columnas={columna.origins} onIrA={onIrA} />
      )}
    </li>
  );
}

/** Hacia delante, desde una columna de un origen. */
function Destinos({
  origen,
  todos,
  onIrA,
}: {
  origen: NodeColumn;
  todos: NodeLineage[];
  onIrA: (nodo: string) => void;
}) {
  const { salidas, usadaEn } = impacto(todos, origen);
  if (salidas.length === 0 && usadaEn.length === 0) {
    return <p className="columna__nada">no llega a ninguna salida</p>;
  }
  return (
    <>
      {salidas.length > 0 ? (
        <Referencias flecha="→" etiqueta="Llega a" columnas={salidas} onIrA={onIrA} />
      ) : (
        <p className="columna__nada">no llega a ninguna salida</p>
      )}
      {usadaEn.length > 0 ? (
        <div className="columna__refs" aria-label="Se usa en">
          <span className="columna__flecha">usada en</span>
          {usadaEn.map((id) => (
            <button key={id} className="ficha ficha--enlace" onClick={() => onIrA(id)}>
              {id}
            </button>
          ))}
        </div>
      ) : null}
    </>
  );
}

function Referencias({
  flecha,
  etiqueta,
  columnas,
  onIrA,
}: {
  flecha: string;
  etiqueta: string;
  columnas: NodeColumn[];
  onIrA: (nodo: string) => void;
}) {
  return (
    <div className="columna__refs" aria-label={etiqueta}>
      {flecha ? (
        <span className="columna__flecha" aria-hidden="true">
          {flecha}
        </span>
      ) : null}
      {columnas.map((c) => (
        <button
          key={`${c.node}.${c.column}`}
          className="ficha ficha--enlace"
          title={`Ir a ${c.node}`}
          onClick={() => onIrA(c.node)}
        >
          <span className="ficha__nodo">{c.node}.</span>
          {c.column}
        </button>
      ))}
    </div>
  );
}

/** Lo que el nodo lee sin pasarlo a su salida. */
function Indirectas({ nodo, onIrA }: { nodo: NodeLineage; onIrA: (nodo: string) => void }) {
  if (nodo.indirect.length === 0) return null;
  return (
    <>
      <p className="campo__ayuda columnas__nota">
        Además lee, para filtrar, unir, agrupar u ordenar:
      </p>
      <Referencias flecha="" etiqueta="Además lee" columnas={nodo.indirect} onIrA={onIrA} />
    </>
  );
}

/**
 * Desde un origen, «no llega a ninguna salida» sólo es seguro si se sabe qué
 * escriben todas. Las que no, se dicen.
 */
function SalidasInciertas({
  todos,
  onIrA,
}: {
  todos: NodeLineage[];
  onIrA: (nodo: string) => void;
}) {
  const inciertas = todos.filter((n) => n.kind === "sink" && n.columns === null);
  if (inciertas.length === 0) return null;
  return (
    <p className="campo__ayuda columnas__nota">
      Sin contar{" "}
      {inciertas.map((n, i) => (
        <span key={n.node}>
          {i > 0 ? ", " : null}
          <button className="enlace" onClick={() => onIrA(n.node)}>
            {n.node}
          </button>
        </span>
      ))}
      : no se sabe qué escribe{inciertas.length > 1 ? "n" : ""} sin ejecutar.
    </p>
  );
}

function Desconocido({
  nodo,
  todos,
  onIrA,
}: {
  nodo: NodeLineage;
  todos: NodeLineage[];
  onIrA: (nodo: string) => void;
}) {
  const corte = nodo.unknown_at ?? nodo.node;
  const esOrigen = todos.find((n) => n.node === corte)?.kind === "source";
  const pista = esOrigen
    ? "un origen que no pudo leer su esquema (¿el fichero aún no existe, o la base de datos no responde?)"
    : "una transformación que no declara de dónde salen sus columnas (un plugin, por ejemplo)";

  return (
    <p className="campo__ayuda">
      No se sabe sin ejecutar:{" "}
      {corte === nodo.node ? (
        <>este nodo es {pista}.</>
      ) : (
        <>
          la cadena se corta en{" "}
          <button className="enlace" onClick={() => onIrA(corte)}>
            {corte}
          </button>
          , {pista}.
        </>
      )}
    </p>
  );
}

/** A qué salidas llega una columna de un origen y qué nodos la usan sin pasarla. */
export function impacto(todos: NodeLineage[], origen: NodeColumn) {
  const es = (c: NodeColumn) => c.node === origen.node && c.column === origen.column;
  const salidas: NodeColumn[] = [];
  const usadaEn: string[] = [];
  for (const n of todos) {
    if (n.kind === "sink") {
      for (const c of n.columns ?? []) {
        if (c.origins.some(es)) salidas.push({ node: n.node, column: c.column });
      }
    }
    if (n.indirect.some(es)) usadaEn.push(n.node);
  }
  return { salidas, usadaEn };
}
