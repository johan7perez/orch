/**
 * El lienzo: el pipeline como grafo, más el inspector del nodo elegido.
 *
 * De momento es de lectura. Dibuja lo que dice el fichero, no el plan que el
 * motor acaba ejecutando: si el pushdown absorbe un `filter` dentro del
 * origen, ver desaparecer el nodo justo mientras lo editas sería
 * desconcertante.
 */
import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Background,
  BackgroundVariant,
  Controls,
  ReactFlow,
  type Edge,
  type Node,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { api, type Graph, type GraphNode, type PipelineInfo } from "../api";
import { aristasDe, disponer } from "../lienzo/disposicion";
import { NodoPipeline } from "../lienzo/NodoPipeline";

const TIPOS_DE_NODO = { pipeline: NodoPipeline };

interface Props {
  pipeline: PipelineInfo;
}

export function Diseno({ pipeline }: Props) {
  const [grafo, setGrafo] = useState<Graph | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [elegido, setElegido] = useState<string | null>(null);

  useEffect(() => {
    let vigente = true;
    // Sin esto, al cambiar de pipeline se queda un instante el grafo del
    // anterior bajo el título del nuevo.
    setGrafo(null);
    setError(null);
    setElegido(null);
    api
      .pipelineGraph(pipeline.path)
      .then((g) => vigente && setGrafo(g))
      .catch((e) => {
        if (!vigente) return;
        setGrafo(null);
        setError(String(e));
      });
    return () => {
      vigente = false;
    };
  }, [pipeline.path]);

  // `elegido` entra en la construcción: el lienzo es controlado y, sin
  // `onNodesChange`, React Flow descarta su propio estado de selección.
  const { nodos, aristas } = useMemo(() => construir(grafo, elegido), [grafo, elegido]);

  const alElegir = useCallback((_: unknown, nodo: Node) => setElegido(nodo.id), []);

  const nodoElegido = grafo?.nodes.find((n) => n.id === elegido) ?? null;

  return (
    <>
      {error ? <p className="nota nota--error nota--suelta">{error}</p> : null}
      {grafo?.problem ? (
        <p className="nota nota--error nota--suelta">{grafo.problem}</p>
      ) : null}

      <div className="lienzo">
        <ReactFlow
          nodes={nodos}
          edges={aristas}
          nodeTypes={TIPOS_DE_NODO}
          onNodeClick={alElegir}
          onPaneClick={() => setElegido(null)}
          fitView
          fitViewOptions={{ padding: 0.1, maxZoom: 1 }}
          minZoom={0.2}
          maxZoom={1.6}
          proOptions={{ hideAttribution: true }}
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable
        >
          <Background variant={BackgroundVariant.Dots} gap={18} size={1} />
          <Controls showInteractive={false} />
        </ReactFlow>

        {nodoElegido ? (
          <Inspector nodo={nodoElegido} onCerrar={() => setElegido(null)} />
        ) : null}
      </div>
    </>
  );
}

function Inspector({ nodo, onCerrar }: { nodo: GraphNode; onCerrar: () => void }) {
  const entradas = Object.entries(nodo.config ?? {});
  return (
    <aside className="inspector">
      <header className="inspector__cabecera">
        <div className="barra__texto">
          <h2 className="inspector__titulo">{nodo.id}</h2>
          <p className="barra__sub">
            {nodo.kind} · {nodo.component}
          </p>
        </div>
        <button className="inspector__cerrar" onClick={onCerrar} aria-label="Cerrar">
          ✕
        </button>
      </header>

      <div className="inspector__cuerpo">
        {!nodo.known ? (
          <p className="nota nota--error">
            El componente <strong>{nodo.component}</strong> no está registrado. El
            pipeline fallará al ejecutarse.
          </p>
        ) : null}

        {entradas.length === 0 ? (
          <p className="nota">Sin configuración.</p>
        ) : (
          <dl className="campos">
            {entradas.map(([clave, valor]) => (
              <div className="campo" key={clave}>
                <dt className="campo__nombre">{clave}</dt>
                <dd className="campo__valor">{formatear(valor)}</dd>
              </div>
            ))}
          </dl>
        )}

        {nodo.after.length > 0 ? (
          <>
            <p className="seccion">Espera a</p>
            <p className="campo__valor">{nodo.after.join(", ")}</p>
          </>
        ) : null}
      </div>
    </aside>
  );
}

/** Un secreto llega sin expandir; se enseña tal cual, que es lo que el fichero dice. */
function formatear(valor: unknown): string {
  if (typeof valor === "string") return valor;
  return JSON.stringify(valor, null, 2) ?? String(valor);
}

function construir(
  grafo: Graph | null,
  elegido: string | null,
): { nodos: Node[]; aristas: Edge[] } {
  if (!grafo) return { nodos: [], aristas: [] };

  const colocados = disponer(grafo);
  const todas = aristasDe(grafo);

  const nodos: Node[] = grafo.nodes.map((nodo) => ({
    id: nodo.id,
    type: "pipeline",
    position: colocados.get(nodo.id) ?? { x: 0, y: 0 },
    data: {
      etiqueta: nodo.id,
      componente: nodo.component,
      tipo: nodo.kind,
      conocido: nodo.known,
      // Los conectores no salen del tipo de nodo sino de lo que realmente
      // llega y sale. Un sink no tiene salida de datos, pero sí puede ser el
      // origen de una barrera `after`, y sin conector la arista no tendría
      // dónde engancharse: desaparecía del dibujo.
      entradas: nodo.kind !== "source" || todas.some((a) => a.to === nodo.id) ? 1 : 0,
      salidas: nodo.kind !== "sink" || todas.some((a) => a.from === nodo.id) ? 1 : 0,
    },
    selected: nodo.id === elegido,
    draggable: false,
  }));

  const aristas: Edge[] = todas.map((arista, i) => ({
    id: `${arista.from}->${arista.to}#${i}`,
    source: arista.from,
    target: arista.to,
    // Una barrera `after` no lleva datos: se dibuja punteada para que no se
    // confunda con el flujo.
    animated: false,
    className: arista.barrera ? "arista arista--barrera" : "arista",
    // El puerto sólo se etiqueta cuando dice algo: por defecto es el id del
    // nodo de origen, y repetirlo sobre la flecha es ruido.
    label: !arista.barrera && arista.port !== arista.from ? arista.port : undefined,
  }));

  return { nodos, aristas };
}
