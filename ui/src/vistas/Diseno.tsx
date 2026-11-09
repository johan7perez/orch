/**
 * El lienzo: el pipeline como grafo, editable, más el inspector del nodo
 * elegido.
 *
 * Dibuja lo que dice el fichero, no el plan que el motor acaba ejecutando: si
 * el pushdown absorbe un `filter` dentro del origen, ver desaparecer el nodo
 * justo mientras lo editas sería desconcertante.
 *
 * La UI no guarda su propia copia editable del pipeline. Cada cambio va al
 * motor, que lo escribe en el YAML tocando sólo lo que cambia, y lo que se
 * pinta es el grafo releído del fichero. No hay un estado aquí que pueda
 * desincronizarse del disco: el fichero es la única verdad.
 */
import { useCallback, useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import {
  Background,
  BackgroundVariant,
  Controls,
  ReactFlow,
  type Connection,
  type Edge,
  type Node,
  type ReactFlowInstance,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import {
  api,
  type Catalog,
  type Edit,
  type EsquemaObjeto,
  type Graph,
  type PipelineInfo,
} from "../api";
import { aristasDe, disponer } from "../lienzo/disposicion";
import { NodoPipeline } from "../lienzo/NodoPipeline";
import { Paleta, type Arrastrado, type TipoNodo } from "../lienzo/Paleta";
import { Inspector } from "./Inspector";

const TIPOS_DE_NODO = { pipeline: NodoPipeline };

// Al encuadrar se deja libre lo que tapan los paneles: la paleta a la
// izquierda y, con un nodo elegido, el inspector a la derecha. Si no, los
// orígenes quedan justo debajo de la paleta.
const ENCUADRE = {
  padding: { top: "28px", bottom: "28px", left: "204px", right: "28px" },
  maxZoom: 1,
} as const;
const ENCUADRE_CON_INSPECTOR = {
  ...ENCUADRE,
  padding: { ...ENCUADRE.padding, right: "332px" },
} as const;

/**
 * Cambios que mueven nodos. El lienzo los coloca solo, así que después de
 * uno de estos lo que estaba a la vista puede haber quedado fuera, o detrás
 * de un panel: se vuelve a encuadrar.
 */
const MUEVEN_NODOS = new Set<Edit["op"]>([
  "add_node",
  "remove_node",
  "connect",
  "disconnect",
  "set_after",
  "rename_node",
]);

interface Props {
  pipeline: PipelineInfo;
  /** Se escribió el fichero: el lateral tiene que refrescar su resumen. */
  onEditado: () => void;
}

interface DatosArista extends Record<string, unknown> {
  /** Una espera `after`, no un flujo de datos. */
  barrera: boolean;
}

export function Diseno({ pipeline, onEditado }: Props) {
  const [grafo, setGrafo] = useState<Graph | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [elegido, setElegido] = useState<string | null>(null);
  const [aristaElegida, setAristaElegida] = useState<string | null>(null);
  const [guardando, setGuardando] = useState(false);
  // Sube con cada cambio rechazado. El inspector se rehace con ella: lo que
  // se estaba escribiendo no se guardó, y dejarlo en el campo haría creer
  // que sí.
  const [rechazos, setRechazos] = useState(0);
  // El catálogo se pide una vez: los componentes registrados no cambian
  // mientras la ventana está abierta.
  const [catalogo, setCatalogo] = useState<Catalog | null>(null);

  const flujo = useRef<ReactFlowInstance | null>(null);
  const arrastrado = useRef<Arrastrado | null>(null);
  // La revisión vive en una ref además de en `grafo`: un cambio que espera en
  // la cola tiene que usar la del grafo que dejó el anterior, no la que había
  // cuando se pidió.
  const revision = useRef<string | null>(null);
  const cola = useRef<Promise<void>>(Promise.resolve());
  const ruta = useRef(pipeline.path);
  const conInspector = useRef(false);

  const pintar = useCallback((g: Graph | null) => {
    revision.current = g?.revision ?? null;
    setGrafo(g);
  }, []);

  useEffect(() => {
    api
      .catalog()
      .then(setCatalogo)
      .catch(() => setCatalogo(null));
  }, []);

  useEffect(() => {
    ruta.current = pipeline.path;
    let vigente = true;
    // Sin esto, al cambiar de pipeline se queda un instante el grafo del
    // anterior bajo el título del nuevo.
    pintar(null);
    setError(null);
    setElegido(null);
    setAristaElegida(null);
    api
      .pipelineGraph(pipeline.path)
      .then((g) => vigente && pintar(g))
      .catch((e) => {
        if (!vigente) return;
        pintar(null);
        setError(String(e));
      });
    return () => {
      vigente = false;
    };
  }, [pipeline.path, pintar]);

  /**
   * Manda cambios al motor y pinta lo que vuelve.
   *
   * Van en cola, uno detrás de otro. Si uno falla —el fichero cambió por
   * debajo, o el cambio no tenía sentido— se enseña el motivo y se recarga lo
   * que hay en disco: lo que se ve tiene que ser siempre lo que dice el
   * fichero.
   */
  const editar = useCallback(
    (edits: Edit[], despues?: () => void) => {
      if (edits.length === 0) return;
      const path = pipeline.path;
      cola.current = cola.current.then(async () => {
        const actual = revision.current;
        if (actual === null || ruta.current !== path) return;
        setGuardando(true);
        try {
          const nuevo = await api.editPipeline(path, actual, edits);
          if (ruta.current !== path) return;
          pintar(nuevo);
          setError(null);
          despues?.();
          onEditado();
          if (edits.some((e) => MUEVEN_NODOS.has(e.op))) {
            // Dos fotogramas: el primero pinta el grafo nuevo y abre o cierra
            // el inspector; el encuadre tiene que saber si está.
            requestAnimationFrame(() =>
              requestAnimationFrame(() =>
                flujo.current?.fitView({
                  ...(conInspector.current ? ENCUADRE_CON_INSPECTOR : ENCUADRE),
                  duration: 200,
                }),
              ),
            );
          }
        } catch (e) {
          if (ruta.current !== path) return;
          setError(String(e));
          setRechazos((n) => n + 1);
          try {
            pintar(await api.pipelineGraph(path));
          } catch {
            // El motivo ya está en pantalla.
          }
        } finally {
          setGuardando(false);
        }
      });
    },
    [pipeline.path, pintar, onEditado],
  );

  const anadir = useCallback(
    (tipo: TipoNodo, componente: string) => {
      if (!grafo) return;
      const id = idLibre(grafo, tipo, componente);
      editar([{ op: "add_node", id, kind: tipo, component: componente }], () => {
        // Se elige al momento: lo siguiente que hace falta es rellenar su
        // config, y el inspector ya marca lo obligatorio que falta.
        setElegido(id);
        setAristaElegida(null);
      });
    },
    [grafo, editar],
  );

  const conectar = useCallback(
    (conexion: Connection) => {
      editar([{ op: "connect", from: conexion.source, to: conexion.target }]);
    },
    [editar],
  );

  // Lo que el motor rechazaría de todas formas, el lienzo ni lo ofrece. Los
  // conectores no bastan para impedirlo: un sink que es origen de una espera
  // `after` tiene conector de salida, pero no puede dar datos.
  const conexionValida = useCallback(
    (conexion: Connection | Edge) => {
      if (!grafo || conexion.source === conexion.target) return false;
      const origen = grafo.nodes.find((n) => n.id === conexion.source);
      const destino = grafo.nodes.find((n) => n.id === conexion.target);
      return (
        origen?.kind !== "sink" &&
        destino?.kind !== "source" &&
        !grafo.edges.some((e) => e.from === conexion.source && e.to === conexion.target)
      );
    },
    [grafo],
  );

  const borrar = useCallback(
    ({ nodes, edges }: { nodes: Node[]; edges: Edge[] }) => {
      if (!grafo) return;
      const quitados = new Set(nodes.map((n) => n.id));
      const edits: Edit[] = [];
      const esperas = new Map<string, string[]>();

      for (const arista of edges) {
        // Las aristas de un nodo que se va, se van con él: pedirlas aparte
        // fallaría porque ya no existen.
        if (quitados.has(arista.source) || quitados.has(arista.target)) continue;
        if ((arista.data as DatosArista | undefined)?.barrera) {
          const actual =
            esperas.get(arista.target) ??
            grafo.nodes.find((n) => n.id === arista.target)?.after ??
            [];
          esperas.set(
            arista.target,
            actual.filter((previo) => previo !== arista.source),
          );
        } else {
          edits.push({ op: "disconnect", from: arista.source, to: arista.target });
        }
      }
      for (const [node, after] of esperas) edits.push({ op: "set_after", node, after });
      for (const id of quitados) edits.push({ op: "remove_node", id });

      editar(edits, () => {
        setElegido(null);
        setAristaElegida(null);
      });
    },
    [grafo, editar],
  );

  const alArrastrarEncima = useCallback((e: DragEvent) => {
    if (!arrastrado.current) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
  }, []);

  const alSoltar = useCallback(
    (e: DragEvent) => {
      const soltado = arrastrado.current;
      arrastrado.current = null;
      if (!soltado) return;
      e.preventDefault();
      anadir(soltado.tipo, soltado.componente);
    },
    [anadir],
  );

  // `elegido` entra en la construcción: el lienzo es controlado y, sin
  // `onNodesChange`, React Flow descarta su propio estado de selección.
  const { nodos, aristas } = useMemo(
    () => construir(grafo, elegido, aristaElegida),
    [grafo, elegido, aristaElegida],
  );

  const nodoElegido = grafo?.nodes.find((n) => n.id === elegido) ?? null;
  conInspector.current = nodoElegido !== null;

  return (
    <>
      {error ? <p className="nota nota--error nota--suelta">{error}</p> : null}
      {grafo?.problem ? (
        <p className="nota nota--error nota--suelta">{grafo.problem}</p>
      ) : null}

      <div className="lienzo" aria-busy={guardando}>
        <ReactFlow
          nodes={nodos}
          edges={aristas}
          nodeTypes={TIPOS_DE_NODO}
          onInit={(instancia) => {
            flujo.current = instancia;
          }}
          onNodeClick={(_, nodo) => {
            setElegido(nodo.id);
            setAristaElegida(null);
          }}
          onEdgeClick={(_, arista) => {
            setAristaElegida(arista.id);
            setElegido(null);
          }}
          onPaneClick={() => {
            setElegido(null);
            setAristaElegida(null);
          }}
          onConnect={conectar}
          isValidConnection={conexionValida}
          onDelete={borrar}
          deleteKeyCode={["Backspace", "Delete"]}
          onDragOver={alArrastrarEncima}
          onDrop={alSoltar}
          fitView
          fitViewOptions={ENCUADRE}
          minZoom={0.2}
          maxZoom={1.6}
          proOptions={{ hideAttribution: true }}
          nodesDraggable={false}
          nodesConnectable
          elementsSelectable
        >
          <Background variant={BackgroundVariant.Dots} gap={18} size={1} />
          <Controls showInteractive={false} />
        </ReactFlow>

        {catalogo ? (
          <Paleta
            catalogo={catalogo}
            onAnadir={anadir}
            onArrastrar={(a) => {
              arrastrado.current = a;
            }}
          />
        ) : null}

        {nodoElegido && grafo ? (
          <Inspector
            key={`${nodoElegido.id}#${rechazos}`}
            nodo={nodoElegido}
            esquema={esquemaDe(catalogo, nodoElegido.kind, nodoElegido.component)}
            otros={grafo.nodes.map((n) => n.id).filter((id) => id !== nodoElegido.id)}
            onEditar={(edits) => editar(edits)}
            onRenombrar={(nuevo) =>
              editar([{ op: "rename_node", from: nodoElegido.id, to: nuevo }], () =>
                setElegido(nuevo),
              )
            }
            onEliminar={() =>
              editar([{ op: "remove_node", id: nodoElegido.id }], () => setElegido(null))
            }
            onCerrar={() => setElegido(null)}
          />
        ) : null}
      </div>
    </>
  );
}

/**
 * Un id libre para un nodo nuevo, con la forma de los que escribe la gente:
 * `leer_csv`, `escribir_parquet`, `filter`. Si ya existe, `_2`, `_3`…
 *
 * El motor sólo acepta minúsculas, números y `_` para ids nuevos, porque el
 * id acaba siendo un nombre de tabla en SQL.
 */
function idLibre(grafo: Graph, tipo: TipoNodo, componente: string): string {
  const prefijo = tipo === "source" ? "leer_" : tipo === "sink" ? "escribir_" : "";
  const base = `${prefijo}${componente}`
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "_")
    .replace(/^(?=[0-9])/, "_");
  const usados = new Set(grafo.nodes.map((n) => n.id));
  if (!usados.has(base)) return base;
  for (let i = 2; ; i++) {
    if (!usados.has(`${base}_${i}`)) return `${base}_${i}`;
  }
}

function construir(
  grafo: Graph | null,
  elegido: string | null,
  aristaElegida: string | null,
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

  const aristas: Edge<DatosArista>[] = todas.map((arista, i) => {
    const id = `${arista.barrera ? "after" : "datos"}:${arista.from}->${arista.to}#${i}`;
    return {
      id,
      source: arista.from,
      target: arista.to,
      data: { barrera: arista.barrera },
      selected: id === aristaElegida,
      // Una barrera `after` no lleva datos: se dibuja punteada para que no se
      // confunda con el flujo.
      animated: false,
      className: arista.barrera ? "arista arista--barrera" : "arista",
      // El puerto sólo se etiqueta cuando dice algo: por defecto es el id del
      // nodo de origen, y repetirlo sobre la flecha es ruido.
      label: !arista.barrera && arista.port !== arista.from ? arista.port : undefined,
    };
  });

  return { nodos, aristas };
}

/**
 * El esquema del componente de un nodo.
 *
 * El catálogo se pide una vez y se reutiliza: los componentes registrados no
 * cambian mientras la ventana está abierta.
 */
function esquemaDe(
  catalogo: Catalog | null,
  kind: string,
  componente: string,
): EsquemaObjeto | null {
  if (!catalogo) return null;
  const lista =
    kind === "source"
      ? catalogo.sources
      : kind === "sink"
        ? catalogo.sinks
        : catalogo.transforms;
  return lista.find((c) => c.name === componente)?.schema ?? null;
}
