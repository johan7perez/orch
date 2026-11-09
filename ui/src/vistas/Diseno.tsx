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
  ControlButton,
  Controls,
  ReactFlow,
  applyNodeChanges,
  type Connection,
  type Edge,
  type Node,
  type NodeChange,
  type ReactFlowInstance,
  type XYPosition,
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
import { ALTO_NODO, ANCHO_NODO, aristasDe, disponer } from "../lienzo/disposicion";
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
 * Cambios que recolocan nodos cuando el lienzo los coloca solo: después de
 * uno de estos lo que estaba a la vista puede haber quedado fuera, o detrás
 * de un panel, y se vuelve a encuadrar. Con las posiciones guardadas no hace
 * falta —cada nodo sigue donde estaba— salvo al borrarlas.
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
  // Los nodos tal y como los tiene React Flow: con lo que midió y, mientras
  // se arrastran, con su posición en vuelo. Se rehacen desde el fichero cada
  // vez que cambia el grafo.
  const [nodos, setNodos] = useState<Node[]>([]);

  const flujo = useRef<ReactFlowInstance | null>(null);
  const lienzo = useRef<HTMLDivElement | null>(null);
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
          const colocado = nuevo.nodes.some((n) => n.position !== null);
          const recoloca =
            edits.some((e) => e.op === "clear_layout") ||
            (!colocado && edits.some((e) => MUEVEN_NODOS.has(e.op)));
          if (recoloca) {
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
    (tipo: TipoNodo, componente: string, donde?: XYPosition) => {
      if (!grafo) return;
      const id = idLibre(grafo, tipo, componente);
      const edits: Edit[] = [{ op: "add_node", id, kind: tipo, component: componente }];
      // Si el usuario ya colocó los nodos a su gusto, el nuevo se queda donde
      // lo soltó, o debajo de todo si vino de un clic. Si no, lo coloca el
      // lienzo como a los demás.
      if (grafo.nodes.some((n) => n.position !== null)) {
        const [x, y] = donde ? [donde.x, donde.y] : debajoDeTodo(grafo);
        edits.push({ op: "set_layout", positions: { [id]: [x, y] } });
      }
      editar(edits, () => {
        // Se elige al momento: lo siguiente que hace falta es rellenar su
        // config, y el inspector ya marca lo obligatorio que falta.
        setElegido(id);
        setAristaElegida(null);
        // El inspector que se abre puede taparlo justo donde se soltó.
        requestAnimationFrame(() =>
          requestAnimationFrame(() => asomar(flujo.current, lienzo.current, id)),
        );
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
      const punto = flujo.current?.screenToFlowPosition({ x: e.clientX, y: e.clientY });
      // Centrado en el puntero, que es donde el ojo espera verlo aparecer.
      const donde = punto && { x: punto.x - ANCHO_NODO / 2, y: punto.y - ALTO_NODO / 2 };
      anadir(soltado.tipo, soltado.componente, donde);
    },
    [anadir],
  );

  // `elegido` entra en la construcción: el lienzo es controlado y, sin
  // `onNodesChange`, React Flow descarta su propio estado de selección.
  const construido = useMemo(
    () => construir(grafo, elegido, aristaElegida),
    [grafo, elegido, aristaElegida],
  );
  const aristas = construido.aristas;

  // Las posiciones de verdad son las del fichero: al cambiar el grafo se
  // rehacen, tanto si la nueva se guardó como si el cambio se rechazó y el
  // nodo vuelve a su sitio. Lo que React Flow ya midió se conserva: un nodo
  // sin `measured` se oculta hasta que lo vuelve a medir, y si su tamaño no
  // cambió, eso puede no ocurrir nunca.
  useEffect(() => {
    setNodos((antes) => {
      const medidas = new Map(antes.map((n) => [n.id, n.measured]));
      return construido.nodos.map((n) => ({ ...n, measured: medidas.get(n.id) }));
    });
  }, [construido.nodos]);

  // De la selección y el borrado se ocupan sus propios manejadores; aquí
  // entran las medidas y el arrastre.
  const alCambiarNodos = useCallback((cambios: NodeChange[]) => {
    const propios = cambios.filter((c) => c.type === "position" || c.type === "dimensions");
    if (propios.length > 0) setNodos((actuales) => applyNodeChanges(propios, actuales));
  }, []);

  /**
   * Al soltar se guarda la posición. La primera vez se guardan las de todos:
   * si sólo se fijara el movido, los demás seguirían recolocándose solos a su
   * alrededor con cada cambio, y el dibujo dejaría de tener sentido.
   */
  const alSoltarNodo = useCallback(
    (_: unknown, __: Node, soltados: Node[]) => {
      if (!grafo) return;
      const colocado = grafo.nodes.some((n) => n.position !== null);
      const posiciones: Record<string, [number, number]> = {};
      if (!colocado) {
        for (const n of construido.nodos) posiciones[n.id] = [n.position.x, n.position.y];
      }
      for (const n of soltados) {
        const antes = grafo.nodes.find((g) => g.id === n.id)?.position;
        const ahora: [number, number] = [Math.round(n.position.x), Math.round(n.position.y)];
        if (!antes || antes[0] !== ahora[0] || antes[1] !== ahora[1]) posiciones[n.id] = ahora;
      }
      if (Object.keys(posiciones).length > 0) {
        editar([{ op: "set_layout", positions: posiciones }]);
      }
    },
    [grafo, construido.nodos, editar],
  );

  const colocado = grafo?.nodes.some((n) => n.position !== null) ?? false;

  const nodoElegido = grafo?.nodes.find((n) => n.id === elegido) ?? null;
  conInspector.current = nodoElegido !== null;

  return (
    <>
      {error ? <p className="nota nota--error nota--suelta">{error}</p> : null}
      {grafo?.problem ? (
        <p className="nota nota--error nota--suelta">{grafo.problem}</p>
      ) : null}

      <div className="lienzo" aria-busy={guardando} ref={lienzo}>
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
          onNodesChange={alCambiarNodos}
          onNodeDragStop={alSoltarNodo}
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
          nodesDraggable
          nodesConnectable
          elementsSelectable
        >
          <Background variant={BackgroundVariant.Dots} gap={18} size={1} />
          <Controls showInteractive={false}>
            {colocado ? (
              <ControlButton
                onClick={() => editar([{ op: "clear_layout" }])}
                title="Colocar los nodos automáticamente (borra las posiciones guardadas)"
                aria-label="Colocar los nodos automáticamente"
              >
                <svg viewBox="0 0 16 16" aria-hidden="true">
                  <rect x="1" y="2" width="5" height="4" rx="1" />
                  <rect x="10" y="2" width="5" height="4" rx="1" />
                  <rect x="10" y="10" width="5" height="4" rx="1" />
                  <path d="M6 4h4M8 4v8h2" fill="none" stroke="currentColor" strokeWidth="1.3" />
                </svg>
              </ControlButton>
            ) : null}
          </Controls>
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
 * Si un nodo quedó debajo de la paleta o del inspector, desplaza el lienzo lo
 * justo para que asome, sin cambiar el zoom. Con las posiciones guardadas no
 * se reencuadra todo después de cada cambio, así que esto hace falta.
 */
function asomar(rf: ReactFlowInstance | null, contenedor: HTMLElement | null, id: string) {
  const nodo = rf?.getNode(id);
  if (!rf || !nodo || !contenedor) return;
  const caja = contenedor.getBoundingClientRect();
  const libre = {
    izquierda: caja.left + 204,
    derecha: caja.right - 332,
    arriba: caja.top + 28,
    abajo: caja.bottom - 28,
  };
  const desde = rf.flowToScreenPosition(nodo.position);
  const hasta = rf.flowToScreenPosition({
    x: nodo.position.x + ANCHO_NODO,
    y: nodo.position.y + ALTO_NODO,
  });
  const dx =
    hasta.x > libre.derecha
      ? hasta.x - libre.derecha
      : desde.x < libre.izquierda
        ? desde.x - libre.izquierda
        : 0;
  const dy =
    hasta.y > libre.abajo ? hasta.y - libre.abajo : desde.y < libre.arriba ? desde.y - libre.arriba : 0;
  if (dx === 0 && dy === 0) return;
  const { x, y, zoom } = rf.getViewport();
  void rf.setViewport({ x: x - dx, y: y - dy, zoom }, { duration: 200 });
}

/** Un hueco para un nodo nuevo añadido con un clic: debajo de todos. */
function debajoDeTodo(grafo: Graph): [number, number] {
  const puestos = grafo.nodes.flatMap((n) => (n.position ? [n.position] : []));
  if (puestos.length === 0) return [0, 0];
  const x = Math.min(...puestos.map(([px]) => px));
  const y = Math.max(...puestos.map(([, py]) => py));
  return [x, y + ALTO_NODO + 28];
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
    // Lo que el usuario colocó manda; lo demás lo coloca el lienzo.
    position: nodo.position
      ? { x: nodo.position[0], y: nodo.position[1] }
      : (colocados.get(nodo.id) ?? { x: 0, y: 0 }),
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
