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
import {
  Profiler,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type DragEvent,
  type ReactNode,
} from "react";
import {
  Background,
  BackgroundVariant,
  ControlButton,
  Controls,
  ReactFlow,
  type Connection,
  type Edge,
  type Node,
  type NodeChange,
  type ReactFlowInstance,
  type Viewport,
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
import { MEDIR, Medidor, alRenderizar, medidas } from "../lienzo/Medidor";
import { NodoPipeline } from "../lienzo/NodoPipeline";
import { Paleta, type Arrastrado, type TipoNodo } from "../lienzo/Paleta";
import type { Linaje } from "./Columnas";
import { Inspector } from "./Inspector";

const TIPOS_DE_NODO = { pipeline: NodoPipeline };

// El botón de encuadre de los controles enseña el grafo entero, dejando libre
// lo que tapa la paleta: si no, los orígenes quedan justo debajo.
const ENCUADRE = {
  padding: { top: "28px", bottom: "28px", left: "204px", right: "28px" },
  maxZoom: 1,
} as const;

/**
 * El tamaño de un nodo, que es fijo. Va declarado en cada uno: un nodo que le
 * llega a React Flow sin `measured` pierde la posición de sus conectores y se
 * oculta, con sus aristas, hasta que lo vuelve a medir, y si su tamaño no
 * cambió eso puede no ocurrir nunca. Pasaba al soltar un nodo arrastrado.
 */
const MEDIDA_NODO = { width: ANCHO_NODO, height: ALTO_NODO };

/** Lo que tapan los paneles, en píxeles de pantalla. */
const MARGEN = { izquierda: 204, derecha: 28, conInspector: 332, vertical: 28 };

/**
 * Por debajo de este zoom el texto de un nodo ya no se lee, y con cientos de
 * nodos a la vista el lienzo deja de ir fluido: cada uno es un elemento que
 * se repinta en cada fotograma. Un pipeline que no cabe entero a este tamaño
 * se abre por el principio en vez de en una vista general ilegible y lenta;
 * alejarse sigue estando a un gesto.
 */
const ZOOM_LEGIBLE = 0.6;

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

type Instancia = ReactFlowInstance<Node, Edge<DatosArista>>;

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
  // El catálogo se pide al abrir cada pipeline: los plugins viven con los
  // pipelines, así que cambiar de carpeta cambia los componentes.
  const [catalogo, setCatalogo] = useState<Catalog | null>(null);
  // Dónde van los nodos mientras se arrastran. El lienzo es controlado: sin
  // esto no se moverían hasta que el motor escribiera la posición al soltar.
  const [arrastrando, setArrastrando] = useState<Record<string, XYPosition>>({});
  // De dónde sale cada columna. Se pide al abrir el inspector y otra vez
  // cada vez que cambia el fichero, no con cada grafo: para saberlo, los
  // orígenes abren su CSV o preguntan a su base de datos.
  const [linaje, setLinaje] = useState<(Linaje & { clave: string }) | null>(null);
  const linajePedido = useRef<string | null>(null);

  const flujo = useRef<Instancia | null>(null);
  const lienzo = useRef<HTMLDivElement | null>(null);
  const arrastrado = useRef<Arrastrado | null>(null);
  // La revisión vive en una ref además de en `grafo`: un cambio que espera en
  // la cola tiene que usar la del grafo que dejó el anterior, no la que había
  // cuando se pidió.
  const revision = useRef<string | null>(null);
  const cola = useRef<Promise<void>>(Promise.resolve());
  const ruta = useRef(pipeline.path);
  const conInspector = useRef(false);
  // El grafo recién abierto se encuadra una vez, cuando ya hay nodos.
  const porEncuadrar = useRef(true);
  // Objetos de nodos y aristas que se reutilizan mientras no cambien: React
  // Flow sólo vuelve a procesar y pintar los que llegan nuevos.
  const reutilizables = useRef<Reutilizables>({ nodos: new Map(), aristas: new Map() });
  const vistos = useRef(new Map<string, Visto>());

  const pintar = useCallback((g: Graph | null) => {
    revision.current = g?.revision ?? null;
    setGrafo(g);
  }, []);

  useEffect(() => {
    api
      .catalog()
      .then(setCatalogo)
      .catch(() => setCatalogo(null));
  }, [pipeline.path]);

  useEffect(() => {
    ruta.current = pipeline.path;
    porEncuadrar.current = true;
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
            requestAnimationFrame(() => requestAnimationFrame(() => encuadrar(200, true)));
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

  // Lo que dice el fichero: depende sólo del grafo.
  const base = useMemo(() => {
    const desde = performance.now();
    const hecho = construir(grafo, reutilizables.current);
    medidas.construir = performance.now() - desde;
    return hecho;
  }, [grafo]);

  // Las posiciones de verdad son las del fichero: con el grafo nuevo se
  // olvidan las de en vuelo, tanto si se guardaron como si el cambio se
  // rechazó y el nodo vuelve a su sitio.
  useEffect(() => setArrastrando({}), [grafo]);

  // Encima, lo efímero: la selección y el arrastre. Elegir un nodo crea dos
  // objetos nuevos —el que se elige y el que se deja—, no quinientos.
  const nodos = useMemo(
    () =>
      base.nodos.map((n) => {
        const enVuelo = arrastrando[n.id];
        const seleccionado = n.id === elegido;
        const previo = vistos.current.get(n.id);
        if (
          previo &&
          previo.base === n &&
          previo.enVuelo === enVuelo &&
          previo.seleccionado === seleccionado
        ) {
          return previo.nodo;
        }
        const nodo: Node = {
          ...n,
          selected: seleccionado,
          ...(enVuelo ? { position: enVuelo } : {}),
        };
        vistos.current.set(n.id, { base: n, enVuelo, seleccionado, nodo });
        return nodo;
      }),
    [base.nodos, arrastrando, elegido],
  );
  const aristas = useMemo(
    () => base.aristas.map((a) => (a.id === aristaElegida ? { ...a, selected: true } : a)),
    [base.aristas, aristaElegida],
  );

  // De la selección y el borrado se ocupan sus propios manejadores, y las
  // medidas ya van en cada nodo; aquí sólo entra el arrastre.
  const alCambiarNodos = useCallback((cambios: NodeChange[]) => {
    const movidos: Record<string, XYPosition> = {};
    for (const cambio of cambios) {
      if (cambio.type === "position" && cambio.position) {
        movidos[cambio.id] = cambio.position;
      }
    }
    if (Object.keys(movidos).length > 0) setArrastrando((antes) => ({ ...antes, ...movidos }));
  }, []);

  /**
   * Encuadra el grafo en lo que dejan libre los paneles.
   *
   * Si cabe entero a un tamaño legible, entero; si no, se empieza por arriba
   * a la izquierda, donde están los orígenes. Con `soloSiCabe`, un grafo que
   * no cabe se deja como está: reencuadrar uno grande después de cada cambio
   * te sacaría de la zona en la que estabas trabajando.
   */
  const encuadrar = useCallback(
    (duracion = 0, soloSiCabe = false) => {
      const rf = flujo.current;
      const caja = lienzo.current;
      if (!rf || !caja || base.nodos.length === 0) return false;
      const vista = encuadre(
        base.nodos,
        caja.clientWidth,
        caja.clientHeight,
        conInspector.current,
      );
      if (soloSiCabe && !vista.cabe) {
        if (elegido) asomar(rf, caja, elegido);
        return true;
      }
      void rf.setViewport(vista.viewport, { duration: duracion });
      return true;
    },
    [base.nodos, elegido],
  );

  // El primer encuadre, en cuanto hay nodos y lienzo.
  useEffect(() => {
    if (porEncuadrar.current && encuadrar()) porEncuadrar.current = false;
  }, [encuadrar]);

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
        for (const n of base.nodos) posiciones[n.id] = [n.position.x, n.position.y];
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
    [grafo, base.nodos, editar],
  );

  const colocado = grafo?.nodes.some((n) => n.position !== null) ?? false;

  const nodoElegido = grafo?.nodes.find((n) => n.id === elegido) ?? null;
  conInspector.current = nodoElegido !== null;

  const hayInspector = nodoElegido !== null;
  const revisionActual = grafo?.revision ?? null;
  useEffect(() => {
    if (!hayInspector || revisionActual === null) return;
    const path = pipeline.path;
    const clave = `${path}#${revisionActual}`;
    if (linajePedido.current === clave) return;
    linajePedido.current = clave;
    setLinaje((actual) => ({
      clave,
      // Mientras llega lo nuevo se sigue viendo lo anterior del mismo
      // pipeline: editar un campo no debe hacer parpadear la sección.
      nodos: actual?.clave.startsWith(`${path}#`) ? actual.nodos : null,
      error: null,
      cargando: true,
    }));
    // Sólo cuenta la respuesta a la última petición: una lenta que llega
    // tarde no pisa a la que la sustituyó.
    const llegada = (resto: Omit<Linaje, "cargando">) =>
      setLinaje((actual) =>
        actual?.clave === clave ? { clave, ...resto, cargando: false } : actual,
      );
    api.pipelineLineage(path).then(
      (nodos) => llegada({ nodos, error: null }),
      (e) => llegada({ nodos: null, error: String(e) }),
    );
  }, [hayInspector, revisionActual, pipeline.path]);

  const irA = useCallback((id: string) => {
    setElegido(id);
    setAristaElegida(null);
    requestAnimationFrame(() => asomar(flujo.current, lienzo.current, id));
  }, []);

  return (
    <>
      {error ? <p className="nota nota--error nota--suelta">{error}</p> : null}
      {grafo?.problem ? (
        <p className="nota nota--error nota--suelta">{grafo.problem}</p>
      ) : null}

      <div className="lienzo" aria-busy={guardando} ref={lienzo}>
        {conProfiler(
          <ReactFlow
            nodes={nodos}
            edges={aristas}
            nodeTypes={TIPOS_DE_NODO}
            onInit={(instancia) => {
              flujo.current = instancia;
              if (porEncuadrar.current && encuadrar()) porEncuadrar.current = false;
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
            minZoom={0.2}
            maxZoom={1.6}
            proOptions={{ hideAttribution: true }}
            nodesDraggable
            // Con Espacio, React Flow mueve la cámara aunque el gesto empiece
            // sobre un nodo, y el nodo se arrastra a la vez. El «soltar» se lo
            // queda la cámara: el nodo se quedaba desplazado en pantalla sin
            // que nada se guardara. Para desplazar basta con arrastrar el fondo.
            panActivationKeyCode={null}
            // Sólo se montan los nodos y aristas a la vista: con un pipeline
            // de cientos de nodos y zoom de trabajo, son unas decenas.
            onlyRenderVisibleElements
            nodesConnectable
            elementsSelectable
          >
            <Background variant={BackgroundVariant.Dots} gap={18} size={1} />
            {/* El botón de encuadre sí enseña el grafo entero, aunque no se
                lea: es lo que se le pide. */}
            <Controls showInteractive={false} fitViewOptions={ENCUADRE}>
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
          </ReactFlow>,
        )}

        {MEDIR ? <Medidor /> : null}

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
            linaje={linaje}
            onIrA={irA}
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

/** En modo medición, el lienzo va dentro de un `Profiler`; si no, tal cual. */
function conProfiler(lienzo: ReactNode) {
  return MEDIR ? (
    <Profiler id="lienzo" onRender={alRenderizar}>
      {lienzo}
    </Profiler>
  ) : (
    lienzo
  );
}

/**
 * Si un nodo quedó debajo de la paleta o del inspector, desplaza el lienzo lo
 * justo para que asome, sin cambiar el zoom. Con las posiciones guardadas no
 * se reencuadra todo después de cada cambio, así que esto hace falta.
 */
function asomar(rf: Instancia | null, contenedor: HTMLElement | null, id: string) {
  const nodo = rf?.getNode(id);
  if (!rf || !nodo || !contenedor) return;
  const caja = contenedor.getBoundingClientRect();
  const libre = {
    izquierda: caja.left + MARGEN.izquierda,
    derecha: caja.right - MARGEN.conInspector,
    arriba: caja.top + MARGEN.vertical,
    abajo: caja.bottom - MARGEN.vertical,
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

interface Reutilizables {
  nodos: Map<string, { firma: string; nodo: Node }>;
  aristas: Map<string, { firma: string; arista: Edge<DatosArista> }>;
}

/** Un nodo tal y como se le pasó a React Flow, con lo que lo produjo. */
interface Visto {
  base: Node;
  enVuelo: XYPosition | undefined;
  seleccionado: boolean;
  nodo: Node;
}

/**
 * Los nodos y aristas del fichero, listos para React Flow.
 *
 * Reutiliza el objeto de cada uno si nada de lo que se dibuja cambió: después
 * de editar un campo o de mover un nodo, React Flow sólo vuelve a procesar
 * lo que cambió de verdad, no los cientos de nodos de un pipeline grande.
 */
function construir(
  grafo: Graph | null,
  previos: Reutilizables,
): { nodos: Node[]; aristas: Edge<DatosArista>[] } {
  if (!grafo) return { nodos: [], aristas: [] };

  const colocados = disponer(grafo);
  const todas = aristasDe(grafo);
  // Quién recibe y quién da, en un solo recorrido: preguntarlo nodo a nodo
  // recorría todas las aristas por cada uno.
  const reciben = new Set(todas.map((a) => a.to));
  const dan = new Set(todas.map((a) => a.from));

  const nodosNuevos: Reutilizables["nodos"] = new Map();
  const nodos: Node[] = grafo.nodes.map((nodo) => {
    // Lo que el usuario colocó manda; lo demás lo coloca el lienzo.
    const position = nodo.position
      ? { x: nodo.position[0], y: nodo.position[1] }
      : (colocados.get(nodo.id) ?? { x: 0, y: 0 });
    const data = {
      etiqueta: nodo.id,
      componente: nodo.component,
      tipo: nodo.kind,
      conocido: nodo.known,
      // Los conectores no salen del tipo de nodo sino de lo que realmente
      // llega y sale. Un sink no tiene salida de datos, pero sí puede ser el
      // origen de una barrera `after`, y sin conector la arista no tendría
      // dónde engancharse: desaparecía del dibujo.
      entradas: nodo.kind !== "source" || reciben.has(nodo.id) ? 1 : 0,
      salidas: nodo.kind !== "sink" || dan.has(nodo.id) ? 1 : 0,
    };
    const firma = JSON.stringify([position.x, position.y, data]);
    const previo = previos.nodos.get(nodo.id);
    const hecho =
      previo?.firma === firma
        ? previo.nodo
        : { id: nodo.id, type: "pipeline", position, data, measured: MEDIDA_NODO };
    nodosNuevos.set(nodo.id, { firma, nodo: hecho });
    return hecho;
  });

  const aristasNuevas: Reutilizables["aristas"] = new Map();
  const aristas: Edge<DatosArista>[] = todas.map((arista, i) => {
    // Sin el índice, el id sobrevive a que se quite una arista anterior; con
    // él sólo se desempatan las repetidas, que un fichero roto puede tener.
    const natural = `${arista.barrera ? "after" : "datos"}:${arista.from}->${arista.to}`;
    const id = aristasNuevas.has(natural) ? `${natural}#${i}` : natural;
    // El puerto sólo se etiqueta cuando dice algo: por defecto es el id del
    // nodo de origen, y repetirlo sobre la flecha es ruido.
    const label = !arista.barrera && arista.port !== arista.from ? arista.port : undefined;
    const firma = label ?? "";
    const previo = previos.aristas.get(id);
    const hecha: Edge<DatosArista> =
      previo?.firma === firma
        ? previo.arista
        : {
            id,
            source: arista.from,
            target: arista.to,
            data: { barrera: arista.barrera },
            // Una barrera `after` no lleva datos: se dibuja punteada para que
            // no se confunda con el flujo.
            animated: false,
            className: arista.barrera ? "arista arista--barrera" : "arista",
            label,
          };
    aristasNuevas.set(id, { firma, arista: hecha });
    return hecha;
  });

  previos.nodos = nodosNuevos;
  previos.aristas = aristasNuevas;
  return { nodos, aristas };
}

/** Dónde poner la cámara para ver el grafo, y si cabe entero legible. */
function encuadre(
  nodos: Node[],
  ancho: number,
  alto: number,
  conInspector: boolean,
): { viewport: Viewport; cabe: boolean } {
  const derecha = conInspector ? MARGEN.conInspector : MARGEN.derecha;
  const libreAncho = Math.max(1, ancho - MARGEN.izquierda - derecha);
  const libreAlto = Math.max(1, alto - 2 * MARGEN.vertical);

  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  for (const { position } of nodos) {
    minX = Math.min(minX, position.x);
    minY = Math.min(minY, position.y);
    maxX = Math.max(maxX, position.x + ANCHO_NODO);
    maxY = Math.max(maxY, position.y + ALTO_NODO);
  }
  const zoomQueCabe = Math.min(libreAncho / (maxX - minX), libreAlto / (maxY - minY), 1);

  if (zoomQueCabe >= ZOOM_LEGIBLE) {
    const zoom = zoomQueCabe;
    return {
      cabe: true,
      viewport: {
        zoom,
        x: MARGEN.izquierda + (libreAncho - (maxX - minX) * zoom) / 2 - minX * zoom,
        y: MARGEN.vertical + (libreAlto - (maxY - minY) * zoom) / 2 - minY * zoom,
      },
    };
  }
  // No cabe legible: por el principio, arriba a la izquierda.
  const zoom = ZOOM_LEGIBLE;
  return {
    cabe: false,
    viewport: {
      zoom,
      x: MARGEN.izquierda - minX * zoom,
      y: MARGEN.vertical - minY * zoom,
    },
  };
}

/** El esquema del componente de un nodo, del catálogo. */
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
