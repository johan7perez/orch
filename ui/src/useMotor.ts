/**
 * Estado de la aplicación: qué pipelines hay, qué ejecuciones existen y qué
 * está pasando ahora mismo.
 *
 * Los eventos en vivo se acumulan aquí en vez de en cada vista, para que
 * cambiar de pestaña a mitad de una ejecución no pierda nada.
 */
import { useCallback, useEffect, useRef, useState } from "react";

import {
  api,
  onFailed,
  onFinished,
  onRunEvent,
  type EventRow,
  type RunEvent,
  type RunSummary,
  type Workspace,
} from "./api";

/** Una ejecución que está ocurriendo ahora mismo. */
export interface EnCurso {
  runId: string;
  pipeline: string;
  eventos: EventRow[];
  /** Estado por nodo, tal y como va llegando. */
  nodos: Map<string, string>;
  terminada: boolean;
  correcta: boolean | null;
}

export function useMotor() {
  const [workspace, setWorkspace] = useState<Workspace | null>(null);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [enCurso, setEnCurso] = useState<EnCurso | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [cargando, setCargando] = useState(true);

  // La secuencia de eventos vivos es local: los del almacén tienen la suya.
  const secuencia = useRef(0);

  const refrescarWorkspace = useCallback(async () => {
    try {
      setWorkspace(await api.workspace());
    } catch (err) {
      setError(String(err));
    }
  }, []);

  const refrescarRuns = useCallback(async () => {
    try {
      setRuns(await api.recentRuns(50));
    } catch (err) {
      setError(String(err));
    }
  }, []);

  useEffect(() => {
    void (async () => {
      await Promise.all([refrescarWorkspace(), refrescarRuns()]);
      setCargando(false);
    })();
  }, [refrescarWorkspace, refrescarRuns]);

  useEffect(() => {
    const suscripciones = [
      onRunEvent((evento) => {
        setEnCurso((anterior) => aplicar(anterior, evento, secuencia));
      }),
      onFinished(() => {
        void refrescarRuns();
      }),
      onFailed((mensaje) => {
        setError(mensaje);
        setEnCurso(null);
      }),
    ];
    return () => {
      // Las suscripciones se resuelven en segundo plano; al desmontar se
      // cancelan en cuanto estén listas.
      for (const suscripcion of suscripciones) {
        void suscripcion.then((cancelar) => cancelar());
      }
    };
  }, [refrescarRuns]);

  const ejecutar = useCallback(async (ruta: string) => {
    setError(null);
    secuencia.current = 0;
    setEnCurso(null);
    try {
      await api.startRun(ruta);
    } catch (err) {
      setError(String(err));
    }
  }, []);

  const elegirDirectorio = useCallback(async (ruta: string) => {
    try {
      setWorkspace(await api.setDirectory(ruta));
    } catch (err) {
      setError(String(err));
    }
  }, []);

  return {
    workspace,
    runs,
    enCurso,
    error,
    cargando,
    ejecutar,
    elegirDirectorio,
    refrescarRuns,
    limpiarError: () => setError(null),
  };
}

function aplicar(
  anterior: EnCurso | null,
  evento: RunEvent,
  secuencia: { current: number },
): EnCurso {
  const base: EnCurso =
    anterior && anterior.runId === evento.run_id
      ? anterior
      : {
          runId: evento.run_id,
          pipeline: "pipeline" in evento ? evento.pipeline : "",
          eventos: [],
          nodos: new Map(),
          terminada: false,
          correcta: null,
        };

  const nodos = new Map(base.nodos);
  let terminada = base.terminada;
  let correcta = base.correcta;
  let pipeline = base.pipeline;
  let detalle = "";

  switch (evento.event) {
    case "run_started":
      pipeline = evento.pipeline;
      detalle = `${evento.pipeline} (${evento.nodes} nodos)`;
      break;
    case "node_started":
      nodos.set(evento.node, "running");
      detalle = `${evento.kind}:${evento.component}${
        evento.attempt > 1 ? ` · intento ${evento.attempt}` : ""
      }`;
      break;
    case "node_finished":
      nodos.set(evento.node, "succeeded");
      detalle = `${evento.output.rows} filas en ${evento.elapsed_ms} ms`;
      break;
    case "node_failed":
      nodos.set(evento.node, evento.will_retry ? "running" : "failed");
      detalle = evento.error;
      break;
    case "node_skipped":
      nodos.set(evento.node, "skipped");
      detalle = evento.reason;
      break;
    case "run_finished":
      terminada = true;
      correcta = evento.succeeded;
      pipeline = evento.pipeline;
      detalle = `${evento.succeeded ? "correcto" : "fallido"} en ${evento.elapsed_ms} ms`;
      break;
  }

  const fila: EventRow = {
    seq: secuencia.current++,
    at: new Date().toISOString(),
    kind: evento.event,
    node: "node" in evento ? evento.node : null,
    detail: detalle,
  };

  return {
    ...base,
    pipeline,
    nodos,
    terminada,
    correcta,
    eventos: [...base.eventos, fila],
  };
}
