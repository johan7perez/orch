/**
 * Único punto de contacto con el motor.
 *
 * Todo lo que cruza son eventos y métricas: los lotes de Arrow nunca salen
 * de Rust. Por eso la ventana puede seguir fluida mientras el motor mueve
 * millones de filas.
 */
import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";

export interface PipelineInfo {
  name: string;
  path: string;
  description: string | null;
  nodes: number;
  trigger: string;
  scheduled: boolean;
  problem: string | null;
}

export interface Workspace {
  directory: string;
  pipelines: PipelineInfo[];
  problem: string | null;
}

export interface GraphNode {
  id: string;
  /** `source`, `transform` o `sink`. */
  kind: string;
  /** Conector u operación: `csv`, `sql`, `postgres`… */
  component: string;
  /**
   * La config tal y como está escrita en el fichero. Los `${env:...}` llegan
   * sin expandir a propósito: un secreto resuelto no debe aparecer nunca en
   * pantalla.
   */
  config: Record<string, unknown>;
  after: string[];
  /** `false` si el conector no existe en el registro. */
  known: boolean;
}

export interface GraphEdge {
  from: string;
  to: string;
  port: string;
}

export interface Graph {
  name: string;
  description: string | null;
  nodes: GraphNode[];
  edges: GraphEdge[];
  /** Por qué no valida, si no valida. El grafo se dibuja igual. */
  problem: string | null;
}

/** Un componente registrado, con el esquema JSON de su config. */
export interface Component {
  name: string;
  schema: EsquemaObjeto | null;
}

export interface Catalog {
  sources: Component[];
  transforms: Component[];
  sinks: Component[];
}

/** Lo que nos interesa del JSON Schema que genera `schemars`. */
export interface EsquemaObjeto {
  properties?: Record<string, EsquemaCampo>;
  required?: string[];
}

export interface EsquemaCampo {
  /** `"string"`, o `["integer", "null"]` cuando el campo es opcional. */
  type?: string | string[];
  /** Sale de los comentarios `///` del struct de Rust. */
  description?: string;
  default?: unknown;
  enum?: unknown[];
}

export interface RunSummary {
  run_id: string;
  pipeline: string;
  started_at: string;
  status: string;
  elapsed_ms: number;
  nodes: number;
  failed_nodes: number;
}

export interface NodeRow {
  node: string;
  kind: string;
  component: string;
  status: string;
  attempts: number;
  rows_in: number;
  rows_out: number;
  elapsed_ms: number;
  stalled_in_ms: number;
  stalled_out_ms: number;
  error: string | null;
  rows_per_second: number;
  busy_pct: number;
  busy_ms: number;
}

export interface EventRow {
  seq: number;
  at: string;
  kind: string;
  node: string | null;
  detail: string | null;
}

export interface RunDetail {
  run_id: string;
  nodes: NodeRow[];
  events: EventRow[];
}

/** Los eventos que emite el motor, tal y como llegan del canal. */
export type RunEvent =
  | { event: "run_started"; run_id: string; pipeline: string; nodes: number }
  | {
      event: "node_started";
      run_id: string;
      node: string;
      kind: string;
      component: string;
      attempt: number;
    }
  | {
      event: "node_finished";
      run_id: string;
      node: string;
      input: IoStats;
      output: IoStats;
      elapsed_ms: number;
    }
  | {
      event: "node_failed";
      run_id: string;
      node: string;
      attempt: number;
      error: string;
      will_retry: boolean;
    }
  | { event: "node_skipped"; run_id: string; node: string; reason: string }
  | {
      event: "run_finished";
      run_id: string;
      pipeline: string;
      succeeded: boolean;
      elapsed_ms: number;
    };

export interface IoStats {
  rows: number;
  batches: number;
  bytes: number;
  stalled_ms: number;
}

export const api = {
  workspace: () => invoke<Workspace>("workspace"),
  setDirectory: (path: string) => invoke<Workspace>("set_directory", { path }),
  recentRuns: (limit = 50) => invoke<RunSummary[]>("recent_runs", { limit }),
  runDetail: (runId: string) => invoke<RunDetail>("run_detail", { runId }),
  startRun: (path: string) => invoke<void>("start_run", { path }),
  pipelineGraph: (path: string) => invoke<Graph>("pipeline_graph", { path }),
  catalog: () => invoke<Catalog>("catalog"),
};

/** Se engancha al flujo de eventos del motor. */
export function onRunEvent(handler: (event: RunEvent) => void): Promise<UnlistenFn> {
  return listen<RunEvent>("orch://event", (message) => handler(message.payload));
}

/** Avisa de que una ejecución terminó, con su informe completo. */
export function onFinished(handler: () => void): Promise<UnlistenFn> {
  return listen("orch://finished", () => handler());
}

/** Avisa de que una ejecución no llegó ni a arrancar. */
export function onFailed(handler: (message: string) => void): Promise<UnlistenFn> {
  return listen<string>("orch://failed", (message) => handler(message.payload));
}
