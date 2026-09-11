import type { RunSummary, Workspace } from "../api";
import { claseDePunto, fechaHora, milisegundos } from "../formato";
import type { Pestana } from "../App";
import type { EnCurso } from "../useMotor";

interface Props {
  pestana: Pestana;
  onPestana: (pestana: Pestana) => void;
  workspace: Workspace | null;
  runs: RunSummary[];
  seleccion: string | null;
  enCurso: EnCurso | null;
  onPipeline: (nombre: string) => void;
  onEjecucion: (runId: string) => void;
  onCarpeta: () => void;
}

export function Lateral({
  pestana,
  onPestana,
  workspace,
  runs,
  seleccion,
  enCurso,
  onPipeline,
  onEjecucion,
  onCarpeta,
}: Props) {
  return (
    <aside className="lateral">
      <header className="lateral__cabecera">
        <div className="marca">
          <span className="marca__glifo" aria-hidden="true">
            <span className="marca__punto" />
            <span className="marca__punto" />
            <span className="marca__punto" />
          </span>
          Orch
        </div>
      </header>

      <div className="pestanas" role="tablist">
        <button
          className="pestana"
          role="tab"
          aria-selected={pestana === "pipelines"}
          onClick={() => onPestana("pipelines")}
        >
          Pipelines
        </button>
        <button
          className="pestana"
          role="tab"
          aria-selected={pestana === "ejecuciones"}
          onClick={() => onPestana("ejecuciones")}
        >
          Ejecuciones
        </button>
      </div>

      <div className="lista">
        {pestana === "pipelines"
          ? (workspace?.pipelines ?? []).map((pipeline) => (
              <button
                key={pipeline.name}
                className="fila"
                aria-selected={seleccion === pipeline.name}
                onClick={() => onPipeline(pipeline.name)}
              >
                <span className="fila__titulo">
                  {enCurso && !enCurso.terminada && enCurso.pipeline === pipeline.name ? (
                    <span className="punto punto--curso" />
                  ) : pipeline.problem ? (
                    <span className="punto punto--fallo" />
                  ) : null}
                  {pipeline.name}
                </span>
                <span className="fila__sub">
                  {pipeline.problem ? "no válido" : pipeline.trigger}
                </span>
              </button>
            ))
          : runs.map((run) => (
              <button
                key={run.run_id}
                className="fila"
                aria-selected={seleccion === run.run_id}
                onClick={() => onEjecucion(run.run_id)}
              >
                <span className="fila__titulo">
                  <span className={claseDePunto(run.status)} />
                  {run.pipeline}
                </span>
                <span className="fila__sub">
                  {fechaHora(run.started_at)} · {milisegundos(run.elapsed_ms)}
                </span>
              </button>
            ))}

        {pestana === "pipelines" && (workspace?.pipelines.length ?? 0) === 0 ? (
          <p className="fila__sub" style={{ padding: "8px 10px" }}>
            No hay pipelines en esta carpeta.
          </p>
        ) : null}
        {pestana === "ejecuciones" && runs.length === 0 ? (
          <p className="fila__sub" style={{ padding: "8px 10px" }}>
            Todavía no se ha ejecutado nada.
          </p>
        ) : null}
      </div>

      <footer className="lateral__pie">
        <button className="ruta" onClick={onCarpeta} title="Cambiar de carpeta">
          {workspace?.directory ?? "…"}
        </button>
      </footer>
    </aside>
  );
}
