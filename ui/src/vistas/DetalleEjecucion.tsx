import { useEffect, useState } from "react";

import {
  api,
  type NodeRow,
  type RunDetail,
  type RunSummary,
  type RunVersion,
  type VersionDiff,
} from "../api";
import { Curva, ritmo } from "../Curva";
import { claseDePunto, estadoLegible, fechaHora, hora, miles, milisegundos } from "../formato";
import { Isotipo } from "../Marca";
import { Cambios } from "./Cambios";
import { claseAlerta } from "./EnVivo";

interface Props {
  detalle: RunDetail | null;
  resumen: RunSummary | null;
}

export function DetalleEjecucion({ detalle, resumen }: Props) {
  if (!detalle) {
    return (
      <div className="vacio">
        <Isotipo className="vacio__marca" />
        <strong>Elige una ejecución</strong>
        <span>Verás sus métricas por nodo y todo lo que ocurrió.</span>
      </div>
    );
  }

  // El nodo que más tiempo pasó trabajando sin esperar a nadie es el que
  // marca el ritmo: el resto le espera. Se mira en absoluto y no en
  // porcentaje, porque un nodo que vive 3 ms y no espera da 100%.
  const cuello = detalle.nodes.reduce<NodeRow | null>(
    (mayor, nodo) => (nodo.busy_ms > (mayor?.busy_ms ?? 0) ? nodo : mayor),
    null,
  );

  // Todas las curvas comparten el eje de tiempo: la ejecución entera.
  const duracion = Math.max(
    resumen?.elapsed_ms ?? 0,
    ...detalle.progress.map((m) => m.at_ms),
  );

  return (
    <>
      <header className="barra">
        <div className="barra__texto">
          <h1 className="barra__titulo">{resumen?.pipeline ?? "Ejecución"}</h1>
          <p className="barra__sub">
            {resumen ? (
              <>
                {fechaHora(resumen.started_at)} · {milisegundos(resumen.elapsed_ms)} ·{" "}
                <span className="estado">
                  <span className={claseDePunto(resumen.status)} />
                  {estadoLegible(resumen.status)}
                </span>
                {detalle.version ? <> · versión {detalle.version.label}</> : null}
              </>
            ) : (
              detalle.run_id
            )}
          </p>
        </div>
      </header>

      <div className="contenido">
        {detalle.alerts.map((alerta, i) => (
          <p className={claseAlerta(alerta.kind)} key={i}>
            {alerta.message}
            <span className="tenue"> · {hora(alerta.at)}</span>
          </p>
        ))}

        <p className="seccion">Nodos</p>
        <table className="tabla">
          <thead>
            <tr>
              <th>Nodo</th>
              <th>Estado</th>
              <th className="num">Filas</th>
              <th className="num">Filas/s</th>
              <th>Ritmo</th>
              <th className="num">Tiempo</th>
              <th className="num">Ocupado</th>
            </tr>
          </thead>
          <tbody>
            {detalle.nodes.map((nodo) => {
              const movidas = nodo.kind === "sink" ? nodo.rows_in : nodo.rows_out;
              const esCuello = cuello?.node === nodo.node;
              return (
                <tr key={nodo.node}>
                  <td>
                    {nodo.node}
                    <span className="tenue"> · {nodo.component}</span>
                  </td>
                  <td>
                    <span className="estado">
                      <span className={claseDePunto(nodo.status)} />
                      {estadoLegible(nodo.status)}
                    </span>
                  </td>
                  <td className="num">{miles(movidas)}</td>
                  <td className="num">{miles(nodo.rows_per_second)}</td>
                  <td>
                    <Curva
                      puntos={ritmo(
                        detalle.progress
                          .filter((m) => m.node === nodo.node)
                          .map((m) => ({
                            t: m.at_ms,
                            filas: nodo.kind === "sink" ? m.rows_in : m.rows_out,
                          })),
                      )}
                      desde={0}
                      hasta={duracion}
                      etiqueta={nodo.node}
                      conTooltip
                    />
                  </td>
                  <td className="num">{milisegundos(nodo.elapsed_ms)}</td>
                  <td>
                    <div className="ocupacion">
                      <span className="tenue">{Math.round(nodo.busy_pct)}%</span>
                      <span className="ocupacion__barra">
                        <span
                          className={
                            esCuello
                              ? "ocupacion__relleno ocupacion__relleno--cuello"
                              : "ocupacion__relleno"
                          }
                          style={{ width: `${Math.min(100, Math.max(0, nodo.busy_pct))}%` }}
                        />
                      </span>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>

        {cuello ? (
          <p className="nota">
            <strong>{cuello.node}</strong> es el que marca el ritmo: pasó{" "}
            {milisegundos(cuello.busy_ms)} trabajando sin esperar a nadie. El resto del
            pipeline le espera.
          </p>
        ) : null}

        {detalle.nodes
          .filter((nodo) => nodo.error)
          .map((nodo) => (
            <p className="nota nota--error" key={nodo.node}>
              <strong>{nodo.node}</strong>: {nodo.error}
            </p>
          ))}

        {detalle.version ? (
          <VersionCorrida version={detalle.version} runId={detalle.run_id} />
        ) : null}

        <p className="seccion">Eventos</p>
        <div className="eventos">
          {detalle.events.map((evento) => (
            <div className="evento" key={evento.seq}>
              <span className="evento__hora">{hora(evento.at)}</span>
              <span className="evento__tipo">{evento.kind}</span>
              <span>{evento.detail}</span>
            </div>
          ))}
        </div>
      </div>
    </>
  );
}

/** Qué versión del pipeline corrió, y qué ha cambiado desde entonces. */
function VersionCorrida({ version, runId }: { version: RunVersion; runId: string }) {
  const [abierto, setAbierto] = useState(false);
  const [diff, setDiff] = useState<VersionDiff | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!abierto || diff) return;
    let vigente = true;
    api
      .pipelineDiff(version.path, { kind: "run", run_id: runId }, { kind: "working" })
      .then((d) => vigente && setDiff(d))
      .catch((e) => vigente && setError(String(e)));
    return () => {
      vigente = false;
    };
  }, [abierto, diff, version.path, runId]);

  return (
    <>
      <p className="seccion">Versión</p>
      <p className="versiones__estado">
        Corrió <code>{version.label}</code>
        {version.git_subject && !version.modified ? <> «{version.git_subject}»</> : null}
        {version.modified && version.git_commit
          ? ", con cambios sin guardar sobre ese commit"
          : null}
        .{" "}
        <button
          className="enlace enlace--texto"
          aria-expanded={abierto}
          onClick={() => setAbierto((a) => !a)}
        >
          {abierto ? "Ocultar qué ha cambiado" : "Qué ha cambiado desde entonces"}
        </button>
      </p>
      {abierto ? (
        error ? (
          <p className="nota nota--error">{error}</p>
        ) : diff ? (
          <Cambios cambios={diff.changes} />
        ) : (
          <p className="tenue">Comparando…</p>
        )
      ) : null}
    </>
  );
}
