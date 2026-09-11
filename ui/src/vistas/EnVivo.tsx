import { useEffect, useRef } from "react";

import type { PipelineInfo } from "../api";
import { hora } from "../formato";
import type { EnCurso } from "../useMotor";

interface Props {
  pipeline: PipelineInfo | null;
  enCurso: EnCurso | null;
  error: string | null;
  cargando: boolean;
  problema: string | null;
  onEjecutar: (ruta: string, nombre: string) => void;
}

export function EnVivo({ pipeline, enCurso, error, cargando, problema, onEjecutar }: Props) {
  const fondo = useRef<HTMLDivElement>(null);

  // El registro se mantiene pegado abajo mientras llegan eventos: es donde
  // está lo que acaba de pasar.
  useEffect(() => {
    fondo.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [enCurso?.eventos.length]);

  if (cargando) {
    return <div className="vacio">Cargando…</div>;
  }

  if (!pipeline) {
    return (
      <div className="contenido">
        {problema ? <p className="nota nota--error">{problema}</p> : null}
        <div className="vacio">
          <strong>Elige un pipeline</strong>
          <span>Se ejecuta aquí y verás cada nodo según ocurra.</span>
        </div>
      </div>
    );
  }

  const corriendo = enCurso !== null && !enCurso.terminada && enCurso.pipeline === pipeline.name;
  const suyo = enCurso?.pipeline === pipeline.name ? enCurso : null;

  return (
    <>
      <header className="barra">
        <div className="barra__texto">
          <h1 className="barra__titulo">{pipeline.name}</h1>
          <p className="barra__sub">
            {pipeline.description ?? `${pipeline.nodes} nodos`} · {pipeline.trigger}
          </p>
        </div>
        <div className="crecer" />
        <button
          className="boton"
          disabled={corriendo || pipeline.problem !== null}
          onClick={() => onEjecutar(pipeline.path, pipeline.name)}
        >
          {corriendo ? "Ejecutando…" : "Ejecutar"}
        </button>
      </header>

      <div className="contenido">
        {problema ? <p className="nota nota--error">{problema}</p> : null}
        {pipeline.problem ? (
          <p className="nota nota--error">{pipeline.problem}</p>
        ) : null}
        {error ? <p className="nota nota--error">{error}</p> : null}

        {suyo ? (
          <>
            <p className="seccion">Nodos</p>
            <table className="tabla">
              <thead>
                <tr>
                  <th>Nodo</th>
                  <th>Estado</th>
                </tr>
              </thead>
              <tbody>
                {[...suyo.nodos.entries()].map(([nodo, estado]) => (
                  <tr key={nodo}>
                    <td>{nodo}</td>
                    <td>
                      <span className="estado">
                        <span className={puntoDe(estado)} />
                        {legible(estado)}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            <p className="seccion">En vivo</p>
            <div className="eventos">
              {suyo.eventos.map((evento) => (
                <div className="evento" key={evento.seq}>
                  <span className="evento__hora">{hora(evento.at)}</span>
                  <span className={claseTipo(evento.kind)}>{evento.kind}</span>
                  <span>
                    {evento.node ? <span className="etiqueta">{evento.node}</span> : null}{" "}
                    {evento.detail}
                  </span>
                </div>
              ))}
              <div ref={fondo} />
            </div>
          </>
        ) : (
          <p className="nota">
            Sin ejecuciones en esta sesión. Pulsa <strong>Ejecutar</strong> para verlo
            funcionar; el historial completo está en la pestaña Ejecuciones.
          </p>
        )}
      </div>
    </>
  );
}

function puntoDe(estado: string): string {
  switch (estado) {
    case "running":
      return "punto punto--curso";
    case "succeeded":
      return "punto punto--ok";
    case "failed":
      return "punto punto--fallo";
    default:
      return "punto punto--omitido";
  }
}

function legible(estado: string): string {
  switch (estado) {
    case "running":
      return "en curso";
    case "succeeded":
      return "correcto";
    case "failed":
      return "fallido";
    default:
      return "omitido";
  }
}

function claseTipo(kind: string): string {
  if (kind === "node_failed") return "evento__tipo evento__tipo--fallo";
  if (kind === "run_finished") return "evento__tipo evento__tipo--fin";
  return "evento__tipo";
}
