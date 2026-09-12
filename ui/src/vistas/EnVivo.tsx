/**
 * Lo que está pasando ahora mismo: estado por nodo y el registro de eventos
 * según llegan. La barra y el botón de ejecutar los pone `Pipeline`.
 */
import { useEffect, useRef } from "react";

import type { PipelineInfo } from "../api";
import { hora } from "../formato";
import type { EnCurso } from "../useMotor";

interface Props {
  pipeline: PipelineInfo;
  enCurso: EnCurso | null;
  error: string | null;
  problema: string | null;
}

export function EnVivo({ pipeline, enCurso, error, problema }: Props) {
  const fondo = useRef<HTMLDivElement>(null);

  // El registro se mantiene pegado abajo mientras llegan eventos: es donde
  // está lo que acaba de pasar.
  useEffect(() => {
    fondo.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [enCurso?.eventos.length]);

  const suyo = enCurso?.pipeline === pipeline.name ? enCurso : null;

  return (
    <div className="contenido">
      {problema ? <p className="nota nota--error">{problema}</p> : null}
      {pipeline.problem ? <p className="nota nota--error">{pipeline.problem}</p> : null}
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
