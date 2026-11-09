/**
 * Lo que está pasando ahora mismo: cada nodo con su ritmo y el registro de
 * eventos según llegan. La barra y el botón de ejecutar los pone `Pipeline`.
 */
import { useEffect, useRef } from "react";

import type { PipelineInfo } from "../api";
import { Curva, ritmo } from "../Curva";
import { hora, miles } from "../formato";
import type { EnCurso } from "../useMotor";

/** Lo que enseña la curva en vivo: el último minuto. */
const VENTANA_MS = 60_000;

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
          {suyo.alertas.map((alerta, i) => (
            <p className={claseAlerta(alerta.tipo)} key={i} role="alert">
              {alerta.mensaje}
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
              </tr>
            </thead>
            <tbody>
              {[...suyo.nodos.entries()]
                .sort(([a], [b]) => posicion(suyo.orden, a) - posicion(suyo.orden, b))
                .map(([nodo, estado]) => {
                  const muestras = suyo.muestras.get(nodo) ?? [];
                  const puntos = ritmo(muestras, 4);
                  const final = suyo.finales.get(nodo);
                  const filas = final?.filas ?? muestras.at(-1)?.filas ?? 0;
                  // En marcha, el ritmo de ahora; terminado, el medio.
                  const porSegundo =
                    estado === "running"
                      ? puntos.at(-1)?.v
                      : final && final.ms > 0
                        ? (final.filas * 1000) / final.ms
                        : undefined;
                  return (
                    <tr key={nodo}>
                      <td>{nodo}</td>
                      <td>
                        <span className="estado">
                          <span className={puntoDe(estado)} />
                          {legible(estado)}
                        </span>
                      </td>
                      <td className="num">{miles(filas)}</td>
                      <td className="num">
                        {porSegundo === undefined ? "—" : miles(porSegundo)}
                      </td>
                      <td>
                        <Curva
                          puntos={puntos}
                          desde={Math.max(0, suyo.transcurrido - VENTANA_MS)}
                          hasta={suyo.transcurrido}
                          etiqueta={nodo}
                        />
                      </td>
                    </tr>
                  );
                })}
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

/**
 * Un fallo o una ejecución colgada son un error; ir lenta es un aviso. El
 * texto va siempre: el color sólo refuerza.
 */
export function claseAlerta(tipo: string): string {
  return tipo === "lenta" ? "nota nota--aviso" : "nota nota--error";
}

/** Dónde va un nodo en la tabla: los que aún no tienen sitio, al final. */
function posicion(orden: string[], nodo: string): number {
  const i = orden.indexOf(nodo);
  return i < 0 ? orden.length : i;
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
