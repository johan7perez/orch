/**
 * Un pipeline elegido, con sus dos vistas.
 *
 * La barra es común a las dos a propósito: el título y el botón de ejecutar
 * no cambian según cómo lo estés mirando, y verlos saltar al cambiar de vista
 * haría pensar que has cambiado de sitio. Lo único que cambia es el cuerpo.
 */
import { useState } from "react";

import type { PipelineInfo } from "../api";
import type { EnCurso } from "../useMotor";
import { Diseno } from "./Diseno";
import { EnVivo } from "./EnVivo";

type Modo = "vivo" | "diseno";

interface Props {
  pipeline: PipelineInfo | null;
  enCurso: EnCurso | null;
  error: string | null;
  cargando: boolean;
  problema: string | null;
  onEjecutar: (ruta: string, nombre: string) => void;
}

export function Pipeline({
  pipeline,
  enCurso,
  error,
  cargando,
  problema,
  onEjecutar,
}: Props) {
  const [modo, setModo] = useState<Modo>("vivo");

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

        <div className="segmentado" role="tablist" aria-label="Vista">
          <button
            role="tab"
            aria-selected={modo === "vivo"}
            className="segmentado__opcion"
            onClick={() => setModo("vivo")}
          >
            Ejecución
          </button>
          <button
            role="tab"
            aria-selected={modo === "diseno"}
            className="segmentado__opcion"
            onClick={() => setModo("diseno")}
          >
            Diseño
          </button>
        </div>

        <button
          className="boton"
          disabled={corriendo || pipeline.problem !== null}
          onClick={() => onEjecutar(pipeline.path, pipeline.name)}
        >
          {corriendo ? "Ejecutando…" : "Ejecutar"}
        </button>
      </header>

      {modo === "vivo" ? (
        <EnVivo pipeline={pipeline} enCurso={enCurso} error={error} problema={problema} />
      ) : (
        <Diseno pipeline={pipeline} />
      )}
    </>
  );
}
