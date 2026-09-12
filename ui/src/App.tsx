import { useCallback, useEffect, useState } from "react";
import { open } from "@tauri-apps/plugin-dialog";

import { api, type RunDetail } from "./api";
import { DetalleEjecucion } from "./vistas/DetalleEjecucion";
import { Pipeline } from "./vistas/Pipeline";
import { Lateral } from "./vistas/Lateral";
import { useMotor } from "./useMotor";

export type Pestana = "pipelines" | "ejecuciones";

export function App() {
  const motor = useMotor();
  const [pestana, setPestana] = useState<Pestana>("pipelines");
  const [seleccion, setSeleccion] = useState<string | null>(null);
  const [detalle, setDetalle] = useState<RunDetail | null>(null);

  // Al arrancar una ejecución, la vista salta sola a lo que está pasando:
  // es lo que el usuario acaba de pedir ver.
  const ejecutar = useCallback(
    async (ruta: string, nombre: string) => {
      setPestana("pipelines");
      setSeleccion(nombre);
      setDetalle(null);
      await motor.ejecutar(ruta);
    },
    [motor],
  );

  const abrirEjecucion = useCallback(async (runId: string) => {
    setPestana("ejecuciones");
    setSeleccion(runId);
    try {
      setDetalle(await api.runDetail(runId));
    } catch {
      setDetalle(null);
    }
  }, []);

  // Cuando termina la que estaba en curso, se recarga su detalle guardado:
  // trae las métricas de contrapresión, que en vivo todavía no existen.
  useEffect(() => {
    if (motor.enCurso?.terminada && pestana === "ejecuciones") {
      void abrirEjecucion(motor.enCurso.runId);
    }
  }, [motor.enCurso?.terminada, motor.enCurso?.runId, pestana, abrirEjecucion]);

  const elegirCarpeta = useCallback(async () => {
    const elegida = await open({ directory: true, multiple: false });
    if (typeof elegida === "string") {
      await motor.elegirDirectorio(elegida);
      setSeleccion(null);
    }
  }, [motor]);

  const pipeline = motor.workspace?.pipelines.find((p) => p.name === seleccion) ?? null;

  return (
    <div className="app">
      <Lateral
        pestana={pestana}
        onPestana={(siguiente) => {
          setPestana(siguiente);
          setSeleccion(null);
          setDetalle(null);
        }}
        workspace={motor.workspace}
        runs={motor.runs}
        seleccion={seleccion}
        enCurso={motor.enCurso}
        onPipeline={(nombre) => {
          setSeleccion(nombre);
          setDetalle(null);
        }}
        onEjecucion={(runId) => void abrirEjecucion(runId)}
        onCarpeta={() => void elegirCarpeta()}
      />

      <main className="principal">
        {pestana === "pipelines" ? (
          <Pipeline
            pipeline={pipeline}
            enCurso={motor.enCurso}
            error={motor.error}
            cargando={motor.cargando}
            problema={motor.workspace?.problem ?? null}
            onEjecutar={(ruta, nombre) => void ejecutar(ruta, nombre)}
          />
        ) : (
          <DetalleEjecucion
            detalle={detalle}
            resumen={motor.runs.find((r) => r.run_id === seleccion) ?? null}
          />
        )}
      </main>
    </div>
  );
}
