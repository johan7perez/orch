/**
 * Medidor de rendimiento del lienzo, para desarrollo.
 *
 *     VITE_ORCH_MEDIR=1 npx --prefix ui tauri dev
 *
 * Es la vara con la que se midió la Fase 2.4 contra un pipeline de cientos de
 * nodos (`cargo run -p orch-edit --example grande`). Enseña cuántos nodos y
 * aristas hay de verdad en el DOM, cuánto cuesta construir el grafo y cada
 * render de React, y cómo van los fotogramas de los dos últimos segundos:
 * al desplazar o hacer zoom, ahí se ve si el lienzo va fluido.
 *
 * Fuera de ese modo no existe: ni el `Profiler` ni el bucle de fotogramas
 * corren en una ventana normal.
 */
import { useEffect, useState, type ProfilerOnRenderCallback } from "react";

export const MEDIR = import.meta.env.VITE_ORCH_MEDIR === "1";

/** Lo que van dejando el lienzo y el `Profiler`; el medidor lo lee. */
export const medidas = {
  construir: 0,
  render: 0,
  renderMax: 0,
};

export const alRenderizar: ProfilerOnRenderCallback = (_id, _fase, duracion) => {
  medidas.render = duracion;
  medidas.renderMax = Math.max(medidas.renderMax, duracion);
};

const VENTANA_MS = 2000;
/** Un fotograma de más de dos refrescos de pantalla ya se nota. */
const LENTO_MS = 34;

export function Medidor() {
  const [texto, setTexto] = useState("");

  useEffect(() => {
    const fotogramas: { en: number; dura: number }[] = [];
    let anterior = performance.now();
    let pedido = 0;
    const bucle = (ahora: number) => {
      fotogramas.push({ en: ahora, dura: ahora - anterior });
      anterior = ahora;
      while (fotogramas.length > 0 && fotogramas[0]!.en < ahora - VENTANA_MS) fotogramas.shift();
      pedido = requestAnimationFrame(bucle);
    };
    pedido = requestAnimationFrame(bucle);

    const intervalo = setInterval(() => {
      const duraciones = fotogramas.map((f) => f.dura).sort((a, b) => a - b);
      const cuantil = (q: number) =>
        duraciones[Math.min(duraciones.length - 1, Math.floor(q * duraciones.length))] ?? 0;
      const nodos = document.querySelectorAll(".react-flow__node").length;
      const aristas = document.querySelectorAll(".react-flow__edge").length;
      setTexto(
        [
          `DOM: ${nodos} nodos · ${aristas} aristas`,
          `construir ${medidas.construir.toFixed(1)} ms · render ${medidas.render.toFixed(0)} ms (máx ${medidas.renderMax.toFixed(0)})`,
          `fotogramas 2 s: p50 ${cuantil(0.5).toFixed(0)} · p95 ${cuantil(0.95).toFixed(0)} · máx ${(duraciones.at(-1) ?? 0).toFixed(0)} ms · lentos ${duraciones.filter((d) => d > LENTO_MS).length}/${duraciones.length}`,
        ].join("\n"),
      );
    }, 250);

    return () => {
      cancelAnimationFrame(pedido);
      clearInterval(intervalo);
    };
  }, []);

  return (
    <pre
      className="medidor"
      title="Clic: poner a cero el máximo"
      onClick={() => {
        medidas.renderMax = 0;
      }}
    >
      {texto}
    </pre>
  );
}
