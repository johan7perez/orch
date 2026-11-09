/**
 * La curva de ritmo de un nodo: filas por segundo a lo largo de la ejecución.
 *
 * Va una por fila de la tabla de nodos, todas con el mismo eje de tiempo, de
 * modo que se ve qué nodo trabajaba cuándo. Es un solo color, el acento: la
 * identidad la da la fila, así que no hay paleta que se quede corta con veinte
 * nodos. El eje Y empieza en cero —una caída de ritmo tiene que parecerlo— y
 * es propio de cada curva; la cifra exacta va en su columna, al lado.
 */
import { useState, type PointerEvent } from "react";

import { miles, milisegundos } from "./formato";

/** Un punto de la curva: `t` en ms desde el arranque, `v` en filas/s. */
export interface Punto {
  t: number;
  v: number;
}

/** Una muestra acumulada: las filas que llevaba movidas un nodo en `t`. */
export interface Muestra {
  t: number;
  filas: number;
}

/**
 * Filas por segundo a partir de muestras acumuladas.
 *
 * Cada punto mira `ventana` muestras atrás: con las del motor en vivo (cuatro
 * por segundo) una ventana de cuatro suaviza el temblor de un lote que llega
 * justo antes o justo después de un tick.
 */
export function ritmo(muestras: Muestra[], ventana = 1): Punto[] {
  const puntos: Punto[] = [];
  for (let i = ventana; i < muestras.length; i++) {
    const antes = muestras[i - ventana]!;
    const ahora = muestras[i]!;
    const dt = ahora.t - antes.t;
    if (dt <= 0) continue;
    puntos.push({ t: ahora.t, v: Math.max(0, ((ahora.filas - antes.filas) * 1000) / dt) });
  }
  return puntos;
}

interface Props {
  puntos: Punto[];
  /** El eje de tiempo, común a todas las curvas de la tabla. */
  desde: number;
  hasta: number;
  /** Para el lector de pantalla: de qué nodo es. */
  etiqueta: string;
  /** Con línea vertical y valor al pasar el puntero (en el detalle). */
  conTooltip?: boolean;
  ancho?: number;
  alto?: number;
}

/** Radio del punto final, más su anillo: lo que hay que dejar libre arriba. */
const MARGEN = 6;

export function Curva({
  puntos: todos,
  desde,
  hasta,
  etiqueta,
  conTooltip = false,
  ancho = 160,
  alto = 28,
}: Props) {
  const [foco, setFoco] = useState<number | null>(null);
  // Lo que cae fuera del eje no se dibuja: en vivo, la ventana va corriendo.
  const puntos = todos.filter((p) => p.t >= desde && p.t <= hasta);

  if (puntos.length < 2) {
    return (
      <span className="tenue" aria-label={`${etiqueta}: sin muestras`}>
        —
      </span>
    );
  }

  const maximo = Math.max(1, ...puntos.map((p) => p.v));
  const x = (t: number) =>
    MARGEN + ((t - desde) / Math.max(1, hasta - desde)) * (ancho - 2 * MARGEN);
  const base = alto - 1;
  const y = (v: number) => base - (v / maximo) * (base - MARGEN);

  const linea = puntos
    .map((p, i) => `${i === 0 ? "M" : "L"}${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`)
    .join("");
  const primero = puntos[0]!;
  const ultimo = puntos[puntos.length - 1]!;
  const area = `${linea}L${x(ultimo.t).toFixed(1)},${base}L${x(primero.t).toFixed(1)},${base}Z`;

  // El puntero busca la X: nadie apunta a una línea de 2 px.
  const alMover = (e: PointerEvent<SVGSVGElement>) => {
    const caja = e.currentTarget.getBoundingClientRect();
    const px = ((e.clientX - caja.left) / caja.width) * ancho;
    let cercano = 0;
    for (let i = 1; i < puntos.length; i++) {
      if (Math.abs(x(puntos[i]!.t) - px) < Math.abs(x(puntos[cercano]!.t) - px)) cercano = i;
    }
    setFoco(cercano);
  };
  const enfocado = foco !== null ? puntos[foco] : null;

  return (
    <span className="curva" style={{ width: ancho, height: alto }}>
      <svg
        width={ancho}
        height={alto}
        viewBox={`0 0 ${ancho} ${alto}`}
        role="img"
        aria-label={`${etiqueta}: ritmo, hasta ${miles(maximo)} filas por segundo`}
        onPointerMove={conTooltip ? alMover : undefined}
        onPointerLeave={conTooltip ? () => setFoco(null) : undefined}
      >
        <path className="curva__area" d={area} />
        <path className="curva__linea" d={linea} />
        {enfocado ? (
          <>
            <line
              className="curva__guia"
              x1={x(enfocado.t)}
              x2={x(enfocado.t)}
              y1={0}
              y2={alto}
            />
            <circle className="curva__punto" cx={x(enfocado.t)} cy={y(enfocado.v)} r={4} />
          </>
        ) : (
          <circle className="curva__punto" cx={x(ultimo.t)} cy={y(ultimo.v)} r={4} />
        )}
      </svg>
      {enfocado ? (
        <span
          className="curva__tooltip"
          role="status"
          style={{ left: Math.min(Math.max(x(enfocado.t), 40), ancho - 40) }}
        >
          {milisegundos(enfocado.t)} · {miles(enfocado.v)} filas/s
        </span>
      ) : null}
    </span>
  );
}
