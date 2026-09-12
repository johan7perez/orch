/**
 * Coloca los nodos del pipeline en capas, de izquierda a derecha.
 *
 * Un pipeline se lee como fluyen los datos, así que la capa de un nodo es la
 * distancia más larga desde cualquier origen: el camino más largo y no el más
 * corto, para que un nodo nunca quede a la izquierda de algo que lo alimenta.
 *
 * Tiene que aguantar grafos rotos. El lienzo también sirve para arreglar un
 * pipeline con un ciclo, y un algoritmo que se cuelgue o no devuelva nada
 * justo entonces es inútil: los nodos que un ciclo deja sin ordenar se
 * colocan al final, en su propia capa.
 */
import type { Graph, GraphEdge } from "../api";

export const ANCHO_NODO = 176;
export const ALTO_NODO = 56;
const SEPARACION_X = 68;
const SEPARACION_Y = 20;

export interface Colocado {
  id: string;
  x: number;
  y: number;
}

/** Aristas de datos y de barrera `after`, que se dibujan distinto. */
export interface Arista extends GraphEdge {
  /** `true` si es una dependencia de orden (`after`) y no de datos. */
  barrera: boolean;
}

export function aristasDe(grafo: Graph): Arista[] {
  const datos = grafo.edges.map((e) => ({ ...e, barrera: false }));
  const barreras = grafo.nodes.flatMap((nodo) =>
    nodo.after.map((previo) => ({
      from: previo,
      to: nodo.id,
      port: previo,
      barrera: true,
    })),
  );
  return [...datos, ...barreras];
}

export function disponer(grafo: Graph): Map<string, Colocado> {
  const ids = grafo.nodes.map((n) => n.id);
  const aristas = aristasDe(grafo).filter(
    // Una arista a un nodo que no existe no debe descolocar a los que sí.
    (a) => ids.includes(a.from) && ids.includes(a.to),
  );

  const entrantes = new Map<string, number>(ids.map((id) => [id, 0]));
  const salientes = new Map<string, string[]>(ids.map((id) => [id, []]));
  for (const arista of aristas) {
    entrantes.set(arista.to, (entrantes.get(arista.to) ?? 0) + 1);
    salientes.get(arista.from)?.push(arista.to);
  }

  // Kahn, quedándonos con la capa más profunda vista para cada nodo.
  const capa = new Map<string, number>(ids.map((id) => [id, 0]));
  const pendientes = new Map(entrantes);
  const cola = ids.filter((id) => pendientes.get(id) === 0);
  const ordenados = new Set<string>();

  while (cola.length > 0) {
    const id = cola.shift()!;
    ordenados.add(id);
    for (const siguiente of salientes.get(id) ?? []) {
      capa.set(siguiente, Math.max(capa.get(siguiente) ?? 0, (capa.get(id) ?? 0) + 1));
      const quedan = (pendientes.get(siguiente) ?? 0) - 1;
      pendientes.set(siguiente, quedan);
      if (quedan === 0) cola.push(siguiente);
    }
  }

  // Lo que un ciclo dejó sin ordenar va al final, junto.
  const enCiclo = ids.filter((id) => !ordenados.has(id));
  if (enCiclo.length > 0) {
    const ultima = Math.max(0, ...[...ordenados].map((id) => capa.get(id) ?? 0));
    for (const id of enCiclo) capa.set(id, ultima + 1);
  }

  // Agrupar por capa conservando el orden del fichero: si el usuario escribió
  // dos ramas en un orden, verlas en otro cada vez que abre es desconcertante.
  const columnas = new Map<number, string[]>();
  for (const id of ids) {
    const n = capa.get(id) ?? 0;
    columnas.set(n, [...(columnas.get(n) ?? []), id]);
  }

  const altoMaximo = Math.max(
    ...[...columnas.values()].map((col) => col.length * (ALTO_NODO + SEPARACION_Y)),
  );

  const colocados = new Map<string, Colocado>();
  for (const [n, columna] of columnas) {
    const alto = columna.length * (ALTO_NODO + SEPARACION_Y);
    const desde = (altoMaximo - alto) / 2;
    columna.forEach((id, i) => {
      colocados.set(id, {
        id,
        x: n * (ANCHO_NODO + SEPARACION_X),
        y: desde + i * (ALTO_NODO + SEPARACION_Y),
      });
    });
  }
  return colocados;
}
