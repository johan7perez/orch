/**
 * Un nodo del lienzo.
 *
 * La jerarquía es deliberada: lo primero que se lee es el id, que es lo que
 * el usuario escribió y por lo que llama al nodo; el componente va debajo, en
 * secundario. El tipo no se escribe, se codifica en el glifo y el color del
 * borde: tres tipos caben en la memoria visual y una palabra más en cada caja
 * es ruido repetido veinte veces.
 */
import { Handle, Position, type NodeProps } from "@xyflow/react";

import { ALTO_NODO, ANCHO_NODO } from "./disposicion";

export interface DatosNodo extends Record<string, unknown> {
  etiqueta: string;
  componente: string;
  tipo: string;
  conocido: boolean;
  entradas: number;
  salidas: number;
}

const GLIFOS: Record<string, string> = {
  source: "▸",
  transform: "◆",
  sink: "■",
};

export function NodoPipeline({ data, selected }: NodeProps) {
  const datos = data as DatosNodo;
  const clases = [
    "nodo",
    `nodo--${datos.tipo}`,
    selected ? "nodo--elegido" : "",
    datos.conocido ? "" : "nodo--desconocido",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={clases} style={{ width: ANCHO_NODO, height: ALTO_NODO }}>
      {/* Un origen no tiene entrada y un destino no tiene salida: enseñar el
          conector sería ofrecer algo que no se puede conectar. */}
      {datos.entradas > 0 ? <Handle type="target" position={Position.Left} /> : null}

      <span className="nodo__glifo" aria-hidden="true">
        {GLIFOS[datos.tipo] ?? "◆"}
      </span>
      <span className="nodo__texto">
        <span className="nodo__id">{datos.etiqueta}</span>
        <span className="nodo__componente">
          {datos.componente}
          {datos.conocido ? "" : " · no registrado"}
        </span>
      </span>

      {datos.salidas > 0 ? <Handle type="source" position={Position.Right} /> : null}
    </div>
  );
}
