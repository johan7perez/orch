/**
 * Qué cambió entre dos versiones de un pipeline, dicho como pipeline.
 *
 * Cada cambio lleva su marca (+, −, ~) además del color: el color ayuda a
 * ojear, pero nunca es lo único que dice si algo entró o salió. Una query de
 * varias líneas se enseña como diff de líneas, que es como se lee un cambio
 * en SQL.
 */
import type { ReactNode } from "react";

import type { Cambio } from "../api";

export function Cambios({ cambios }: { cambios: Cambio[] }) {
  if (cambios.length === 0) {
    return <p className="campo__ayuda">Sin cambios.</p>;
  }
  return (
    <ul className="cambios">
      {cambios.map((cambio, i) => (
        <li className="cambio" key={i}>
          <CambioUno cambio={cambio} />
        </li>
      ))}
    </ul>
  );
}

function CambioUno({ cambio }: { cambio: Cambio }) {
  switch (cambio.change) {
    case "pipeline":
      return <Campo nombre={cambio.field} antes={cambio.before} despues={cambio.after} />;
    case "node":
      return (
        <Campo
          nombre={
            <>
              <code>{cambio.node}</code> · {cambio.field}
            </>
          }
          antes={cambio.before}
          despues={cambio.after}
        />
      );
    case "node_added":
      return (
        <Linea marca="+">
          Nodo nuevo <code>{cambio.node}</code>{" "}
          <span className="tenue">
            {cambio.kind} · {cambio.component}
          </span>
        </Linea>
      );
    case "node_removed":
      return (
        <Linea marca="−">
          Nodo quitado <code>{cambio.node}</code>{" "}
          <span className="tenue">
            {cambio.kind} · {cambio.component}
          </span>
        </Linea>
      );
    case "node_renamed":
      return (
        <Linea marca="~">
          <code>{cambio.from}</code> se llama ahora <code>{cambio.to}</code>
        </Linea>
      );
    case "edge_added":
      return (
        <Linea marca="+">
          Arista nueva <Arista desde={cambio.from} hasta={cambio.to} puerto={cambio.port} />
        </Linea>
      );
    case "edge_removed":
      return (
        <Linea marca="−">
          Arista quitada <Arista desde={cambio.from} hasta={cambio.to} puerto={cambio.port} />
        </Linea>
      );
    case "layout":
      return (
        <Linea marca="·">
          <span className="tenue">
            {cambio.nodes.length === 1
              ? `Se movió ${cambio.nodes[0]} en el lienzo`
              : `Se movieron ${cambio.nodes.length} nodos en el lienzo`}
          </span>
        </Linea>
      );
  }
}

function Linea({ marca, children }: { marca: "+" | "−" | "~" | "·"; children: ReactNode }) {
  const clase =
    marca === "+" ? "cambio__marca--mas" : marca === "−" ? "cambio__marca--menos" : "";
  return (
    <div className="cambio__linea">
      <span className={`cambio__marca ${clase}`} aria-hidden="true">
        {marca}
      </span>
      <div className="cambio__cuerpo">{children}</div>
    </div>
  );
}

function Arista({ desde, hasta, puerto }: { desde: string; hasta: string; puerto: string }) {
  return (
    <>
      <code>
        {desde} → {hasta}
      </code>
      {puerto !== desde ? <span className="tenue"> (puerto {puerto})</span> : null}
    </>
  );
}

function Campo({
  nombre,
  antes,
  despues,
}: {
  nombre: ReactNode;
  antes: unknown;
  despues: unknown;
}) {
  const hay = (v: unknown) => v !== null && v !== undefined;

  if (!hay(antes)) {
    return (
      <Linea marca="+">
        {nombre}: <Valor valor={despues} />
      </Linea>
    );
  }
  if (!hay(despues)) {
    return (
      <Linea marca="−">
        {nombre}: <Valor valor={antes} />
      </Linea>
    );
  }
  if (
    typeof antes === "string" &&
    typeof despues === "string" &&
    (antes.includes("\n") || despues.includes("\n"))
  ) {
    return (
      <Linea marca="~">
        {nombre}
        <pre className="diff-lineas">
          {diffDeLineas(antes, despues).map(([marca, linea], i) => (
            <span
              key={i}
              className={
                marca === "+"
                  ? "diff-lineas__mas"
                  : marca === "-"
                    ? "diff-lineas__menos"
                    : undefined
              }
            >
              {marca === " " ? " " : marca === "-" ? "−" : "+"} {linea}
              {"\n"}
            </span>
          ))}
        </pre>
      </Linea>
    );
  }
  return (
    <Linea marca="~">
      {nombre}: <Valor valor={antes} /> <span aria-label="pasa a">→</span>{" "}
      <Valor valor={despues} />
    </Linea>
  );
}

function Valor({ valor }: { valor: unknown }) {
  const texto = typeof valor === "string" ? `"${valor}"` : JSON.stringify(valor);
  return <code className="cambio__valor">{texto}</code>;
}

/**
 * Diff de líneas por subsecuencia común más larga. Las queries son cortas:
 * la tabla cuadrática no importa.
 */
export function diffDeLineas(antes: string, ahora: string): [" " | "-" | "+", string][] {
  const a = antes.split("\n");
  const b = ahora.split("\n");
  const lcs: number[][] = Array.from({ length: a.length + 1 }, () =>
    new Array<number>(b.length + 1).fill(0),
  );
  for (let i = a.length - 1; i >= 0; i--) {
    for (let j = b.length - 1; j >= 0; j--) {
      lcs[i]![j] =
        a[i] === b[j] ? lcs[i + 1]![j + 1]! + 1 : Math.max(lcs[i + 1]![j]!, lcs[i]![j + 1]!);
    }
  }
  const salida: [" " | "-" | "+", string][] = [];
  let i = 0;
  let j = 0;
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) {
      salida.push([" ", a[i]!]);
      i++;
      j++;
    } else if (lcs[i + 1]![j]! >= lcs[i]![j + 1]!) {
      salida.push(["-", a[i]!]);
      i++;
    } else {
      salida.push(["+", b[j]!]);
      j++;
    }
  }
  for (; i < a.length; i++) salida.push(["-", a[i]!]);
  for (; j < b.length; j++) salida.push(["+", b[j]!]);
  return salida;
}
