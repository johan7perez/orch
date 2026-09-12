/**
 * La configuración del nodo elegido, generada a partir del esquema.
 *
 * La diferencia con volcar el `config` es que el esquema conoce **todos** los
 * campos, no sólo los que alguien escribió: se ve lo que hay, lo que se puede
 * poner y con qué valor se queda si no lo pones. Las descripciones salen de
 * los comentarios `///` de los structs de Rust, así que no hay documentación
 * separada que se quede vieja.
 */
import type { EsquemaCampo, EsquemaObjeto, GraphNode } from "../api";

interface Props {
  nodo: GraphNode;
  esquema: EsquemaObjeto | null;
  onCerrar: () => void;
}

export function Inspector({ nodo, esquema, onCerrar }: Props) {
  const campos = combinar(nodo, esquema);

  return (
    <aside className="inspector">
      <header className="inspector__cabecera">
        <div className="barra__texto">
          <h2 className="inspector__titulo">{nodo.id}</h2>
          <p className="barra__sub">
            {nodo.kind} · {nodo.component}
          </p>
        </div>
        <button className="inspector__cerrar" onClick={onCerrar} aria-label="Cerrar">
          ✕
        </button>
      </header>

      <div className="inspector__cuerpo">
        {!nodo.known ? (
          <p className="nota nota--error">
            El componente <strong>{nodo.component}</strong> no está registrado. El
            pipeline fallará al ejecutarse.
          </p>
        ) : null}

        {campos.length === 0 ? (
          <p className="nota">Este componente no lleva configuración.</p>
        ) : (
          <dl className="campos">
            {campos.map((campo) => (
              <Campo campo={campo} key={campo.nombre} />
            ))}
          </dl>
        )}

        {nodo.after.length > 0 ? (
          <>
            <p className="seccion">Espera a</p>
            <p className="campo__valor">{nodo.after.join(", ")}</p>
          </>
        ) : null}
      </div>
    </aside>
  );
}

function Campo({ campo }: { campo: CampoVisible }) {
  return (
    <div className={campo.puesto ? "campo" : "campo campo--ausente"}>
      <dt className="campo__nombre">
        {campo.nombre}
        <span className="campo__tipo">{campo.tipo}</span>
        {campo.obligatorio && !campo.puesto ? (
          <span className="campo__falta">falta</span>
        ) : null}
        {campo.sobra ? <span className="campo__falta">no existe</span> : null}
      </dt>
      <dd className="campo__valor">{campo.valor}</dd>
      {campo.descripcion ? <dd className="campo__ayuda">{campo.descripcion}</dd> : null}
    </div>
  );
}

interface CampoVisible {
  nombre: string;
  tipo: string;
  valor: string;
  descripcion?: string;
  /** Escrito en el fichero, frente a heredado del valor por defecto. */
  puesto: boolean;
  obligatorio: boolean;
  /** Está en el fichero pero el esquema no lo conoce: fallará al ejecutar. */
  sobra: boolean;
}

function combinar(nodo: GraphNode, esquema: EsquemaObjeto | null): CampoVisible[] {
  const config = nodo.config ?? {};
  const propiedades = esquema?.properties ?? {};
  const obligatorios = esquema?.required ?? [];

  const delEsquema = Object.entries(propiedades).map(([nombre, def]) => {
    const puesto = nombre in config;
    return {
      nombre,
      tipo: tipoLegible(def),
      valor: puesto ? formatear(config[nombre]) : pordefecto(def),
      descripcion: def.description ? parrafos(def.description) : undefined,
      puesto,
      obligatorio: obligatorios.includes(nombre),
      sobra: false,
    };
  });

  // Sin esquema —un conector que no está registrado— se enseña lo que haya
  // escrito; es mejor que no enseñar nada.
  if (!esquema?.properties) {
    return Object.entries(config).map(([nombre, valor]) => ({
      nombre,
      tipo: "",
      valor: formatear(valor),
      puesto: true,
      obligatorio: false,
      sobra: false,
    }));
  }

  // Lo que está en el fichero y el esquema no conoce. Los structs de config
  // llevan `deny_unknown_fields`, así que esto no se ignora: revienta al
  // ejecutar, y verlo aquí ahorra el viaje.
  const sobrantes = Object.keys(config)
    .filter((nombre) => !(nombre in propiedades))
    .map((nombre) => ({
      nombre,
      tipo: "",
      valor: formatear(config[nombre]),
      puesto: true,
      obligatorio: false,
      sobra: true,
    }));

  return [...delEsquema, ...sobrantes];
}

/**
 * Junta los saltos de línea de un comentario de Rust.
 *
 * Un `///` viene partido a la anchura del código fuente, que no tiene nada
 * que ver con la del inspector: respetarlos deja el texto quebrado a media
 * frase. Una línea en blanco sí separa párrafos y se conserva.
 */
function parrafos(texto: string): string {
  return texto
    .split(/\n\s*\n/)
    .map((parrafo) => parrafo.replace(/\s*\n\s*/g, " ").trim())
    .join("\n\n");
}

function tipoLegible(def: EsquemaCampo): string {
  if (def.enum) return def.enum.map(String).join(" | ");
  const bruto = Array.isArray(def.type)
    ? // Un campo opcional sale como ["integer", "null"]: interesa el otro.
      def.type.find((t) => t !== "null")
    : def.type;
  switch (bruto) {
    case "string":
      return "texto";
    case "integer":
      return "entero";
    case "number":
      return "número";
    case "boolean":
      return "booleano";
    case "array":
      return "lista";
    case "object":
      return "mapa";
    default:
      return bruto ?? "";
  }
}

function pordefecto(def: EsquemaCampo): string {
  if (def.default === undefined || def.default === null) return "—";
  return formatear(def.default);
}

/** Un secreto llega sin expandir; se enseña tal cual, que es lo que dice el fichero. */
function formatear(valor: unknown): string {
  if (typeof valor === "string") return valor;
  return JSON.stringify(valor, null, 2) ?? String(valor);
}
