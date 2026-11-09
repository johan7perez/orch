/**
 * La configuración del nodo elegido, generada a partir del esquema, y
 * editable.
 *
 * La diferencia con volcar el `config` es que el esquema conoce **todos** los
 * campos, no sólo los que alguien escribió: se ve lo que hay, lo que se puede
 * poner y con qué valor se queda si no lo pones. Las descripciones salen de
 * los comentarios `///` de los structs de Rust, así que no hay documentación
 * separada que se quede vieja.
 *
 * Cada campo se guarda al salir de él, o con Intro: no hay botón de guardar,
 * igual que en un panel de propiedades. Lo escrito va al motor, que lo pone
 * en el YAML tocando sólo esa línea. Vaciar un campo lo quita del fichero, y
 * el campo vuelve a su valor por defecto.
 */
import { useRef, useState, type KeyboardEvent } from "react";

import type { Edit, EsquemaCampo, EsquemaObjeto, GraphNode } from "../api";

interface Props {
  nodo: GraphNode;
  esquema: EsquemaObjeto | null;
  /** Los demás nodos: los candidatos para `after`. */
  otros: string[];
  onEditar: (edits: Edit[]) => void;
  onRenombrar: (nuevo: string) => void;
  onEliminar: () => void;
  onCerrar: () => void;
}

export function Inspector({
  nodo,
  esquema,
  otros,
  onEditar,
  onRenombrar,
  onEliminar,
  onCerrar,
}: Props) {
  const campos = combinar(nodo, esquema);
  const disponibles = otros.filter((id) => !nodo.after.includes(id));

  const poner = (clave: string, valor: unknown) =>
    onEditar([
      valor === undefined
        ? { op: "set_config", node: nodo.id, key: clave }
        : { op: "set_config", node: nodo.id, key: clave, value: valor },
    ]);
  const esperar = (after: string[]) => onEditar([{ op: "set_after", node: nodo.id, after }]);

  return (
    <aside className="inspector">
      <header className="inspector__cabecera">
        <div className="barra__texto">
          <Nombre id={nodo.id} onRenombrar={onRenombrar} />
          <p className="barra__sub">
            {nodo.kind} · {nodo.component}
          </p>
        </div>
        <button className="inspector__accion" onClick={onEliminar} title="Eliminar el nodo (Supr)">
          Eliminar
        </button>
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
          <div className="campos">
            {campos.map((campo) => (
              <Campo
                // Si el valor cambia en el fichero, el campo se rehace con él
                // y no se queda con lo que se estaba escribiendo.
                key={`${campo.nombre}=${JSON.stringify(campo.crudo) ?? ""}`}
                campo={campo}
                onPoner={(valor) => poner(campo.nombre, valor)}
              />
            ))}
          </div>
        )}

        <p className="seccion">Espera a</p>
        <div className="esperas">
          {nodo.after.map((previo) => (
            <span className="ficha" key={previo}>
              {previo}
              <button
                className="ficha__quitar"
                aria-label={`Dejar de esperar a ${previo}`}
                onClick={() => esperar(nodo.after.filter((p) => p !== previo))}
              >
                ✕
              </button>
            </span>
          ))}
          {disponibles.length > 0 ? (
            <select
              className="control control--pequeno"
              value=""
              aria-label="Añadir un nodo al que esperar"
              onChange={(e) => {
                if (e.target.value) esperar([...nodo.after, e.target.value]);
              }}
            >
              <option value="">Añadir…</option>
              {disponibles.map((id) => (
                <option key={id} value={id}>
                  {id}
                </option>
              ))}
            </select>
          ) : null}
        </div>
        <p className="campo__ayuda">
          No arranca hasta que estos terminen. Ordena, pero no les pasa datos.
        </p>
      </div>
    </aside>
  );
}

/** El id, que se renombra haciendo clic en él. */
function Nombre({ id, onRenombrar }: { id: string; onRenombrar: (nuevo: string) => void }) {
  const [editando, setEditando] = useState(false);
  const [borrador, setBorrador] = useState(id);
  // Escape quita el campo de la pantalla, y quitar un campo con el foco
  // dentro dispara `blur` en algunos motores: sin esto se guardaría igual.
  const cancelado = useRef(false);

  if (!editando) {
    return (
      <button
        className="inspector__titulo inspector__titulo--boton"
        title="Renombrar"
        onClick={() => {
          cancelado.current = false;
          setBorrador(id);
          setEditando(true);
        }}
      >
        {id}
      </button>
    );
  }

  const terminar = () => {
    setEditando(false);
    const nuevo = borrador.trim();
    if (!cancelado.current && nuevo && nuevo !== id) onRenombrar(nuevo);
  };

  return (
    <input
      className="control inspector__nombre"
      aria-label="Id del nodo"
      autoFocus
      spellCheck={false}
      value={borrador}
      onChange={(e) => setBorrador(e.target.value)}
      onBlur={terminar}
      onKeyDown={(e) => {
        if (e.key === "Enter") e.currentTarget.blur();
        if (e.key === "Escape") {
          cancelado.current = true;
          e.currentTarget.blur();
        }
      }}
    />
  );
}

// --- Un campo ----------------------------------------------------------------

interface CampoVisible {
  nombre: string;
  tipo: string;
  /** El valor escrito en el fichero; `undefined` si no está. */
  crudo: unknown;
  def: EsquemaCampo | null;
  porDefecto: string;
  descripcion?: string;
  /** Escrito en el fichero, frente a heredado del valor por defecto. */
  puesto: boolean;
  obligatorio: boolean;
  /** Está en el fichero pero el esquema no lo conoce: fallará al ejecutar. */
  sobra: boolean;
}

/** Cómo se edita un campo, según su tipo. */
type Control =
  | "texto"
  | "parrafo"
  | "entero"
  | "numero"
  | "booleano"
  | "opciones"
  | "lista"
  | "mapa"
  | "json";

function Campo({ campo, onPoner }: { campo: CampoVisible; onPoner: (valor: unknown) => void }) {
  const control = controlDe(campo);
  const id = `campo-${campo.nombre}`;
  const clases = ["campo", campo.puesto ? "" : "campo--ausente"].filter(Boolean).join(" ");

  return (
    <div className={clases}>
      <div className="campo__nombre">
        <label htmlFor={id}>{campo.nombre}</label>
        <span className="campo__tipo">{campo.tipo}</span>
        {campo.obligatorio && !campo.puesto ? <span className="campo__falta">falta</span> : null}
        {campo.sobra ? <span className="campo__falta">no existe</span> : null}
        {campo.puesto ? (
          <button
            className="campo__quitar"
            onClick={() => onPoner(undefined)}
            title={campo.sobra ? "Quitarlo del fichero" : "Quitarlo: vuelve al valor por defecto"}
          >
            {campo.sobra ? "quitar" : "↺"}
          </button>
        ) : null}
      </div>

      {control === "booleano" ? (
        <label className="interruptor">
          <input
            id={id}
            type="checkbox"
            checked={Boolean(campo.puesto ? campo.crudo : campo.def?.default)}
            onChange={(e) => onPoner(e.target.checked)}
          />
          <span>{(campo.puesto ? campo.crudo : campo.def?.default) ? "sí" : "no"}</span>
        </label>
      ) : control === "opciones" ? (
        <select
          id={id}
          className="control"
          value={campo.puesto ? String(campo.crudo) : ""}
          onChange={(e) => onPoner(e.target.value === "" ? undefined : e.target.value)}
        >
          <option value="">
            {campo.obligatorio ? "Elige…" : `Por defecto (${campo.porDefecto})`}
          </option>
          {opcionesDe(campo.def).map((opcion) => (
            <option key={String(opcion)} value={String(opcion)}>
              {String(opcion)}
            </option>
          ))}
        </select>
      ) : (
        <EditorDeTexto id={id} campo={campo} control={control} onPoner={onPoner} />
      )}

      {campo.descripcion ? <p className="campo__ayuda">{campo.descripcion}</p> : null}
    </div>
  );
}

function EditorDeTexto({
  id,
  campo,
  control,
  onPoner,
}: {
  id: string;
  campo: CampoVisible;
  control: Control;
  onPoner: (valor: unknown) => void;
}) {
  const inicial = aTexto(campo.crudo, control);
  const [borrador, setBorrador] = useState(inicial);
  const [fallo, setFallo] = useState<string | null>(null);
  const multilinea = control === "parrafo" || control === "mapa" || control === "json";

  const confirmar = () => {
    if (borrador === inicial) {
      setFallo(null);
      return;
    }
    const leido = desdeTexto(borrador, control);
    if ("error" in leido) {
      setFallo(leido.error);
      return;
    }
    setFallo(null);
    onPoner(leido.valor);
  };

  const teclas = (e: KeyboardEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    // En un párrafo, Intro es un salto de línea; se confirma con ⌘/Ctrl+Intro.
    if (e.key === "Enter" && (!multilinea || e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      e.currentTarget.blur();
    }
    if (e.key === "Escape") {
      setBorrador(inicial);
      setFallo(null);
    }
  };

  const comunes = {
    id,
    className: fallo ? "control control--mal" : "control",
    value: borrador,
    spellCheck: false,
    placeholder: campo.obligatorio && !campo.puesto ? "obligatorio" : campo.porDefecto,
    onBlur: confirmar,
    onKeyDown: teclas,
  };

  return (
    <>
      {multilinea ? (
        <textarea
          {...comunes}
          rows={Math.min(10, Math.max(2, borrador.split("\n").length))}
          onChange={(e) => setBorrador(e.target.value)}
        />
      ) : (
        <input
          {...comunes}
          type="text"
          inputMode={control === "entero" || control === "numero" ? "decimal" : undefined}
          onChange={(e) => setBorrador(e.target.value)}
        />
      )}
      {fallo ? <p className="campo__error">{fallo}</p> : null}
    </>
  );
}

// --- Del esquema al control, y del texto al valor -------------------------------

function tiposDe(def: EsquemaCampo | null): string[] {
  if (!def?.type) return [];
  return (Array.isArray(def.type) ? def.type : [def.type]).filter((t) => t !== "null");
}

function opcionesDe(def: EsquemaCampo | null): unknown[] {
  if (def?.enum) return def.enum.filter((v) => v !== null);
  if (def?.oneOf?.every((o) => o.const !== undefined)) return def.oneOf.map((o) => o.const);
  return [];
}

function controlDe(campo: CampoVisible): Control {
  const { def, crudo } = campo;

  // Sin esquema —un campo que sobra, o un conector no registrado— se decide
  // por lo que hay escrito.
  if (!def) {
    if (typeof crudo === "boolean") return "booleano";
    if (typeof crudo === "number") return "numero";
    if (typeof crudo === "string") return crudo.includes("\n") ? "parrafo" : "texto";
    if (Array.isArray(crudo) && crudo.every((v) => typeof v === "string")) return "lista";
    return "json";
  }

  if (opcionesDe(def).length > 0) return "opciones";
  const [tipo] = tiposDe(def);
  switch (tipo) {
    case "boolean":
      return "booleano";
    case "integer":
      return "entero";
    case "number":
      return "numero";
    case "string":
      // Una query se escribe en varias líneas aunque ahora quepa en una.
      return campo.nombre === "query" ||
        (typeof crudo === "string" && crudo.includes("\n"))
        ? "parrafo"
        : "texto";
    case "array":
      return tiposDe(def.items ?? null)[0] === "string" ? "lista" : "json";
    case "object": {
      const valores = def.additionalProperties;
      return typeof valores === "object" && tiposDe(valores)[0] === "string" ? "mapa" : "json";
    }
    default:
      // Un struct anidado (`$ref`): TLS, paginación…
      return "json";
  }
}

function aTexto(valor: unknown, control: Control): string {
  if (valor === undefined || valor === null) return "";
  switch (control) {
    case "lista":
      return Array.isArray(valor) ? valor.join(", ") : String(valor);
    case "mapa":
      return typeof valor === "object"
        ? Object.entries(valor as Record<string, unknown>)
            .map(([k, v]) => `${k}: ${String(v)}`)
            .join("\n")
        : String(valor);
    case "json":
      return JSON.stringify(valor, null, 2);
    default:
      return typeof valor === "string" ? valor : JSON.stringify(valor);
  }
}

/** `valor: undefined` quiere decir «quitarlo del fichero». */
function desdeTexto(
  texto: string,
  control: Control,
): { valor: unknown } | { error: string } {
  // Un texto se respeta tal cual: un espacio puede ser el delimitador.
  if (control === "texto" || control === "parrafo") {
    return { valor: texto === "" ? undefined : texto };
  }
  const limpio = texto.trim();
  if (limpio === "") return { valor: undefined };

  switch (control) {
    case "entero":
      return /^-?\d+$/.test(limpio)
        ? { valor: Number(limpio) }
        : { error: "Tiene que ser un número entero." };
    case "numero": {
      const n = Number(limpio.replace(",", "."));
      return Number.isFinite(n) ? { valor: n } : { error: "Tiene que ser un número." };
    }
    case "lista":
      return {
        valor: limpio
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean),
      };
    case "mapa": {
      const mapa: Record<string, string> = {};
      for (const linea of limpio.split("\n")) {
        if (!linea.trim()) continue;
        // La primera `:` separa: el valor puede llevar más (`sum(x)::numeric`).
        const corte = linea.indexOf(":");
        if (corte <= 0) return { error: "Cada línea es «clave: valor»." };
        mapa[linea.slice(0, corte).trim()] = linea.slice(corte + 1).trim();
      }
      return { valor: mapa };
    }
    default:
      try {
        return { valor: JSON.parse(limpio) };
      } catch {
        return { error: "No es JSON válido." };
      }
  }
}

// --- Esquema + config ---------------------------------------------------------

function combinar(nodo: GraphNode, esquema: EsquemaObjeto | null): CampoVisible[] {
  const config = nodo.config ?? {};
  const propiedades = esquema?.properties ?? {};
  const obligatorios = esquema?.required ?? [];

  // Sin esquema —un conector que no está registrado— se enseña lo que haya
  // escrito; es mejor que no enseñar nada.
  if (!esquema?.properties) {
    return Object.entries(config).map(([nombre, valor]) => ({
      nombre,
      tipo: "",
      crudo: valor,
      def: null,
      porDefecto: "",
      puesto: true,
      obligatorio: false,
      sobra: false,
    }));
  }

  const delEsquema = Object.entries(propiedades).map(([nombre, def]) => {
    const puesto = nombre in config;
    return {
      nombre,
      tipo: tipoLegible(def),
      crudo: puesto ? config[nombre] : undefined,
      def,
      porDefecto: pordefecto(def),
      descripcion: def.description ? parrafos(def.description) : undefined,
      puesto,
      obligatorio: obligatorios.includes(nombre),
      sobra: false,
    };
  });

  // Lo que está en el fichero y el esquema no conoce. Los structs de config
  // llevan `deny_unknown_fields`, así que esto no se ignora: revienta al
  // ejecutar, y verlo aquí ahorra el viaje.
  const sobrantes = Object.keys(config)
    .filter((nombre) => !(nombre in propiedades))
    .map((nombre) => ({
      nombre,
      tipo: "",
      crudo: config[nombre],
      def: null,
      porDefecto: "",
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
  const opciones = opcionesDe(def);
  if (opciones.length > 0) return opciones.map(String).join(" | ");
  switch (tiposDe(def)[0]) {
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
      return def.$ref ? "estructura" : "";
  }
}

function pordefecto(def: EsquemaCampo): string {
  if (def.default === undefined || def.default === null) return "—";
  return typeof def.default === "string" ? def.default : JSON.stringify(def.default);
}
