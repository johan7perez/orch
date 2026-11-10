/**
 * Las versiones de un pipeline: lo que hay sin guardar, su historial en git y
 * lo que corrió de cada versión.
 *
 * Tres cosas escriben, y las tres sólo cuando alguien pulsa: guardar una
 * versión (un commit sólo de este fichero), descartar los cambios y
 * recuperar una versión anterior. Recuperar no reescribe la historia: el
 * texto viejo vuelve al fichero como un cambio sin guardar, que se guarda o
 * se descarta como cualquier otro. Descartar pide confirmación, porque es lo
 * único que pierde trabajo.
 */
import { useEffect, useState, type FormEvent } from "react";

import {
  api,
  type CommitRuns,
  type PipelineHistory,
  type PipelineInfo,
  type VersionDiff,
  type VersionRef,
  type VersionRuns,
} from "../api";
import { claseDePunto, estadoLegible, fechaHora } from "../formato";
import { Cambios } from "./Cambios";

interface Props {
  pipeline: PipelineInfo;
  /** El fichero cambió (descartar, recuperar): el lateral tiene que enterarse. */
  onEditado: () => void;
}

/** Una versión de la lista, venga de git o sólo del historial de ejecuciones. */
interface Entrada {
  clave: string;
  ref: VersionRef;
  titulo: string;
  detalle: string;
  /** Huella de su texto, para saber si es lo que hay ahora. */
  hash: string | null;
  runs: VersionRuns | null;
  /** La versión anterior, para enseñar qué cambió en ésta. */
  anterior: VersionRef | null;
}

export function Versiones({ pipeline, onEditado }: Props) {
  const [historia, setHistoria] = useState<PipelineHistory | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [aviso, setAviso] = useState<string | null>(null);
  const [ocupado, setOcupado] = useState(false);
  const [vuelta, setVuelta] = useState(0);
  const [elegida, setElegida] = useState<string | null>(null);
  const [pendientes, setPendientes] = useState<VersionDiff | null>(null);

  useEffect(() => {
    let vigente = true;
    setError(null);
    api
      .pipelineHistory(pipeline.path)
      .then((h) => {
        if (!vigente) return;
        setHistoria(h);
        // Lo que hay sin guardar, comparado con la última versión.
        if (h.git?.modified && h.git.commit) {
          api
            .pipelineDiff(pipeline.path, { kind: "commit", rev: "HEAD" }, { kind: "working" })
            .then((d) => vigente && setPendientes(d))
            .catch((e) => vigente && setError(String(e)));
        } else {
          setPendientes(null);
        }
      })
      .catch((e) => {
        if (!vigente) return;
        setHistoria(null);
        setError(String(e));
      });
    return () => {
      vigente = false;
    };
  }, [pipeline.path, vuelta]);

  // Al cambiar de pipeline no se queda elegida una versión del anterior.
  useEffect(() => {
    setElegida(null);
    setAviso(null);
    setHistoria(null);
  }, [pipeline.path]);

  /** Hace algo que escribe, y vuelve a mirar cómo quedó. */
  const hacer = async (accion: () => Promise<string | null>, tocaFichero: boolean) => {
    setOcupado(true);
    setError(null);
    setAviso(null);
    try {
      setAviso(await accion());
      if (tocaFichero) onEditado();
    } catch (e) {
      setError(String(e));
    } finally {
      setOcupado(false);
      setVuelta((v) => v + 1);
    }
  };

  if (!historia) {
    return (
      <div className="contenido">
        {error ? <p className="nota nota--error">{error}</p> : <p className="tenue">Cargando…</p>}
      </div>
    );
  }

  const entradas = listar(historia);
  const actual = entradas.find((e) => e.clave === elegida) ?? null;

  return (
    <div className="contenido versiones">
      {error ? <p className="nota nota--error">{error}</p> : null}
      {aviso ? <p className="nota" role="status">{aviso}</p> : null}

      <Ahora
        historia={historia}
        pendientes={pendientes}
        ocupado={ocupado}
        onGuardar={(mensaje) =>
          hacer(async () => {
            const commit = await api.saveVersion(pipeline.path, mensaje);
            return `Guardada como ${commit.hash.slice(0, 7)}.`;
          }, false)
        }
        onDescartar={() =>
          hacer(async () => {
            await api.discardChanges(pipeline.path, historia.hash);
            return "Cambios descartados: el fichero vuelve a ser su última versión.";
          }, true)
        }
      />

      <p className="seccion">
        {historia.git ? "Historial en git" : "Versiones que han corrido"}
      </p>
      {entradas.length === 0 ? (
        <p className="campo__ayuda">
          {historia.git
            ? "Todavía no hay ninguna versión guardada."
            : "Todavía no ha corrido ninguna versión."}
        </p>
      ) : (
        <ul className="historial" aria-label="Versiones">
          {entradas.map((entrada) => (
            <li key={entrada.clave}>
              <button
                className="fila"
                aria-expanded={entrada.clave === elegida}
                onClick={() => setElegida(entrada.clave === elegida ? null : entrada.clave)}
              >
                <span className="fila__titulo">
                  {entrada.titulo}
                  {entrada.hash === historia.hash ? (
                    <span className="etiqueta">la actual</span>
                  ) : null}
                </span>
                <span className="fila__sub">
                  {entrada.detalle}
                  {" · "}
                  <Ejecuciones runs={entrada.runs} />
                </span>
              </button>
              {actual?.clave === entrada.clave ? (
                <DetalleVersion
                  key={entrada.clave}
                  ruta={pipeline.path}
                  entrada={entrada}
                  huellaActual={historia.hash}
                  ocupado={ocupado}
                  onRecuperar={() =>
                    hacer(async () => {
                      await api.restoreVersion(pipeline.path, entrada.ref, historia.hash);
                      return (
                        "Recuperada: está en el fichero como cambio sin guardar. " +
                        "Guárdala o descártala arriba."
                      );
                    }, true)
                  }
                />
              ) : null}
            </li>
          ))}
        </ul>
      )}
      {historia.git && historia.others.length > 0 ? (
        <p className="campo__ayuda">
          Al final de la lista, lo que corrió con cambios que nunca se guardaron así.
        </p>
      ) : null}
    </div>
  );
}

/** El estado de ahora: al día, cambios sin guardar, o fuera de git. */
function Ahora({
  historia,
  pendientes,
  ocupado,
  onGuardar,
  onDescartar,
}: {
  historia: PipelineHistory;
  pendientes: VersionDiff | null;
  ocupado: boolean;
  onGuardar: (mensaje: string) => void;
  onDescartar: () => void;
}) {
  const git = historia.git;
  if (!git) {
    return (
      <p className="nota">
        Este pipeline no está en un repositorio de git, o git no está instalado. Cada ejecución
        guarda igualmente qué versión corrió: abajo están las que han corrido, y se pueden
        comparar con la actual o recuperar.
      </p>
    );
  }
  if (!git.modified && git.commit) {
    return (
      <p className="versiones__estado">
        Al día con <code>{git.commit.hash.slice(0, 7)}</code> «{git.commit.subject}».
      </p>
    );
  }

  return (
    <section className="versiones__pendiente" aria-label="Cambios sin guardar">
      <p className="seccion">
        {git.commit ? "Cambios sin guardar" : "Nunca guardado en git"}
      </p>
      {git.commit ? (
        pendientes ? (
          <Cambios cambios={pendientes.changes} />
        ) : (
          <p className="tenue">Comparando…</p>
        )
      ) : (
        <p className="campo__ayuda">Guarda una primera versión para empezar su historial.</p>
      )}
      <Guardar
        ocupado={ocupado}
        puedeDescartar={git.commit !== null}
        onGuardar={onGuardar}
        onDescartar={onDescartar}
      />
    </section>
  );
}

function Guardar({
  ocupado,
  puedeDescartar,
  onGuardar,
  onDescartar,
}: {
  ocupado: boolean;
  puedeDescartar: boolean;
  onGuardar: (mensaje: string) => void;
  onDescartar: () => void;
}) {
  const [mensaje, setMensaje] = useState("");
  const [confirmando, setConfirmando] = useState(false);

  const enviar = (e: FormEvent) => {
    e.preventDefault();
    if (!mensaje.trim()) return;
    onGuardar(mensaje.trim());
    setMensaje("");
  };

  if (confirmando) {
    return (
      <div className="versiones__acciones" role="alertdialog" aria-label="Confirmar descarte">
        <span className="versiones__pregunta">
          Se perderán los cambios sin guardar de este fichero.
        </span>
        <button
          className="boton boton--peligro"
          disabled={ocupado}
          onClick={() => {
            setConfirmando(false);
            onDescartar();
          }}
        >
          Descartar
        </button>
        <button className="boton boton--suave" onClick={() => setConfirmando(false)} autoFocus>
          Cancelar
        </button>
      </div>
    );
  }

  return (
    <form className="versiones__acciones" onSubmit={enviar}>
      <input
        className="control"
        aria-label="Mensaje de la versión"
        placeholder="Qué cambia (el mensaje del commit)"
        value={mensaje}
        onChange={(e) => setMensaje(e.target.value)}
        disabled={ocupado}
      />
      <button className="boton" type="submit" disabled={ocupado || !mensaje.trim()}>
        Guardar versión
      </button>
      {puedeDescartar ? (
        <button
          className="boton boton--suave"
          type="button"
          disabled={ocupado}
          onClick={() => setConfirmando(true)}
        >
          Descartar cambios
        </button>
      ) : null}
    </form>
  );
}

type Comparacion = "en" | "desde";

/** Lo que se ve al elegir una versión: qué cambió en ella o desde ella. */
function DetalleVersion({
  ruta,
  entrada,
  huellaActual,
  ocupado,
  onRecuperar,
}: {
  ruta: string;
  entrada: Entrada;
  /** La huella del fichero ahora: si cambia, «desde ella» se recalcula. */
  huellaActual: string;
  ocupado: boolean;
  onRecuperar: () => void;
}) {
  const esLaActual = entrada.hash === huellaActual;
  const opciones: Comparacion[] = [
    ...(entrada.anterior ? (["en"] as const) : []),
    ...(!esLaActual ? (["desde"] as const) : []),
  ];
  const [como, setComo] = useState<Comparacion | null>(opciones[0] ?? null);
  const [diff, setDiff] = useState<VersionDiff | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Las entradas se rehacen en cada render del padre: el efecto depende de
  // lo que se compara, escrito como texto, y no del objeto.
  const par: [VersionRef, VersionRef] | null =
    como === "en" && entrada.anterior
      ? [entrada.anterior, entrada.ref]
      : como === "desde"
        ? [entrada.ref, { kind: "working" }]
        : null;
  const clave = par ? `${JSON.stringify(par)}@${como === "desde" ? huellaActual : ""}` : null;

  useEffect(() => {
    if (!par) return;
    let vigente = true;
    setDiff(null);
    setError(null);
    api
      .pipelineDiff(ruta, par[0], par[1])
      .then((d) => vigente && setDiff(d))
      .catch((e) => vigente && setError(String(e)));
    return () => {
      vigente = false;
    };
    // `par` está entero en `clave`.
  }, [clave, ruta]);

  return (
    <div className="versiones__detalle">
      {opciones.length > 1 ? (
        <div className="segmentado segmentado--pequeno" role="tablist" aria-label="Comparar">
          <button
            role="tab"
            aria-selected={como === "en"}
            className="segmentado__opcion"
            onClick={() => setComo("en")}
          >
            Qué cambió en ella
          </button>
          <button
            role="tab"
            aria-selected={como === "desde"}
            className="segmentado__opcion"
            onClick={() => setComo("desde")}
          >
            Qué ha cambiado desde ella
          </button>
        </div>
      ) : (
        <p className="campo__ayuda">
          {como === "en"
            ? "Qué cambió en ella:"
            : como === "desde"
              ? "Qué ha cambiado desde ella:"
              : "Es la primera versión, y la que hay ahora."}
        </p>
      )}

      {error ? <p className="nota nota--error">{error}</p> : null}
      {como && !diff && !error ? <p className="tenue">Comparando…</p> : null}
      {diff ? <Cambios cambios={diff.changes} /> : null}

      {!esLaActual ? (
        <div className="versiones__acciones">
          <button className="boton boton--suave" disabled={ocupado} onClick={onRecuperar}>
            Recuperar esta versión
          </button>
          <span className="campo__ayuda">
            Vuelve al fichero como cambio sin guardar; el historial no se toca.
          </span>
        </div>
      ) : null}
    </div>
  );
}

function Ejecuciones({ runs }: { runs: VersionRuns | null }) {
  if (!runs) return <>sin ejecuciones</>;
  return (
    <>
      {runs.runs === 1 ? "1 ejecución" : `${runs.runs} ejecuciones`}, la última{" "}
      <span className="estado">
        <span className={claseDePunto(runs.last_status)} />
        {estadoLegible(runs.last_status)}
      </span>{" "}
      {fechaHora(runs.last_started)}
    </>
  );
}

/** La lista de versiones: los commits y, después, lo que corrió sin estar en git. */
function listar(historia: PipelineHistory): Entrada[] {
  const deCommit = (c: CommitRuns, i: number, todos: CommitRuns[]): Entrada => {
    const anterior = todos[i + 1];
    return {
      clave: c.commit.hash,
      ref: { kind: "commit", rev: c.commit.hash },
      titulo: c.commit.subject || "(sin mensaje)",
      detalle: `${c.commit.hash.slice(0, 7)} · ${c.commit.author} · ${fechaHora(c.commit.date)}`,
      hash: c.hash,
      runs: c.runs,
      anterior: anterior ? { kind: "commit", rev: anterior.commit.hash } : null,
    };
  };
  const deEjecucion = (v: VersionRuns): Entrada => ({
    clave: `run:${v.last_run}`,
    ref: { kind: "run", run_id: v.last_run },
    titulo: v.version.label,
    detalle: `corrió por última vez en ${v.last_run.slice(0, 8)}`,
    hash: v.version.hash,
    runs: v,
    anterior: null,
  });

  if (!historia.git) {
    return [
      ...(historia.current ? [deEjecucion(historia.current)] : []),
      ...historia.others.map(deEjecucion),
    ];
  }
  return [...historia.commits.map(deCommit), ...historia.others.map(deEjecucion)];
}
