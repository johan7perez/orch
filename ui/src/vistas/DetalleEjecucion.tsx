import type { NodeRow, RunDetail, RunSummary } from "../api";
import { claseDePunto, estadoLegible, fechaHora, hora, miles, milisegundos } from "../formato";

interface Props {
  detalle: RunDetail | null;
  resumen: RunSummary | null;
}

export function DetalleEjecucion({ detalle, resumen }: Props) {
  if (!detalle) {
    return (
      <div className="vacio">
        <strong>Elige una ejecución</strong>
        <span>Verás sus métricas por nodo y todo lo que ocurrió.</span>
      </div>
    );
  }

  // El nodo que más tiempo pasó trabajando sin esperar a nadie es el que
  // marca el ritmo: el resto le espera. Se mira en absoluto y no en
  // porcentaje, porque un nodo que vive 3 ms y no espera da 100%.
  const cuello = detalle.nodes.reduce<NodeRow | null>(
    (mayor, nodo) => (nodo.busy_ms > (mayor?.busy_ms ?? 0) ? nodo : mayor),
    null,
  );

  return (
    <>
      <header className="barra">
        <div className="barra__texto">
          <h1 className="barra__titulo">{resumen?.pipeline ?? "Ejecución"}</h1>
          <p className="barra__sub">
            {resumen ? (
              <>
                {fechaHora(resumen.started_at)} · {milisegundos(resumen.elapsed_ms)} ·{" "}
                <span className="estado">
                  <span className={claseDePunto(resumen.status)} />
                  {estadoLegible(resumen.status)}
                </span>
              </>
            ) : (
              detalle.run_id
            )}
          </p>
        </div>
      </header>

      <div className="contenido">
        <p className="seccion">Nodos</p>
        <table className="tabla">
          <thead>
            <tr>
              <th>Nodo</th>
              <th>Estado</th>
              <th className="num">Filas</th>
              <th className="num">Filas/s</th>
              <th className="num">Tiempo</th>
              <th className="num">Ocupado</th>
            </tr>
          </thead>
          <tbody>
            {detalle.nodes.map((nodo) => {
              const movidas = nodo.kind === "sink" ? nodo.rows_in : nodo.rows_out;
              const esCuello = cuello?.node === nodo.node;
              return (
                <tr key={nodo.node}>
                  <td>
                    {nodo.node}
                    <span className="tenue"> · {nodo.component}</span>
                  </td>
                  <td>
                    <span className="estado">
                      <span className={claseDePunto(nodo.status)} />
                      {estadoLegible(nodo.status)}
                    </span>
                  </td>
                  <td className="num">{miles(movidas)}</td>
                  <td className="num">{miles(nodo.rows_per_second)}</td>
                  <td className="num">{milisegundos(nodo.elapsed_ms)}</td>
                  <td>
                    <div className="ocupacion">
                      <span className="tenue">{Math.round(nodo.busy_pct)}%</span>
                      <span className="ocupacion__barra">
                        <span
                          className={
                            esCuello
                              ? "ocupacion__relleno ocupacion__relleno--cuello"
                              : "ocupacion__relleno"
                          }
                          style={{ width: `${Math.min(100, Math.max(0, nodo.busy_pct))}%` }}
                        />
                      </span>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>

        {cuello ? (
          <p className="nota">
            <strong>{cuello.node}</strong> es el que marca el ritmo: pasó{" "}
            {milisegundos(cuello.busy_ms)} trabajando sin esperar a nadie. El resto del
            pipeline le espera.
          </p>
        ) : null}

        {detalle.nodes
          .filter((nodo) => nodo.error)
          .map((nodo) => (
            <p className="nota nota--error" key={nodo.node}>
              <strong>{nodo.node}</strong>: {nodo.error}
            </p>
          ))}

        <p className="seccion">Eventos</p>
        <div className="eventos">
          {detalle.events.map((evento) => (
            <div className="evento" key={evento.seq}>
              <span className="evento__hora">{hora(evento.at)}</span>
              <span className="evento__tipo">{evento.kind}</span>
              <span>{evento.detail}</span>
            </div>
          ))}
        </div>
      </div>
    </>
  );
}
