/** Formateo de cifras y tiempos, en un solo sitio. */

export function milisegundos(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)} s`;
  const minutos = Math.floor(ms / 60_000);
  const segundos = Math.floor((ms % 60_000) / 1000);
  return `${minutos}m ${String(segundos).padStart(2, "0")}s`;
}

/**
 * Separa millares con espacio fino. Se hace a mano y no con `toLocaleString`
 * para que la tabla se lea igual en cualquier máquina.
 */
export function miles(valor: number): string {
  const entero = Math.round(valor).toString();
  return entero.replace(/\B(?=(\d{3})+(?!\d))/g, " ");
}

export function hora(iso: string): string {
  const fecha = new Date(iso);
  return fecha.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function fechaHora(iso: string): string {
  const fecha = new Date(iso);
  const hoy = new Date();
  const mismoDia = fecha.toDateString() === hoy.toDateString();
  if (mismoDia) return `hoy ${hora(iso)}`;
  return fecha.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function estadoLegible(estado: string): string {
  switch (estado) {
    case "succeeded":
      return "correcto";
    case "failed":
      return "fallido";
    case "skipped":
      return "omitido";
    case "running":
      return "en curso";
    default:
      return estado;
  }
}

export function claseDePunto(estado: string): string {
  switch (estado) {
    case "succeeded":
      return "punto punto--ok";
    case "failed":
      return "punto punto--fallo";
    case "skipped":
      return "punto punto--omitido";
    case "running":
      return "punto punto--curso";
    default:
      return "punto";
  }
}
