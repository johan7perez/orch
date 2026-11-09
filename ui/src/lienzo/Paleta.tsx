/**
 * Los componentes registrados, para llevarlos al lienzo.
 *
 * Se arrastran, que es lo natural en un lienzo, pero un clic también los
 * añade: arrastrar con un trackpad cuesta, y con el teclado no se puede.
 * Como el lienzo coloca los nodos solo, soltar en un sitio u otro da igual.
 */
import type { Catalog } from "../api";
import { GLIFOS } from "./NodoPipeline";

export type TipoNodo = "source" | "transform" | "sink";

/** Lo que se está arrastrando desde la paleta. */
export interface Arrastrado {
  tipo: TipoNodo;
  componente: string;
}

interface Props {
  catalogo: Catalog;
  onAnadir: (tipo: TipoNodo, componente: string) => void;
  /**
   * Lo arrastrado viaja por aquí y no por `dataTransfer`: los tipos MIME
   * propios no llegan igual a todos los webviews (WebView2, WebKitGTK), y
   * esto no depende de ninguno.
   */
  onArrastrar: (arrastrado: Arrastrado | null) => void;
}

const GRUPOS: { tipo: TipoNodo; titulo: string; lista: keyof Catalog }[] = [
  { tipo: "source", titulo: "Orígenes", lista: "sources" },
  { tipo: "transform", titulo: "Transformaciones", lista: "transforms" },
  { tipo: "sink", titulo: "Destinos", lista: "sinks" },
];

export function Paleta({ catalogo, onAnadir, onArrastrar }: Props) {
  return (
    <nav className="paleta" aria-label="Componentes">
      {GRUPOS.map(({ tipo, titulo, lista }) => (
        <section key={tipo} className="paleta__grupo">
          <h3 className="paleta__titulo">{titulo}</h3>
          {catalogo[lista].map((componente) => (
            <button
              key={componente.name}
              className={`paleta__item paleta__item--${tipo}`}
              draggable
              onDragStart={(e) => {
                // Sin datos, algunos motores no llegan a empezar el arrastre.
                e.dataTransfer.setData("text/plain", componente.name);
                e.dataTransfer.effectAllowed = "copy";
                onArrastrar({ tipo, componente: componente.name });
              }}
              onDragEnd={() => onArrastrar(null)}
              onClick={() => onAnadir(tipo, componente.name)}
              title={`Arrastra al lienzo, o haz clic, para añadir un ${componente.name}`}
            >
              <span className="paleta__glifo" aria-hidden="true">
                {GLIFOS[tipo]}
              </span>
              {componente.name}
            </button>
          ))}
        </section>
      ))}
    </nav>
  );
}
