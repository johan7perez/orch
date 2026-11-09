/**
 * Los componentes registrados, para llevarlos al lienzo.
 *
 * Se arrastran, que es lo natural en un lienzo, pero un clic también los
 * añade: arrastrar con un trackpad cuesta, y con el teclado no se puede.
 * Como el lienzo coloca los nodos solo, soltar en un sitio u otro da igual.
 */
import type { Catalog, Component } from "../api";
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

export function Paleta({ catalogo, onAnadir, onArrastrar }: Props) {
  // Los plugins van aparte: quien arma el pipeline tiene que saber qué viene
  // con Orch y qué trae la carpeta. Se usan igual que el resto.
  const grupos: { tipo: TipoNodo; titulo: string; lista: Component[] }[] = [
    { tipo: "source", titulo: "Orígenes", lista: catalogo.sources },
    {
      tipo: "transform",
      titulo: "Transformaciones",
      lista: catalogo.transforms.filter((c) => !c.plugin),
    },
    { tipo: "transform", titulo: "Plugins", lista: catalogo.transforms.filter((c) => c.plugin) },
    { tipo: "sink", titulo: "Destinos", lista: catalogo.sinks },
  ];

  return (
    <nav className="paleta" aria-label="Componentes">
      {grupos
        .filter(({ lista }) => lista.length > 0)
        .map(({ tipo, titulo, lista }) => (
          <section key={titulo} className="paleta__grupo">
            <h3 className="paleta__titulo">{titulo}</h3>
            {lista.map((componente) => (
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
