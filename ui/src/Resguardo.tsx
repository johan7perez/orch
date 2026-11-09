/**
 * Lo que se enseña si una vista falla al dibujarse.
 *
 * Sin esto, un error de render en cualquier sitio deja la ventana entera en
 * blanco, lateral incluido, sin decir nada. Con esto la vista que falló dice
 * por qué y el resto de la aplicación sigue funcionando: se puede elegir otro
 * pipeline o reintentar.
 */
import { Component, type ErrorInfo, type ReactNode } from "react";

interface Props {
  children: ReactNode;
  /** Cuando cambia, se vuelve a intentar: al elegir otra cosa en el lateral. */
  clave: string;
}

interface Estado {
  error: Error | null;
  clave: string;
}

export class Resguardo extends Component<Props, Estado> {
  state: Estado = { error: null, clave: this.props.clave };

  static getDerivedStateFromError(error: Error): Partial<Estado> {
    return { error };
  }

  static getDerivedStateFromProps(props: Props, estado: Estado): Partial<Estado> | null {
    return props.clave !== estado.clave ? { error: null, clave: props.clave } : null;
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("una vista falló al dibujarse", error, info.componentStack);
  }

  render() {
    if (!this.state.error) return this.props.children;
    return (
      <div className="contenido">
        <p className="nota nota--error">
          Esta vista falló al dibujarse: {this.state.error.message}
        </p>
        <button className="boton boton--suave" onClick={() => this.setState({ error: null })}>
          Reintentar
        </button>
      </div>
    );
  }
}
