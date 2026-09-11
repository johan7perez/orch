import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  // Tauri ya imprime lo suyo; borrar la pantalla esconde sus mensajes.
  clearScreen: false,
  server: {
    port: 5173,
    strictPort: true,
  },
  build: {
    // La ventana siempre es WebView2/WebKit reciente: no hace falta
    // transpilar a navegadores viejos.
    target: "es2022",
    sourcemap: true,
  },
});
