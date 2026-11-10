# Publicar una versión

Una versión sale de una etiqueta `v<versión>`. Al subirla, el workflow
[`Publicar`](../.github/workflows/publicar.yml) construye en GitHub, para cada
sistema, los instaladores de la aplicación y un comprimido con la CLI, y los
deja en un **borrador** de release con las notas del
[CHANGELOG.md](../CHANGELOG.md). Nada se publica solo: el borrador se mira y
se publica a mano.

## Pasos

1. **Que la CI esté en verde** en `main`: formato, clippy y tests en Linux,
   Windows y macOS ([`CI`](../.github/workflows/ci.yml)).

2. **La versión, en un solo sitio**: `version` en `[workspace.package]` del
   `Cargo.toml` de la raíz. De ahí la toman todos los crates, la CLI
   (`orch --version`) y los instaladores; no hay que tocar
   `tauri.conf.json` ni `package.json`. Después, `cargo check` para que se
   actualice el `Cargo.lock`.

3. **Las notas**: en el `CHANGELOG.md`, la sección «Sin publicar» pasa a
   llamarse como la versión, con la fecha, y se deja una «Sin publicar»
   vacía encima:

   ```markdown
   ## Sin publicar

   ## 0.1.0 — 2026-11-20
   ```

   El workflow busca exactamente `## 0.1.0` y falla si no la encuentra.

4. **Commit, etiqueta y subirla**:

   ```sh
   git commit -am "Versión 0.1.0"
   git tag v0.1.0
   git push origin main v0.1.0
   ```

   Si la etiqueta no coincide con la versión del `Cargo.toml`, el workflow
   se para antes de construir nada.

5. **Mirar el borrador** en *Releases* cuando termine (tarda: cada sistema
   compila DuckDB y DataFusion en release). Tiene que traer:

   | Sistema | Aplicación | CLI |
   |---|---|---|
   | Linux x86-64 | `.deb`, `.rpm`, `.AppImage` | `orch-<versión>-x86_64-unknown-linux-gnu.tar.gz` |
   | Windows x86-64 | instalador `-setup.exe` (NSIS) y `.msi` | `orch-<versión>-x86_64-pc-windows-msvc.zip` |
   | macOS Apple Silicon | `.dmg` | `orch-<versión>-aarch64-apple-darwin.tar.gz` |
   | macOS Intel | `.dmg` | `orch-<versión>-x86_64-apple-darwin.tar.gz` |

   Si un sistema falló, se puede relanzar sólo ese trabajo: reutiliza el
   borrador y sustituye lo que ya hubiera subido.

6. **Publicarlo** desde la página del borrador.

## Lo que todavía no hace

- **Firmar.** Los instaladores salen sin firmar. En macOS, la primera vez
  hay que abrir la aplicación con clic derecho → *Abrir* (o quitarle la
  cuarentena: `xattr -dr com.apple.quarantine /Applications/Orch.app`); en
  Windows, SmartScreen avisa y hay que darle a *Más información* → *Ejecutar
  de todas formas*. Firmar necesita un certificado de Apple Developer y otro
  de firma de código de Windows, que se pasarían al workflow como secretos.
- **Actualizarse sola.** No hay actualizador: cada versión se descarga e
  instala encima de la anterior. El historial y las preferencias no se tocan,
  porque viven en la carpeta de datos del usuario, no en la de la aplicación.
- **Linux ARM.** Sólo x86-64; un worker en una máquina ARM se compila desde
  el código.
- El runner de Linux es `ubuntu-22.04` a propósito: lo construido con una
  glibc funciona con las posteriores, no al revés. Cuando GitHub lo retire,
  se cambia por el siguiente más antiguo que quede.
