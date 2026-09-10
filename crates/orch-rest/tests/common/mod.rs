//! Servidor HTTP mínimo para los tests.
//!
//! Se escribe a mano en vez de traer una dependencia: sólo hace falta
//! responder con cuerpos preparados y contar peticiones, y así el test
//! controla exactamente qué devuelve el servidor en cada intento — que es
//! justo lo que hay que provocar para probar reintentos y paginación.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Lo que el servidor devuelve en una petición.
#[derive(Debug, Clone)]
pub struct Reply {
    pub status: u16,
    pub body: String,
    pub headers: Vec<(String, String)>,
}

impl Reply {
    pub fn ok(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            body: body.into(),
            headers: Vec::new(),
        }
    }

    pub fn status(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
            headers: Vec::new(),
        }
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }
}

/// Una petición tal y como llegó.
#[derive(Debug, Clone)]
pub struct Received {
    pub method: String,
    /// Ruta con su query string.
    pub target: String,
    pub body: String,
}

impl Received {
    /// Valor de un parámetro de la query.
    pub fn query(&self, key: &str) -> Option<String> {
        let (_, query) = self.target.split_once('?')?;
        query.split('&').find_map(|pair| {
            let (name, value) = pair.split_once('=')?;
            (name == key).then(|| value.to_string())
        })
    }

    pub fn path(&self) -> &str {
        self.target.split('?').next().unwrap_or(&self.target)
    }
}

#[derive(Default)]
struct State {
    /// Respuestas en cola, en orden. Al agotarse se repite la última.
    replies: Vec<Reply>,
    served: usize,
    received: Vec<Received>,
}

/// Servidor de pruebas. Se apaga al soltarse.
pub struct TestServer {
    pub url: String,
    state: Arc<Mutex<State>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl TestServer {
    /// Arranca un servidor que responde con `replies` en orden; cuando se
    /// agotan, repite la última indefinidamente.
    pub async fn start(replies: Vec<Reply>) -> Self {
        assert!(!replies.is_empty(), "hace falta al menos una respuesta");
        let state = Arc::new(Mutex::new(State {
            replies,
            ..State::default()
        }));

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("no se pudo abrir el puerto");
        let addr = listener.local_addr().expect("dirección local");
        let (shutdown, mut stop) = tokio::sync::oneshot::channel();

        let served_state = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    result = listener.accept() => result,
                    _ = &mut stop => break,
                };
                let Ok((stream, _)) = accepted else { break };
                let state = Arc::clone(&served_state);
                tokio::spawn(async move {
                    let _ = serve(stream, state).await;
                });
            }
        });

        Self {
            url: format!("http://{addr}"),
            state,
            shutdown: Some(shutdown),
        }
    }

    /// Un servidor que siempre responde lo mismo.
    pub async fn always(reply: Reply) -> Self {
        Self::start(vec![reply]).await
    }

    pub fn requests(&self) -> Vec<Received> {
        self.state.lock().expect("mutex sano").received.clone()
    }

    pub fn request_count(&self) -> usize {
        self.state.lock().expect("mutex sano").received.len()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

async fn serve(mut stream: tokio::net::TcpStream, state: Arc<Mutex<State>>) -> std::io::Result<()> {
    let mut raw = Vec::new();
    let mut buffer = [0u8; 4096];

    // Leer hasta el final de las cabeceras, y después el cuerpo que anuncie
    // `Content-Length`.
    let headers_end = loop {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        raw.extend_from_slice(&buffer[..read]);
        if let Some(position) = find(&raw, b"\r\n\r\n") {
            break position + 4;
        }
    };

    let head = String::from_utf8_lossy(&raw[..headers_end]).to_string();
    let content_length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);

    while raw.len() < headers_end + content_length {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        raw.extend_from_slice(&buffer[..read]);
    }

    let mut request_line = head.lines().next().unwrap_or_default().split_whitespace();
    let received = Received {
        method: request_line.next().unwrap_or_default().to_string(),
        target: request_line.next().unwrap_or_default().to_string(),
        body: String::from_utf8_lossy(&raw[headers_end..]).to_string(),
    };

    let reply = {
        let mut state = state.lock().expect("mutex sano");
        state.received.push(received);
        let index = state.served.min(state.replies.len() - 1);
        state.served += 1;
        state.replies[index].clone()
    };

    let mut response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        reply.status,
        reason(reply.status),
        reply.body.len()
    );
    for (name, value) in &reply.headers {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str("\r\n");
    response.push_str(&reply.body);

    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

/// Cuenta cuántas peticiones llegaron a cada ruta.
pub fn count_by_path(requests: &[Received]) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for request in requests {
        *counts.entry(request.path().to_string()).or_insert(0) += 1;
    }
    counts
}
