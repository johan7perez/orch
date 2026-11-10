//! El servidor Flight que sirve el flujo de un sink.
//!
//! Sirve **un** flujo, con **un** nombre, a **un** consumidor: el sink es un
//! nodo de un pipeline, y su entrada se recorre una vez. Lo que Flight
//! define y aquí no tiene sentido (subir datos, acciones, listar vuelos)
//! responde «no implementado» en vez de fingir.

use std::sync::Mutex;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use orch_core::arrow::datatypes::SchemaRef;
use orch_core::arrow::ipc::writer::IpcWriteOptions;
use orch_core::arrow::record_batch::RecordBatch;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

pub(crate) type Lote = Result<RecordBatch, FlightError>;

/// Un flujo listo para que alguien lo pida.
pub(crate) struct Flujo {
    pub ticket: String,
    pub token: Option<String>,
    pub schema: Option<SchemaRef>,
    /// Los lotes del sink. Se lo lleva el primer consumidor.
    pub pendiente: Mutex<Option<mpsc::Receiver<Lote>>>,
    /// Avisa al sink de que alguien empezó a leer, y desde dónde.
    pub tomado: Mutex<Option<oneshot::Sender<String>>>,
}

impl Flujo {
    pub fn nuevo(
        ticket: String,
        token: Option<String>,
        schema: Option<SchemaRef>,
        lotes: mpsc::Receiver<Lote>,
        tomado: oneshot::Sender<String>,
    ) -> Self {
        Self {
            ticket,
            token,
            schema,
            pendiente: Mutex::new(Some(lotes)),
            tomado: Mutex::new(Some(tomado)),
        }
    }

    /// Con `token`, la petición tiene que traer `Authorization: Bearer <token>`.
    fn autorizar<T>(&self, peticion: &Request<T>) -> Result<(), Status> {
        let Some(token) = &self.token else {
            return Ok(());
        };
        let traido = peticion
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        match traido {
            Some(traido) if iguales(traido.as_bytes(), token.as_bytes()) => Ok(()),
            // No se dice qué falló: ni si faltaba ni si no coincidía.
            _ => Err(Status::unauthenticated(
                "hace falta el token de este flujo (Authorization: Bearer …)",
            )),
        }
    }

    fn es_el_mio(&self, nombre: &[u8]) -> Result<(), Status> {
        if nombre == self.ticket.as_bytes() {
            Ok(())
        } else {
            Err(Status::not_found(format!(
                "aquí se sirve el flujo `{}`, no `{}`",
                self.ticket,
                String::from_utf8_lossy(nombre)
            )))
        }
    }
}

/// Compara sin cortar en la primera diferencia: el tiempo de respuesta no
/// dice cuántos caracteres del token se acertaron.
fn iguales(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn no_aqui<T>(que: &str) -> Result<T, Status> {
    Err(Status::unimplemented(format!(
        "{que}: un sink de Orch sólo sirve su flujo, con DoGet"
    )))
}

#[tonic::async_trait]
impl FlightService for Flujo {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn do_get(
        &self,
        peticion: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.autorizar(&peticion)?;
        self.es_el_mio(&peticion.get_ref().ticket)?;

        let lotes = self
            .pendiente
            .lock()
            .expect("el mutex del flujo no debería envenenarse")
            .take()
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "otro consumidor ya está leyendo `{}`: cada flujo se lee una vez",
                    self.ticket
                ))
            })?;
        let quien = peticion
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "?".into());
        if let Some(aviso) = self.tomado.lock().expect("mutex").take() {
            let _ = aviso.send(quien);
        }

        // El esquema va por delante aunque no llegue ni un lote: quien lee
        // sabe qué columnas tiene un resultado vacío.
        let mut codificador = FlightDataEncoderBuilder::new();
        if let Some(schema) = &self.schema {
            codificador = codificador.with_schema(SchemaRef::clone(schema));
        }
        let datos = codificador
            .build(ReceiverStream::new(lotes))
            .map_err(Status::from);
        Ok(Response::new(datos.boxed()))
    }

    async fn get_schema(
        &self,
        peticion: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.autorizar(&peticion)?;
        let ruta = &peticion.get_ref().path;
        match ruta.as_slice() {
            [nombre] => self.es_el_mio(nombre.as_bytes())?,
            _ => return Err(Status::invalid_argument("se pide por el nombre del flujo")),
        }
        let schema = self.schema.as_ref().ok_or_else(|| {
            Status::unavailable("el esquema no se conoce hasta que llegue el primer lote")
        })?;
        let resultado: SchemaResult = SchemaAsIpc::new(schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e: orch_core::arrow::error::ArrowError| Status::internal(e.to_string()))?;
        Ok(Response::new(resultado))
    }

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        no_aqui("Handshake (el token va en cada petición)")
    }

    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        no_aqui("ListFlights")
    }

    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        no_aqui("GetFlightInfo")
    }

    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        no_aqui("PollFlightInfo")
    }

    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        no_aqui("DoPut")
    }

    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        no_aqui("DoExchange")
    }

    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        no_aqui("DoAction")
    }

    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        no_aqui("ListActions")
    }
}

#[cfg(test)]
mod tests {
    use super::iguales;

    #[test]
    fn comparar_tokens() {
        assert!(iguales(b"secreto", b"secreto"));
        assert!(!iguales(b"secreto", b"secretO"));
        assert!(!iguales(b"secreto", b"secret"));
    }
}
