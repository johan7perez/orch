//! # orch-flight
//!
//! Aristas remotas sobre Arrow Flight.
//!
//! Un sink `flight` sirve el flujo que le llega; un origen `flight` lo lee de
//! cualquier servidor Flight. Juntos, unen dos pipelines que corren en
//! máquinas distintas como si fueran una arista más: los lotes viajan como
//! Arrow IPC sobre gRPC, con la contrapresión de HTTP/2 haciendo el papel del
//! canal acotado, y un fallo en un lado llega al otro como fallo.
//!
//! Son conectores normales: el motor no sabe que la arista cruza la red. Es
//! lo que pedía el plan —reutilizar los traits de conector sobre un
//! transporte remoto— y la base sobre la que un coordinador podría repartir
//! un pipeline entre máquinas.
//!
//! Va en su propio crate, como REST o PostgreSQL, para que quien no lo use no
//! arrastre gRPC.

pub mod cluster;
mod servidor;
mod sink;
mod source;
mod tls;

use orch_core::Registry;

pub use sink::{FlightSink, FlightSinkConfig};
pub use source::{FlightSource, FlightSourceConfig};
pub use tls::{ClientTls, ServerTls};

/// Añade el origen y el sink `flight` a un registro existente.
pub fn register(registry: &mut Registry) {
    source::register(registry);
    sink::register(registry);
}
