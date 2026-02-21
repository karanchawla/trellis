use crate::errors::{ConnectError, RecvError, SendError};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite};
use trellis_core::Run;
use trellis_protocol::{BrokerMessage, NdJsonCodec, WorkerMessage};
use trellis_store::{ObjectStore, read_state};

pub struct BrokerConnection {
    reader: FramedRead<OwnedReadHalf, NdJsonCodec<BrokerMessage>>,
    writer: FramedWrite<OwnedWriteHalf, NdJsonCodec<WorkerMessage>>,
}

impl BrokerConnection {
    pub async fn connect(store: &dyn ObjectStore, run_id: &str) -> Result<Self, ConnectError> {
        let broker_addr = discover_broker_addr(store, run_id).await?;
        Self::connect_to_addr(&broker_addr).await
    }

    async fn connect_to_addr(addr: &str) -> Result<Self, ConnectError> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| ConnectError::Connection(e.to_string()))?;
        let (read_half, write_half) = stream.into_split();
        Ok(Self {
            reader: FramedRead::new(read_half, NdJsonCodec::<BrokerMessage>::new()),
            writer: FramedWrite::new(write_half, NdJsonCodec::<WorkerMessage>::new()),
        })
    }

    pub async fn send(&mut self, message: WorkerMessage) -> Result<(), SendError> {
        self.writer
            .send(message)
            .await
            .map_err(|e| SendError::Send(e.to_string()))
    }

    pub async fn recv(&mut self) -> Result<BrokerMessage, RecvError> {
        match self.reader.next().await {
            Some(Ok(message)) => Ok(message),
            Some(Err(error)) => Err(RecvError::Receive(error.to_string())),
            None => Err(RecvError::ConnectionClosed),
        }
    }
}

pub async fn discover_broker_addr(
    store: &dyn ObjectStore,
    run_id: &str,
) -> Result<String, ConnectError> {
    let cas = read_state::<Run>(store, run_id).await?;
    cas.run.broker.ok_or(ConnectError::MissingBroker)
}
