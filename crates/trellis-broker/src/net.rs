use crate::PendingRequest;
use crate::errors::BrokerError;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::codec::{FramedRead, FramedWrite};
use trellis_protocol::{BrokerMessage, NdJsonCodec, WorkerMessage};

pub async fn bind_listener(addr: SocketAddr) -> Result<TcpListener, BrokerError> {
    TcpListener::bind(addr)
        .await
        .map_err(|e| BrokerError::Bind(e.to_string()))
}

pub async fn run_accept_loop(
    listener: TcpListener,
    tx: mpsc::Sender<PendingRequest>,
) -> Result<(), BrokerError> {
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(|e| BrokerError::Listener(e.to_string()))?;
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let _ = handle_connection(stream, tx_clone).await;
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    tx: mpsc::Sender<PendingRequest>,
) -> Result<(), BrokerError> {
    let (read_half, write_half) = stream.into_split();
    let mut reader = FramedRead::new(read_half, NdJsonCodec::<WorkerMessage>::new());
    let mut writer = FramedWrite::new(write_half, NdJsonCodec::<BrokerMessage>::new());

    while let Some(next) = reader.next().await {
        let message = match next {
            Ok(message) => message,
            Err(error) => {
                return Err(BrokerError::Connection(error.to_string()));
            }
        };

        let (response_tx, response_rx) = oneshot::channel();
        tx.send(PendingRequest {
            message,
            response_tx,
        })
        .await
        .map_err(|_| BrokerError::ChannelClosed)?;

        let response = match response_rx.await {
            Ok(response) => response,
            Err(_) => return Err(BrokerError::ChannelClosed),
        };

        writer
            .send(response)
            .await
            .map_err(|e| BrokerError::Connection(e.to_string()))?;
    }

    Ok(())
}
