use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error(transparent)]
    Store(#[from] trellis_store::StoreError),
    #[error("run does not currently advertise a broker address")]
    MissingBroker,
    #[error("connection failed: {0}")]
    Connection(String),
}

#[derive(Debug, Error)]
pub enum SendError {
    #[error("send failed: {0}")]
    Send(String),
}

#[derive(Debug, Error)]
pub enum RecvError {
    #[error("connection closed")]
    ConnectionClosed,
    #[error("receive failed: {0}")]
    Receive(String),
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error(transparent)]
    Send(#[from] SendError),
    #[error(transparent)]
    Receive(#[from] RecvError),
    #[error(transparent)]
    Store(#[from] trellis_store::StoreError),
    #[error("runtime error: {0}")]
    Runtime(String),
}
