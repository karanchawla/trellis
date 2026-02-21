use thiserror::Error;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error(transparent)]
    Store(#[from] trellis_store::StoreError),
    #[error("failed to bind listener: {0}")]
    Bind(String),
    #[error("listener error: {0}")]
    Listener(String),
    #[error("connection error: {0}")]
    Connection(String),
    #[error("channel closed")]
    ChannelClosed,
    #[error("run is terminal and cannot be broker-owned: {0}")]
    TerminalRun(String),
}
