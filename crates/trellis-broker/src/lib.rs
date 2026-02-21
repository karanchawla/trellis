mod commit;
mod errors;
mod handlers;
mod net;

pub use errors::BrokerError;

use chrono::Utc;
use commit::{RequestBuffer, run_commit_loop};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use trellis_core::{Run, RunStatus};
use trellis_protocol::{BrokerMessage, WorkerMessage};
use trellis_store::{ObjectStore, StoreError, read_state, write_state};

pub struct BrokerConfig {
    pub run_id: String,
    pub listen_addr: SocketAddr,
    pub store: Arc<dyn ObjectStore>,
    pub max_batch_wait: Duration,
    pub request_buffer_size: usize,
}

impl BrokerConfig {
    pub fn new(
        run_id: impl Into<String>,
        listen_addr: SocketAddr,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            listen_addr,
            store,
            max_batch_wait: Duration::from_millis(200),
            request_buffer_size: 1024,
        }
    }
}

pub struct PendingRequest {
    pub message: WorkerMessage,
    pub response_tx: oneshot::Sender<BrokerMessage>,
}

pub async fn run(config: BrokerConfig) -> Result<(), BrokerError> {
    let listener = net::bind_listener(config.listen_addr).await?;
    let broker_addr = listener
        .local_addr()
        .map_err(|e| BrokerError::Bind(e.to_string()))?;

    let (run, etag) = claim_startup_ownership(
        config.store.as_ref(),
        &config.run_id,
        &broker_addr.to_string(),
    )
    .await?;

    let (tx, rx) = mpsc::channel(config.request_buffer_size);
    let accept_task = tokio::spawn(net::run_accept_loop(listener, tx));

    let loop_result = run_commit_loop(
        &config.run_id,
        Arc::clone(&config.store),
        run,
        etag,
        RequestBuffer::new(rx),
        config.max_batch_wait,
        broker_addr.to_string(),
    )
    .await;

    accept_task.abort();
    loop_result
}

async fn claim_startup_ownership(
    store: &dyn ObjectStore,
    run_id: &str,
    broker_addr: &str,
) -> Result<(Run, String), BrokerError> {
    loop {
        let cas = read_state::<Run>(store, run_id).await?;
        let mut run = cas.run;

        if run.status.is_terminal() {
            return Err(BrokerError::TerminalRun(format!(
                "run {run_id} is in terminal status {:?}",
                run.status
            )));
        }

        run.broker = Some(broker_addr.to_string());
        if run.status == RunStatus::Pending {
            run.status = RunStatus::Running;
            if run.started_at.is_none() {
                run.started_at = Some(Utc::now());
            }
        }

        match write_state(store, run_id, &run, &cas.etag).await {
            Ok(next_etag) => return Ok((run, next_etag)),
            Err(StoreError::PreconditionFailed(_)) => continue,
            Err(error) => return Err(BrokerError::Store(error)),
        }
    }
}
