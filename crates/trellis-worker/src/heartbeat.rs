use crate::net::BrokerConnection;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use trellis_protocol::{BrokerMessage, WorkerMessage};
use trellis_store::ObjectStore;

pub struct HeartbeatHandle {
    stop_tx: Option<oneshot::Sender<()>>,
    join_handle: JoinHandle<bool>,
}

impl HeartbeatHandle {
    pub async fn stop(mut self) -> bool {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        self.join_handle.await.unwrap_or(false)
    }
}

pub fn spawn_heartbeat(
    store: Arc<dyn ObjectStore>,
    run_id: String,
    task_id: String,
    lease_id: String,
    timeout_seconds: u32,
) -> HeartbeatHandle {
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let interval_seconds = std::cmp::max(1, timeout_seconds / 3);

    let join_handle = tokio::spawn(async move {
        loop {
            let sleep = tokio::time::sleep(Duration::from_secs(interval_seconds as u64));
            tokio::pin!(sleep);

            tokio::select! {
                _ = &mut stop_rx => return false,
                _ = &mut sleep => {}
            }

            let mut connection = match BrokerConnection::connect(store.as_ref(), &run_id).await {
                Ok(connection) => connection,
                Err(_) => continue,
            };

            if connection
                .send(WorkerMessage::Heartbeat {
                    task_id: task_id.clone(),
                    lease_id: lease_id.clone(),
                })
                .await
                .is_err()
            {
                continue;
            }

            match connection.recv().await {
                Ok(BrokerMessage::Ack) => {}
                Ok(BrokerMessage::Rejected { .. }) => return true,
                Ok(_) => {}
                Err(_) => continue,
            }
        }
    });

    HeartbeatHandle {
        stop_tx: Some(stop_tx),
        join_handle,
    }
}
