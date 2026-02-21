use crate::PendingRequest;
use crate::errors::BrokerError;
use crate::handlers::{apply_message, check_run_completion, timeout_scan, update_current_layer};
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use trellis_core::Run;
use trellis_protocol::{BrokerMessage, RejectionReason};
use trellis_store::{ObjectStore, StoreError, read_state, write_state};

pub struct RequestBuffer {
    rx: mpsc::Receiver<PendingRequest>,
}

impl RequestBuffer {
    pub fn new(rx: mpsc::Receiver<PendingRequest>) -> Self {
        Self { rx }
    }

    pub async fn drain(&mut self, max_wait: Duration) -> Vec<PendingRequest> {
        let mut batch = Vec::new();
        let first = if self.rx.is_empty() {
            (tokio::time::timeout(max_wait, self.rx.recv()).await).unwrap_or_default()
        } else {
            self.rx.recv().await
        };

        if let Some(first) = first {
            batch.push(first);
            while let Ok(next) = self.rx.try_recv() {
                batch.push(next);
            }
        }

        batch
    }
}

pub async fn run_commit_loop(
    run_id: &str,
    store: Arc<dyn ObjectStore>,
    mut run: Run,
    mut etag: String,
    mut request_buffer: RequestBuffer,
    max_wait: Duration,
    broker_addr: String,
) -> Result<(), BrokerError> {
    loop {
        let batch = request_buffer.drain(max_wait).await;

        let now = Utc::now();
        let (responses, mut changed) = apply_batch(&mut run, &batch, now);
        changed |= timeout_scan(&mut run, now);
        changed |= update_current_layer(&mut run);
        changed |= check_run_completion(&mut run, now);

        if !changed {
            send_responses(batch, responses);
            continue;
        }

        match write_state(store.as_ref(), run_id, &run, &etag).await {
            Ok(next_etag) => {
                etag = next_etag;
                send_responses(batch, responses);
            }
            Err(StoreError::PreconditionFailed(_)) => {
                let reloaded = read_state::<Run>(store.as_ref(), run_id).await?;
                if reloaded.run.broker.as_deref() != Some(broker_addr.as_str()) {
                    reject_all(batch, RejectionReason::BrokerReplaced);
                    return Ok(());
                }
                run = reloaded.run;
                etag = reloaded.etag;

                let now = Utc::now();
                let (retry_responses, mut retry_changed) = apply_batch(&mut run, &batch, now);
                retry_changed |= timeout_scan(&mut run, now);
                retry_changed |= update_current_layer(&mut run);
                retry_changed |= check_run_completion(&mut run, now);

                if !retry_changed {
                    send_responses(batch, retry_responses);
                    continue;
                }

                match write_state(store.as_ref(), run_id, &run, &etag).await {
                    Ok(next_etag) => {
                        etag = next_etag;
                        send_responses(batch, retry_responses);
                    }
                    Err(StoreError::PreconditionFailed(_)) => {
                        let reloaded = read_state::<Run>(store.as_ref(), run_id).await?;
                        if reloaded.run.broker.as_deref() != Some(broker_addr.as_str()) {
                            reject_all(batch, RejectionReason::BrokerReplaced);
                            return Ok(());
                        }
                        reject_all(batch, RejectionReason::CasConflict);
                    }
                    Err(error) => return Err(BrokerError::Store(error)),
                }
            }
            Err(error) => return Err(BrokerError::Store(error)),
        }
    }
}

fn apply_batch(
    run: &mut Run,
    batch: &[PendingRequest],
    now: chrono::DateTime<Utc>,
) -> (Vec<BrokerMessage>, bool) {
    let mut responses = Vec::with_capacity(batch.len());
    let mut changed = false;

    for pending in batch {
        let (response, mutated) = apply_message(run, &pending.message, now);
        responses.push(response);
        changed |= mutated;
    }

    (responses, changed)
}

fn send_responses(batch: Vec<PendingRequest>, responses: Vec<BrokerMessage>) {
    for (pending, response) in batch.into_iter().zip(responses.into_iter()) {
        let _ = pending.response_tx.send(response);
    }
}

fn reject_all(batch: Vec<PendingRequest>, reason: RejectionReason) {
    for pending in batch {
        let _ = pending.response_tx.send(BrokerMessage::Rejected {
            reason: reason.clone(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use trellis_core::DagBuilder;
    use trellis_protocol::WorkerMessage;
    use trellis_store::LocalFsStore;

    #[tokio::test]
    async fn conflict_with_replaced_owner_rejects_batch_and_exits() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let store = Arc::new(LocalFsStore::new(temp.path()));

        let mut dag = DagBuilder::new("ownership");
        dag.task("task").task_type("echo");
        let mut run = dag.build().expect("build run");
        run.run_id = "run-ownership".to_string();
        run.status = trellis_core::RunStatus::Running;
        run.broker = Some("old-owner".to_string());

        let state_key = format!("runs/{}/state.json", run.run_id);
        let body = trellis_core::serialize_run(&run).expect("serialize run");
        let etag_old = store
            .put_if_none_match(&state_key, body)
            .await
            .expect("create state");

        let mut replacement = run.clone();
        replacement.broker = Some("new-owner".to_string());
        replacement.started_at = Some(Utc::now());
        let _etag_new = write_state(store.as_ref(), &run.run_id, &replacement, &etag_old)
            .await
            .expect("replace owner");

        let (tx, rx) = mpsc::channel(8);
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        tx.send(PendingRequest {
            message: WorkerMessage::Claim {
                worker_id: "worker-1".to_string(),
                task_types: vec!["echo".to_string()],
            },
            response_tx,
        })
        .await
        .expect("enqueue request");
        drop(tx);

        let run_id = run.run_id.clone();
        let result = run_commit_loop(
            &run_id,
            store as Arc<dyn ObjectStore>,
            run,
            etag_old,
            RequestBuffer::new(rx),
            Duration::from_millis(10),
            "old-owner".to_string(),
        )
        .await;

        assert!(result.is_ok());
        let response = response_rx.await.expect("response");
        assert_eq!(
            response,
            BrokerMessage::Rejected {
                reason: RejectionReason::BrokerReplaced
            }
        );
    }
}
