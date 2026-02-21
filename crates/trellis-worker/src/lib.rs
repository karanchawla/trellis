mod errors;
mod heartbeat;
mod net;
mod registry;

pub use errors::{ConnectError, RecvError, SendError, WorkerError};
pub use registry::{HandlerRegistry, InputMap, LifecycleHook, TaskHandler};

use crate::heartbeat::spawn_heartbeat;
use crate::net::BrokerConnection;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use trellis_core::TaskError;
use trellis_protocol::{BrokerMessage, RejectionReason, WorkerMessage};
use trellis_store::{ObjectStore, download_data, upload_output};

pub struct WorkerConfig {
    pub run_id: String,
    pub worker_id: String,
    pub store: Arc<dyn ObjectStore>,
    pub no_work_min_delay: Duration,
    pub no_work_max_delay: Duration,
    pub reconnect_delay: Duration,
}

impl WorkerConfig {
    pub fn new(
        run_id: impl Into<String>,
        worker_id: impl Into<String>,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            worker_id: worker_id.into(),
            store,
            no_work_min_delay: Duration::from_millis(100),
            no_work_max_delay: Duration::from_secs(5),
            reconnect_delay: Duration::from_secs(1),
        }
    }
}

pub struct Worker {
    config: WorkerConfig,
    registry: HandlerRegistry,
}

impl Worker {
    pub fn new(config: WorkerConfig) -> Self {
        Self {
            config,
            registry: HandlerRegistry::new(),
        }
    }

    pub fn registry_mut(&mut self) -> &mut HandlerRegistry {
        &mut self.registry
    }

    pub async fn run(&mut self) -> Result<(), WorkerError> {
        self.registry.on_worker_start();

        let mut no_work_delay = self.config.no_work_min_delay;
        let mut connection: Option<BrokerConnection> = None;

        loop {
            if connection.is_none() {
                match BrokerConnection::connect(self.config.store.as_ref(), &self.config.run_id)
                    .await
                {
                    Ok(conn) => connection = Some(conn),
                    Err(_) => {
                        tokio::time::sleep(self.config.reconnect_delay).await;
                        continue;
                    }
                }
            }

            let claim = WorkerMessage::Claim {
                worker_id: self.config.worker_id.clone(),
                task_types: self.registry.task_types(),
            };

            let Some(conn) = connection.as_mut() else {
                continue;
            };

            if conn.send(claim).await.is_err() {
                connection = None;
                tokio::time::sleep(self.config.reconnect_delay).await;
                continue;
            }

            let response = match conn.recv().await {
                Ok(response) => response,
                Err(_) => {
                    connection = None;
                    tokio::time::sleep(self.config.reconnect_delay).await;
                    continue;
                }
            };

            match response {
                BrokerMessage::Assigned {
                    task_id,
                    task_type,
                    lease_id,
                    timeout_seconds,
                    input_refs,
                } => {
                    no_work_delay = self.config.no_work_min_delay;
                    let disposition = match self
                        .execute_assignment(
                            conn,
                            task_id,
                            task_type,
                            lease_id,
                            timeout_seconds,
                            input_refs,
                        )
                        .await
                    {
                        Ok(disposition) => disposition,
                        Err(_) => {
                            connection = None;
                            tokio::time::sleep(self.config.reconnect_delay).await;
                            continue;
                        }
                    };
                    if disposition == PostTaskDisposition::Reconnect {
                        connection = None;
                    }
                }
                BrokerMessage::NoWork => {
                    tokio::time::sleep(jitter(no_work_delay)).await;
                    no_work_delay = (no_work_delay * 2).min(self.config.no_work_max_delay);
                }
                BrokerMessage::Rejected {
                    reason: RejectionReason::BrokerReplaced,
                } => {
                    connection = None;
                }
                BrokerMessage::Rejected { .. } => {}
                BrokerMessage::Ack => {}
            }
        }
    }

    async fn execute_assignment(
        &self,
        connection: &mut BrokerConnection,
        task_id: String,
        task_type: String,
        lease_id: String,
        timeout_seconds: u32,
        input_refs: std::collections::HashMap<String, String>,
    ) -> Result<PostTaskDisposition, WorkerError> {
        let heartbeat = spawn_heartbeat(
            Arc::clone(&self.config.store),
            self.config.run_id.clone(),
            task_id.clone(),
            lease_id.clone(),
            timeout_seconds,
        );

        let mut handler_inputs = std::collections::HashMap::new();
        for (key, object_key) in input_refs {
            let payload = download_data(self.config.store.as_ref(), &object_key).await?;
            let value = serde_json::from_slice::<serde_json::Value>(&payload).map_err(|e| {
                WorkerError::Runtime(format!(
                    "failed to deserialize input {key} for {task_id}: {e}"
                ))
            })?;
            handler_inputs.insert(key, value);
        }

        let handler = match self.registry.get(&task_type) {
            Some(handler) => Arc::clone(handler),
            None => {
                let heartbeat_rejected = heartbeat.stop().await;
                if heartbeat_rejected {
                    return Ok(PostTaskDisposition::Continue);
                }
                return self
                    .send_fail_and_handle_response(
                        connection,
                        task_id,
                        lease_id,
                        format!("no handler registered for task type {task_type}"),
                        false,
                    )
                    .await;
            }
        };

        let outcome = handler(handler_inputs);
        let heartbeat_rejected = heartbeat.stop().await;
        if heartbeat_rejected {
            return Ok(PostTaskDisposition::Continue);
        }

        match outcome {
            Ok(output) => {
                let encoded = serde_json::to_vec(&output).map_err(|e| {
                    WorkerError::Runtime(format!("failed to serialize output for {task_id}: {e}"))
                })?;
                let output_ref = upload_output(
                    self.config.store.as_ref(),
                    &self.config.run_id,
                    &task_id,
                    &lease_id,
                    bytes::Bytes::from(encoded),
                )
                .await?;
                self.send_complete_and_handle_response(connection, task_id, lease_id, output_ref)
                    .await
            }
            Err(TaskError::Retryable(error)) => {
                self.send_fail_and_handle_response(connection, task_id, lease_id, error, true)
                    .await
            }
            Err(TaskError::Permanent(error)) => {
                self.send_fail_and_handle_response(connection, task_id, lease_id, error, false)
                    .await
            }
        }
    }

    async fn send_complete_and_handle_response(
        &self,
        connection: &mut BrokerConnection,
        task_id: String,
        lease_id: String,
        output_ref: String,
    ) -> Result<PostTaskDisposition, WorkerError> {
        connection
            .send(WorkerMessage::Complete {
                task_id,
                lease_id,
                output_ref,
            })
            .await?;

        self.handle_terminal_ack(connection).await
    }

    async fn send_fail_and_handle_response(
        &self,
        connection: &mut BrokerConnection,
        task_id: String,
        lease_id: String,
        error: String,
        retryable: bool,
    ) -> Result<PostTaskDisposition, WorkerError> {
        connection
            .send(WorkerMessage::Fail {
                task_id,
                lease_id,
                error,
                retryable,
            })
            .await?;

        self.handle_terminal_ack(connection).await
    }

    async fn handle_terminal_ack(
        &self,
        connection: &mut BrokerConnection,
    ) -> Result<PostTaskDisposition, WorkerError> {
        match connection.recv().await? {
            BrokerMessage::Ack => Ok(PostTaskDisposition::Continue),
            BrokerMessage::Rejected {
                reason: RejectionReason::BrokerReplaced,
            } => Ok(PostTaskDisposition::Reconnect),
            BrokerMessage::Rejected {
                reason: RejectionReason::LeaseMismatch,
            }
            | BrokerMessage::Rejected {
                reason: RejectionReason::CasConflict,
            } => Ok(PostTaskDisposition::Continue),
            BrokerMessage::NoWork | BrokerMessage::Assigned { .. } => {
                Ok(PostTaskDisposition::Continue)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PostTaskDisposition {
    Continue,
    Reconnect,
}

fn jitter(base: Duration) -> Duration {
    let base_ms = base.as_millis() as i64;
    let jitter = ((base_ms as f64) * 0.1).round() as i64;
    let mut rng = rand::thread_rng();
    let delta = if jitter == 0 {
        0
    } else {
        rng.gen_range(-jitter..=jitter)
    };
    let adjusted = std::cmp::max(0, base_ms + delta) as u64;
    Duration::from_millis(adjusted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::net::TcpListener as StdTcpListener;
    use std::thread;
    use tempfile::tempdir;
    use trellis_broker::{BrokerConfig, run as run_broker};
    use trellis_client::Client;
    use trellis_core::{DagBuilder, Run, RunStatus, TaskStatus};
    use trellis_store::{LocalFsStore, ObjectStore, read_state};

    fn free_port() -> u16 {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
        listener.local_addr().expect("local addr").port()
    }

    #[tokio::test]
    async fn distributed_execution_completes_with_broker_and_two_workers() {
        let temp = tempdir().expect("create temp dir");
        let store = Arc::new(LocalFsStore::new(temp.path()));
        let client = Client::new(Arc::clone(&store) as Arc<dyn ObjectStore>);

        let mut dag = DagBuilder::new("distributed");
        dag.task("extract").task_type("echo");
        dag.task("transform")
            .task_type("echo")
            .depends_on(["extract"]);

        let mut inputs = HashMap::new();
        inputs.insert("extract".to_string(), serde_json::json!({"value": 1}));
        let run_id = client.submit(dag, inputs).await.expect("submit run");

        let broker_config = BrokerConfig::new(
            run_id.clone(),
            "127.0.0.1:0".parse().expect("parse addr"),
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        );
        let broker_task = tokio::spawn(async move { run_broker(broker_config).await });

        let mut worker_one = Worker::new(WorkerConfig::new(
            run_id.clone(),
            "w-1",
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        ));
        let mut worker_two = Worker::new(WorkerConfig::new(
            run_id.clone(),
            "w-2",
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        ));

        let echo_handler: TaskHandler = Arc::new(|inputs| Ok(serde_json::json!(inputs)));
        worker_one
            .registry_mut()
            .register_fallback(Arc::clone(&echo_handler));
        worker_two.registry_mut().register_fallback(echo_handler);

        let worker_one_task = tokio::spawn(async move { worker_one.run().await });
        let worker_two_task = tokio::spawn(async move { worker_two.run().await });

        let mut completed = false;
        for _ in 0..200 {
            let snapshot = client.status(&run_id).await.expect("fetch status");
            if snapshot.status == RunStatus::Completed {
                completed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        worker_one_task.abort();
        worker_two_task.abort();
        broker_task.abort();

        assert!(completed, "run did not complete in time");
    }

    #[tokio::test]
    async fn worker_crash_triggers_timeout_retry_and_second_worker_completion() {
        let temp = tempdir().expect("create temp dir");
        let store = Arc::new(LocalFsStore::new(temp.path()));
        let client = Client::new(Arc::clone(&store) as Arc<dyn ObjectStore>);

        let mut dag = DagBuilder::new("retry");
        dag.task("task")
            .task_type("slow")
            .timeout_seconds(1)
            .max_retries(2);

        let mut inputs = HashMap::new();
        inputs.insert("task".to_string(), serde_json::json!({"value": 1}));
        let run_id = client.submit(dag, inputs).await.expect("submit run");

        let broker_port = free_port();
        let broker_config = BrokerConfig::new(
            run_id.clone(),
            format!("127.0.0.1:{broker_port}")
                .parse()
                .expect("parse broker addr"),
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        );
        let broker_task = tokio::spawn(async move { run_broker(broker_config).await });

        let mut worker_a = Worker::new(WorkerConfig::new(
            run_id.clone(),
            "w-a",
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        ));
        let slow_handler: TaskHandler = Arc::new(|inputs| {
            thread::sleep(Duration::from_secs(3));
            Ok(serde_json::json!(inputs))
        });
        worker_a.registry_mut().register("slow", slow_handler);
        let worker_a_task = tokio::spawn(async move { worker_a.run().await });

        let mut claimed_by_a = false;
        for _ in 0..100 {
            let cas = read_state::<Run>(store.as_ref(), &run_id)
                .await
                .expect("read state");
            let task = cas.run.tasks.get("task").expect("task");
            if task.status == TaskStatus::Running && task.worker_id.as_deref() == Some("w-a") {
                claimed_by_a = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(claimed_by_a, "worker A never claimed task");

        worker_a_task.abort();

        let mut worker_b = Worker::new(WorkerConfig::new(
            run_id.clone(),
            "w-b",
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        ));
        worker_b
            .registry_mut()
            .register_fallback(Arc::new(|inputs| Ok(serde_json::json!(inputs))));
        let worker_b_task = tokio::spawn(async move { worker_b.run().await });

        let mut completed = false;
        for _ in 0..300 {
            let snapshot = client.status(&run_id).await.expect("status");
            if snapshot.status == RunStatus::Completed {
                completed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let final_state = read_state::<Run>(store.as_ref(), &run_id)
            .await
            .expect("read final state");

        worker_b_task.abort();
        broker_task.abort();

        assert!(completed, "run did not complete");
        assert_eq!(final_state.run.status, RunStatus::Completed);
        assert!(final_state.run.tasks["task"].retry_count >= 1);
        assert!(final_state.run.tasks["task"].attempt >= 2);
    }

    #[tokio::test]
    async fn broker_failover_reconnects_workers_and_completes_run() {
        let temp = tempdir().expect("create temp dir");
        let store = Arc::new(LocalFsStore::new(temp.path()));
        let client = Client::new(Arc::clone(&store) as Arc<dyn ObjectStore>);

        let mut dag = DagBuilder::new("failover");
        dag.task("t0").task_type("echo").timeout_seconds(5);
        dag.task("t1")
            .task_type("echo")
            .timeout_seconds(5)
            .depends_on(["t0"]);
        dag.task("t2")
            .task_type("echo")
            .timeout_seconds(5)
            .depends_on(["t1"]);
        dag.task("t3")
            .task_type("echo")
            .timeout_seconds(5)
            .depends_on(["t2"]);
        dag.task("t4")
            .task_type("echo")
            .timeout_seconds(5)
            .depends_on(["t3"]);

        let mut inputs = HashMap::new();
        inputs.insert("t0".to_string(), serde_json::json!({"start": true}));
        let run_id = client.submit(dag, inputs).await.expect("submit run");

        let broker_a_port = free_port();
        let broker_b_port = free_port();

        let broker_a_addr = format!("127.0.0.1:{broker_a_port}");
        let broker_b_addr = format!("127.0.0.1:{broker_b_port}");

        let broker_a_config = BrokerConfig::new(
            run_id.clone(),
            broker_a_addr.parse().expect("parse broker a"),
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        );
        let broker_a_task = tokio::spawn(async move { run_broker(broker_a_config).await });

        let slow_echo: TaskHandler = Arc::new(|inputs| {
            thread::sleep(Duration::from_millis(150));
            Ok(serde_json::json!(inputs))
        });

        let mut worker_one = Worker::new(WorkerConfig::new(
            run_id.clone(),
            "w-1",
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        ));
        worker_one
            .registry_mut()
            .register_fallback(Arc::clone(&slow_echo));
        let mut worker_two = Worker::new(WorkerConfig::new(
            run_id.clone(),
            "w-2",
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        ));
        worker_two.registry_mut().register_fallback(slow_echo);

        let worker_one_task = tokio::spawn(async move { worker_one.run().await });
        let worker_two_task = tokio::spawn(async move { worker_two.run().await });

        let mut first_completion_observed = false;
        for _ in 0..200 {
            let cas = read_state::<Run>(store.as_ref(), &run_id)
                .await
                .expect("read state");
            if cas
                .run
                .tasks
                .values()
                .any(|task| task.status == TaskStatus::Completed)
            {
                first_completion_observed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            first_completion_observed,
            "no completed tasks before failover"
        );

        broker_a_task.abort();

        let broker_b_config = BrokerConfig::new(
            run_id.clone(),
            broker_b_addr.parse().expect("parse broker b"),
            Arc::clone(&store) as Arc<dyn ObjectStore>,
        );
        let broker_b_task = tokio::spawn(async move { run_broker(broker_b_config).await });

        let mut completed = false;
        for _ in 0..400 {
            let snapshot = client.status(&run_id).await.expect("status");
            if snapshot.status == RunStatus::Completed {
                completed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let final_state = read_state::<Run>(store.as_ref(), &run_id)
            .await
            .expect("read final state");

        worker_one_task.abort();
        worker_two_task.abort();
        broker_b_task.abort();

        assert!(completed, "run did not complete after failover");
        assert_eq!(final_state.run.status, RunStatus::Completed);
        assert_eq!(
            final_state.run.broker.as_deref(),
            Some(broker_b_addr.as_str())
        );
    }
}
