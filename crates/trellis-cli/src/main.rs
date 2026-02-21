use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser, Subcommand};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use trellis_broker::{BrokerConfig, run as run_broker};
use trellis_client::Client;
use trellis_core::{DagBuilder, FailurePolicy};
use trellis_store::{LocalFsStore, ObjectStore, S3Store};
use trellis_worker::{TaskHandler, Worker, WorkerConfig};

#[derive(Parser)]
#[command(name = "trellis")]
#[command(about = "Trellis CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Broker(BrokerArgs),
    Worker(WorkerArgs),
    Submit(SubmitArgs),
    Status(StatusArgs),
}

#[derive(Args)]
struct BrokerArgs {
    #[arg(long)]
    run_id: String,
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 9090)]
    port: u16,
    #[command(flatten)]
    store: StoreArgs,
}

#[derive(Args)]
struct WorkerArgs {
    #[arg(long)]
    run_id: String,
    #[arg(long)]
    worker_id: String,
    #[arg(long, default_value = "echo")]
    handler_builtin: String,
    #[command(flatten)]
    store: StoreArgs,
}

#[derive(Args)]
struct SubmitArgs {
    #[arg(long)]
    dag_file: PathBuf,
    #[arg(long, value_name = "TASK_ID=JSON")]
    input: Vec<String>,
    #[command(flatten)]
    store: StoreArgs,
}

#[derive(Args)]
struct StatusArgs {
    #[arg(long)]
    run_id: String,
    #[arg(long, default_value_t = false)]
    watch: bool,
    #[arg(long, default_value_t = 1000)]
    interval_ms: u64,
    #[command(flatten)]
    store: StoreArgs,
}

#[derive(Args, Clone)]
struct StoreArgs {
    #[arg(long, default_value = "local")]
    store_type: String,
    #[arg(long)]
    store_path: Option<PathBuf>,
    #[arg(long)]
    s3_endpoint: Option<String>,
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,
    #[arg(long, default_value = "minioadmin")]
    s3_access_key: String,
    #[arg(long, default_value = "minioadmin")]
    s3_secret_key: String,
    #[arg(long)]
    s3_bucket: Option<String>,
}

#[derive(serde::Deserialize)]
struct DagFile {
    name: String,
    tasks: Vec<DagTask>,
}

#[derive(serde::Deserialize)]
struct DagTask {
    task_id: String,
    task_type: String,
    #[serde(default)]
    depends_on: Vec<String>,
    timeout_seconds: Option<u32>,
    max_retries: Option<u32>,
    on_failure: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Broker(args) => run_broker_command(args).await,
        Commands::Worker(args) => run_worker_command(args).await,
        Commands::Submit(args) => run_submit_command(args).await,
        Commands::Status(args) => run_status_command(args).await,
    }
}

async fn run_broker_command(args: BrokerArgs) -> Result<()> {
    let store = build_store(&args.store).await?;
    let addr = SocketAddr::from_str(&format!("{}:{}", args.host, args.port))
        .context("invalid broker listen address")?;

    let config = BrokerConfig::new(args.run_id, addr, store);
    run_broker(config).await.map_err(anyhow::Error::from)
}

async fn run_worker_command(args: WorkerArgs) -> Result<()> {
    let store = build_store(&args.store).await?;
    let config = WorkerConfig::new(args.run_id, args.worker_id, Arc::clone(&store));
    let mut worker = Worker::new(config);

    if args.handler_builtin == "echo" {
        let handler: TaskHandler = Arc::new(|inputs| Ok(serde_json::json!(inputs)));
        worker.registry_mut().register_fallback(handler);
    }

    worker.run().await.map_err(anyhow::Error::from)
}

async fn run_submit_command(args: SubmitArgs) -> Result<()> {
    let store = build_store(&args.store).await?;
    let client = Client::new(store);

    let raw = tokio::fs::read_to_string(&args.dag_file)
        .await
        .with_context(|| format!("failed to read {}", args.dag_file.display()))?;
    let dag_file: DagFile = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse DAG file {}", args.dag_file.display()))?;

    let dag = build_dag(dag_file)?;
    let inputs = parse_inputs(&args.input)?;
    let run_id = client.submit(dag, inputs).await?;
    println!("{run_id}");
    Ok(())
}

async fn run_status_command(args: StatusArgs) -> Result<()> {
    let store = build_store(&args.store).await?;
    let client = Client::new(store);

    loop {
        let snapshot = client.status(&args.run_id).await?;
        println!("{snapshot}");

        if !args.watch {
            break;
        }

        tokio::time::sleep(Duration::from_millis(args.interval_ms)).await;
    }

    Ok(())
}

async fn build_store(args: &StoreArgs) -> Result<Arc<dyn ObjectStore>> {
    match args.store_type.as_str() {
        "local" => {
            let path = args
                .store_path
                .as_ref()
                .ok_or_else(|| anyhow!("--store-path is required for --store-type local"))?;
            Ok(Arc::new(LocalFsStore::new(path)))
        }
        "s3" => {
            let endpoint = args
                .s3_endpoint
                .as_deref()
                .ok_or_else(|| anyhow!("--s3-endpoint is required for --store-type s3"))?;
            let bucket = args
                .s3_bucket
                .as_deref()
                .ok_or_else(|| anyhow!("--s3-bucket is required for --store-type s3"))?;
            let store = S3Store::from_endpoint(
                endpoint,
                &args.s3_region,
                &args.s3_access_key,
                &args.s3_secret_key,
                bucket,
            )
            .await?;
            Ok(Arc::new(store))
        }
        other => Err(anyhow!("unsupported --store-type {other}")),
    }
}

fn parse_inputs(raw: &[String]) -> Result<HashMap<String, serde_json::Value>> {
    let mut inputs = HashMap::new();
    for item in raw {
        let (task_id, value) = item
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid --input value '{item}', expected TASK_ID=JSON"))?;
        let json = serde_json::from_str::<serde_json::Value>(value)
            .with_context(|| format!("invalid JSON input for task {task_id}"))?;
        inputs.insert(task_id.to_string(), json);
    }
    Ok(inputs)
}

fn build_dag(file: DagFile) -> Result<DagBuilder> {
    let mut builder = DagBuilder::new(&file.name);
    for task in file.tasks {
        let mut task_builder = builder.task(&task.task_id).task_type(task.task_type);

        if !task.depends_on.is_empty() {
            task_builder = task_builder.depends_on(task.depends_on);
        }

        if let Some(timeout_seconds) = task.timeout_seconds {
            task_builder = task_builder.timeout_seconds(timeout_seconds);
        }

        if let Some(max_retries) = task.max_retries {
            task_builder = task_builder.max_retries(max_retries);
        }

        if let Some(on_failure) = task.on_failure {
            let policy = parse_failure_policy(&on_failure)?;
            task_builder = task_builder.on_failure(policy);
        }

        let _ = task_builder;
    }

    Ok(builder)
}

fn parse_failure_policy(value: &str) -> Result<FailurePolicy> {
    match value {
        "fail_dag" => Ok(FailurePolicy::FailDag),
        "continue" => Ok(FailurePolicy::Continue),
        "skip_downstream" => Ok(FailurePolicy::SkipDownstream),
        other => Err(anyhow!("invalid failure policy '{other}'")),
    }
}
