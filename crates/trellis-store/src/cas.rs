use crate::{ObjectStore, StoreError};
use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug, Clone, PartialEq)]
pub struct CasState<T> {
    pub run: T,
    pub etag: String,
}

pub async fn read_state<T>(store: &dyn ObjectStore, run_id: &str) -> Result<CasState<T>, StoreError>
where
    T: DeserializeOwned,
{
    let key = state_key(run_id);
    let (body, etag) = store.get(&key).await?;
    let run = serde_json::from_slice::<T>(&body)
        .map_err(|e| StoreError::Other(format!("failed to deserialize state.json: {e}")))?;
    Ok(CasState { run, etag })
}

pub async fn write_state<T>(
    store: &dyn ObjectStore,
    run_id: &str,
    run: &T,
    etag: &str,
) -> Result<String, StoreError>
where
    T: Serialize,
{
    let key = state_key(run_id);
    let body = serde_json::to_vec(run)
        .map_err(|e| StoreError::Other(format!("failed to serialize state.json: {e}")))?;
    store
        .put_if_match(&key, bytes::Bytes::from(body), etag)
        .await
}

fn state_key(run_id: &str) -> String {
    format!("runs/{run_id}/state.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalFsStore;
    use crate::ObjectStore;
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct State {
        status: String,
        value: u32,
    }

    #[tokio::test]
    async fn read_modify_write_cycle_works() {
        let temp = tempdir().expect("create temp dir");
        let store = LocalFsStore::new(temp.path());
        let run_id = "run-1";
        let key = format!("runs/{run_id}/state.json");

        let state = State {
            status: "pending".to_string(),
            value: 1,
        };
        let body = serde_json::to_vec(&state).expect("serialize");
        store
            .put(&key, Bytes::from(body))
            .await
            .expect("seed state.json");

        let mut cas = read_state::<State>(&store, run_id)
            .await
            .expect("read state");
        cas.run.status = "running".to_string();
        let new_etag = write_state(&store, run_id, &cas.run, &cas.etag)
            .await
            .expect("write state");

        let cas_after = read_state::<State>(&store, run_id)
            .await
            .expect("read state after write");
        assert_eq!(cas_after.run.status, "running");
        assert_eq!(cas_after.run.value, 1);
        assert_eq!(cas_after.etag, new_etag);
    }

    #[tokio::test]
    async fn stale_etag_fails_with_precondition_failed() {
        let temp = tempdir().expect("create temp dir");
        let store = LocalFsStore::new(temp.path());
        let run_id = "run-2";
        let key = format!("runs/{run_id}/state.json");

        let state = State {
            status: "pending".to_string(),
            value: 1,
        };
        store
            .put(
                &key,
                Bytes::from(serde_json::to_vec(&state).expect("serialize state")),
            )
            .await
            .expect("seed state.json");

        let cas = read_state::<State>(&store, run_id)
            .await
            .expect("read state");
        let updated = State {
            status: "running".to_string(),
            value: 2,
        };
        let _fresh_etag = write_state(&store, run_id, &updated, &cas.etag)
            .await
            .expect("write with fresh etag");

        let err = write_state(&store, run_id, &updated, &cas.etag)
            .await
            .expect_err("stale etag should fail");
        assert_eq!(
            err,
            StoreError::PreconditionFailed(format!("runs/{run_id}/state.json"))
        );
    }
}
