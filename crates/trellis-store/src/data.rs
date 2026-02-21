use crate::{ObjectStore, StoreError};
use bytes::Bytes;

pub async fn upload_input(
    store: &dyn ObjectStore,
    run_id: &str,
    task_id: &str,
    data: Bytes,
) -> Result<String, StoreError> {
    let key = format!("data/{run_id}/{task_id}/input");
    store.put(&key, data).await?;
    Ok(key)
}

pub async fn upload_output(
    store: &dyn ObjectStore,
    run_id: &str,
    task_id: &str,
    lease_id: &str,
    data: Bytes,
) -> Result<String, StoreError> {
    let key = format!("data/{run_id}/{task_id}/output/{lease_id}");
    store.put(&key, data).await?;
    Ok(key)
}

pub async fn download_data(store: &dyn ObjectStore, key: &str) -> Result<Bytes, StoreError> {
    let (data, _) = store.get(key).await?;
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalFsStore;
    use tempfile::tempdir;

    #[tokio::test]
    async fn upload_and_download_input_round_trip() {
        let temp = tempdir().expect("create temp dir");
        let store = LocalFsStore::new(temp.path());
        let run_id = "run-input";
        let task_id = "extract";
        let payload = Bytes::from_static(br#"{"records":[1,2,3]}"#);

        let key = upload_input(&store, run_id, task_id, payload.clone())
            .await
            .expect("upload input");
        assert_eq!(key, "data/run-input/extract/input");

        let downloaded = download_data(&store, &key).await.expect("download input");
        assert_eq!(downloaded, payload);
    }

    #[tokio::test]
    async fn output_upload_is_lease_scoped() {
        let temp = tempdir().expect("create temp dir");
        let store = LocalFsStore::new(temp.path());
        let run_id = "run-output";
        let task_id = "transform";

        let key_a = upload_output(
            &store,
            run_id,
            task_id,
            "lease-a",
            Bytes::from_static(br#"{"value":1}"#),
        )
        .await
        .expect("upload output a");
        let key_b = upload_output(
            &store,
            run_id,
            task_id,
            "lease-b",
            Bytes::from_static(br#"{"value":2}"#),
        )
        .await
        .expect("upload output b");

        assert!(key_a.contains("lease-a"));
        assert!(key_b.contains("lease-b"));
        assert_ne!(key_a, key_b);

        let value_a = download_data(&store, &key_a).await.expect("download a");
        let value_b = download_data(&store, &key_b).await.expect("download b");
        assert_eq!(value_a, Bytes::from_static(br#"{"value":1}"#));
        assert_eq!(value_b, Bytes::from_static(br#"{"value":2}"#));
    }
}
