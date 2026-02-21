use crate::{ObjectStore, StoreError};
use async_trait::async_trait;
use bytes::Bytes;
use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::path::{Component, Path, PathBuf};
use tokio::task::spawn_blocking;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct LocalFsStore {
    base_dir: PathBuf,
}

impl LocalFsStore {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }
}

#[async_trait]
impl ObjectStore for LocalFsStore {
    async fn get(&self, key: &str) -> Result<(Bytes, String), StoreError> {
        let base_dir = self.base_dir.clone();
        let key = key.to_string();
        run_blocking(move || {
            let path = key_to_path(&base_dir, &key)?;
            if !path.exists() {
                return Err(StoreError::NotFound(key));
            }
            let body = fs::read(&path).map_err(io_err)?;
            let etag = content_etag(&body);
            Ok((Bytes::from(body), etag))
        })
        .await
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<String, StoreError> {
        let base_dir = self.base_dir.clone();
        let key = key.to_string();
        let body = body.to_vec();
        run_blocking(move || {
            let path = key_to_path(&base_dir, &key)?;
            atomic_write(&path, &body)?;
            Ok(content_etag(&body))
        })
        .await
    }

    async fn put_if_match(&self, key: &str, body: Bytes, etag: &str) -> Result<String, StoreError> {
        let base_dir = self.base_dir.clone();
        let key = key.to_string();
        let expected_etag = etag.to_string();
        let body = body.to_vec();
        run_blocking(move || {
            with_key_lock(&base_dir, &key, |path| {
                if !path.exists() {
                    return Err(StoreError::NotFound(key.clone()));
                }
                let current = fs::read(&path).map_err(io_err)?;
                let current_etag = content_etag(&current);
                if current_etag != expected_etag {
                    return Err(StoreError::PreconditionFailed(key.clone()));
                }
                atomic_write(&path, &body)?;
                Ok(content_etag(&body))
            })
        })
        .await
    }

    async fn put_if_none_match(&self, key: &str, body: Bytes) -> Result<String, StoreError> {
        let base_dir = self.base_dir.clone();
        let key = key.to_string();
        let body = body.to_vec();
        run_blocking(move || {
            with_key_lock(&base_dir, &key, |path| {
                if path.exists() {
                    return Err(StoreError::AlreadyExists(key.clone()));
                }
                atomic_write(&path, &body)?;
                Ok(content_etag(&body))
            })
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let base_dir = self.base_dir.clone();
        let key = key.to_string();
        run_blocking(move || {
            let path = key_to_path(&base_dir, &key)?;
            if !path.exists() {
                return Ok(());
            }
            fs::remove_file(path).map_err(io_err)?;
            Ok(())
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let base_dir = self.base_dir.clone();
        let prefix = prefix.to_string();
        run_blocking(move || {
            if !base_dir.exists() {
                return Ok(Vec::new());
            }
            let mut keys = Vec::new();
            for entry in WalkDir::new(&base_dir) {
                let entry = entry.map_err(|e| StoreError::Io(e.to_string()))?;
                if !entry.file_type().is_file() {
                    continue;
                }
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("lock") {
                    continue;
                }
                let rel = path
                    .strip_prefix(&base_dir)
                    .map_err(|e| StoreError::Other(e.to_string()))?;
                let key = rel
                    .iter()
                    .map(|part| part.to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                if key.starts_with(&prefix) {
                    keys.push(key);
                }
            }
            keys.sort();
            Ok(keys)
        })
        .await
    }

    async fn exists(&self, key: &str) -> Result<bool, StoreError> {
        let base_dir = self.base_dir.clone();
        let key = key.to_string();
        run_blocking(move || {
            let path = key_to_path(&base_dir, &key)?;
            Ok(path.exists())
        })
        .await
    }
}

async fn run_blocking<T, F>(f: F) -> Result<T, StoreError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, StoreError> + Send + 'static,
{
    spawn_blocking(f)
        .await
        .map_err(|e| StoreError::Other(format!("blocking task join error: {e}")))?
}

fn key_to_path(base_dir: &Path, key: &str) -> Result<PathBuf, StoreError> {
    if key.is_empty() {
        return Err(StoreError::Other("key cannot be empty".to_string()));
    }
    let key_path = Path::new(key);
    for component in key_path.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(StoreError::Other(format!("invalid key path: {key}")));
            }
        }
    }
    Ok(base_dir.join(key_path))
}

fn with_key_lock<T, F>(base_dir: &Path, key: &str, f: F) -> Result<T, StoreError>
where
    F: FnOnce(PathBuf) -> Result<T, StoreError>,
{
    let path = key_to_path(base_dir, key)?;
    let lock_path = PathBuf::from(format!("{}.lock", path.to_string_lossy()));
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).map_err(io_err)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(io_err)?;
    lock_exclusive(&file)?;
    let result = f(path);
    unlock(&file)?;
    result
}

fn lock_exclusive(file: &File) -> Result<(), StoreError> {
    file.lock_exclusive().map_err(io_err)
}

fn unlock(file: &File) -> Result<(), StoreError> {
    FileExt::unlock(file).map_err(io_err)
}

fn atomic_write(path: &Path, body: &[u8]) -> Result<(), StoreError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(io_err)?;
    }
    let tmp = PathBuf::from(format!("{}.tmp-{}", path.to_string_lossy(), Uuid::new_v4()));
    fs::write(&tmp, body).map_err(io_err)?;
    if path.exists() {
        fs::remove_file(path).map_err(io_err)?;
    }
    fs::rename(&tmp, path).map_err(io_err)?;
    Ok(())
}

fn content_etag(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    format!("{:x}", hasher.finalize())
}

fn io_err(error: std::io::Error) -> StoreError {
    StoreError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn crud_cycle_works() {
        let temp = tempdir().expect("create temp dir");
        let store = LocalFsStore::new(temp.path());
        let key = "runs/test/state.json";

        let body = Bytes::from_static(br#"{"status":"pending"}"#);
        let etag = store.put(key, body.clone()).await.expect("put");
        assert!(!etag.is_empty());

        let (fetched, fetched_etag) = store.get(key).await.expect("get");
        assert_eq!(fetched, body);
        assert_eq!(fetched_etag, etag);
        assert!(store.exists(key).await.expect("exists"));

        let listed = store.list("runs/test").await.expect("list");
        assert_eq!(listed, vec![key.to_string()]);

        store.delete(key).await.expect("delete");
        assert!(!store.exists(key).await.expect("exists"));
    }

    #[tokio::test]
    async fn cas_put_if_match_succeeds_and_fails_on_stale_etag() {
        let temp = tempdir().expect("create temp dir");
        let store = LocalFsStore::new(temp.path());
        let key = "runs/test/state.json";

        let initial_etag = store
            .put(key, Bytes::from_static(br#"{"v":1}"#))
            .await
            .expect("put initial");
        let next_etag = store
            .put_if_match(key, Bytes::from_static(br#"{"v":2}"#), &initial_etag)
            .await
            .expect("cas success");
        assert_ne!(initial_etag, next_etag);

        let err = store
            .put_if_match(key, Bytes::from_static(br#"{"v":3}"#), &initial_etag)
            .await
            .expect_err("stale etag should fail");
        assert_eq!(err, StoreError::PreconditionFailed(key.to_string()));
    }

    #[tokio::test]
    async fn put_if_none_match_is_atomic_create() {
        let temp = tempdir().expect("create temp dir");
        let store = LocalFsStore::new(temp.path());
        let key = "runs/test/state.json";

        store
            .put_if_none_match(key, Bytes::from_static(br#"{"v":1}"#))
            .await
            .expect("first create");
        let err = store
            .put_if_none_match(key, Bytes::from_static(br#"{"v":2}"#))
            .await
            .expect_err("second create should fail");
        assert_eq!(err, StoreError::AlreadyExists(key.to_string()));
    }

    #[tokio::test]
    async fn concurrent_cas_allows_only_one_winner() {
        let temp = tempdir().expect("create temp dir");
        let store = Arc::new(LocalFsStore::new(temp.path()));
        let key = "runs/test/state.json";

        let etag = store
            .put(key, Bytes::from_static(br#"{"v":0}"#))
            .await
            .expect("seed put");

        let mut handles = Vec::new();
        for idx in 0..8 {
            let store = Arc::clone(&store);
            let expected_etag = etag.clone();
            let body = Bytes::from(format!(r#"{{"v":{}}}"#, idx + 1));
            handles.push(tokio::spawn(async move {
                store.put_if_match(key, body, &expected_etag).await
            }));
        }

        let mut success = 0usize;
        let mut precondition_failed = 0usize;
        for handle in handles {
            let result = handle.await.expect("join handle");
            match result {
                Ok(_) => success += 1,
                Err(StoreError::PreconditionFailed(_)) => precondition_failed += 1,
                Err(other) => panic!("unexpected error: {other}"),
            }
        }

        assert_eq!(success, 1);
        assert_eq!(precondition_failed, 7);
    }
}
