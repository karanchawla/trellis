mod cas;
mod data;
mod local;
mod s3;

pub use cas::{CasState, read_state, write_state};
pub use data::{download_data, upload_input, upload_output};
pub use local::LocalFsStore;
pub use s3::S3Store;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StoreError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("precondition failed for key: {0}")]
    PreconditionFailed(String),
    #[error("object already exists: {0}")]
    AlreadyExists(String),
    #[error("I/O error: {0}")]
    Io(String),
    #[error("store error: {0}")]
    Other(String),
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<(Bytes, String), StoreError>;
    async fn put(&self, key: &str, body: Bytes) -> Result<String, StoreError>;
    async fn put_if_match(&self, key: &str, body: Bytes, etag: &str) -> Result<String, StoreError>;
    async fn put_if_none_match(&self, key: &str, body: Bytes) -> Result<String, StoreError>;
    async fn delete(&self, key: &str) -> Result<(), StoreError>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
    async fn exists(&self, key: &str) -> Result<bool, StoreError>;
}
