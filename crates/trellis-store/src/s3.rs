use crate::{ObjectStore, StoreError};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct S3Store {
    client: Client,
    bucket: String,
}

impl S3Store {
    pub fn new(client: Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
        }
    }

    pub async fn from_endpoint(
        endpoint: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        bucket: impl Into<String>,
    ) -> Result<Self, StoreError> {
        let credentials = aws_sdk_s3::config::Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "trellis-static",
        );
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new(region.to_string()))
            .endpoint_url(endpoint.to_string())
            .credentials_provider(credentials)
            .force_path_style(true)
            .build();

        Ok(Self::new(Client::from_conf(config), bucket))
    }
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn get(&self, key: &str) -> Result<(Bytes, String), StoreError> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| map_get_error(key, &e))?;

        let etag = response
            .e_tag()
            .map(|value| value.trim_matches('"').to_string())
            .unwrap_or_default();

        let collected = response
            .body
            .collect()
            .await
            .map_err(|e| StoreError::Other(format!("failed to read body stream: {e}")))?;
        Ok((collected.into_bytes(), etag))
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<String, StoreError> {
        let response = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .map_err(|e| map_put_error(key, &e, PutOperation::Put))?;

        let etag = response
            .e_tag()
            .map(|value| value.trim_matches('"').to_string())
            .unwrap_or_else(|| content_etag(&body));
        Ok(etag)
    }

    async fn put_if_match(&self, key: &str, body: Bytes, etag: &str) -> Result<String, StoreError> {
        let response = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .if_match(etag)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .map_err(|e| map_put_error(key, &e, PutOperation::IfMatch))?;

        let etag = response
            .e_tag()
            .map(|value| value.trim_matches('"').to_string())
            .unwrap_or_else(|| content_etag(&body));
        Ok(etag)
    }

    async fn put_if_none_match(&self, key: &str, body: Bytes) -> Result<String, StoreError> {
        let response = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .map_err(|e| map_put_error(key, &e, PutOperation::IfNoneMatch))?;

        let etag = response
            .e_tag()
            .map(|value| value.trim_matches('"').to_string())
            .unwrap_or_else(|| content_etag(&body));
        Ok(etag)
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| StoreError::Other(format!("delete object failed for {key}: {e}")))?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let mut continuation: Option<String> = None;
        let mut keys = Vec::new();

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = continuation.as_ref() {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|e| StoreError::Other(format!("list failed: {e}")))?;

            for object in response.contents() {
                if let Some(key) = object.key() {
                    keys.push(key.to_string());
                }
            }

            if response.is_truncated().unwrap_or(false) {
                continuation = response
                    .next_continuation_token()
                    .map(std::string::ToString::to_string);
            } else {
                break;
            }
        }

        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool, StoreError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(error) => map_head_exists_error(key, &error),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum PutOperation {
    Put,
    IfMatch,
    IfNoneMatch,
}

fn map_get_error(key: &str, error: &SdkError<GetObjectError>) -> StoreError {
    if is_not_found_code(
        error
            .as_service_error()
            .and_then(ProvideErrorMetadata::code),
    ) {
        return StoreError::NotFound(key.to_string());
    }

    StoreError::Other(format!("get object failed for {key}: {error}"))
}

fn map_put_error(
    key: &str,
    error: &SdkError<PutObjectError>,
    operation: PutOperation,
) -> StoreError {
    let code = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code);

    if is_precondition_failed_code(code) {
        match operation {
            PutOperation::IfNoneMatch => {
                return StoreError::AlreadyExists(key.to_string());
            }
            PutOperation::Put | PutOperation::IfMatch => {
                return StoreError::PreconditionFailed(key.to_string());
            }
        }
    }

    if is_not_found_code(code) {
        return StoreError::NotFound(key.to_string());
    }

    StoreError::Other(format!("put object failed for {key}: {error}"))
}

fn map_head_exists_error(key: &str, error: &SdkError<HeadObjectError>) -> Result<bool, StoreError> {
    let code = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code);
    if is_not_found_code(code) {
        Ok(false)
    } else {
        Err(StoreError::Other(format!(
            "head object failed for {key}: {error}"
        )))
    }
}

fn is_not_found_code(code: Option<&str>) -> bool {
    matches!(code, Some("NotFound" | "NoSuchKey" | "NoSuchBucket"))
}

fn is_precondition_failed_code(code: Option<&str>) -> bool {
    matches!(
        code,
        Some("PreconditionFailed" | "ConditionalRequestConflict")
    )
}

fn content_etag(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_classification_matches_expected_values() {
        assert!(is_not_found_code(Some("NoSuchKey")));
        assert!(is_not_found_code(Some("NotFound")));
        assert!(!is_not_found_code(Some("AccessDenied")));

        assert!(is_precondition_failed_code(Some("PreconditionFailed")));
        assert!(is_precondition_failed_code(Some(
            "ConditionalRequestConflict"
        )));
        assert!(!is_precondition_failed_code(Some("NoSuchKey")));
    }
}
