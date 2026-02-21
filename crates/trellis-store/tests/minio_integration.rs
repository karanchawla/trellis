#![cfg(feature = "integration")]

use bytes::Bytes;
use trellis_store::{ObjectStore, S3Store};

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:9000";
const DEFAULT_REGION: &str = "us-east-1";
const DEFAULT_ACCESS_KEY: &str = "minioadmin";
const DEFAULT_SECRET_KEY: &str = "minioadmin";
const DEFAULT_BUCKET: &str = "trellis-it";

#[tokio::test]
async fn minio_put_get_and_cas_round_trip() {
    let endpoint =
        std::env::var("TRELLIS_MINIO_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string());
    let region =
        std::env::var("TRELLIS_MINIO_REGION").unwrap_or_else(|_| DEFAULT_REGION.to_string());
    let access_key = std::env::var("TRELLIS_MINIO_ACCESS_KEY")
        .unwrap_or_else(|_| DEFAULT_ACCESS_KEY.to_string());
    let secret_key = std::env::var("TRELLIS_MINIO_SECRET_KEY")
        .unwrap_or_else(|_| DEFAULT_SECRET_KEY.to_string());
    let bucket =
        std::env::var("TRELLIS_MINIO_BUCKET").unwrap_or_else(|_| DEFAULT_BUCKET.to_string());

    let raw_client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new(region.clone()))
            .endpoint_url(endpoint.clone())
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                access_key.clone(),
                secret_key.clone(),
                None,
                None,
                "trellis-it",
            ))
            .force_path_style(true)
            .build(),
    );
    let _ = raw_client.create_bucket().bucket(&bucket).send().await;

    let store = S3Store::from_endpoint(&endpoint, &region, &access_key, &secret_key, &bucket)
        .await
        .expect("create s3 store");
    let key = "runs/minio-it/state.json";

    let etag = store
        .put(key, Bytes::from_static(br#"{"status":"pending"}"#))
        .await
        .expect("put state");
    let (fetched, fetched_etag) = store.get(key).await.expect("get state");
    assert_eq!(fetched, Bytes::from_static(br#"{"status":"pending"}"#));
    assert_eq!(fetched_etag, etag);

    let fresh = store
        .put_if_match(key, Bytes::from_static(br#"{"status":"running"}"#), &etag)
        .await
        .expect("cas write");
    let stale = store
        .put_if_match(key, Bytes::from_static(br#"{"status":"failed"}"#), &etag)
        .await;
    assert!(fresh != etag);
    assert!(stale.is_err());
}
