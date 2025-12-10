//! Real cloud provider integration tests
//!
//! These tests require real cloud credentials and are marked with #[ignore].
//! Run them explicitly with:
//!
//! ```bash
//! cargo test --test cloud_real_providers --features cloud-aws,cloud-azure,cloud-gcp -- --ignored
//! ```
//!
//! Prerequisites:
//! - AWS: Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, TEST_S3_BUCKET
//! - Azure: Set AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, TEST_AZURE_CONTAINER
//! - GCP: Set GOOGLE_APPLICATION_CREDENTIALS, TEST_GCS_BUCKET

#[allow(unused_imports)]
use bytes::Bytes;
#[allow(unused_imports)]
use std::env;

// ============================================================================
// AWS S3 REAL PROVIDER TESTS
// ============================================================================

#[cfg(feature = "cloud-aws")]
mod aws_tests {
    use super::*;
    use cntryl_midge::cloud::aws::AwsS3Backend;
    use cntryl_midge::cloud::StorageBackend;

    #[test]
    #[ignore = "requires real AWS credentials"]
    fn should_upload_to_s3_given_credentials_configured_when_putting() {
        // Arrange
        let bucket = env::var("TEST_S3_BUCKET").expect("TEST_S3_BUCKET must be set");
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let backend = AwsS3Backend::new(&bucket, &region).expect("failed to create S3 backend");
        let test_key = format!("test-key-{}", uuid::Uuid::new_v4());
        let test_data = Bytes::from("test data from midge");

        // Act
        let result = backend.put_blob(&test_key, test_data.clone());

        // Assert
        assert!(result.is_ok(), "Upload should succeed");

        // Cleanup
        let _ = backend.delete_blob(&test_key);
    }

    #[test]
    #[ignore = "requires real AWS credentials"]
    fn should_download_from_s3_given_blob_exists_when_getting() {
        // Arrange
        let bucket = env::var("TEST_S3_BUCKET").expect("TEST_S3_BUCKET must be set");
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let backend = AwsS3Backend::new(&bucket, &region).expect("failed to create S3 backend");
        let test_key = format!("test-key-{}", uuid::Uuid::new_v4());
        let test_data = Bytes::from("download test data");

        backend
            .put_blob(&test_key, test_data.clone())
            .expect("upload failed");

        // Act
        let downloaded = backend.get_blob(&test_key);

        // Assert
        assert!(downloaded.is_ok(), "Download should succeed");
        assert_eq!(downloaded.unwrap(), test_data);

        // Cleanup
        let _ = backend.delete_blob(&test_key);
    }

    #[test]
    #[ignore = "requires real AWS credentials"]
    fn should_list_blobs_from_s3_given_prefix_when_listing() {
        // Arrange
        let bucket = env::var("TEST_S3_BUCKET").expect("TEST_S3_BUCKET must be set");
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let backend = AwsS3Backend::new(&bucket, &region).expect("failed to create S3 backend");
        let prefix = format!("test-prefix-{}/", uuid::Uuid::new_v4());
        let key1 = format!("{}file1.dat", prefix);
        let key2 = format!("{}file2.dat", prefix);

        backend
            .put_blob(&key1, Bytes::from("data1"))
            .expect("upload 1 failed");
        backend
            .put_blob(&key2, Bytes::from("data2"))
            .expect("upload 2 failed");

        // Act
        let list_result = backend.list_blobs(&prefix);

        // Assert
        assert!(list_result.is_ok(), "List should succeed");
        let keys = list_result.unwrap();
        assert!(keys.contains(&key1), "Should contain key1");
        assert!(keys.contains(&key2), "Should contain key2");

        // Cleanup
        let _ = backend.delete_blob(&key1);
        let _ = backend.delete_blob(&key2);
    }

    #[test]
    #[ignore = "requires real AWS credentials"]
    fn should_perform_ranged_read_from_s3_given_offset_when_reading() {
        // Arrange
        let bucket = env::var("TEST_S3_BUCKET").expect("TEST_S3_BUCKET must be set");
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let backend = AwsS3Backend::new(&bucket, &region).expect("failed to create S3 backend");
        let test_key = format!("test-range-{}", uuid::Uuid::new_v4());
        let test_data = Bytes::from("0123456789abcdefghij");

        backend
            .put_blob(&test_key, test_data.clone())
            .expect("upload failed");

        // Act - Read bytes 5-10
        let range_result = backend.get_blob_range(&test_key, 5, Some(10));

        // Assert
        assert!(range_result.is_ok(), "Range read should succeed");
        assert_eq!(range_result.unwrap(), Bytes::from("56789"));

        // Cleanup
        let _ = backend.delete_blob(&test_key);
    }
}

// ============================================================================
// AZURE BLOB STORAGE REAL PROVIDER TESTS
// ============================================================================

#[cfg(feature = "cloud-azure")]
mod azure_tests {
    use super::*;
    use cntryl_midge::cloud::azure::AzureBlobBackend;
    use cntryl_midge::cloud::StorageBackend;

    #[test]
    #[ignore = "requires real Azure credentials"]
    fn should_upload_to_azure_given_credentials_configured_when_putting() {
        // Arrange
        let account = env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT must be set");
        let container = env::var("TEST_AZURE_CONTAINER").expect("TEST_AZURE_CONTAINER must be set");

        let backend =
            AzureBlobBackend::new(&account, &container).expect("failed to create backend");
        let test_key = format!("test-key-{}", uuid::Uuid::new_v4());
        let test_data = Bytes::from("test data from midge");

        // Act
        let result = backend.put_blob(&test_key, test_data.clone());

        // Assert
        assert!(result.is_ok(), "Upload should succeed");

        // Cleanup
        let _ = backend.delete_blob(&test_key);
    }

    #[test]
    #[ignore = "requires real Azure credentials"]
    fn should_download_from_azure_given_blob_exists_when_getting() {
        // Arrange
        let account = env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT must be set");
        let container = env::var("TEST_AZURE_CONTAINER").expect("TEST_AZURE_CONTAINER must be set");

        let backend =
            AzureBlobBackend::new(&account, &container).expect("failed to create backend");
        let test_key = format!("test-key-{}", uuid::Uuid::new_v4());
        let test_data = Bytes::from("download test data");

        backend
            .put_blob(&test_key, test_data.clone())
            .expect("upload failed");

        // Act
        let downloaded = backend.get_blob(&test_key);

        // Assert
        assert!(downloaded.is_ok(), "Download should succeed");
        assert_eq!(downloaded.unwrap(), test_data);

        // Cleanup
        let _ = backend.delete_blob(&test_key);
    }
}

// ============================================================================
// GCP CLOUD STORAGE REAL PROVIDER TESTS
// ============================================================================

#[cfg(feature = "cloud-gcp")]
mod gcp_tests {
    use super::*;
    use cntryl_midge::cloud::gcp::GcpStorageBackend;
    use cntryl_midge::cloud::StorageBackend;

    #[test]
    #[ignore = "requires real GCP credentials"]
    fn should_upload_to_gcs_given_credentials_configured_when_putting() {
        // Arrange
        let bucket = env::var("TEST_GCS_BUCKET").expect("TEST_GCS_BUCKET must be set");

        let backend = GcpStorageBackend::new(&bucket).expect("failed to create GCS backend");
        let test_key = format!("test-key-{}", uuid::Uuid::new_v4());
        let test_data = Bytes::from("test data from midge");

        // Act
        let result = backend.put_blob(&test_key, test_data.clone());

        // Assert
        assert!(result.is_ok(), "Upload should succeed");

        // Cleanup
        let _ = backend.delete_blob(&test_key);
    }

    #[test]
    #[ignore = "requires real GCP credentials"]
    fn should_download_from_gcs_given_blob_exists_when_getting() {
        // Arrange
        let bucket = env::var("TEST_GCS_BUCKET").expect("TEST_GCS_BUCKET must be set");

        let backend = GcpStorageBackend::new(&bucket).expect("failed to create GCS backend");
        let test_key = format!("test-key-{}", uuid::Uuid::new_v4());
        let test_data = Bytes::from("download test data");

        backend
            .put_blob(&test_key, test_data.clone())
            .expect("upload failed");

        // Act
        let downloaded = backend.get_blob(&test_key);

        // Assert
        assert!(downloaded.is_ok(), "Download should succeed");
        assert_eq!(downloaded.unwrap(), test_data);

        // Cleanup
        let _ = backend.delete_blob(&test_key);
    }
}
