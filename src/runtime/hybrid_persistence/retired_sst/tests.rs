use super::*;
use crate::storage::cloud::{BlockingCloud, CloudStorage, MockCloudBackend};
use crate::storage::HybridStorage;
use std::sync::Arc;
use std::time::Duration;

struct Fixture {
    backend: Arc<MockCloudBackend>,
    cloud: Arc<CloudStorage>,
    storage: Arc<HybridStorage>,
    original: FileMeta,
    key: String,
}

impl Fixture {
    fn new(bytes: &[u8]) -> Self {
        let backend = Arc::new(MockCloudBackend::new());
        let cloud = Arc::new(CloudStorage::new(backend.clone(), "retired".into()));
        let storage = Arc::new(HybridStorage::with_policy(
            cloud.clone(),
            Default::default(),
        ));
        let original = FileMeta {
            name: crate::cloud_layout::file_name(0, 1, 7),
            cf_id: 0,
            level: 1,
            sst_seq: 7,
            size_bytes: bytes.len() as u64,
            content_crc32c: Some(crc32c::crc32c(bytes)),
            ..FileMeta::default()
        };
        let key = crate::cloud_layout::object_key(&original.name);
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        BlockingCloud::new(&cloud, &deadline)
            .put_with_headers(&key, bytes.to_vec(), Vec::new())
            .unwrap();
        Self {
            backend,
            cloud,
            storage,
            original,
            key,
        }
    }

    fn body(&self) -> Option<Vec<u8>> {
        let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
        BlockingCloud::new(&self.cloud, &deadline)
            .get_optional(&self.key)
            .unwrap()
    }
}

#[test]
fn should_delete_original_bytes_when_every_bounded_read_matches_retired_identity() {
    // Arrange
    let fixture = Fixture::new(&vec![0x41; READ_CHUNK as usize * 2 + 7]);
    fixture.backend.clear_history();

    // Act
    CloudPersistence::new(fixture.storage.clone())
        .delete_retired_sst_object_within(
            &fixture.original,
            &OperationDeadline::from_budget(Duration::from_secs(5)),
        )
        .unwrap();

    // Assert
    assert!(fixture.body().is_none());
    assert_eq!(fixture.backend.get_range_downloads().len(), 3);
}

#[test]
fn should_retain_equal_length_replacement_when_original_checksum_differs() {
    // Arrange
    let fixture = Fixture::new(b"original");
    let deadline = OperationDeadline::from_budget(Duration::from_secs(5));
    BlockingCloud::new(&fixture.cloud, &deadline)
        .put_with_headers(&fixture.key, b"replaced".to_vec(), Vec::new())
        .unwrap();

    // Act
    let result = CloudPersistence::new(fixture.storage.clone())
        .delete_retired_sst_object_within(&fixture.original, &deadline);

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(fixture.body().unwrap(), b"replaced");
}

#[test]
fn should_retain_remote_bytes_when_original_delete_proof_has_no_checksum() {
    // Arrange
    let fixture = Fixture::new(b"original");
    let mut unknown = fixture.original.clone();
    unknown.content_crc32c = None;

    // Act
    let result = CloudPersistence::new(fixture.storage.clone()).delete_retired_sst_object_within(
        &unknown,
        &OperationDeadline::from_budget(Duration::from_secs(5)),
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Corruption(_))));
    assert_eq!(fixture.body().unwrap(), b"original");
}

#[test]
fn should_retain_original_identity_when_final_writer_validation_fails() {
    // Arrange
    let fixture = Fixture::new(b"original");
    fixture
        .storage
        .configure_write_authority(Arc::new(|| {
            Err(MidgeError::Fenced("successor owns the lease".into()))
        }))
        .unwrap();

    // Act
    let result = CloudPersistence::new(fixture.storage.clone()).delete_retired_sst_object_within(
        &fixture.original,
        &OperationDeadline::from_budget(Duration::from_secs(5)),
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Fenced(_))));
    assert_eq!(fixture.body().unwrap(), b"original");
}

#[cfg(feature = "failpoints")]
#[test]
fn should_retain_replacement_when_identity_changes_after_original_checksum_read() {
    let _guard = crate::failpoints::test_failpoint_guard();
    let _scenario = fail::FailScenario::setup();
    // Arrange
    let fixture = Fixture::new(b"original");
    let cloud = Arc::clone(&fixture.cloud);
    let key = fixture.key.clone();
    fail::cfg_callback("midge::cloud::before_compaction_orphan_delete", move || {
        BlockingCloud::new(
            &cloud,
            &OperationDeadline::from_budget(Duration::from_secs(5)),
        )
        .put_with_headers(&key, b"replaced".to_vec(), Vec::new())
        .unwrap();
    })
    .unwrap();

    // Act
    let result = CloudPersistence::new(fixture.storage.clone()).delete_retired_sst_object_within(
        &fixture.original,
        &OperationDeadline::from_budget(Duration::from_secs(5)),
    );

    // Assert: the final condition rejects the newer identity, although the
    // previously streamed bytes matched the original proof completely.
    assert!(result.is_err());
    assert_eq!(fixture.body().unwrap(), b"replaced");
}
