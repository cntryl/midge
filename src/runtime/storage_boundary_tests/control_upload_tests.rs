//! Root-level authority regression for prepared control uploads.

use cntryl_midge_core::common::resource_budget::ResourceBudget;
use cntryl_midge_core::common::{MidgeError, OperationDeadline};
use cntryl_midge_durability::lease::LeaseValidity;
use cntryl_midge_storage::cloud::{
    CloudBackend, CloudCallback, CloudDeleteCapability, CloudHeadCapability, CloudListCapability,
    CloudReadCapability, CloudStorage, CloudWriteCapability, MockCloudBackend,
};
use cntryl_midge_storage::hybrid::HybridStorage;
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct PendingUpload {
    inner: MockCloudBackend,
    pending: parking_lot::Mutex<Option<(Vec<u8>, CloudCallback)>>,
}

impl CloudReadCapability for PendingUpload {
    cntryl_midge_storage::cloud::unsupported_cloud_backend!(submit_get, submit_get_with_metadata);

    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        self.inner.submit_get_range(key, start, end, callback);
    }
}

impl CloudWriteCapability for PendingUpload {
    fn submit_put(
        &self,
        _key: &str,
        bytes: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        *self.pending.lock() = Some((bytes, callback));
    }
}

impl CloudDeleteCapability for PendingUpload {
    cntryl_midge_storage::cloud::unsupported_cloud_backend!(submit_delete);
}
impl CloudListCapability for PendingUpload {
    cntryl_midge_storage::cloud::unsupported_cloud_backend!(submit_list);
}
impl CloudHeadCapability for PendingUpload {
    fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.inner.submit_head(key, callback);
    }
}
impl CloudBackend for PendingUpload {}

#[cfg(feature = "failpoints")]
#[test]
fn should_release_prepared_upload_when_validity_expires_before_submission() {
    // Arrange
    let _guard = cntryl_midge_core::failpoints::test_failpoint_guard();
    let _scenario = fail::FailScenario::setup();
    let provider = Arc::new(PendingUpload::default());
    let storage = HybridStorage::with_policy(
        Arc::new(CloudStorage::new(provider.clone(), String::new())),
        cntryl_midge_storage::hybrid::policy::StorageBudgetPolicy::default(),
    );
    let validity = Arc::new(LeaseValidity::new());
    validity
        .activate(7, std::time::Instant::now() + Duration::from_mins(1))
        .unwrap();
    let source = Arc::clone(&validity);
    storage
        .configure_write_authority(Arc::new(move || {
            source
                .remaining(7)
                .map(|_| ())
                .map_err(|error| MidgeError::Fenced(error.to_string()))
        }))
        .unwrap();
    fail::cfg_callback("midge::control::after_prepare_before_write", move || {
        validity.expire_for_test();
    })
    .unwrap();
    let budget = ResourceBudget::new(1024 * 1024);

    // Act
    let result = storage.write_control_object(
        "metadata/catalog",
        None,
        &[7; 4096],
        &budget,
        &OperationDeadline::from_budget(Duration::from_millis(200)),
    );

    // Assert
    assert!(matches!(result, Err(MidgeError::Fenced(_))), "{result:?}");
    assert!(provider.pending.lock().is_none());
    assert_eq!(budget.used(), 0);
}
