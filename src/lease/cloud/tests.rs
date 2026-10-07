use super::*;
use crate::common::MidgeError;
use crate::lease::traits::{CloudMetadataGeneration, CloudMetadataHead, CloudMetadataObject};
use std::path::PathBuf;
use std::sync::Arc;

#[path = "tests/concurrent_renewal.rs"]
mod concurrent_renewal;

#[path = "tests/renewal_read_budget.rs"]
mod renewal_read_budget;

#[cfg(feature = "cloud-all")]
#[path = "tests/startup_deadline.rs"]
mod startup_deadline;

static TEMP_PATH_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn test_config() -> CloudLeaseConfig {
    CloudLeaseConfig {
        bucket: "test-bucket".to_string(),
        prefix: "test/prefix".to_string(),
    }
}

/// Test double that intercepts conditional lease or marker-activation PUTs
/// and returns a scripted [`crate::storage::cloud::CloudError`]
/// instead of delegating to the wrapped backend. Everything else
/// delegates to an inner `MockCloudBackend`, so HEAD/GET/DELETE/LIST
/// still behave normally.
enum ScriptedPutTarget {
    Lease,
    SentinelActivation,
}

struct ScriptedConditionalPutBackend {
    inner: crate::storage::cloud::MockCloudBackend,
    scripted_error: crate::storage::cloud::CloudError,
    apply_before_error: bool,
    scripted_puts_remaining: std::sync::atomic::AtomicUsize,
    scripted_target: ScriptedPutTarget,
}

impl crate::storage::cloud::CloudBackend for ScriptedConditionalPutBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        let is_conditional = match self.scripted_target {
            ScriptedPutTarget::Lease => {
                key.ends_with(LEASE_OBJECT_KEY)
                    && headers.iter().any(|(name, _)| {
                        name.eq_ignore_ascii_case("if-match")
                            || name.eq_ignore_ascii_case("if-none-match")
                    })
            }
            ScriptedPutTarget::SentinelActivation => {
                key.ends_with(AUTHORITY_SENTINEL_KEY)
                    && headers
                        .iter()
                        .any(|(name, _)| name.eq_ignore_ascii_case("if-match"))
            }
        };
        let scripted = is_conditional
            && self
                .scripted_puts_remaining
                .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    (remaining > 0).then_some(remaining.saturating_sub(1))
                })
                .is_ok();
        if scripted {
            if self.apply_before_error {
                let (inner_callback, inner_result) = std::sync::mpsc::channel();
                crate::storage::cloud::CloudBackend::submit_put(
                    &self.inner,
                    key,
                    data,
                    headers,
                    inner_callback,
                );
                let applied = inner_result
                    .recv_timeout(Duration::from_secs(1))
                    .expect("scripted lease write should complete");
                assert!(
                    matches!(
                        applied,
                        crate::storage::cloud::CloudEvent::Put {
                            result: crate::storage::cloud::CloudOutcome::Ok(()),
                            ..
                        }
                    ),
                    "scripted backend must apply the conditional write before losing its response"
                );
            }
            let _ = callback.send(crate::storage::cloud::CloudEvent::Put {
                key: key.to_string(),
                result: Err(self.scripted_error.clone()),
            });
            return;
        }
        crate::storage::cloud::CloudBackend::submit_put(&self.inner, key, data, headers, callback);
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_get(&self.inner, key, callback);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_get_with_metadata(&self.inner, key, callback);
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_get_range(
            &self.inner,
            key,
            start,
            end,
            callback,
        );
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_delete(&self.inner, key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_list(&self.inner, prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_head(&self.inner, key, callback);
    }
}

fn lease_with_scripted_conditional_put_error(
    scripted_error: crate::storage::cloud::CloudError,
) -> Arc<CloudStorageLease> {
    let backend = Arc::new(ScriptedConditionalPutBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        scripted_error,
        apply_before_error: false,
        scripted_puts_remaining: std::sync::atomic::AtomicUsize::new(usize::MAX),
        scripted_target: ScriptedPutTarget::Lease,
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend,
        "midge".to_string(),
    ));
    Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ))
}

fn lease_with_applied_conditional_put_error(
    scripted_error: crate::storage::cloud::CloudError,
) -> Arc<CloudStorageLease> {
    let backend = Arc::new(ScriptedConditionalPutBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        scripted_error,
        apply_before_error: true,
        scripted_puts_remaining: std::sync::atomic::AtomicUsize::new(usize::MAX),
        scripted_target: ScriptedPutTarget::Lease,
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend,
        "midge".to_string(),
    ));
    Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ))
}

fn lease_with_scripted_marker_activation(
    apply_before_error: bool,
) -> (
    Arc<crate::storage::cloud::CloudStorage>,
    Arc<CloudStorageLease>,
) {
    let backend = Arc::new(ScriptedConditionalPutBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        scripted_error: crate::storage::cloud::CloudError::ServerError(
            "injected marker activation response loss".to_string(),
        ),
        apply_before_error,
        scripted_puts_remaining: std::sync::atomic::AtomicUsize::new(1),
        scripted_target: ScriptedPutTarget::SentinelActivation,
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend,
        "midge".to_string(),
    ));
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    (cloud, lease)
}

/// Test double whose HEAD responses always report no etag and no
/// generation, regardless of what the wrapped backend actually stored —
/// simulating a provider response that omits the fields a conditional
/// write needs.
struct NoCasTokenBackend {
    inner: crate::storage::cloud::MockCloudBackend,
}

struct BlockingRenewalBackend {
    inner: crate::storage::cloud::MockCloudBackend,
    conditional_puts: std::sync::atomic::AtomicUsize,
    renewal_seen: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    allow_renewal: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

type PendingPut = (String, Vec<u8>, Vec<(String, String)>);

struct LateCommitRenewalBackend {
    inner: crate::storage::cloud::MockCloudBackend,
    conditional_puts: std::sync::atomic::AtomicUsize,
    pending: std::sync::Mutex<Option<PendingPut>>,
    old_read_seen: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    late_commit_complete: std::sync::atomic::AtomicBool,
}

impl LateCommitRenewalBackend {
    fn commit_pending(&self) {
        let (key, data, headers) = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("late renewal PUT is pending");
        let (callback, result) = std::sync::mpsc::channel();
        crate::storage::cloud::CloudBackend::submit_put(&self.inner, &key, data, headers, callback);
        let result = result.recv_timeout(Duration::from_secs(5));
        self.late_commit_complete.store(true, Ordering::Release);
        assert!(matches!(
            result,
            Ok(crate::storage::cloud::CloudEvent::Put {
                result: crate::storage::cloud::CloudOutcome::Ok(()),
                ..
            })
        ));
    }
}

impl crate::storage::cloud::CloudBackend for LateCommitRenewalBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        let conditional = key.ends_with(LEASE_OBJECT_KEY)
            && headers.iter().any(|(name, _)| {
                name.eq_ignore_ascii_case("if-match") || name.eq_ignore_ascii_case("if-none-match")
            });
        let conditional_ordinal = conditional.then(|| {
            self.conditional_puts
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
        });
        if conditional_ordinal == Some(1) {
            *self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some((key.to_string(), data, headers));
            let _ = callback.send(crate::storage::cloud::CloudEvent::Put {
                key: key.to_string(),
                result: Err(crate::storage::cloud::CloudError::Timeout(
                    "scripted renewal timeout before late commit".to_string(),
                )),
            });
            return;
        }
        if conditional_ordinal.is_some_and(|ordinal| ordinal > 1) {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !self.late_commit_complete.load(Ordering::Acquire) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "late renewal commit did not finish before cleanup CAS"
                );
                std::thread::yield_now();
            }
        }
        crate::storage::cloud::CloudBackend::submit_put(&self.inner, key, data, headers, callback);
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        if self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
        {
            if let Some(signal) = self
                .old_read_seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                signal.send(()).expect("signal old reconciliation read");
            }
        }
        crate::storage::cloud::CloudBackend::submit_get(&self.inner, key, callback);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_get_with_metadata(&self.inner, key, callback);
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_get_range(
            &self.inner,
            key,
            start,
            end,
            callback,
        );
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_delete(&self.inner, key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_list(&self.inner, prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_head(&self.inner, key, callback);
    }
}

impl crate::storage::cloud::CloudBackend for BlockingRenewalBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        let conditional = key.ends_with(LEASE_OBJECT_KEY)
            && headers.iter().any(|(name, _)| {
                name.eq_ignore_ascii_case("if-match") || name.eq_ignore_ascii_case("if-none-match")
            });
        if conditional
            && self
                .conditional_puts
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                == 1
        {
            if let Some(signal) = self
                .renewal_seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                signal.send(()).expect("signal blocked renewal");
            }
            self.allow_renewal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .recv()
                .expect("release blocked renewal");
        }
        crate::storage::cloud::CloudBackend::submit_put(&self.inner, key, data, headers, callback);
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_get(&self.inner, key, callback);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_get_with_metadata(&self.inner, key, callback);
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_get_range(
            &self.inner,
            key,
            start,
            end,
            callback,
        );
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_delete(&self.inner, key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_list(&self.inner, prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_head(&self.inner, key, callback);
    }
}

impl crate::storage::cloud::CloudBackend for NoCasTokenBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_put(&self.inner, key, data, headers, callback);
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_get(&self.inner, key, callback);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        crate::storage::cloud::CloudBackend::submit_get_with_metadata(&self.inner, key, tx);
        let event = match rx.recv() {
            Ok(crate::storage::cloud::CloudEvent::GetWithMetadata {
                key,
                result: Ok((bytes, metadata)),
            }) => crate::storage::cloud::CloudEvent::GetWithMetadata {
                key,
                result: Ok((
                    bytes,
                    crate::storage::cloud::ObjectMetadata::new(metadata.size, String::new()),
                )),
            },
            Ok(other) => other,
            Err(error) => crate::storage::cloud::CloudEvent::GetWithMetadata {
                key: key.to_string(),
                result: Err(crate::storage::cloud::CloudError::Transport(
                    error.to_string(),
                )),
            },
        };
        let _ = callback.send(event);
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_get_range(
            &self.inner,
            key,
            start,
            end,
            callback,
        );
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_delete(&self.inner, key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_list(&self.inner, prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        crate::storage::cloud::CloudBackend::submit_head(&self.inner, key, tx);
        let event = match rx.recv() {
            Ok(crate::storage::cloud::CloudEvent::Head {
                key,
                result: Ok(metadata),
            }) => crate::storage::cloud::CloudEvent::Head {
                key,
                result: Ok(crate::storage::cloud::ObjectMetadata::new(
                    metadata.size,
                    String::new(),
                )),
            },
            Ok(other) => other,
            Err(_) => return,
        };
        let _ = callback.send(event);
    }
}

fn temp_cache_path() -> PathBuf {
    let counter = TEMP_PATH_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "midge_cloud_lease_test_{}_{}_{}",
        std::process::id(),
        counter,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

// === Provider conditional-write classification: the whole point of
// typing CloudError before stringification is that only a genuine
// precondition race becomes LeaseHeld. Every other failure mode must
// surface as LeaseUnavailable (via LeaseError::IoError), never as
// confirmed contention. ===

#[test]
fn should_not_treat_unauthorized_conditional_write_as_confirmed_contention() {
    // Arrange
    let lease = lease_with_scripted_conditional_put_error(
        crate::storage::cloud::CloudError::Unauthorized("403 Forbidden".to_string()),
    );

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    let Err(error) = result else {
        panic!("expected an unauthorized conditional write to fail");
    };
    assert!(
        matches!(error, LeaseError::IoError(_)),
        "an auth failure is not proof another instance holds the lease, got: {error:?}"
    );
    assert!(matches!(
        MidgeError::from(error),
        MidgeError::LeaseUnavailable(_)
    ));
}

#[test]
fn should_not_treat_server_error_conditional_write_as_confirmed_contention() {
    // Arrange
    let lease = lease_with_scripted_conditional_put_error(
        crate::storage::cloud::CloudError::ServerError("500 Internal Server Error".to_string()),
    );

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    let Err(error) = result else {
        panic!("expected a server-error conditional write to fail");
    };
    assert!(
        matches!(error, LeaseError::IoError(_)),
        "a provider outage is not proof another instance holds the lease, got: {error:?}"
    );
}

#[test]
fn should_not_treat_transport_failure_conditional_write_as_confirmed_contention() {
    // Arrange
    let lease = lease_with_scripted_conditional_put_error(
        crate::storage::cloud::CloudError::Transport("connection reset".to_string()),
    );

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    let Err(error) = result else {
        panic!("expected a transport-failure conditional write to fail");
    };
    assert!(
        matches!(error, LeaseError::IoError(_)),
        "a network failure is not proof another instance holds the lease, got: {error:?}"
    );
}

#[test]
fn should_treat_precondition_failed_conditional_write_as_confirmed_contention() {
    // Arrange
    let lease = lease_with_scripted_conditional_put_error(
        crate::storage::cloud::CloudError::PreconditionFailed("412".to_string()),
    );

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    let Err(error) = result else {
        panic!("expected a lost precondition race to fail acquisition");
    };
    assert!(
        matches!(error, LeaseError::AcquisitionFailed(_)),
        "a genuine conditional-write race is confirmed contention, got: {error:?}"
    );
    assert!(matches!(MidgeError::from(error), MidgeError::LeaseHeld(_)));
}

#[test]
fn should_confirm_own_lease_write_by_readback_given_success_response_is_lost() {
    // Arrange
    let lease = lease_with_applied_conditional_put_error(
        crate::storage::cloud::CloudError::ServerError("503 response lost after apply".to_string()),
    );

    // Act
    let _guard = Arc::clone(&lease)
        .try_acquire()
        .expect("readback should confirm the caller's applied lease document");

    // Assert
    assert!(lease.epoch() > 0);
    assert!(lease.acquired.load(Ordering::Acquire));
}

#[test]
fn should_map_lease_unavailable_error_types_distinctly_through_midge_error() {
    // Arrange
    let lease = lease_with_scripted_conditional_put_error(
        crate::storage::cloud::CloudError::ServerError("500".to_string()),
    );

    // Act
    let result = Arc::clone(&lease).try_acquire();
    let Err(error) = result else {
        panic!("expected a server-error conditional write to fail");
    };
    let midge_error = MidgeError::from(error);

    // Assert: exact public variant, not conflated with LeaseHeld.
    assert!(matches!(midge_error, MidgeError::LeaseUnavailable(_)));
}

#[test]
fn should_not_treat_missing_cas_token_as_confirmed_contention() {
    // Arrange: seed an already-expired lease document so acquisition
    // proceeds to a takeover attempt instead of short-circuiting on
    // "another instance holds it".
    let backend = Arc::new(NoCasTokenBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend,
        "midge".to_string(),
    ));
    provider_create_authority_sentinel(&cloud, Duration::from_secs(1))
        .expect("seed initialized authority");
    let now = chrono::Utc::now();
    let expired = LeaseDocument {
        version: LeaseDocumentVersion::V2,
        epoch: Some(1),
        holder_id: "old-holder@host".to_string(),
        owner_token: Some("old-token".to_string()),
        acquired_at: (now - chrono::Duration::seconds(120)).to_rfc3339(),
        expires_at: (now - chrono::Duration::seconds(60)).to_rfc3339(),
        committed_metadata: None,
    };
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(
        LEASE_OBJECT_KEY,
        format_lease_document(&expired).into_bytes(),
        vec![],
        tx,
    );
    rx.recv().expect("seed expired lease document");

    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ));

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    let Err(error) = result else {
        panic!("expected takeover to fail without a conditional-update token");
    };
    assert!(
        matches!(error, LeaseError::IoError(_)),
        "a missing CAS token is not proof another instance holds the lease, got: {error:?}"
    );
    assert!(matches!(
        MidgeError::from(error),
        MidgeError::LeaseUnavailable(_)
    ));
}

#[test]
fn should_acquire_lease_when_no_existing_lease() {
    // Arrange
    let cache_path = temp_cache_path();
    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path.clone()));

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    assert!(result.is_ok());
    assert!(lease_file_exists(&cache_path));
}

#[test]
fn should_reject_double_acquire_when_already_held() {
    // Arrange
    let cache_path = temp_cache_path();
    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

    // Act
    let _guard = Arc::clone(&lease).try_acquire().unwrap();
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_acquire_when_another_holder_active() {
    // Arrange
    let cache_path = temp_cache_path();

    // Simulate another holder's lease
    let now = chrono::Utc::now();
    let other_doc = LeaseDocument {
        version: LeaseDocumentVersion::Legacy,
        epoch: None,
        holder_id: "other_process@other_host".to_string(),
        owner_token: None,
        acquired_at: now.to_rfc3339(),
        expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        committed_metadata: None,
    };
    let lease_path = cache_path.join(LEASE_OBJECT_KEY);
    std::fs::write(&lease_path, format_lease_document(&other_doc)).unwrap();

    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    assert!(result.is_err());
    if let Err(LeaseError::AcquisitionFailed(msg)) = result {
        assert!(msg.contains("another"));
    }
}

#[test]
fn should_acquire_lease_when_existing_lease_expired() {
    // Arrange
    let cache_path = temp_cache_path();

    // Simulate an expired lease from another holder
    let past = chrono::Utc::now() - chrono::Duration::seconds(120);
    let expired_doc = LeaseDocument {
        version: LeaseDocumentVersion::Legacy,
        epoch: None,
        holder_id: "old_process@old_host".to_string(),
        owner_token: None,
        acquired_at: (past - chrono::Duration::seconds(60)).to_rfc3339(),
        expires_at: past.to_rfc3339(),
        committed_metadata: None,
    };
    let lease_path = cache_path.join(LEASE_OBJECT_KEY);
    std::fs::write(&lease_path, format_lease_document(&expired_doc)).unwrap();

    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_refuse_simulated_takeover_given_malformed_expiry() {
    // Arrange
    let cache_path = temp_cache_path();
    let document = "epoch: 41\nholder_id: ambiguous-holder@host\nowner_token: ambiguous-token\nacquired_at: 2026-07-31T12:00:00Z\nexpires_at: not-a-timestamp\n";
    std::fs::write(cache_path.join(LEASE_OBJECT_KEY), document).unwrap();
    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path.clone()));

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    assert!(matches!(result, Err(LeaseError::Indeterminate(_))));
    assert_eq!(
        std::fs::read_to_string(cache_path.join(LEASE_OBJECT_KEY)).unwrap(),
        document
    );
    assert_eq!(lease.epoch(), 0);
}

#[test]
fn should_renew_lease_when_held() {
    // Arrange
    let cache_path = temp_cache_path();
    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path.clone()));
    let _guard = Arc::clone(&lease).try_acquire().unwrap();
    let lease_path = cache_path.join(LEASE_OBJECT_KEY);
    let before = parse_lease_document(&std::fs::read_to_string(&lease_path).unwrap()).unwrap();

    // Act
    let result = lease.renew();

    // Assert
    assert!(result.is_ok());
    let after = parse_lease_document(&std::fs::read_to_string(&lease_path).unwrap()).unwrap();
    assert_eq!(
        before.epoch, after.epoch,
        "renewal must not change the fencing epoch"
    );
    assert!(
        after.expires_at > before.expires_at,
        "renewal must extend the lease expiry: before={}, after={}",
        before.expires_at,
        after.expires_at
    );
}

#[test]
fn should_repair_malformed_expiry_when_current_simulated_owner_renews() {
    // Arrange
    let cache_path = temp_cache_path();
    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let mut owned = lease
        .read_lease_file()
        .expect("read lease")
        .expect("lease exists");
    owned.expires_at = "not-a-timestamp".to_string();
    lease
        .write_lease_file(&owned)
        .expect("write malformed expiry");

    // Act
    let result = lease.renew();

    // Assert
    assert!(result.is_ok());
    let repaired = lease
        .read_lease_file()
        .expect("read repaired lease")
        .expect("repaired lease exists");
    assert!(chrono::DateTime::parse_from_rfc3339(&repaired.expires_at).is_ok());
}

#[test]
fn should_fail_renew_when_not_acquired() {
    // Arrange
    let cache_path = temp_cache_path();
    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

    // Act
    let result = lease.renew();

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_not_extend_persisted_lease_given_watchdog_fenced_before_renewal() {
    // Arrange
    let lease = Arc::new(CloudStorageLease::new(test_config(), temp_cache_path()));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let epoch = lease.epoch();
    let before = lease
        .read_lease_file()
        .expect("read lease document")
        .expect("lease document exists");
    lease.validity.fence(epoch);

    // Act
    let result = lease.renew();
    let after = lease
        .read_lease_file()
        .expect("read lease document")
        .expect("lease document remains");

    // Assert
    assert!(matches!(result, Err(LeaseError::RenewalFailed(_))));
    assert_eq!(after, before);
    assert!(!lease.acquired.load(Ordering::Acquire));
    assert_eq!(lease.acquired_epoch.load(Ordering::Acquire), 0);
    assert!(lease.validity.remaining(epoch).is_err());
}

#[test]
fn should_not_resurrect_authority_when_provider_put_lands_after_watchdog_fence() {
    // Arrange
    let (renewal_seen_tx, renewal_seen_rx) = std::sync::mpsc::channel();
    let (allow_renewal_tx, allow_renewal_rx) = std::sync::mpsc::channel();
    let backend = Arc::new(BlockingRenewalBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        conditional_puts: std::sync::atomic::AtomicUsize::new(0),
        renewal_seen: std::sync::Mutex::new(Some(renewal_seen_tx)),
        allow_renewal: std::sync::Mutex::new(allow_renewal_rx),
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend,
        "midge".to_string(),
    ));
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let epoch = lease.epoch();
    let renewing = {
        let lease = Arc::clone(&lease);
        std::thread::spawn(move || lease.renew())
    };
    renewal_seen_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("renewal reached backing store");

    // Act: deterministically model monotonic expiry while the provider
    // request is live, then let that stale PUT land.
    lease.validity.fence(epoch);
    allow_renewal_tx.send(()).expect("release renewal PUT");
    let result = renewing.join().expect("renewal thread");

    // Assert
    assert!(matches!(result, Err(LeaseError::RenewalFailed(_))));
    assert!(!lease.acquired.load(Ordering::Acquire));
    assert_eq!(lease.acquired_epoch.load(Ordering::Acquire), 0);
    assert!(lease.validity.remaining(epoch).is_err());
    let persisted = provider_read_doc(&cloud)
        .expect("read reconciled lease")
        .expect("lease record retained for epoch history");
    assert!(
        persisted
            .is_expired_with_tolerance(lease.clock_skew_tolerance)
            .expect("parse reconciled expiry"),
        "late provider side effect must be conditionally expired"
    );
}

#[test]
fn should_eventually_expire_late_renewal_after_timeout_readback_saw_old_object() {
    // Arrange
    let (old_read_tx, old_read_rx) = std::sync::mpsc::channel();
    let backend = Arc::new(LateCommitRenewalBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        conditional_puts: std::sync::atomic::AtomicUsize::new(0),
        pending: std::sync::Mutex::new(None),
        old_read_seen: std::sync::Mutex::new(Some(old_read_tx)),
        late_commit_complete: std::sync::atomic::AtomicBool::new(false),
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        Arc::clone(&backend) as Arc<dyn crate::storage::cloud::CloudBackend>,
        "midge".to_string(),
    ));
    let stale = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&stale)
        .try_acquire()
        .expect("acquire stale lease");

    // Act: renewal reports timeout, its immediate ambiguity read observes
    // the old record, and only then does the accepted PUT become visible.
    let renew_result = stale.renew();
    old_read_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("immediate reconciliation read old object");
    backend.commit_pending();

    let cleanup_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let current = provider_read_doc(&cloud)
            .expect("read eventual cleanup state")
            .expect("lease record retained");
        if current
            .is_expired_with_tolerance(stale.clock_skew_tolerance)
            .expect("parse cleanup expiry")
        {
            break;
        }
        assert!(
            Instant::now() < cleanup_deadline,
            "late committed renewal was not eventually expired"
        );
        std::thread::yield_now();
    }

    let takeover = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _takeover_guard = Arc::clone(&takeover)
        .try_acquire()
        .expect("acquire after reconciled renewal");
    let takeover_epoch = takeover.epoch();
    let takeover_owner = takeover.owner_token.clone();
    std::thread::sleep(Duration::from_millis(30));
    let after_takeover = provider_read_doc(&cloud)
        .expect("read takeover lease")
        .expect("takeover lease remains");

    // Assert
    assert!(matches!(renew_result, Err(LeaseError::IoError(_))));
    assert!(!stale.acquired.load(Ordering::Acquire));
    assert_eq!(stale.acquired_epoch.load(Ordering::Acquire), 0);
    assert_eq!(after_takeover.epoch, Some(takeover_epoch));
    assert_eq!(
        after_takeover.owner_token.as_deref(),
        Some(takeover_owner.as_str())
    );
    assert!(!after_takeover
        .is_expired_with_tolerance(takeover.clock_skew_tolerance)
        .expect("parse takeover expiry"));
}

#[test]
fn should_reject_excessive_public_clock_skew_override_given_safe_default() {
    // Arrange
    let default_lease = CloudStorageLease::new(test_config(), temp_cache_path());
    let excessive = Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS + 1);

    // Act
    let override_result = CloudStorageLease::new(test_config(), temp_cache_path())
        .with_clock_skew_tolerance(excessive);

    // Assert
    assert_eq!(
        default_lease.clock_skew_tolerance,
        Duration::from_secs(DEFAULT_CLOUD_LEASE_TTL_SECS / 2)
    );
    assert!(matches!(override_result, Err(LeaseError::Internal(_))));
}

#[test]
fn should_release_lease_when_held() {
    // Arrange
    let cache_path = temp_cache_path();
    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path.clone()));
    let _guard = Arc::clone(&lease).try_acquire().unwrap();

    // Act
    let result = lease.release();

    // Assert
    assert!(result.is_ok());
    assert!(!lease_file_exists(&cache_path));
}

#[test]
fn should_retry_simulated_lease_release_after_coordination_lock_collision() {
    // Arrange
    let cache_path = temp_cache_path();
    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path.clone()));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let epoch = lease.epoch();
    let lock_path = cache_path.join(".midge_leader.lock");
    std::fs::write(&lock_path, b"competing lease mutation")
        .expect("hold coordination lock during release");

    // Act
    let blocked_release = lease.release();
    assert_eq!(lease.epoch(), 0);
    assert!(lease.validity.remaining(epoch).is_err());
    assert!(matches!(
        Arc::clone(&lease).try_acquire(),
        Err(LeaseError::AlreadyAcquired(_))
    ));
    std::fs::remove_file(&lock_path).expect("release coordination lock");
    let retried_release = lease.release();

    // Assert
    assert!(
        matches!(blocked_release, Err(LeaseError::AcquisitionFailed(_))),
        "coordination collision must be retryable: {blocked_release:?}"
    );
    retried_release.expect("retry must release the persisted lease");
    assert!(!lease_file_exists(&cache_path));
    let successor = Arc::new(CloudStorageLease::new(test_config(), cache_path));
    let _successor_guard = successor
        .clone()
        .try_acquire()
        .expect("successor acquires immediately after retry");
    successor.release().expect("release successor lease");
}

#[test]
fn should_allow_removing_missing_simulated_lease() {
    // Arrange
    let cache_path = temp_cache_path();
    let lease = CloudStorageLease::new(test_config(), cache_path);

    // Act
    let result = lease.remove_lease_file();

    // Assert
    assert!(result.is_ok());
}

#[cfg(unix)]
#[test]
fn should_reject_simulated_lease_read_through_symlink() {
    // Arrange
    let cache_path = temp_cache_path();
    let outside_path = temp_cache_path().join("outside-lease");
    let now = chrono::Utc::now();
    let document = LeaseDocument {
        version: LeaseDocumentVersion::Legacy,
        epoch: Some(1),
        holder_id: "outside-holder@host".to_string(),
        owner_token: Some("outside-owner-token".to_string()),
        acquired_at: now.to_rfc3339(),
        expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        committed_metadata: None,
    };
    std::fs::write(&outside_path, format_lease_document(&document)).unwrap();
    std::os::unix::fs::symlink(&outside_path, cache_path.join(LEASE_OBJECT_KEY)).unwrap();
    let lease = CloudStorageLease::new(test_config(), cache_path);

    // Act
    let result = lease.read_lease_file();

    // Assert
    assert!(matches!(result, Err(LeaseError::IoError(_))));
}

#[cfg(unix)]
#[test]
fn should_reject_simulated_lease_removal_through_symlink() {
    // Arrange
    let cache_path = temp_cache_path();
    let outside_path = temp_cache_path().join("outside-lease");
    std::fs::write(&outside_path, "outside lease").unwrap();
    let lease_path = cache_path.join(LEASE_OBJECT_KEY);
    std::os::unix::fs::symlink(&outside_path, &lease_path).unwrap();
    let lease = CloudStorageLease::new(test_config(), cache_path);

    // Act
    let result = lease.remove_lease_file();

    // Assert
    assert!(matches!(result, Err(LeaseError::IoError(_))));
    assert!(lease_path.symlink_metadata().is_ok());
    assert_eq!(
        std::fs::read_to_string(outside_path).unwrap(),
        "outside lease"
    );
}

#[test]
fn should_preserve_newer_owner_given_stale_guard_drop_when_releasing() {
    // Arrange
    let cache_path = temp_cache_path();
    let stale = Arc::new(
        CloudStorageLease::new(test_config(), cache_path.clone())
            .with_clock_skew_tolerance(Duration::ZERO)
            .expect("zero skew tolerance"),
    );
    let _stale_guard = Arc::clone(&stale).try_acquire().unwrap();
    let mut expired = stale
        .read_lease_file()
        .expect("read stale lease")
        .expect("stale lease exists");
    expired.expires_at = (chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339();
    stale.write_lease_file(&expired).unwrap();

    let current = Arc::new(
        CloudStorageLease::new(test_config(), cache_path.clone())
            .with_clock_skew_tolerance(Duration::ZERO)
            .expect("zero skew tolerance"),
    );
    assert_eq!(stale.holder_id(), current.holder_id());
    let _current_guard = Arc::clone(&current).try_acquire().unwrap();

    // Act
    stale.release().unwrap();

    // Assert
    assert!(lease_file_exists(&cache_path));
}

#[test]
fn should_not_renew_newer_simulated_lease_from_stale_same_process_holder() {
    // Arrange
    let cache_path = temp_cache_path();
    let stale = Arc::new(
        CloudStorageLease::new(test_config(), cache_path.clone())
            .with_clock_skew_tolerance(Duration::ZERO)
            .expect("zero skew tolerance"),
    );
    let _stale_guard = Arc::clone(&stale).try_acquire().unwrap();
    let mut expired = stale
        .read_lease_file()
        .expect("read stale lease")
        .expect("stale lease exists");
    expired.expires_at = (chrono::Utc::now() - chrono::Duration::seconds(1)).to_rfc3339();
    stale.write_lease_file(&expired).unwrap();

    let current = Arc::new(
        CloudStorageLease::new(test_config(), cache_path)
            .with_clock_skew_tolerance(Duration::ZERO)
            .expect("zero skew tolerance"),
    );
    let _current_guard = Arc::clone(&current).try_acquire().unwrap();
    let before = current
        .read_lease_file()
        .expect("read current lease")
        .expect("current lease exists");

    // Act
    let result = stale.renew();

    // Assert
    assert!(matches!(result, Err(LeaseError::RenewalFailed(_))));
    let after = current
        .read_lease_file()
        .expect("read current lease")
        .expect("current lease exists");
    assert_eq!(
        format_lease_document(&before),
        format_lease_document(&after)
    );
    assert!(current.renew().is_ok());
}

#[test]
fn should_persist_lease_identity_in_simulated_document() {
    // Arrange
    let cache_path = temp_cache_path();
    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

    // Act
    let _guard = Arc::clone(&lease).try_acquire().unwrap();
    let document = lease
        .read_lease_file()
        .expect("read lease")
        .expect("lease exists");

    // Assert
    assert_eq!(
        document.owner_token.as_deref(),
        Some(lease.owner_token.as_str())
    );
    assert_eq!(document.epoch, Some(lease.epoch()));
}

#[test]
fn should_allow_only_one_concurrent_simulated_lease_acquisition() {
    // Arrange
    let cache_path = temp_cache_path();
    let first = Arc::new(CloudStorageLease::new(test_config(), cache_path.clone()));
    let second = Arc::new(CloudStorageLease::new(test_config(), cache_path));
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let first_thread = {
        let first = Arc::clone(&first);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            first.try_acquire()
        })
    };
    let second_thread = {
        let second = Arc::clone(&second);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            second.try_acquire()
        })
    };

    // Act
    barrier.wait();
    let results = [
        first_thread.join().expect("first acquirer panicked"),
        second_thread.join().expect("second acquirer panicked"),
    ];

    // Assert
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
}

#[test]
fn should_not_delete_new_holder_lease_given_stale_process_release_when_releasing() {
    // Arrange
    let cache_path = temp_cache_path();
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        cache_path,
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease).try_acquire().unwrap();

    let now = chrono::Utc::now();
    let new_holder_doc = LeaseDocument {
        version: LeaseDocumentVersion::V2,
        epoch: Some(lease.epoch().saturating_add(1)),
        holder_id: "new-holder@host".to_string(),
        owner_token: Some("new-holder-token".to_string()),
        acquired_at: now.to_rfc3339(),
        expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        committed_metadata: None,
    };
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(
        LEASE_OBJECT_KEY,
        format_lease_document(&new_holder_doc).into_bytes(),
        vec![],
        tx,
    );
    let _ = rx.recv().unwrap();

    // Act
    lease.release().unwrap();

    // Assert
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_get(LEASE_OBJECT_KEY, tx);
    let event = rx.recv().unwrap();
    let bytes = match event {
        CloudEvent::Get {
            result: CloudOutcome::Ok(bytes),
            ..
        } => bytes,
        other => panic!("expected surviving lease doc, got {other:?}"),
    };
    let content = String::from_utf8(bytes).unwrap();
    let doc = parse_lease_document(&content).unwrap();
    assert_eq!(doc.holder_id, "new-holder@host");
}

#[test]
fn should_increment_remote_epoch_across_provider_cache_directories() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let first = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let second = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ));
    assert!(first.get_leader_store().is_some());
    assert!(second.get_leader_store().is_some());

    // Act
    let _first_guard = Arc::clone(&first)
        .try_acquire()
        .expect("acquire first lease");
    let first_epoch = first.epoch();
    first.release().expect("release first lease");
    let _second_guard = Arc::clone(&second)
        .try_acquire()
        .expect("acquire second lease");

    // Assert
    assert_eq!(first_epoch, 1);
    assert_eq!(second.epoch(), 2);
}

#[test]
fn should_grant_exactly_one_acquisition_given_concurrent_provider_racers_when_racing() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let first = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let second = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ));
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let spawn = |lease: Arc<CloudStorageLease>| {
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            lease.try_acquire()
        })
    };
    let first_thread = spawn(first);
    let second_thread = spawn(second);

    // Act
    barrier.wait();
    let results = [first_thread.join().unwrap(), second_thread.join().unwrap()];

    // Assert
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
}

#[test]
fn should_validate_provider_epoch_through_leader_store() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease)
        .try_acquire()
        .expect("acquire provider lease");
    let acquired_epoch = lease.epoch();
    let leader_store = lease
        .get_leader_store()
        .expect("provider lease should expose its remote leader store");
    let now = chrono::Utc::now();
    let newer = LeaseDocument {
        version: LeaseDocumentVersion::V2,
        epoch: Some(acquired_epoch + 1),
        holder_id: "new-holder@host".to_string(),
        owner_token: Some("new-holder-token".to_string()),
        acquired_at: now.to_rfc3339(),
        expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        committed_metadata: None,
    };

    // Act
    let holder_id = lease.holder_id();
    let current_result = leader_store.validate_epoch(&holder_id, acquired_epoch);
    put_remote_lease(&cloud, format_lease_document(&newer));
    let stale_result = leader_store.validate_epoch(&holder_id, acquired_epoch);

    // Assert
    assert!(current_result.is_ok());
    assert!(matches!(stale_result, Err(LeaseError::RenewalFailed(_))));
}

#[test]
fn should_reject_provider_epoch_validation_after_lease_release() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let store = lease.get_leader_store().expect("leader store");
    let holder_id = lease.holder_id();
    let epoch = lease.epoch();
    store
        .validate_epoch_with_timeout(&holder_id, epoch, Duration::from_secs(1))
        .expect("active lease validates");

    // Act
    lease.release().expect("release lease");
    let validated = store.validate_epoch_with_timeout(&holder_id, epoch, Duration::from_secs(1));

    // Assert
    assert!(matches!(validated, Err(LeaseError::RenewalFailed(_))));
}

#[test]
fn should_reject_provider_epoch_validation_when_persisted_expiry_is_past() {
    // Arrange: local monotonic validity is still active, but the persisted
    // lease no longer excludes a successor.
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let mut document = provider_read_doc(&cloud).unwrap().expect("lease document");
    document.expires_at = (chrono::Utc::now() - chrono::Duration::seconds(60)).to_rfc3339();
    put_remote_lease(&cloud, format_lease_document(&document));

    // Act
    let result = lease
        .get_leader_store()
        .unwrap()
        .validate_epoch_with_timeout(&lease.holder_id(), lease.epoch(), Duration::from_secs(1));

    // Assert
    assert!(matches!(result, Err(LeaseError::RenewalFailed(_))));
    assert!(lease.validity.remaining(lease.epoch()).is_ok());
}

#[test]
fn should_not_overwrite_newer_provider_epoch_on_stale_release() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease)
        .try_acquire()
        .expect("acquire provider lease");
    let now = chrono::Utc::now();
    let newer = LeaseDocument {
        version: LeaseDocumentVersion::V2,
        epoch: Some(2),
        holder_id: lease.holder_id(),
        owner_token: Some("new-owner-token".to_string()),
        acquired_at: now.to_rfc3339(),
        expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        committed_metadata: None,
    };
    put_remote_lease(&cloud, format_lease_document(&newer));

    // Act
    lease.release().expect("stale release is harmless");

    // Assert
    let current = parse_lease_document(&read_remote_lease(&cloud)).expect("parse remote lease");
    assert_eq!(current.epoch, Some(2));
    assert!(!current.is_expired().expect("valid expiry"));
}

#[test]
fn should_increment_remote_epoch_given_expired_provider_lease_when_reacquiring() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    provider_create_authority_sentinel(&cloud, Duration::from_secs(1))
        .expect("seed initialized authority");
    let past = chrono::Utc::now() - chrono::Duration::seconds(60);
    put_remote_lease(
        &cloud,
        format_lease_document(&LeaseDocument {
            version: LeaseDocumentVersion::V2,
            epoch: Some(41),
            holder_id: "old-holder@host".to_string(),
            owner_token: Some("old-token".to_string()),
            acquired_at: (past - chrono::Duration::seconds(30)).to_rfc3339(),
            expires_at: past.to_rfc3339(),
            committed_metadata: None,
        }),
    );
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ));

    // Act
    let _guard = Arc::clone(&lease)
        .try_acquire()
        .expect("acquire expired provider lease");

    // Assert
    assert_eq!(lease.epoch(), 42);
}

#[test]
fn should_refuse_provider_takeover_given_malformed_expiry() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    provider_create_authority_sentinel(&cloud, Duration::from_secs(1))
        .expect("seed initialized authority");
    let document = format_lease_document(&LeaseDocument {
        version: LeaseDocumentVersion::V2,
        epoch: Some(41),
        holder_id: "ambiguous-holder@host".to_string(),
        owner_token: Some("ambiguous-token".to_string()),
        acquired_at: "2026-07-31T12:00:00Z".to_string(),
        expires_at: "not-a-timestamp".to_string(),
        committed_metadata: None,
    });
    put_remote_lease(&cloud, document.clone());
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    assert!(matches!(result, Err(LeaseError::Indeterminate(_))));
    assert_eq!(read_remote_lease(&cloud), document);
    assert_eq!(lease.epoch(), 0);
}

#[test]
fn should_repair_malformed_expiry_when_current_provider_owner_renews() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let mut owned = parse_lease_document(&read_remote_lease(&cloud)).expect("parse lease");
    owned.expires_at = "not-a-timestamp".to_string();
    put_remote_lease(&cloud, format_lease_document(&owned));

    // Act
    let result = lease.renew();

    // Assert
    assert!(result.is_ok());
    let repaired = parse_lease_document(&read_remote_lease(&cloud)).expect("parse repaired");
    assert!(chrono::DateTime::parse_from_rfc3339(&repaired.expires_at).is_ok());
}

#[test]
fn should_fail_closed_on_legacy_provider_lease_even_after_expiry() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let now = chrono::Utc::now();
    put_remote_lease(
        &cloud,
        format!(
            "holder_id: legacy-holder@host\nacquired_at: {}\nexpires_at: {}\n",
            now.to_rfc3339(),
            (now + chrono::Duration::seconds(60)).to_rfc3339()
        ),
    );
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));

    // Act
    let active_result = Arc::clone(&lease).try_acquire();
    let past = now - chrono::Duration::seconds(60);
    put_remote_lease(
        &cloud,
        format!(
            "holder_id: legacy-holder@host\nacquired_at: {}\nexpires_at: {}\n",
            (past - chrono::Duration::seconds(30)).to_rfc3339(),
            past.to_rfc3339()
        ),
    );
    let expired_result = Arc::clone(&lease).try_acquire();

    // Assert
    assert!(matches!(active_result, Err(LeaseError::Indeterminate(_))));
    assert!(matches!(expired_result, Err(LeaseError::Indeterminate(_))));
    assert_eq!(lease.epoch(), 0);
}

#[test]
fn should_preserve_remote_epoch_when_provider_lease_renews() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease)
        .try_acquire()
        .expect("acquire provider lease");

    // Act
    lease.renew().expect("renew provider lease");
    let content = read_remote_lease(&cloud);

    // Assert
    assert!(content.lines().any(|line| line == "fencing_epoch: 1"));
    assert_eq!(lease.epoch(), 1);
}

#[test]
fn should_report_provider_ownership_change_when_owner_token_changes() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease)
        .try_acquire()
        .expect("acquire provider lease");
    let now = chrono::Utc::now();
    let successor = LeaseDocument {
        version: LeaseDocumentVersion::V2,
        epoch: Some(lease.epoch()),
        holder_id: lease.holder_id(),
        owner_token: Some("successor-owner-token".to_string()),
        acquired_at: now.to_rfc3339(),
        expires_at: (now + chrono::Duration::seconds(60)).to_rfc3339(),
        committed_metadata: None,
    };
    put_remote_lease(&cloud, format_lease_document(&successor));

    // Act
    let result = lease.renew();

    // Assert
    assert!(matches!(
        result,
        Err(LeaseError::RenewalFailed(message))
            if message.contains("ownership changed")
    ));
}

#[test]
fn should_allow_reacquire_after_release() {
    // Arrange
    let cache_path = temp_cache_path();
    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));
    let guard = Arc::clone(&lease).try_acquire().unwrap();
    let first_epoch = lease.epoch();
    guard.release();
    lease.release().unwrap();

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    assert!(result.is_ok());
    assert!(
        lease.epoch() > first_epoch,
        "reacquiring after a genuine release must mint a fresh, higher fencing epoch \
         (first={first_epoch}, second={}), not silently keep the old one",
        lease.epoch()
    );
}

#[test]
fn should_format_holder_id_with_process_info() {
    // Arrange
    let cache_path = temp_cache_path();
    let lease = Arc::new(CloudStorageLease::new(test_config(), cache_path));

    // Act
    let holder = lease.holder_id();

    // Assert
    assert!(holder.contains('@'));
    assert!(holder.contains(&std::process::id().to_string()));
}

#[test]
fn should_construct_lease_key_with_prefix() {
    // Arrange
    let cache_path = temp_cache_path();
    let lease = CloudStorageLease::new(test_config(), cache_path);

    // Act
    let key = lease.lease_key();

    // Assert
    assert_eq!(key, format!("test/prefix/{LEASE_OBJECT_KEY}"));
}

#[test]
fn should_construct_lease_key_without_prefix() {
    // Arrange
    let config = CloudLeaseConfig {
        bucket: "bucket".to_string(),
        prefix: String::new(),
    };
    let cache_path = temp_cache_path();
    let lease = Arc::new(CloudStorageLease::new(config, cache_path));

    // Act
    let key = lease.lease_key();

    // Assert
    assert_eq!(key, LEASE_OBJECT_KEY);
}

#[test]
fn should_parse_lease_document_roundtrip() {
    // Arrange
    let doc = LeaseDocument {
        version: LeaseDocumentVersion::Legacy,
        epoch: Some(7),
        holder_id: "123@host".to_string(),
        owner_token: Some("owner-token".to_string()),
        acquired_at: "2026-02-07T12:00:00Z".to_string(),
        expires_at: "2026-02-07T12:00:30Z".to_string(),
        committed_metadata: None,
    };

    // Act
    let serialized = format_lease_document(&doc);
    let parsed = parse_lease_document(&serialized);

    // Assert
    let parsed = parsed.unwrap();
    assert_eq!(parsed.epoch, Some(7));
    assert_eq!(parsed.holder_id, "123@host");
    assert_eq!(parsed.owner_token.as_deref(), Some("owner-token"));
    assert_eq!(parsed.acquired_at, "2026-02-07T12:00:00Z");
    assert_eq!(parsed.expires_at, "2026-02-07T12:00:30Z");
}

#[test]
fn should_detect_expired_lease() {
    // Arrange
    let past = chrono::Utc::now() - chrono::Duration::seconds(60);
    let doc = LeaseDocument {
        version: LeaseDocumentVersion::Legacy,
        epoch: None,
        holder_id: "test".to_string(),
        owner_token: None,
        acquired_at: (past - chrono::Duration::seconds(30)).to_rfc3339(),
        expires_at: past.to_rfc3339(),
        committed_metadata: None,
    };

    // Act
    let expired = doc.is_expired().expect("valid expiry");

    // Assert
    assert!(expired);
}

#[test]
fn should_delay_takeover_until_clock_skew_tolerance_elapses() {
    // Arrange
    let now = chrono::Utc::now();
    let document = LeaseDocument {
        version: LeaseDocumentVersion::Legacy,
        epoch: Some(7),
        holder_id: "skewed-holder".to_string(),
        owner_token: Some("token".to_string()),
        acquired_at: (now - chrono::Duration::seconds(30)).to_rfc3339(),
        expires_at: (now - chrono::Duration::seconds(5)).to_rfc3339(),
        committed_metadata: None,
    };

    // Act
    let without_tolerance = document.is_expired_with_tolerance(Duration::ZERO).unwrap();
    let with_tolerance = document
        .is_expired_with_tolerance(Duration::from_secs(15))
        .unwrap();

    // Assert
    assert!(without_tolerance);
    assert!(!with_tolerance);
}

#[test]
fn should_detect_active_lease() {
    // Arrange
    let future = chrono::Utc::now() + chrono::Duration::seconds(60);
    let doc = LeaseDocument {
        version: LeaseDocumentVersion::Legacy,
        epoch: None,
        holder_id: "test".to_string(),
        owner_token: None,
        acquired_at: chrono::Utc::now().to_rfc3339(),
        expires_at: future.to_rfc3339(),
        committed_metadata: None,
    };

    // Act
    let expired = doc.is_expired().expect("valid expiry");

    // Assert
    assert!(!expired);
}

#[test]
fn should_grant_epoch_above_minimum_when_provider_lease_object_is_deleted() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = CloudStorageLease::new_provider_backed(test_config(), temp_cache_path(), cloud);
    let catalog_fencing_epoch = 9;

    // Act
    let record = lease
        .leader_store
        .acquire_leadership_with_minimum_epoch(&lease.holder_id, catalog_fencing_epoch)
        .expect("acquire with epoch floor");

    // Assert
    assert_eq!(record.epoch, catalog_fencing_epoch + 1);
}

fn lease_file_exists(cache_path: &std::path::Path) -> bool {
    cache_path.join(LEASE_OBJECT_KEY).exists()
}

fn put_remote_lease(cloud: &CloudStorage, content: String) {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(LEASE_OBJECT_KEY, content.into_bytes(), vec![], tx);
    match rx.recv().expect("receive remote lease put") {
        CloudEvent::Put {
            result: CloudOutcome::Ok(()),
            ..
        } => {}
        other => panic!("expected remote lease put, got {other:?}"),
    }
}

fn read_remote_lease(cloud: &CloudStorage) -> String {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_get(LEASE_OBJECT_KEY, tx);
    let bytes = match rx.recv().expect("receive remote lease get") {
        CloudEvent::Get {
            result: CloudOutcome::Ok(bytes),
            ..
        } => bytes,
        other => panic!("expected remote lease get, got {other:?}"),
    };
    String::from_utf8(bytes).expect("remote lease is UTF-8")
}

fn delete_remote_lease(cloud: &CloudStorage) {
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_delete(LEASE_OBJECT_KEY, tx);
    match rx.recv().expect("receive remote lease delete") {
        CloudEvent::Delete {
            result: CloudOutcome::Ok(()),
            ..
        } => {}
        other => panic!("expected remote lease delete, got {other:?}"),
    }
}

#[test]
fn should_refuse_fresh_lease_when_initialized_authority_loses_lease_object() {
    // Arrange: a committed pointer was live before the lease object was lost.
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let first = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _first_guard = Arc::clone(&first).try_acquire().expect("acquire first");
    let generation = test_metadata_generation(1);
    first
        .get_leader_store()
        .unwrap()
        .publish_committed_metadata(
            &first.holder_id(),
            first.epoch(),
            None,
            generation,
            Duration::from_secs(1),
        )
        .expect("commit metadata pointer");
    first.release().expect("release first");
    assert_eq!(
        provider_read_authority_sentinel(&cloud, Duration::from_secs(1)).unwrap(),
        AuthoritySentinelState::Active
    );
    delete_remote_lease(&cloud);
    let second = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));

    // Act
    let read_result = second
        .get_leader_store()
        .unwrap()
        .read_committed_metadata(Duration::from_secs(1));
    let acquire_result = Arc::clone(&second).try_acquire();

    // Assert
    assert!(matches!(read_result, Err(LeaseError::Indeterminate(_))));
    assert!(matches!(acquire_result, Err(LeaseError::Indeterminate(_))));
    assert!(provider_read_doc(&cloud).unwrap().is_none());
    assert_eq!(
        provider_read_authority_sentinel(&cloud, Duration::from_secs(1)).unwrap(),
        AuthoritySentinelState::Active
    );
}

#[test]
fn should_retry_first_lease_when_pending_sentinel_outlives_failed_create() {
    // Arrange: the marker write succeeds, but the first lease If-None-Match
    // request fails without creating the lease object.
    let backend = Arc::new(ScriptedConditionalPutBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        scripted_error: crate::storage::cloud::CloudError::ServerError(
            "injected first lease create failure".to_string(),
        ),
        apply_before_error: false,
        scripted_puts_remaining: std::sync::atomic::AtomicUsize::new(1),
        scripted_target: ScriptedPutTarget::Lease,
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend,
        "midge".to_string(),
    ));
    let make_lease = || {
        Arc::new(CloudStorageLease::new_provider_backed(
            test_config(),
            temp_cache_path(),
            Arc::clone(&cloud),
        ))
    };
    let first = make_lease();

    // Act: a new process retries the same still-empty namespace.
    assert!(Arc::clone(&first).try_acquire().is_err());
    assert!(provider_read_doc(&cloud).unwrap().is_none());
    assert!(matches!(
        first
            .get_leader_store()
            .unwrap()
            .read_committed_metadata(Duration::from_secs(1)),
        Err(LeaseError::Indeterminate(_))
    ));
    let second = make_lease();
    let acquired = Arc::clone(&second).try_acquire();

    // Assert: the successful retry activates the marker before using the lease.
    if let Err(error) = acquired {
        panic!("first-open retry failed: {error:?}");
    }
    assert_eq!(second.epoch(), 1);
    assert!(matches!(
        second
            .get_leader_store()
            .unwrap()
            .read_committed_metadata(Duration::from_secs(1)),
        Ok(CloudMetadataHead::Uncommitted)
    ));
    second.release().expect("release acquired lease");
}

#[test]
fn should_keep_first_lease_inactive_when_marker_activation_fails() {
    // Arrange: the first lease CAS succeeds, but pending-to-active marker CAS
    // is rejected before it applies.
    let (cloud, first) = lease_with_scripted_marker_activation(false);

    // Act
    let first_result = Arc::clone(&first).try_acquire();

    // Assert: a pending marker never authorizes metadata publication.
    assert!(first_result.is_err());
    assert!(!first.acquired.load(Ordering::Acquire));
    assert_eq!(
        provider_read_authority_sentinel(&cloud, Duration::from_secs(1)).unwrap(),
        AuthoritySentinelState::Pending
    );
    assert!(matches!(
        first
            .get_leader_store()
            .unwrap()
            .read_committed_metadata(Duration::from_secs(1)),
        Err(LeaseError::Indeterminate(_))
    ));
}

#[test]
fn should_activate_pending_marker_when_first_activation_failed() {
    // Arrange: the first lease is stored, but its marker activation fails.
    let (cloud, first) = lease_with_scripted_marker_activation(false);
    assert!(Arc::clone(&first).try_acquire().is_err());

    // Arrange: let the inactive lease expire without discarding its pointer.
    let mut expired = provider_read_doc(&cloud)
        .unwrap()
        .expect("first lease exists");
    assert!(expired.committed_metadata.is_none());
    expired.expires_at = (chrono::Utc::now() - chrono::Duration::seconds(60)).to_rfc3339();
    put_remote_lease(&cloud, format_lease_document(&expired));
    let second = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));

    // Act
    let second_result = Arc::clone(&second).try_acquire();

    // Assert
    if let Err(error) = second_result {
        panic!("successor could not activate pending marker: {error:?}");
    }
    assert_eq!(second.epoch(), expired.epoch.unwrap() + 1);
    assert_eq!(
        provider_read_authority_sentinel(&cloud, Duration::from_secs(1)).unwrap(),
        AuthoritySentinelState::Active
    );
    second.release().expect("release successor");
}

#[test]
fn should_confirm_ambiguous_marker_activation_by_exact_readback() {
    // Arrange: the marker CAS applies but its response reports a server error.
    let (cloud, lease) = lease_with_scripted_marker_activation(true);

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    if let Err(error) = result {
        panic!("applied marker activation was not confirmed: {error:?}");
    }
    assert_eq!(
        provider_read_authority_sentinel(&cloud, Duration::from_secs(1)).unwrap(),
        AuthoritySentinelState::Active
    );
    lease.release().expect("release activated lease");
}

#[test]
fn should_reject_pending_marker_with_committed_lease_pointer() {
    // Arrange: this combination cannot be emitted by the two-phase protocol.
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    provider_create_authority_sentinel(&cloud, Duration::from_secs(1))
        .expect("create pending marker");
    let old = LeaseDocument {
        version: LeaseDocumentVersion::V2,
        epoch: Some(7),
        holder_id: "previous@host".into(),
        owner_token: Some("previous-token".into()),
        acquired_at: (chrono::Utc::now() - chrono::Duration::seconds(120)).to_rfc3339(),
        expires_at: (chrono::Utc::now() - chrono::Duration::seconds(60)).to_rfc3339(),
        committed_metadata: Some(test_metadata_generation(5)),
    };
    put_remote_lease(&cloud, format_lease_document(&old));
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    assert!(matches!(result, Err(LeaseError::Indeterminate(_))));
    assert_eq!(provider_read_doc(&cloud).unwrap(), Some(old));
    assert_eq!(
        provider_read_authority_sentinel(&cloud, Duration::from_secs(1)).unwrap(),
        AuthoritySentinelState::Pending
    );
}

#[test]
fn should_reject_malformed_authority_marker_before_first_lease_create() {
    // Arrange: a near-match must not be treated as a valid pending marker.
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let mut malformed = AUTHORITY_SENTINEL_PENDING_BODY.to_vec();
    malformed.push(b' ');
    let (tx, rx) = std::sync::mpsc::channel();
    cloud.submit_put(AUTHORITY_SENTINEL_KEY, malformed, vec![], tx);
    assert!(matches!(
        rx.recv().expect("receive marker put"),
        CloudEvent::Put {
            result: CloudOutcome::Ok(()),
            ..
        }
    ));
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));

    // Act
    let result = Arc::clone(&lease).try_acquire();

    // Assert
    assert!(matches!(result, Err(LeaseError::Indeterminate(_))));
    assert!(provider_read_doc(&cloud).unwrap().is_none());
}

#[test]
fn should_refuse_v2_lease_when_permanent_authority_sentinel_is_missing() {
    // Arrange: a V2 lease without its durable marker could lose the pointer
    // undetectably if the lease object were later deleted.
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let now = chrono::Utc::now();
    put_remote_lease(
        &cloud,
        format_lease_document(&LeaseDocument {
            version: LeaseDocumentVersion::V2,
            epoch: Some(5),
            holder_id: "previous-holder@host".into(),
            owner_token: Some("previous-token".into()),
            acquired_at: (now - chrono::Duration::seconds(120)).to_rfc3339(),
            expires_at: (now - chrono::Duration::seconds(60)).to_rfc3339(),
            committed_metadata: Some(test_metadata_generation(3)),
        }),
    );
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let store = lease.get_leader_store().expect("leader store");

    // Act
    let read_result = store.read_committed_metadata(Duration::from_secs(1));
    let acquire_result = Arc::clone(&lease).try_acquire();

    // Assert
    assert!(matches!(read_result, Err(LeaseError::Indeterminate(_))));
    assert!(matches!(acquire_result, Err(LeaseError::Indeterminate(_))));
    assert_eq!(provider_read_doc(&cloud).unwrap().unwrap().epoch, Some(5));
}

fn test_metadata_generation(sequence: u64) -> CloudMetadataGeneration {
    let manifest_name = crate::metadata::files::MANIFEST_SNAPSHOT;
    let format_name = crate::metadata::files::FORMAT;
    CloudMetadataGeneration {
        manifest_sequence: sequence,
        objects: vec![
            CloudMetadataObject {
                file_name: manifest_name.to_string(),
                object_key: format!(
                    "metadata/generations/{}/{manifest_name}",
                    uuid::Uuid::new_v4()
                ),
                len: 11,
                crc32c: 0x1234_5678,
            },
            CloudMetadataObject {
                file_name: format_name.to_string(),
                object_key: format!(
                    "metadata/generations/{}/{format_name}",
                    uuid::Uuid::new_v4()
                ),
                len: 3,
                crc32c: 0x1122_3344,
            },
        ],
    }
}

#[test]
fn should_preserve_committed_metadata_pointer_when_provider_lease_renews() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let store = lease.get_leader_store().expect("provider leader store");
    assert_eq!(
        store
            .read_committed_metadata(Duration::from_secs(1))
            .unwrap(),
        CloudMetadataHead::MissingLease
    );
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    assert_eq!(
        store
            .read_committed_metadata(Duration::from_secs(1))
            .unwrap(),
        CloudMetadataHead::Uncommitted
    );
    let mut generation = test_metadata_generation(7);
    generation.objects.push(CloudMetadataObject {
        file_name: crate::metadata::files::JOURNAL.to_string(),
        object_key: format!(
            "metadata/generations/{}/{}",
            uuid::Uuid::new_v4(),
            crate::metadata::files::JOURNAL
        ),
        len: 3,
        crc32c: 0x1122_3344,
    });

    // Act
    store
        .publish_committed_metadata(
            &lease.holder_id(),
            lease.epoch(),
            None,
            generation.clone(),
            Duration::from_secs(1),
        )
        .expect("commit generation");
    lease.renew().expect("renew lease");
    lease.release().expect("release lease");

    // Assert
    assert_eq!(
        store
            .read_committed_metadata(Duration::from_secs(1))
            .unwrap(),
        CloudMetadataHead::Committed(generation)
    );
    let wire = read_remote_lease(&cloud);
    assert!(wire.starts_with("lease_version: 2\n"), "{wire}");
    assert!(!wire.contains("holder_id: "), "old readers must reject V2");
}

#[test]
fn should_fence_stale_metadata_pointer_publication_after_new_provider_epoch() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let first = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let second = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _first_guard = Arc::clone(&first).try_acquire().expect("acquire first");
    let old_store = first.get_leader_store().expect("first store");
    let first_generation = test_metadata_generation(1);
    old_store
        .publish_committed_metadata(
            &first.holder_id(),
            first.epoch(),
            None,
            first_generation.clone(),
            Duration::from_secs(1),
        )
        .expect("first commit");
    first.release().expect("release first");
    let _second_guard = Arc::clone(&second).try_acquire().expect("acquire second");
    let new_store = second.get_leader_store().expect("second store");
    let second_generation = test_metadata_generation(2);
    new_store
        .publish_committed_metadata(
            &second.holder_id(),
            second.epoch(),
            Some(&first_generation),
            second_generation.clone(),
            Duration::from_secs(1),
        )
        .expect("second commit");

    // Act
    let stale = old_store.publish_committed_metadata(
        &first.holder_id(),
        first.epoch(),
        Some(&first_generation),
        test_metadata_generation(3),
        Duration::from_secs(1),
    );

    // Assert
    assert!(matches!(stale, Err(LeaseError::RenewalFailed(_))));
    assert_eq!(
        new_store
            .read_committed_metadata(Duration::from_secs(1))
            .unwrap(),
        CloudMetadataHead::Committed(second_generation)
    );
}

#[test]
fn should_reject_stale_snapshot_when_same_epoch_pointer_advanced_during_upload() {
    // Arrange: both snapshots were captured under one lease epoch, but the
    // older caller finishes its immutable uploads after the newer commit.
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let store = lease.get_leader_store().expect("leader store");
    let first = test_metadata_generation(12);
    let newer = test_metadata_generation(12);
    let stale = test_metadata_generation(12);
    store
        .publish_committed_metadata(
            &lease.holder_id(),
            lease.epoch(),
            None,
            first.clone(),
            Duration::from_secs(1),
        )
        .expect("commit first");
    store
        .publish_committed_metadata(
            &lease.holder_id(),
            lease.epoch(),
            Some(&first),
            newer.clone(),
            Duration::from_secs(1),
        )
        .expect("commit newer snapshot");

    // Act
    let stale_result = store.publish_committed_metadata(
        &lease.holder_id(),
        lease.epoch(),
        Some(&first),
        stale,
        Duration::from_secs(1),
    );

    // Assert
    assert!(matches!(stale_result, Err(LeaseError::RenewalFailed(_))));
    assert_eq!(
        store
            .read_committed_metadata(Duration::from_secs(1))
            .unwrap(),
        CloudMetadataHead::Committed(newer)
    );
}

#[test]
fn should_reject_metadata_pointer_cas_that_lands_after_successor_acquires() {
    // Arrange: pause A's provider CAS after its lease read, then allow B to
    // acquire and commit a newer pointer before A's request reaches storage.
    let (put_seen_tx, put_seen_rx) = std::sync::mpsc::channel();
    let (allow_put_tx, allow_put_rx) = std::sync::mpsc::channel();
    let backend = Arc::new(BlockingRenewalBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        conditional_puts: std::sync::atomic::AtomicUsize::new(0),
        renewal_seen: std::sync::Mutex::new(Some(put_seen_tx)),
        allow_renewal: std::sync::Mutex::new(allow_put_rx),
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend,
        "midge".to_string(),
    ));
    let first = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let second = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _first_guard = Arc::clone(&first).try_acquire().expect("acquire first");
    let stale_generation = test_metadata_generation(9);
    let publishing = {
        let store = first.get_leader_store().expect("first store");
        let holder_id = first.holder_id();
        let epoch = first.epoch();
        std::thread::spawn(move || {
            store.publish_committed_metadata(
                &holder_id,
                epoch,
                None,
                stale_generation,
                Duration::from_secs(5),
            )
        })
    };
    put_seen_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("stale pointer CAS entered backend");
    first.release().expect("release first while CAS is paused");
    let _second_guard = Arc::clone(&second).try_acquire().expect("acquire second");
    let winner = test_metadata_generation(10);
    let second_store = second.get_leader_store().expect("second store");
    second_store
        .publish_committed_metadata(
            &second.holder_id(),
            second.epoch(),
            None,
            winner.clone(),
            Duration::from_secs(5),
        )
        .expect("successor commits its pointer");

    // Act
    allow_put_tx.send(()).expect("release stale CAS");
    let stale = publishing.join().expect("publishing thread");

    // Assert
    assert!(
        matches!(stale, Err(LeaseError::RenewalFailed(_))),
        "{stale:?}"
    );
    assert_eq!(
        second_store
            .read_committed_metadata(Duration::from_secs(1))
            .unwrap(),
        CloudMetadataHead::Committed(winner)
    );
}

#[test]
fn should_retry_metadata_pointer_cas_after_same_epoch_heartbeat_renews() {
    // Arrange
    let (put_seen_tx, put_seen_rx) = std::sync::mpsc::channel();
    let (allow_put_tx, allow_put_rx) = std::sync::mpsc::channel();
    let backend = Arc::new(BlockingRenewalBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        conditional_puts: std::sync::atomic::AtomicUsize::new(0),
        renewal_seen: std::sync::Mutex::new(Some(put_seen_tx)),
        allow_renewal: std::sync::Mutex::new(allow_put_rx),
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend,
        "midge".to_string(),
    ));
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let generation = test_metadata_generation(11);
    let publishing = {
        let store = lease.get_leader_store().expect("store");
        let holder_id = lease.holder_id();
        let epoch = lease.epoch();
        let generation = generation.clone();
        std::thread::spawn(move || {
            store.publish_committed_metadata(
                &holder_id,
                epoch,
                None,
                generation,
                Duration::from_secs(5),
            )
        })
    };
    put_seen_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("pointer CAS entered backend");

    // Act
    lease.renew().expect("heartbeat wins the first CAS race");
    allow_put_tx.send(()).expect("release pointer CAS");
    publishing
        .join()
        .expect("publishing thread")
        .expect("retry pointer CAS after renewal");

    // Assert
    assert_eq!(
        lease
            .get_leader_store()
            .unwrap()
            .read_committed_metadata(Duration::from_secs(1))
            .unwrap(),
        CloudMetadataHead::Committed(generation)
    );
}

#[test]
fn should_retry_provider_renewal_when_same_epoch_pointer_cas_wins() {
    // Arrange: hold the first renewal CAS after it reads the lease, then
    // commit a metadata pointer through the same lease object.
    let (renewal_seen_tx, renewal_seen_rx) = std::sync::mpsc::channel();
    let (allow_renewal_tx, allow_renewal_rx) = std::sync::mpsc::channel();
    let backend = Arc::new(BlockingRenewalBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        conditional_puts: std::sync::atomic::AtomicUsize::new(0),
        renewal_seen: std::sync::Mutex::new(Some(renewal_seen_tx)),
        allow_renewal: std::sync::Mutex::new(allow_renewal_rx),
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend,
        "midge".to_string(),
    ));
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let epoch = lease.epoch();
    let store = lease.get_leader_store().expect("leader store");
    let renewing = {
        let lease = Arc::clone(&lease);
        std::thread::spawn(move || lease.renew())
    };
    renewal_seen_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("renewal CAS reached backend");
    let generation = test_metadata_generation(19);
    store
        .publish_committed_metadata(
            &lease.holder_id(),
            epoch,
            None,
            generation.clone(),
            Duration::from_secs(5),
        )
        .expect("pointer commit wins first CAS");

    // Act
    allow_renewal_tx.send(()).expect("release renewal CAS");
    let renewed = renewing.join().expect("renewal thread");

    // Assert
    assert!(renewed.is_ok(), "{renewed:?}");
    assert_eq!(lease.epoch(), epoch);
    assert!(lease.validity.remaining(epoch).is_ok());
    assert_eq!(
        store
            .read_committed_metadata(Duration::from_secs(1))
            .unwrap(),
        CloudMetadataHead::Committed(generation)
    );
}

#[test]
fn should_not_report_pointer_commit_success_after_local_validity_is_fenced() {
    // Arrange: the provider accepts the pointer CAS after the local watchdog
    // has fenced this acquisition.
    let (put_seen_tx, put_seen_rx) = std::sync::mpsc::channel();
    let (allow_put_tx, allow_put_rx) = std::sync::mpsc::channel();
    let backend = Arc::new(BlockingRenewalBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        conditional_puts: std::sync::atomic::AtomicUsize::new(0),
        renewal_seen: std::sync::Mutex::new(Some(put_seen_tx)),
        allow_renewal: std::sync::Mutex::new(allow_put_rx),
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend,
        "midge".to_string(),
    ));
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let epoch = lease.epoch();
    let publishing = {
        let store = lease.get_leader_store().expect("leader store");
        let holder_id = lease.holder_id();
        std::thread::spawn(move || {
            store.publish_committed_metadata(
                &holder_id,
                epoch,
                None,
                test_metadata_generation(21),
                Duration::from_secs(5),
            )
        })
    };
    put_seen_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("pointer CAS reached backend");

    // Act
    lease.validity.fence(epoch);
    allow_put_tx.send(()).expect("release pointer CAS");
    let result = publishing.join().expect("publisher thread");

    // Assert
    assert!(
        matches!(result, Err(LeaseError::RenewalFailed(_))),
        "{result:?}"
    );
    assert!(lease.validity.remaining(epoch).is_err());
}

#[test]
fn should_retry_provider_release_when_same_epoch_pointer_cas_wins() {
    // Arrange
    let (release_seen_tx, release_seen_rx) = std::sync::mpsc::channel();
    let (allow_release_tx, allow_release_rx) = std::sync::mpsc::channel();
    let backend = Arc::new(BlockingRenewalBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        conditional_puts: std::sync::atomic::AtomicUsize::new(0),
        renewal_seen: std::sync::Mutex::new(Some(release_seen_tx)),
        allow_renewal: std::sync::Mutex::new(allow_release_rx),
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        backend,
        "midge".to_string(),
    ));
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let epoch = lease.epoch();
    let store = lease.get_leader_store().expect("leader store");
    let releasing = {
        let lease = Arc::clone(&lease);
        std::thread::spawn(move || lease.release())
    };
    release_seen_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("release CAS reached backend");
    let generation = test_metadata_generation(20);
    store
        .publish_committed_metadata(
            &lease.holder_id(),
            epoch,
            None,
            generation.clone(),
            Duration::from_secs(5),
        )
        .expect("pointer commit wins first CAS");

    // Act
    allow_release_tx.send(()).expect("release release CAS");
    let released = releasing.join().expect("release thread");

    // Assert
    assert!(released.is_ok(), "{released:?}");
    assert_eq!(lease.epoch(), 0);
    assert!(lease.validity.remaining(epoch).is_err());
    let persisted = provider_read_doc(&cloud).unwrap().expect("lease document");
    assert_eq!(persisted.committed_metadata, Some(generation));
    assert!(persisted
        .is_expired_with_tolerance(lease.clock_skew_tolerance)
        .unwrap());
}

#[test]
fn should_deactivate_local_validity_when_provider_release_fails() {
    // Arrange
    let backend = Arc::new(FlakyGetBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        fail_next_get: std::sync::atomic::AtomicBool::new(false),
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        Arc::clone(&backend) as Arc<dyn crate::storage::cloud::CloudBackend>,
        "midge".to_string(),
    ));
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        Arc::clone(&cloud),
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let epoch = lease.epoch();
    backend.fail_next_get.store(true, Ordering::Release);

    // Act
    let failed = lease.release();

    // Assert
    assert!(matches!(failed, Err(LeaseError::IoError(_))), "{failed:?}");
    assert_eq!(lease.epoch(), 0);
    assert!(lease.validity.remaining(epoch).is_err());
    lease.release().expect("retry provider lease release");
    let persisted = provider_read_doc(&cloud).unwrap().expect("lease document");
    assert!(persisted
        .is_expired_with_tolerance(lease.clock_skew_tolerance)
        .expect("released lease expiry"));
}

#[test]
fn should_release_provider_lease_after_terminal_renewal_failure() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = Arc::new(
        CloudStorageLease::new_provider_backed_with_clock_skew_tolerance_and_ttl(
            test_config(),
            temp_cache_path(),
            Arc::clone(&cloud),
            Duration::from_secs(1),
            Duration::from_secs(10),
        ),
    );
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let epoch = lease.epoch();

    // Act: the short test TTL cannot meet the provider renewal write margin.
    let failed_renewal = lease.renew();
    let released = lease.release();

    // Assert
    assert!(
        matches!(failed_renewal, Err(LeaseError::RenewalFailed(_))),
        "short TTL must fail renewal: {failed_renewal:?}"
    );
    assert_eq!(lease.epoch(), 0);
    assert!(lease.validity.remaining(epoch).is_err());
    released.expect("release must conditionally expire the old lease");
    let persisted = provider_read_doc(&cloud).unwrap().expect("lease document");
    assert!(persisted
        .is_expired_with_tolerance(lease.clock_skew_tolerance)
        .expect("released lease expiry"));
}

#[test]
fn should_reject_noncanonical_v2_lease_documents() {
    // Arrange
    let document = LeaseDocument {
        version: LeaseDocumentVersion::V2,
        epoch: Some(8),
        holder_id: "holder@host".to_string(),
        owner_token: Some("owner-token".to_string()),
        acquired_at: "2026-09-28T12:00:00Z".to_string(),
        expires_at: "2026-09-28T12:00:30Z".to_string(),
        committed_metadata: Some(test_metadata_generation(4)),
    };
    let wire = format_lease_document(&document);

    // Act
    let duplicate = wire.replacen(
        "holder: holder@host\n",
        "holder: holder@host\nholder: forged\n",
        1,
    );
    let unknown = wire.replacen(
        "owner: owner-token\n",
        "owner: owner-token\nunknown: value\n",
        1,
    );
    let altered_version = wire.replacen("lease_version: 2", "lease_version: 3", 1);

    // Assert
    assert_eq!(parse_lease_document(&wire), Some(document));
    assert!(parse_legacy_lease_document(&wire).is_none());
    assert!(parse_lease_document(&duplicate).is_none());
    assert!(parse_lease_document(&unknown).is_none());
    assert!(parse_lease_document(&altered_version).is_none());
}

#[test]
fn should_reject_metadata_descriptor_with_duplicate_names_or_unsafe_keys() {
    // Arrange
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::with_mock());
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let store = lease.get_leader_store().expect("leader store");
    let mut duplicate = test_metadata_generation(2);
    duplicate.objects.push(duplicate.objects[0].clone());
    let mut unsafe_key = test_metadata_generation(2);
    unsafe_key.objects[0].object_key = "metadata/generations/../manifest.snapshot.json".into();
    let mut incomplete = test_metadata_generation(2);
    incomplete
        .objects
        .retain(|object| object.file_name != crate::metadata::files::FORMAT);

    // Act
    let duplicate_result = store.publish_committed_metadata(
        &lease.holder_id(),
        lease.epoch(),
        None,
        duplicate,
        Duration::from_secs(1),
    );
    let unsafe_result = store.publish_committed_metadata(
        &lease.holder_id(),
        lease.epoch(),
        None,
        unsafe_key,
        Duration::from_secs(1),
    );
    let incomplete_result = store.publish_committed_metadata(
        &lease.holder_id(),
        lease.epoch(),
        None,
        incomplete,
        Duration::from_secs(1),
    );

    // Assert
    assert!(matches!(duplicate_result, Err(LeaseError::Internal(_))));
    assert!(matches!(unsafe_result, Err(LeaseError::Internal(_))));
    assert!(matches!(incomplete_result, Err(LeaseError::Internal(_))));
    assert_eq!(
        store
            .read_committed_metadata(Duration::from_secs(1))
            .unwrap(),
        CloudMetadataHead::Uncommitted
    );
}

#[test]
fn should_persist_expiry_not_before_local_validity_when_ttl_has_fractional_seconds() {
    // Arrange: the holder trusts its lease for the full TTL, so the persisted
    // expiry other writers read must not be earlier, or another writer can
    // take over while this one still accepts writes.
    let dir = tempfile::tempdir().expect("lease dir");
    let ttl = Duration::from_millis(1500);
    let lease = Arc::new(CloudStorageLease::new_with_clock_skew_tolerance_and_ttl(
        test_config(),
        dir.path(),
        Duration::ZERO,
        ttl,
    ));

    // Act
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");

    // Assert
    let stored = std::fs::read_to_string(dir.path().join(LEASE_OBJECT_KEY)).expect("lease doc");
    let doc = parse_lease_document(&stored).expect("parse lease doc");
    let acquired = chrono::DateTime::parse_from_rfc3339(&doc.acquired_at).expect("acquired_at");
    let expires = chrono::DateTime::parse_from_rfc3339(&doc.expires_at).expect("expires_at");
    let persisted = (expires - acquired)
        .to_std()
        .expect("positive lease duration");
    assert!(
        persisted >= ttl,
        "persisted lease duration {persisted:?} is shorter than the holder's validity {ttl:?}"
    );
}

/// Backend whose next lease GET fails with a provider outage once armed.
struct FlakyGetBackend {
    inner: crate::storage::cloud::MockCloudBackend,
    fail_next_get: std::sync::atomic::AtomicBool,
}

impl FlakyGetBackend {
    fn take_failure(&self, key: &str, callback: &crate::storage::cloud::CloudCallback) -> bool {
        if !self.fail_next_get.swap(false, Ordering::AcqRel) {
            return false;
        }
        let _ = callback.send(crate::storage::cloud::CloudEvent::Get {
            key: key.to_string(),
            result: Err(crate::storage::cloud::CloudError::ServerError(
                "503 Service Unavailable".to_string(),
            )),
        });
        true
    }
}

impl crate::storage::cloud::CloudBackend for FlakyGetBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_put(&self.inner, key, data, headers, callback);
    }

    fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        if !self.take_failure(key, &callback) {
            crate::storage::cloud::CloudBackend::submit_get(&self.inner, key, callback);
        }
    }

    fn submit_get_with_metadata(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        if !self.take_failure(key, &callback) {
            crate::storage::cloud::CloudBackend::submit_get_with_metadata(
                &self.inner,
                key,
                callback,
            );
        }
    }

    fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_get_range(
            &self.inner,
            key,
            start,
            end,
            callback,
        );
    }

    fn submit_delete(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: crate::storage::cloud::CloudCallback,
    ) {
        crate::storage::cloud::CloudBackend::submit_delete(&self.inner, key, headers, callback);
    }

    fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_list(&self.inner, prefix, callback);
    }

    fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
        crate::storage::cloud::CloudBackend::submit_head(&self.inner, key, callback);
    }
}

#[test]
fn should_keep_cloud_lease_active_when_renewal_read_fails_transiently() {
    // Arrange
    let backend = Arc::new(FlakyGetBackend {
        inner: crate::storage::cloud::MockCloudBackend::new(),
        fail_next_get: std::sync::atomic::AtomicBool::new(false),
    });
    let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
        Arc::clone(&backend) as Arc<dyn crate::storage::cloud::CloudBackend>,
        "midge".to_string(),
    ));
    let lease = Arc::new(CloudStorageLease::new_provider_backed(
        test_config(),
        temp_cache_path(),
        cloud,
    ));
    let _guard = Arc::clone(&lease).try_acquire().expect("acquire lease");
    let epoch = lease.epoch();
    backend.fail_next_get.store(true, Ordering::Release);

    // Act
    let failed = lease.renew();
    let retried = lease.renew();

    // Assert: nothing was written, so the lease stays valid for a retry.
    assert!(
        matches!(
            failed,
            Err(LeaseError::IoError(_) | LeaseError::Indeterminate(_))
        ),
        "{failed:?}"
    );
    assert!(retried.is_ok(), "{retried:?}");
    assert!(lease.acquired.load(Ordering::Acquire));
    assert!(lease.validity.remaining(epoch).is_ok());
}
