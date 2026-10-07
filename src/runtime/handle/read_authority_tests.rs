use super::*;
use std::sync::atomic::AtomicBool;

fn live_handle() -> (
    RuntimeHandle,
    Arc<crate::lease::LeaseValidity>,
    Arc<AtomicBool>,
) {
    let (_, mut handle) = crate::runtime::Runtime::new();
    let validity = Arc::new(crate::lease::LeaseValidity::new());
    validity
        .activate(7, std::time::Instant::now() + Duration::from_mins(1))
        .unwrap();
    let healthy = Arc::new(AtomicBool::new(true));
    handle.read_authority = Some(CloudReadAuthority {
        healthy: Some(Arc::clone(&healthy)),
        validity: Some(Arc::clone(&validity)),
        epoch: 7,
    });
    (handle, validity, healthy)
}

#[test]
fn should_discard_read_outcomes_when_lease_expires_during_the_read() {
    // Arrange: include a stale storage error and end-of-range as well as a row.
    for outcome in [Ok(Some(42)), Ok(None), Err(MidgeError::NotFound)] {
        let (handle, validity, healthy) = live_handle();

        // Act: authority expires inside the read, before the caller receives it.
        let result = handle.read_with_authority(|| {
            validity.expire_for_test();
            outcome
        });

        // Assert: notification is unnecessary, and stale I/O errors do not escape.
        assert!(healthy.load(Ordering::Acquire));
        assert!(matches!(result, Err(MidgeError::Fenced(_))), "{result:?}");
        assert!(matches!(
            handle.ensure_read_authority(),
            Err(MidgeError::Fenced(_))
        ));
    }
}

#[test]
fn should_reject_read_without_io_when_heartbeat_is_unhealthy() {
    // Arrange: monotonic validity alone must not resurrect a lost holder.
    let (handle, validity, healthy) = live_handle();
    healthy.store(false, Ordering::Release);
    assert!(validity.remaining(7).is_ok());

    // Act
    let result: MidgeResult<()> = handle.read_with_authority(|| panic!("fenced read reached I/O"));

    // Assert
    assert!(matches!(result, Err(MidgeError::Fenced(_))));
}

#[test]
fn should_preserve_read_outcome_when_cloud_lease_remains_valid() {
    // Arrange
    let (handle, _, _) = live_handle();

    // Act
    let row = handle.read_with_authority(|| Ok(Some(42)));
    let error: MidgeResult<()> = handle.read_with_authority(|| Err(MidgeError::NotFound));

    // Assert
    assert_eq!(row.unwrap(), Some(42));
    assert!(matches!(error, Err(MidgeError::NotFound)));
}
