//! Completion ownership must outlive the synchronous adapter's timeout.

use super::*;
use crate::common::resource_budget::ResourceBudget;
use std::sync::{mpsc, Mutex};

#[derive(Default)]
struct PendingPut(Mutex<Option<(Vec<u8>, CloudCallback)>>);

impl CloudBackend for PendingPut {
    fn submit_put(
        &self,
        _key: &str,
        data: Vec<u8>,
        _headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((data, callback));
    }

    fn submit_get_range(
        &self,
        _key: &str,
        _start: u64,
        _end: Option<u64>,
        _callback: CloudCallback,
    ) {
        unreachable!("PUT-only test backend")
    }
}

#[test]
fn should_keep_upload_charged_when_callback_adapter_times_out_before_backend_completion() {
    // Arrange
    let backend = Arc::new(PendingPut::default());
    let cloud = CloudStorage::new(backend.clone(), String::new());
    let budget = ResourceBudget::new(128 * 1024);
    let reservation = Arc::new(budget.reserve(1024, "test upload").unwrap());
    let (tx, rx) = mpsc::channel();

    // Act
    cloud.submit_write_with_reservation(
        "object",
        vec![7; 128],
        Vec::new(),
        std::time::Duration::from_millis(1),
        reservation,
        tx,
    );
    let result = rx.recv().unwrap();
    let retained_after_timeout = budget.used();
    drop(
        backend
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take(),
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while budget.used() != 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }

    // Assert
    assert!(matches!(
        result,
        StorageEvent::WriteComplete {
            result: StorageOutcome::Err(_),
            ..
        }
    ));
    assert_eq!(retained_after_timeout, 1024 + 64 * 1024);
    assert_eq!(budget.used(), 0);
}
