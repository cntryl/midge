use std::any::Any;

/// Spawn a background thread that protects the provided closure from unwinding
/// into the test runner. On panic the optional `hooks` will be notified via
/// `record_worker_panic(kind)`. An optional `on_panic` callback can be provided
/// to perform custom cleanup (e.g., completing a promise) when a panic occurs.
pub fn spawn_guarded<F, P>(
    kind: &str,
    hooks: Option<crate::common::test_hooks::TestHooks>,
    f: F,
    on_panic: Option<P>,
) -> std::thread::JoinHandle<()>
where
    F: FnOnce() + Send + 'static,
    P: FnOnce(Box<dyn Any + Send>) + Send + 'static,
{
    let kind_owned = kind.to_string();
    std::thread::spawn(move || {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        if let Err(payload) = r {
            eprintln!(
                "[worker-guard] worker '{}' panicked: {:?}",
                kind_owned, payload
            );
            if let Some(h) = hooks {
                h.record_worker_panic(&kind_owned);
            }
            if let Some(cb) = on_panic {
                cb(payload);
            }
            // Swallow the panic to avoid aborting the test runner.
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn spawn_guarded_calls_on_panic_and_records_hook() {
        let hooks = crate::common::test_hooks::TestHooks::new();
        let hooks_clone = hooks.clone();
        let called = Arc::new(AtomicBool::new(false));
        let called2 = called.clone();

        let handle = spawn_guarded(
            "test-kind",
            Some(hooks_clone),
            move || panic!("boom"),
            Some(move |_payload| {
                called2.store(true, Ordering::SeqCst);
            }),
        );

        handle.join().unwrap();
        assert!(called.load(Ordering::SeqCst));
        assert!(hooks.worker_panic_count() >= 1);
    }
}
