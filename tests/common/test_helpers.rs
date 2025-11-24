use std::sync::mpsc::Receiver;
use std::time::Duration;

/// Recommended default timeout for channel receives in tests.
/// Keep this moderate to allow CI noise while failing reasonably quickly.
#[allow(dead_code)]
pub const TEST_RECV_TIMEOUT: Duration = Duration::from_secs(5);

/// Recommended default timeout for gates and similar waits.
#[allow(dead_code)]
pub const TEST_GATE_TIMEOUT: Duration = Duration::from_secs(10);

/// Wait for a signal on an `mpsc::Receiver` using a standard test timeout.
/// Returns the received value or panics with a descriptive message on timeout.
#[allow(dead_code)]
pub fn wait_for_signal<T>(rx: &Receiver<T>, timeout: Duration) -> T {
    rx.recv_timeout(timeout)
        .expect(&format!("Timed out waiting for test signal after {:?}", timeout))
}

/// Convenience wrapper using the default `TEST_RECV_TIMEOUT`.
#[allow(dead_code)]
pub fn wait_for_signal_default<T>(rx: &Receiver<T>) -> T {
    wait_for_signal(rx, TEST_RECV_TIMEOUT)
}
