//! Test demonstrating deadlock detector usage

mod common;

use common::deadlock_detector::{assert_completes_within, DeadlockDetector, StressTestCoordinator};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn should_detect_fast_completion() {
    let _detector = DeadlockDetector::new("fast_test", Duration::from_secs(5));
    
    // Fast operation
    std::thread::sleep(Duration::from_millis(10));
    
    // Detector is dropped here, test passes without warnings
}

#[test]
fn should_use_with_timeout_assertion() {
    // This will panic if operation takes >100ms
    let result = assert_completes_within(Duration::from_millis(100), || {
        std::thread::sleep(Duration::from_millis(10));
        42
    });
    
    assert_eq!(result, 42);
}

#[test]
fn should_run_stress_test_without_deadlock() {
    let coordinator = StressTestCoordinator::new(
        "concurrent_counter",
        10, // 10 threads
        Duration::from_secs(5)
    );
    
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    
    coordinator.run_concurrent(10, move || {
        for _ in 0..1000 {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        }
    });
    
    assert_eq!(counter.load(Ordering::Relaxed), 10_000);
    println!("Stress test completed in {:?}", coordinator.elapsed());
}

#[test]
#[ignore] // Ignored by default - this test intentionally hangs to demonstrate detector
fn should_warn_on_potential_deadlock() {
    let _detector = DeadlockDetector::new("hanging_test", Duration::from_secs(2));
    
    // Simulate a deadlock
    std::thread::sleep(Duration::from_secs(5));
}
