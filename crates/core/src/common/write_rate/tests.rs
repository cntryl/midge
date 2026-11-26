use super::*;

#[test]
fn should_release_slow_worker_promptly_when_hard_pressure_requires_progress() {
    // Arrange
    let urgent = Arc::new(AtomicBool::new(false));
    let limiter = Arc::new(BackgroundWriteLimiter::new(
        1,
        urgent.clone(),
        Arc::default(),
    ));
    let worker_limiter = limiter.clone();
    let worker = std::thread::spawn(move || worker_limiter.acquire(64 * 1024));
    let wait = Instant::now() + Duration::from_secs(2);
    while *limiter.next.lock().unwrap() <= Instant::now() {
        assert!(
            Instant::now() < wait,
            "worker must reserve its rate interval"
        );
        std::thread::yield_now();
    }

    // Act
    let released = Instant::now();
    urgent.store(true, Ordering::Release);
    worker.join().unwrap().unwrap();

    // Assert: the configured eighteen-hour byte allowance does not hold a
    // hard-stalled flush or shutdown drain hostage.
    assert!(released.elapsed() < Duration::from_secs(1));
}
