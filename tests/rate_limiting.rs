//! Tests for rate limiting functionality.
//!
//! These tests verify the token bucket rate limiter used for controlling
//! I/O throughput during cloud uploads and compaction operations.

use cntryl_midge::common::rate_limiter::RateLimiter;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// =============================================================================
// BASIC RATE LIMITER TESTS
// =============================================================================

#[test]
fn should_create_limiter_given_rate_and_burst_when_constructing() {
    // Arrange
    let rate = 1_000_000; // 1 MB/s
    let burst = 100_000; // 100 KB

    // Act
    let limiter = RateLimiter::new(rate, burst);

    // Assert
    assert_eq!(limiter.bytes_per_sec(), rate);
}

#[test]
fn should_create_unlimited_limiter_given_zero_rate_when_constructing() {
    // Arrange - nothing to set up

    // Act
    let limiter = RateLimiter::unlimited();

    // Assert
    assert_eq!(limiter.bytes_per_sec(), 0); // 0 means unlimited
}

#[test]
fn should_grant_request_immediately_given_unlimited_limiter_when_requesting() {
    // Arrange
    let limiter = RateLimiter::unlimited();
    let start = Instant::now();

    // Act - request a large amount
    limiter.request(100_000_000); // 100 MB

    // Assert - should complete immediately
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(10),
        "unlimited limiter should not block, took {:?}",
        elapsed
    );
}

#[test]
fn should_grant_small_request_given_within_burst_when_requesting() {
    // Arrange
    let limiter = RateLimiter::new(1_000_000, 10_000); // 1 MB/s, 10 KB burst
    let start = Instant::now();

    // Act - request within burst capacity
    limiter.request(5_000); // 5 KB

    // Assert - should complete quickly (within burst)
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(50),
        "small request should be fast, took {:?}",
        elapsed
    );
}

#[test]
fn should_delay_request_given_rate_exceeded_when_requesting_large_amount() {
    // Arrange
    let limiter = RateLimiter::new(10_000, 1_000); // 10 KB/s, 1 KB burst (very slow for testing)

    // Exhaust burst capacity
    limiter.request(1_000);

    let start = Instant::now();

    // Act - request more than available (should delay)
    limiter.request(1_000); // Need to wait for refill

    // Assert - should have some delay
    let elapsed = start.elapsed();
    // At 10 KB/s, 1 KB takes ~100ms
    // We're not testing exact timing, just that delay occurred
    assert!(
        elapsed >= Duration::from_millis(50),
        "should have delayed for refill, took {:?}",
        elapsed
    );
}

// =============================================================================
// DYNAMIC RATE UPDATE TESTS
// =============================================================================

#[test]
fn should_update_rate_given_set_bytes_per_sec_when_adjusting_dynamically() {
    // Arrange
    let limiter = RateLimiter::new(100_000, 10_000); // 100 KB/s
    assert_eq!(limiter.bytes_per_sec(), 100_000);

    // Act
    limiter.set_bytes_per_sec(500_000); // Change to 500 KB/s

    // Assert
    assert_eq!(limiter.bytes_per_sec(), 500_000);
}

#[test]
fn should_switch_to_unlimited_given_zero_rate_when_updating() {
    // Arrange
    let limiter = RateLimiter::new(100_000, 10_000);

    // Act - set to unlimited
    limiter.set_bytes_per_sec(0);
    let start = Instant::now();
    limiter.request(10_000_000); // 10 MB

    // Assert - should be fast now
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(10),
        "should be unlimited now, took {:?}",
        elapsed
    );
}

// =============================================================================
// REQUEST WITH TIMEOUT TESTS
// =============================================================================

#[test]
fn should_succeed_given_timeout_not_exceeded_when_requesting_with_timeout() {
    // Arrange
    let limiter = RateLimiter::new(100_000, 50_000); // 100 KB/s, 50 KB burst

    // Act
    let result = limiter.request_with_timeout(10_000, Duration::from_secs(5));

    // Assert
    assert!(result, "should succeed within timeout");
}

#[test]
fn should_fail_given_timeout_exceeded_when_rate_too_slow() {
    // Arrange - very slow rate
    let limiter = RateLimiter::new(100, 10); // 100 bytes/s, 10 byte burst

    // Exhaust burst
    limiter.request(10);

    // Act - request large amount with short timeout
    let result = limiter.request_with_timeout(10_000, Duration::from_millis(50));

    // Assert - should timeout (10KB at 100 bytes/s = 100 seconds)
    assert!(!result, "should timeout when rate is too slow");
}

// =============================================================================
// AVAILABLE TOKENS TESTS
// =============================================================================

#[test]
fn should_report_available_tokens_given_no_requests_when_querying() {
    // Arrange
    let limiter = RateLimiter::new(1_000_000, 50_000); // 1 MB/s, 50 KB burst

    // Act
    let available = limiter.available_tokens();

    // Assert - should have burst capacity available
    assert!(
        available > 0 && available <= 50_000,
        "should have burst tokens available: {}",
        available
    );
}

#[test]
fn should_decrease_available_given_request_consumed_when_checking_tokens() {
    // Arrange
    let limiter = RateLimiter::new(1_000_000, 50_000);
    let initial = limiter.available_tokens();

    // Act
    limiter.request(10_000);
    let after = limiter.available_tokens();

    // Assert - tokens should have decreased
    // Note: Due to concurrent refill, might not be exact
    assert!(
        after < initial || after == 0,
        "available should decrease from {} to {}",
        initial,
        after
    );
}

// =============================================================================
// CONCURRENT ACCESS TESTS
// =============================================================================

#[test]
fn should_handle_concurrent_requests_given_multiple_threads_when_requesting() {
    // Arrange
    let limiter = Arc::new(RateLimiter::new(10_000_000, 1_000_000)); // 10 MB/s
    let num_threads = 4;
    let requests_per_thread = 100;

    // Act - concurrent requests
    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let limiter = Arc::clone(&limiter);
            thread::spawn(move || {
                for _ in 0..requests_per_thread {
                    limiter.request(1000); // 1 KB each
                }
            })
        })
        .collect();

    // Wait for all threads
    for handle in handles {
        handle.join().expect("thread join");
    }

    // Assert - limiter should still function
    assert!(limiter.bytes_per_sec() == 10_000_000);
}

#[test]
fn should_enforce_rate_globally_given_concurrent_threads_when_measuring() {
    // Arrange - slower rate to make measurement more reliable
    let limiter = Arc::new(RateLimiter::new(50_000, 10_000)); // 50 KB/s
    let total_bytes = 20_000; // 20 KB total

    // Act - measure time to transfer with concurrency
    let start = Instant::now();
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let limiter = Arc::clone(&limiter);
            thread::spawn(move || {
                // Each thread requests portion of total
                limiter.request(total_bytes / 4);
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("join");
    }
    let elapsed = start.elapsed();

    // Assert - should take approximately expected time
    // 20 KB at 50 KB/s = 400ms minimum (minus burst)
    // With 10 KB burst, need to rate-limit 10 KB = 200ms
    // Give wide tolerance for timing variance
    assert!(
        elapsed >= Duration::from_millis(100),
        "should take some time for rate limiting, took {:?}",
        elapsed
    );
}

// =============================================================================
// REFILL BEHAVIOR TESTS
// =============================================================================

#[test]
fn should_refill_tokens_given_time_elapsed_when_waiting() {
    // Arrange
    let limiter = RateLimiter::new(100_000, 10_000); // 100 KB/s, 10 KB burst

    // Consume all burst tokens
    limiter.request(10_000);
    let empty = limiter.available_tokens();
    assert!(empty < 10_000, "should have consumed tokens: {}", empty);

    // Act - wait for refill
    thread::sleep(Duration::from_millis(150));

    // Assert - should have refilled some tokens
    let refilled = limiter.available_tokens();
    // At 100 KB/s, 150ms = 15 KB refill, capped at burst 10 KB
    assert!(
        refilled > empty,
        "should have refilled: {} -> {}",
        empty,
        refilled
    );
}

// =============================================================================
// EDGE CASE TESTS
// =============================================================================

#[test]
fn should_handle_zero_byte_request_given_empty_request_when_calling() {
    // Arrange
    let limiter = RateLimiter::new(100_000, 10_000);
    let start = Instant::now();

    // Act
    limiter.request(0);

    // Assert - should complete immediately
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_millis(5));
}

#[test]
fn should_handle_very_large_request_given_multi_mb_when_requesting() {
    // Arrange
    let limiter = RateLimiter::new(100_000_000, 10_000_000); // 100 MB/s, 10 MB burst
    let start = Instant::now();

    // Act - request within burst
    limiter.request(5_000_000); // 5 MB

    // Assert
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(100),
        "large request within burst should be fast: {:?}",
        elapsed
    );
}

#[test]
fn should_use_default_unlimited_given_default_constructor_when_creating() {
    // Arrange - nothing to set up

    // Act
    let limiter = RateLimiter::default();

    // Assert - default should be unlimited
    assert_eq!(limiter.bytes_per_sec(), 0);
}

// =============================================================================
// SHARED/CLONE TESTS
// =============================================================================

#[test]
fn should_share_state_given_cloned_limiter_when_requesting() {
    // Arrange
    let limiter1 = RateLimiter::new(100_000, 20_000);
    let limiter2 = limiter1.clone();

    // Act - consume tokens via limiter1
    limiter1.request(15_000);

    // Assert - limiter2 sees reduced tokens (they share state)
    let available = limiter2.available_tokens();
    assert!(
        available < 20_000,
        "cloned limiter should share state: {}",
        available
    );
}

#[test]
fn should_reflect_rate_change_given_cloned_limiter_when_updating() {
    // Arrange
    let limiter1 = RateLimiter::new(100_000, 10_000);
    let limiter2 = limiter1.clone();

    // Act
    limiter1.set_bytes_per_sec(500_000);

    // Assert - clone sees the change
    assert_eq!(limiter2.bytes_per_sec(), 500_000);
}
