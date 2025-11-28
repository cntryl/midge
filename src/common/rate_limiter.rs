use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// Rate limiter for controlling I/O throughput.
///
/// Uses a token bucket algorithm to limit bytes per second.
/// Threads can request tokens and will be delayed if the rate limit is exceeded.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    inner: Arc<RateLimiterInner>,
}

#[derive(Debug)]
struct RateLimiterInner {
    /// Maximum bytes per second (0 = unlimited)
    bytes_per_sec: AtomicU64,
    /// Available tokens (in bytes)
    available_tokens: AtomicU64,
    /// Last refill timestamp (microseconds since epoch)
    last_refill_micros: AtomicU64,
    /// Maximum burst size (bytes)
    max_burst_bytes: u64,
}

impl RateLimiter {
    /// Create a new rate limiter with the specified bytes per second limit.
    ///
    /// # Arguments
    /// * `bytes_per_sec` - Maximum bytes per second (0 = unlimited)
    /// * `max_burst_bytes` - Maximum burst size in bytes (allows temporary spikes)
    pub fn new(bytes_per_sec: u64, max_burst_bytes: u64) -> Self {
        let now = Self::now_micros();
        Self {
            inner: Arc::new(RateLimiterInner {
                bytes_per_sec: AtomicU64::new(bytes_per_sec),
                available_tokens: AtomicU64::new(max_burst_bytes),
                last_refill_micros: AtomicU64::new(now),
                max_burst_bytes,
            }),
        }
    }

    /// Create an unlimited rate limiter (no throttling)
    #[inline]
    pub fn unlimited() -> Self {
        Self::new(0, 0)
    }

    /// Request permission to perform an I/O operation of the given size.
    ///
    /// Returns immediately if unlimited or tokens available.
    /// Sleeps if rate limit exceeded to enforce the limit.
    ///
    /// # Arguments
    /// * `bytes` - Number of bytes to request
    #[inline]
    pub fn request(&self, bytes: u64) {
        let limit = self.inner.bytes_per_sec.load(Ordering::Relaxed);

        // Unlimited - return immediately
        if limit == 0 {
            return;
        }

        // Refill tokens based on elapsed time
        self.refill();

        // Try to consume tokens
        loop {
            let available = self.inner.available_tokens.load(Ordering::Relaxed);

            if available >= bytes {
                // Enough tokens available - try to consume
                if self
                    .inner
                    .available_tokens
                    .compare_exchange(
                        available,
                        available - bytes,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    // Successfully consumed tokens
                    return;
                }
                // CAS failed, retry
            } else {
                // Not enough tokens - calculate sleep time
                let deficit = bytes.saturating_sub(available);
                let sleep_micros = (deficit * 1_000_000) / limit;

                // Cap sleep time to avoid excessive delays
                let sleep_micros = sleep_micros.min(1_000_000); // Max 1 second

                std::thread::sleep(Duration::from_micros(sleep_micros));

                // Refill after sleep
                self.refill();
            }
        }
    }

    /// Request with a timeout. Returns true if granted, false if timed out.
    pub fn request_with_timeout(&self, bytes: u64, timeout: Duration) -> bool {
        let limit = self.inner.bytes_per_sec.load(Ordering::Relaxed);

        // Unlimited - return immediately
        if limit == 0 {
            return true;
        }

        let start = Instant::now();

        // Refill tokens
        self.refill();

        // Try to consume tokens with timeout
        while start.elapsed() < timeout {
            let available = self.inner.available_tokens.load(Ordering::Relaxed);

            if available >= bytes {
                if self
                    .inner
                    .available_tokens
                    .compare_exchange(
                        available,
                        available - bytes,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return true;
                }
            } else {
                // Calculate sleep time, but respect timeout
                let deficit = bytes.saturating_sub(available);
                let sleep_micros = (deficit * 1_000_000) / limit;
                let sleep_duration = Duration::from_micros(sleep_micros.min(100_000)); // Max 100ms chunks

                let remaining = timeout.saturating_sub(start.elapsed());
                let sleep_duration = sleep_duration.min(remaining);

                if sleep_duration.is_zero() {
                    return false; // Timeout
                }

                std::thread::sleep(sleep_duration);
                self.refill();
            }
        }

        false // Timeout
    }

    /// Update the rate limit dynamically
    pub fn set_bytes_per_sec(&self, bytes_per_sec: u64) {
        self.inner
            .bytes_per_sec
            .store(bytes_per_sec, Ordering::Relaxed);
    }

    /// Get current rate limit
    pub fn bytes_per_sec(&self) -> u64 {
        self.inner.bytes_per_sec.load(Ordering::Relaxed)
    }

    /// Get current available tokens
    pub fn available_tokens(&self) -> u64 {
        self.inner.available_tokens.load(Ordering::Relaxed)
    }

    /// Refill tokens based on elapsed time since last refill
    #[inline]
    fn refill(&self) {
        let now = Self::now_micros();
        let last = self.inner.last_refill_micros.load(Ordering::Relaxed);

        if now <= last {
            return; // Clock skew or no time elapsed
        }

        let elapsed_micros = now - last;

        // Try to update last refill time
        if self
            .inner
            .last_refill_micros
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return; // Another thread updated, let them handle refill
        }

        // Calculate tokens to add
        let limit = self.inner.bytes_per_sec.load(Ordering::Relaxed);
        if limit == 0 {
            return; // Unlimited
        }

        let tokens_to_add = (limit * elapsed_micros) / 1_000_000;

        if tokens_to_add == 0 {
            return; // Not enough time elapsed
        }

        // Add tokens (capped at max burst)
        loop {
            let current = self.inner.available_tokens.load(Ordering::Relaxed);
            let new_tokens = (current + tokens_to_add).min(self.inner.max_burst_bytes);

            if self
                .inner
                .available_tokens
                .compare_exchange(current, new_tokens, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Get current time in microseconds
    #[inline]
    fn now_micros() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::unlimited()
    }
}

// Global rate limiter accessor for components that want a simple shared
// throttling policy (e.g. cloud upload paths). By default this is unlimited.
static GLOBAL_RATE_LIMITER: OnceLock<Arc<RateLimiter>> = OnceLock::new();

/// Set the global rate limiter. Subsequent calls will be ignored; call this
/// early during initialization if you want to enable throttling globally.
pub fn set_global_rate_limiter(limiter: Arc<RateLimiter>) {
    let _ = GLOBAL_RATE_LIMITER.set(limiter);
}

/// Get the global rate limiter. If none was set, returns an unlimited limiter.
pub fn global_rate_limiter() -> Arc<RateLimiter> {
    GLOBAL_RATE_LIMITER
        .get()
        .cloned()
        .unwrap_or_else(|| Arc::new(RateLimiter::unlimited()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_allow_unlimited_requests_when_unlimited() {
        // Arrange
        let limiter = RateLimiter::unlimited();

        // Act
        let start = Instant::now();
        limiter.request(1_000_000_000);
        let elapsed = start.elapsed();

        // Assert
        assert!(elapsed < Duration::from_millis(10));
    }

    #[test]
    fn should_limit_requests_given_rate_limit_when_burst_exhausted() {
        // Arrange
        let limiter = RateLimiter::new(1_000_000, 1_000_000);

        // Act
        let start = Instant::now();
        limiter.request(500_000);
        let elapsed = start.elapsed();

        limiter.request(500_000);

        let start2 = Instant::now();
        limiter.request(500_000);
        let elapsed2 = start2.elapsed();

        // Assert
        assert!(elapsed < Duration::from_millis(50));
        assert!(elapsed2 >= Duration::from_millis(400));
    }

    #[test]
    fn should_refill_tokens_given_time_passes_when_waiting() {
        // Arrange
        let limiter = RateLimiter::new(1_000_000, 1_000_000);
        limiter.request(1_000_000);

        // Act
        std::thread::sleep(Duration::from_millis(600));
        limiter.refill();
        let available = limiter.available_tokens();

        // Assert
        assert!(
            available >= 300_000,
            "Expected at least 300KB, got {}",
            available
        );
        assert!(
            available <= 800_000,
            "Expected at most 800KB, got {}",
            available
        );
    }

    #[test]
    fn should_apply_new_rate_given_rate_change_when_set_rate_called() {
        // Arrange
        let limiter = RateLimiter::new(1_000_000, 1_000_000);

        // Act
        limiter.set_bytes_per_sec(2_000_000);
        let new_rate = limiter.bytes_per_sec();

        limiter.set_bytes_per_sec(0);
        let unlimited_rate = limiter.bytes_per_sec();

        let start = Instant::now();
        limiter.request(10_000_000);
        let elapsed = start.elapsed();

        // Assert
        assert_eq!(new_rate, 2_000_000);
        assert_eq!(unlimited_rate, 0);
        assert!(elapsed < Duration::from_millis(10));
    }

    #[test]
    fn should_respect_timeout_given_insufficient_tokens_when_request_with_timeout() {
        // Arrange
        let limiter = RateLimiter::new(1000, 1000);
        limiter.request_with_timeout(500, Duration::from_millis(100));

        // Act
        let result = limiter.request_with_timeout(1_000, Duration::from_millis(100));

        std::thread::sleep(Duration::from_millis(500));
        let result2 = limiter.request_with_timeout(500, Duration::from_millis(100));

        // Assert
        assert!(!result);
        assert!(result2);
    }

    #[test]
    fn should_handle_concurrent_requests_given_multiple_threads_when_requesting() {
        // Arrange
        use std::sync::Arc;
        use std::thread;

        let limiter = Arc::new(RateLimiter::new(10_000_000, 10_000_000));
        let mut handles = vec![];

        for _ in 0..4 {
            let limiter_clone = limiter.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    limiter_clone.request(100_000);
                }
            }));
        }

        // Act
        for handle in handles {
            handle.join().unwrap();
        }

        // Assert
        assert!(limiter.available_tokens() <= 10_000_000);
    }
}
