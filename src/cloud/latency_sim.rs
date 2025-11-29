//! Realistic cloud latency simulator for MockCloudBackend.
//!
//! Provides configurable latency simulation with:
//! - Base latency + jitter (normal distribution)
//! - Size-based transfer time simulation
//! - Operation-specific latency profiles (read vs write vs list)
//! - Percentile-based tail latency spikes (p99, p99.9)
//! - Optional "zero-cost" mode for benchmarks
//!
//! # Design
//!
//! Real cloud storage latency has several components:
//! 1. **Network RTT** - Base latency to reach the cloud (~5-50ms)
//! 2. **Service processing** - Time for the cloud to process the request (~1-10ms)
//! 3. **Transfer time** - Proportional to data size (bandwidth-limited)
//! 4. **Tail latency** - Occasional spikes due to retries, GC, etc.
//!
//! This simulator models all of these with configurable parameters.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Latency simulation mode for MockCloudBackend.
#[derive(Debug, Clone, Default)]
pub enum LatencyMode {
    /// No latency simulation - operations complete instantly.
    /// Use for pure throughput benchmarks.
    #[default]
    None,

    /// Fixed latency per operation (legacy behavior).
    /// Simple but unrealistic - use for basic testing only.
    Fixed(Duration),

    /// Realistic cloud latency simulation with configurable parameters.
    Realistic(LatencyConfig),
}

/// Configuration for realistic latency simulation.
#[derive(Debug, Clone)]
pub struct LatencyConfig {
    /// Base network round-trip time (default: 5ms for same-region)
    pub base_rtt_us: u64,

    /// Jitter as percentage of base RTT (default: 20% = ±10%)
    /// Actual jitter is uniformly distributed in [-jitter/2, +jitter/2]
    pub jitter_percent: u32,

    /// Simulated bandwidth in bytes per microsecond (default: 100 = ~100MB/s)
    /// Transfer time = size_bytes / bandwidth_bytes_per_us
    pub bandwidth_bytes_per_us: u64,

    /// Additional latency for write operations (default: 2ms)
    /// Writes typically have higher latency due to durability guarantees
    pub write_penalty_us: u64,

    /// Additional latency for list operations (default: 10ms)
    /// List operations scan metadata and are typically slower
    pub list_penalty_us: u64,

    /// Probability of a p99 latency spike (default: 0.01 = 1%)
    /// When triggered, latency is multiplied by `p99_multiplier`
    pub p99_probability: f32,

    /// Multiplier for p99 latency spikes (default: 5x)
    pub p99_multiplier: u32,

    /// Probability of a p99.9 latency spike (default: 0.001 = 0.1%)
    /// When triggered, latency is multiplied by `p999_multiplier`
    pub p999_probability: f32,

    /// Multiplier for p99.9 latency spikes (default: 20x)
    pub p999_multiplier: u32,

    /// Whether to actually sleep or just compute the latency.
    /// Set to false for "accounting only" mode where latency is tracked
    /// but threads aren't blocked. Useful for fast benchmarks that still
    /// want to measure simulated latency.
    pub actually_sleep: bool,
}

impl Default for LatencyConfig {
    fn default() -> Self {
        Self::same_region()
    }
}

impl LatencyConfig {
    /// Preset: Same-region cloud storage (e.g., us-east-1 to S3 in us-east-1)
    /// ~5ms base RTT, high bandwidth
    pub fn same_region() -> Self {
        Self {
            base_rtt_us: 5_000,        // 5ms
            jitter_percent: 20,        // ±10%
            bandwidth_bytes_per_us: 100, // ~100MB/s
            write_penalty_us: 2_000,   // +2ms for writes
            list_penalty_us: 10_000,   // +10ms for list
            p99_probability: 0.01,
            p99_multiplier: 5,
            p999_probability: 0.001,
            p999_multiplier: 20,
            actually_sleep: true,
        }
    }

    /// Preset: Cross-region cloud storage (e.g., us-east-1 to S3 in eu-west-1)
    /// ~80ms base RTT, moderate bandwidth
    pub fn cross_region() -> Self {
        Self {
            base_rtt_us: 80_000,       // 80ms
            jitter_percent: 30,        // ±15%
            bandwidth_bytes_per_us: 50, // ~50MB/s
            write_penalty_us: 10_000,  // +10ms for writes
            list_penalty_us: 30_000,   // +30ms for list
            p99_probability: 0.02,
            p99_multiplier: 4,
            p999_probability: 0.002,
            p999_multiplier: 15,
            actually_sleep: true,
        }
    }

    /// Preset: Fast local simulation (minimal latency for integration tests)
    /// ~100μs base, no sleep - just accounting
    pub fn fast_simulation() -> Self {
        Self {
            base_rtt_us: 100,          // 100μs
            jitter_percent: 10,
            bandwidth_bytes_per_us: 1000, // ~1GB/s (fast local disk)
            write_penalty_us: 50,
            list_penalty_us: 200,
            p99_probability: 0.01,
            p99_multiplier: 3,
            p999_probability: 0.001,
            p999_multiplier: 10,
            actually_sleep: false, // Don't block threads
        }
    }

    /// Preset: Benchmark mode (zero latency, no blocking)
    /// Use when you want to measure pure engine throughput
    pub fn benchmark() -> Self {
        Self {
            base_rtt_us: 0,
            jitter_percent: 0,
            bandwidth_bytes_per_us: u64::MAX,
            write_penalty_us: 0,
            list_penalty_us: 0,
            p99_probability: 0.0,
            p99_multiplier: 1,
            p999_probability: 0.0,
            p999_multiplier: 1,
            actually_sleep: false,
        }
    }

    /// Builder: set base RTT
    pub fn with_base_rtt(mut self, rtt: Duration) -> Self {
        self.base_rtt_us = rtt.as_micros() as u64;
        self
    }

    /// Builder: set bandwidth in MB/s
    pub fn with_bandwidth_mbps(mut self, mbps: u64) -> Self {
        // Convert MB/s to bytes/μs: MB/s * 1_000_000 bytes/MB / 1_000_000 μs/s = bytes/μs
        self.bandwidth_bytes_per_us = mbps;
        self
    }

    /// Builder: enable/disable actual sleeping
    pub fn with_sleep(mut self, sleep: bool) -> Self {
        self.actually_sleep = sleep;
        self
    }
}

/// Latency simulator that computes and optionally applies latency.
///
/// Thread-safe and lock-free for the hot path.
pub struct LatencySimulator {
    config: LatencyConfig,
    /// Simple LCG state for fast, deterministic pseudo-random jitter.
    /// Using atomics for thread-safety without locks.
    rng_state: AtomicU64,
    /// Accumulated simulated latency (for accounting mode)
    total_simulated_us: AtomicU64,
    /// Number of operations simulated
    operation_count: AtomicU64,
}

impl LatencySimulator {
    pub fn new(config: LatencyConfig) -> Self {
        Self {
            config,
            rng_state: AtomicU64::new(0xDEAD_BEEF_CAFE_BABE),
            total_simulated_us: AtomicU64::new(0),
            operation_count: AtomicU64::new(0),
        }
    }

    /// Create a no-op simulator (zero latency, no blocking)
    pub fn none() -> Self {
        Self::new(LatencyConfig::benchmark())
    }

    /// Fast pseudo-random number in [0, bound) using LCG.
    /// Not cryptographically secure, but fast and deterministic.
    #[inline]
    fn next_random(&self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        // LCG parameters from Numerical Recipes
        const A: u64 = 6364136223846793005;
        const C: u64 = 1442695040888963407;

        let old = self.rng_state.fetch_add(1, Ordering::Relaxed);
        let new = old.wrapping_mul(A).wrapping_add(C);
        new % bound
    }

    /// Get a random float in [0, 1) for probability checks
    #[inline]
    fn random_probability(&self) -> f32 {
        (self.next_random(1_000_000) as f32) / 1_000_000.0
    }

    /// Compute latency for a read operation
    #[inline]
    pub fn compute_read_latency(&self, size_bytes: usize) -> Duration {
        self.compute_latency_internal(size_bytes, false, false)
    }

    /// Compute latency for a write operation
    #[inline]
    pub fn compute_write_latency(&self, size_bytes: usize) -> Duration {
        self.compute_latency_internal(size_bytes, true, false)
    }

    /// Compute latency for a list operation
    #[inline]
    pub fn compute_list_latency(&self) -> Duration {
        self.compute_latency_internal(0, false, true)
    }

    /// Compute latency for a metadata operation (head)
    #[inline]
    pub fn compute_head_latency(&self) -> Duration {
        // Head is like a small read
        self.compute_latency_internal(0, false, false)
    }

    #[inline]
    fn compute_latency_internal(
        &self,
        size_bytes: usize,
        is_write: bool,
        is_list: bool,
    ) -> Duration {
        let cfg = &self.config;

        // Base latency
        let mut latency_us = cfg.base_rtt_us;

        // Add jitter: uniform in [-jitter/2, +jitter/2] percent of base
        if cfg.jitter_percent > 0 && cfg.base_rtt_us > 0 {
            let jitter_range = (cfg.base_rtt_us * cfg.jitter_percent as u64) / 100;
            let jitter = self.next_random(jitter_range + 1) as i64 - (jitter_range as i64 / 2);
            latency_us = (latency_us as i64 + jitter).max(0) as u64;
        }

        // Add transfer time based on size
        if size_bytes > 0 && cfg.bandwidth_bytes_per_us > 0 && cfg.bandwidth_bytes_per_us < u64::MAX {
            latency_us += (size_bytes as u64) / cfg.bandwidth_bytes_per_us;
        }

        // Add operation-specific penalties
        if is_write {
            latency_us += cfg.write_penalty_us;
        }
        if is_list {
            latency_us += cfg.list_penalty_us;
        }

        // Check for tail latency spikes
        let prob = self.random_probability();
        if prob < cfg.p999_probability {
            latency_us *= cfg.p999_multiplier as u64;
        } else if prob < cfg.p99_probability {
            latency_us *= cfg.p99_multiplier as u64;
        }

        Duration::from_micros(latency_us)
    }

    /// Simulate latency for a read operation.
    /// If `actually_sleep` is true, blocks the thread.
    /// Always updates accounting counters.
    #[inline]
    pub fn simulate_read(&self, size_bytes: usize) {
        let latency = self.compute_read_latency(size_bytes);
        self.apply_latency(latency);
    }

    /// Simulate latency for a write operation.
    #[inline]
    pub fn simulate_write(&self, size_bytes: usize) {
        let latency = self.compute_write_latency(size_bytes);
        self.apply_latency(latency);
    }

    /// Simulate latency for a list operation.
    #[inline]
    pub fn simulate_list(&self) {
        let latency = self.compute_list_latency();
        self.apply_latency(latency);
    }

    /// Simulate latency for a head/metadata operation.
    #[inline]
    pub fn simulate_head(&self) {
        let latency = self.compute_head_latency();
        self.apply_latency(latency);
    }

    #[inline]
    fn apply_latency(&self, latency: Duration) {
        let us = latency.as_micros() as u64;

        // Update accounting
        self.total_simulated_us.fetch_add(us, Ordering::Relaxed);
        self.operation_count.fetch_add(1, Ordering::Relaxed);

        // Actually sleep if configured
        if self.config.actually_sleep && us > 0 {
            std::thread::sleep(latency);
        }
    }

    /// Get total simulated latency (useful for accounting mode)
    pub fn total_simulated_latency(&self) -> Duration {
        Duration::from_micros(self.total_simulated_us.load(Ordering::Relaxed))
    }

    /// Get number of operations simulated
    pub fn operation_count(&self) -> u64 {
        self.operation_count.load(Ordering::Relaxed)
    }

    /// Get average latency per operation
    pub fn average_latency(&self) -> Duration {
        let count = self.operation_count();
        if count == 0 {
            return Duration::ZERO;
        }
        let total_us = self.total_simulated_us.load(Ordering::Relaxed);
        Duration::from_micros(total_us / count)
    }

    /// Reset accounting counters
    pub fn reset_stats(&self) {
        self.total_simulated_us.store(0, Ordering::Relaxed);
        self.operation_count.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencySimulator {
    fn default() -> Self {
        Self::none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_zero_latency_when_mode_is_benchmark() {
        // Arrange
        let sim = LatencySimulator::new(LatencyConfig::benchmark());

        // Act
        let latency = sim.compute_read_latency(1024);

        // Assert
        assert_eq!(latency, Duration::ZERO);
    }

    #[test]
    fn should_include_base_rtt_in_latency() {
        // Arrange
        let config = LatencyConfig {
            base_rtt_us: 5000,
            jitter_percent: 0, // No jitter for deterministic test
            bandwidth_bytes_per_us: u64::MAX,
            write_penalty_us: 0,
            list_penalty_us: 0,
            p99_probability: 0.0,
            p99_multiplier: 1,
            p999_probability: 0.0,
            p999_multiplier: 1,
            actually_sleep: false,
        };
        let sim = LatencySimulator::new(config);

        // Act
        let latency = sim.compute_read_latency(0);

        // Assert
        assert_eq!(latency, Duration::from_micros(5000));
    }

    #[test]
    fn should_add_write_penalty_for_writes() {
        // Arrange
        let config = LatencyConfig {
            base_rtt_us: 1000,
            jitter_percent: 0,
            bandwidth_bytes_per_us: u64::MAX,
            write_penalty_us: 500,
            list_penalty_us: 0,
            p99_probability: 0.0,
            p99_multiplier: 1,
            p999_probability: 0.0,
            p999_multiplier: 1,
            actually_sleep: false,
        };
        let sim = LatencySimulator::new(config);

        // Act
        let read_latency = sim.compute_read_latency(0);
        let write_latency = sim.compute_write_latency(0);

        // Assert
        assert_eq!(read_latency, Duration::from_micros(1000));
        assert_eq!(write_latency, Duration::from_micros(1500));
    }

    #[test]
    fn should_add_transfer_time_based_on_size() {
        // Arrange
        let config = LatencyConfig {
            base_rtt_us: 1000,
            jitter_percent: 0,
            bandwidth_bytes_per_us: 100, // 100 bytes per μs = 100MB/s
            write_penalty_us: 0,
            list_penalty_us: 0,
            p99_probability: 0.0,
            p99_multiplier: 1,
            p999_probability: 0.0,
            p999_multiplier: 1,
            actually_sleep: false,
        };
        let sim = LatencySimulator::new(config);

        // Act
        // 10KB at 100 bytes/μs = 100μs transfer time
        let latency = sim.compute_read_latency(10_000);

        // Assert
        assert_eq!(latency, Duration::from_micros(1100)); // 1000 base + 100 transfer
    }

    #[test]
    fn should_track_total_simulated_latency() {
        // Arrange
        let config = LatencyConfig {
            base_rtt_us: 1000,
            jitter_percent: 0,
            bandwidth_bytes_per_us: u64::MAX,
            write_penalty_us: 0,
            list_penalty_us: 0,
            p99_probability: 0.0,
            p99_multiplier: 1,
            p999_probability: 0.0,
            p999_multiplier: 1,
            actually_sleep: false, // Don't actually sleep
        };
        let sim = LatencySimulator::new(config);

        // Act
        sim.simulate_read(0);
        sim.simulate_read(0);
        sim.simulate_read(0);

        // Assert
        assert_eq!(sim.operation_count(), 3);
        assert_eq!(sim.total_simulated_latency(), Duration::from_micros(3000));
        assert_eq!(sim.average_latency(), Duration::from_micros(1000));
    }

    #[test]
    fn should_apply_jitter_within_expected_range() {
        // Arrange
        let config = LatencyConfig {
            base_rtt_us: 10_000,
            jitter_percent: 20, // ±10%
            bandwidth_bytes_per_us: u64::MAX,
            write_penalty_us: 0,
            list_penalty_us: 0,
            p99_probability: 0.0,
            p99_multiplier: 1,
            p999_probability: 0.0,
            p999_multiplier: 1,
            actually_sleep: false,
        };
        let sim = LatencySimulator::new(config);

        // Act
        let mut min_latency = Duration::MAX;
        let mut max_latency = Duration::ZERO;
        for _ in 0..1000 {
            let latency = sim.compute_read_latency(0);
            min_latency = min_latency.min(latency);
            max_latency = max_latency.max(latency);
        }

        // Assert - should be within ±10% of base (9ms to 11ms)
        assert!(min_latency >= Duration::from_micros(9000));
        assert!(max_latency <= Duration::from_micros(11000));
    }

    #[test]
    fn should_use_same_region_preset_values() {
        // Arrange
        let config = LatencyConfig::same_region();

        // Assert
        assert_eq!(config.base_rtt_us, 5_000);
        assert_eq!(config.write_penalty_us, 2_000);
        assert_eq!(config.list_penalty_us, 10_000);
        assert!(config.actually_sleep);
    }

    #[test]
    fn should_use_cross_region_preset_values() {
        // Arrange
        let config = LatencyConfig::cross_region();

        // Assert
        assert_eq!(config.base_rtt_us, 80_000);
        assert!(config.write_penalty_us > 0);
    }

    #[test]
    fn should_use_fast_simulation_preset_without_sleeping() {
        // Arrange
        let config = LatencyConfig::fast_simulation();

        // Assert
        assert_eq!(config.base_rtt_us, 100);
        assert!(!config.actually_sleep);
    }

    #[test]
    fn should_reset_stats_correctly() {
        // Arrange
        let sim = LatencySimulator::new(LatencyConfig {
            base_rtt_us: 1000,
            jitter_percent: 0,
            bandwidth_bytes_per_us: u64::MAX,
            write_penalty_us: 0,
            list_penalty_us: 0,
            p99_probability: 0.0,
            p99_multiplier: 1,
            p999_probability: 0.0,
            p999_multiplier: 1,
            actually_sleep: false,
        });
        sim.simulate_read(0);
        sim.simulate_read(0);

        // Act
        sim.reset_stats();

        // Assert
        assert_eq!(sim.operation_count(), 0);
        assert_eq!(sim.total_simulated_latency(), Duration::ZERO);
    }
}
