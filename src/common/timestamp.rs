//! Monotonic timestamp generation with optional NTP synchronization and periodic drift correction.
//!
//! This module provides a thread-safe monotonic clock that:
//! - Guarantees monotonically increasing timestamps
//! - Optionally syncs with NTP servers on startup
//! - Periodically corrects for drift every few hours
//! - Falls back to system time if NTP is unavailable
//! - Uses monotonic time elapsed since initialization
//! - **Tracks time at millisecond precision** (not micro/nano)
//!
//! # Precision
//!
//! All timestamps are tracked internally at **millisecond precision**.
//! - `now_millis()` - Full precision (milliseconds)
//! - `now_secs()` - Lossless (seconds)
//! - `now_nanos()` - **LOSSY** (last 6 digits always zero)
//!
//! If you need true nanosecond precision for benchmarking or fine-grained timing,
//! use `std::time::Instant` for relative measurements.
//!
//! # Environment Variables
//!
//! - `SHALE_TIME_SERVER`: Controls time source
//!   - `"system"`: Use system time only (no NTP)
//!   - `"default"` or empty: Try NTP servers in order, fallback to system
//!   - Custom address: `"time.google.com:123"` etc.
//!
//! # Example
//!
//! ```no_run
//! use cntryl_midge::common::timestamp;
//!
//! // Get timestamp in different units
//! let millis = timestamp::now_millis();  // Milliseconds since epoch
//! let secs = timestamp::now_secs();      // Seconds since epoch  
//! let nanos = timestamp::now_nanos();    // Nanoseconds since epoch (LOSSY - millis precision)
//!
//! // Fast variants (skip NTP offset check)
//! let millis_fast = timestamp::now_millis_fast();
//! let secs_fast = timestamp::now_secs_fast();
//!
//! // Get as SystemTime
//! let system_time = timestamp::now();
//!
//! // All timestamps are monotonic
//! let later = timestamp::now_millis();
//! assert!(later >= millis);
//! ```

use std::net::{ToSocketAddrs, UdpSocket};
use std::sync::{
    atomic::{AtomicI64, Ordering},
    OnceLock,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

const TIME_SERVER_ENV: &str = "SHALE_TIME_SERVER";
const DEFAULT_NTP_SERVERS: &[&str] = &[
    "time.google.com:123",
    "time.aws.com:123",
    "time.cloudflare.com:123",
    "time.windows.com:123",
];

static GLOBAL_CLOCK: OnceLock<Clock> = OnceLock::new();
static CLOCK_OFFSET: AtomicI64 = AtomicI64::new(0);

#[derive(Debug, Clone)]
struct Clock {
    start_time_millis: i64,
    start_instant: Instant,
}

impl Clock {
    fn new(start_time: SystemTime) -> Self {
        let start_time_millis = start_time
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        Self {
            start_time_millis,
            start_instant: Instant::now(),
        }
    }

    #[inline]
    fn now_millis(&self) -> u64 {
        let elapsed = self.start_instant.elapsed();
        let base = self.start_time_millis + elapsed.as_millis() as i64;
        let offset = CLOCK_OFFSET.load(Ordering::Relaxed);
        base.saturating_add(offset).max(0) as u64
    }

    /// Fast path when offset is known to be zero (common case).
    ///
    /// Saves one atomic load and saturating_add when NTP offset hasn't been applied.
    #[inline(always)]
    fn now_millis_fast(&self) -> u64 {
        let elapsed = self.start_instant.elapsed();
        (self.start_time_millis + elapsed.as_millis() as i64).max(0) as u64
    }
}

fn init_clock() -> Clock {
    let start_time = match get_current_time() {
        Ok(time) => {
            debug!("Clock initialized successfully");
            time
        }
        Err(e) => {
            warn!("Failed to initialize clock, using system time: {}", e);
            SystemTime::now()
        }
    };

    // Start background resync
    start_resync_thread();

    Clock::new(start_time)
}

/// Periodically re-syncs NTP offset without breaking monotonic guarantees.
fn start_resync_thread() {
    thread::spawn(|| loop {
        thread::sleep(Duration::from_secs(6 * 3600)); // every 6h
        if let Ok(ntp_time) = get_current_time() {
            let system_now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);

            let ntp_now = ntp_time
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(system_now);

            let new_offset = ntp_now - system_now;
            CLOCK_OFFSET.store(new_offset, Ordering::Relaxed);
            debug!("NTP resync applied, offset={} ms", new_offset);
        } else {
            warn!("Periodic NTP resync failed");
        }
    });
}

fn get_current_time() -> Result<SystemTime, String> {
    let server = std::env::var(TIME_SERVER_ENV).unwrap_or_else(|_| "default".to_string());

    match server.as_str() {
        "system" => {
            debug!("Using system time (SHALE_TIME_SERVER=system)");
            Ok(SystemTime::now())
        }
        "default" | "" => {
            let mut last_error = String::new();
            for &ntp_server in DEFAULT_NTP_SERVERS {
                match fetch_ntp_time(ntp_server) {
                    Ok(time) => {
                        debug!("Successfully fetched time from {}", ntp_server);
                        return Ok(time);
                    }
                    Err(e) => {
                        warn!("NTP server {} failed: {}", ntp_server, e);
                        last_error = e;
                    }
                }
            }
            Err(format!("All NTP servers failed: {}", last_error))
        }
        custom => match fetch_ntp_time(custom) {
            Ok(time) => {
                debug!("Fetched time from custom server {}", custom);
                Ok(time)
            }
            Err(e) => Err(format!("Custom NTP server failed: {}", e)),
        },
    }
}

fn fetch_ntp_time(server: &str) -> Result<SystemTime, String> {
    let addr = server
        .to_socket_addrs()
        .map_err(|e| format!("Resolve {}: {}", server, e))?
        .next()
        .ok_or_else(|| format!("No addresses for {}", server))?;

    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| format!("Bind: {}", e))?;
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| format!("Set read timeout: {}", e))?;
    socket
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| format!("Set write timeout: {}", e))?;

    let mut request = [0u8; 48];
    request[0] = 0x1B;
    socket
        .send_to(&request, addr)
        .map_err(|e| format!("Send: {}", e))?;

    let mut response = [0u8; 48];
    let (len, _) = socket
        .recv_from(&mut response)
        .map_err(|e| format!("Recv: {}", e))?;
    if len < 48 {
        return Err(format!("Short NTP response: {}", len));
    }

    let seconds = u32::from_be_bytes([response[40], response[41], response[42], response[43]]);
    let fraction = u32::from_be_bytes([response[44], response[45], response[46], response[47]]);
    if seconds == 0 {
        return Err("Invalid NTP response: zero timestamp".to_string());
    }

    const NTP_UNIX_OFFSET: u64 = 2_208_988_800;
    let ntp_seconds = seconds as u64;
    let ntp_fraction_ns = (fraction as u64 * 1_000_000_000) >> 32;
    let unix_seconds = ntp_seconds
        .checked_sub(NTP_UNIX_OFFSET)
        .ok_or_else(|| "NTP timestamp before Unix epoch".to_string())?;

    let ntp_time =
        UNIX_EPOCH + Duration::from_secs(unix_seconds) + Duration::from_nanos(ntp_fraction_ns);

    // sanity check: within 24h of system time
    let system_time = SystemTime::now();
    let difference = ntp_time
        .duration_since(system_time)
        .or_else(|_| system_time.duration_since(ntp_time))
        .unwrap_or(Duration::ZERO);

    if difference > Duration::from_secs(24 * 3600) {
        return Err(format!(
            "NTP time differs by {}s (>24h)",
            difference.as_secs()
        ));
    }

    Ok(ntp_time)
}

#[inline]
pub fn now_millis() -> u64 {
    let clock = GLOBAL_CLOCK.get_or_init(init_clock);
    clock.now_millis()
}

/// Get current Unix epoch time in seconds.
#[inline]
pub fn now_secs() -> u64 {
    now_millis() / 1000
}

/// Get current Unix epoch time in nanoseconds.
///
/// **WARNING: This is LOSSY!**
/// Since we track time internally in milliseconds, this function multiplies
/// milliseconds by 1,000,000. The last 6 digits (microseconds + nanoseconds)
/// will always be zero.
///
/// If you need true nanosecond precision, use `Instant::now()` for relative
/// timing or consider tracking a higher-resolution clock.
///
/// # Example
/// ```
/// // These will have zeros in the last 6 digits:
/// // 1730419200123000000  (millis=1730419200123, last 6 digits are 000000)
/// // 1730419200456000000  (millis=1730419200456, last 6 digits are 000000)
/// ```
#[inline]
pub fn now_nanos() -> u128 {
    now_millis() as u128 * 1_000_000
}

/// Fast timestamp generation without NTP offset check.
///
/// This is faster than `now_millis()` but doesn't include NTP drift correction.
/// Use this in hot paths where nanosecond precision isn't critical and you
/// accept slight clock drift over time.
///
/// Saves ~3ns per call by avoiding atomic load and saturating_add.
#[inline]
pub fn now_millis_fast() -> u64 {
    let clock = GLOBAL_CLOCK.get_or_init(init_clock);
    // Check if offset is zero (common case)
    if CLOCK_OFFSET.load(Ordering::Relaxed) == 0 {
        clock.now_millis_fast()
    } else {
        clock.now_millis()
    }
}

/// Fast timestamp generation in seconds without NTP offset check.
#[inline]
pub fn now_secs_fast() -> u64 {
    now_millis_fast() / 1000
}

/// Fast timestamp generation in nanoseconds without NTP offset check.
///
/// **WARNING: This is LOSSY!** See `now_nanos()` for details.
#[inline]
pub fn now_nanos_fast() -> u128 {
    now_millis_fast() as u128 * 1_000_000
}

//----------------------------------------------------------------------
// Test-only helpers (enabled under the `test-hooks` feature)
//----------------------------------------------------------------------
/// Advance the global clock offset by `delta_ms` milliseconds.
///
/// This is intended for tests only (feature-gated). It adjusts the internal
/// CLOCK_OFFSET so calls to `now_millis()` and friends will reflect the
/// adjusted time. Passing a negative delta will move the clock backwards.
pub fn add_clock_offset_millis(delta_ms: i64) {
    use std::sync::atomic::Ordering;
    CLOCK_OFFSET.fetch_add(delta_ms, Ordering::Relaxed);
}

/// Set the global clock offset to `offset_ms` (absolute value). Tests can use
/// this to jump time to a desired offset relative to system time.
pub fn set_clock_offset_millis(offset_ms: i64) {
    use std::sync::atomic::Ordering;
    CLOCK_OFFSET.store(offset_ms, Ordering::Relaxed);
}

/// Get current time as SystemTime.
///
/// This converts the monotonic millisecond timestamp back to SystemTime.
/// Useful for APIs that require SystemTime.
pub fn now() -> SystemTime {
    let millis = now_millis();
    UNIX_EPOCH + Duration::from_millis(millis)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn should_generate_monotonically_increasing_timestamps() {
        // Arrange
        let ts1 = now_millis();
        thread::sleep(Duration::from_millis(10));

        // Act
        let ts2 = now_millis();
        thread::sleep(Duration::from_millis(10));
        let ts3 = now_millis();

        // Assert
        assert!(ts2 >= ts1);
        assert!(ts3 >= ts2);
    }

    #[test]
    fn should_generate_seconds_timestamp() {
        // Arrange
        let millis = now_millis();

        // Act
        let secs = now_secs();

        // Assert
        assert_eq!(secs, millis / 1000);
        assert!(secs > 1_700_000_000); // After 2023
    }

    #[test]
    fn should_generate_nanoseconds_timestamp() {
        // Arrange
        let millis = now_millis();

        // Act
        let nanos = now_nanos();

        // Assert
        assert_eq!(nanos, millis as u128 * 1_000_000);
        assert!(nanos > 1_700_000_000_000_000_000); // After 2023 in nanos
    }

    #[test]
    fn should_maintain_relationship_between_time_units() {
        // Arrange
        // Act
        let nanos = now_nanos();
        let millis = now_millis();
        let secs = now_secs();

        // Assert
        // Allow small drift due to time passing between calls
        assert!((nanos / 1_000_000) as u64 >= millis - 1);
        assert!((nanos / 1_000_000) as u64 <= millis + 1);
        assert!(millis / 1000 >= secs - 1);
        assert!(millis / 1000 <= secs + 1);
    }

    #[test]
    fn should_support_fast_variants_for_all_units() {
        // Arrange
        // Act
        let millis_fast = now_millis_fast();
        let secs_fast = now_secs_fast();
        let nanos_fast = now_nanos_fast();

        // Assert
        assert!(millis_fast > 0);
        assert!(secs_fast > 0);
        assert!(nanos_fast > 0);
        assert_eq!(secs_fast, millis_fast / 1000);
        assert_eq!(nanos_fast, millis_fast as u128 * 1_000_000);
    }

    #[test]
    fn should_generate_unique_timestamps_under_concurrent_access() {
        // Arrange
        let timestamps = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let mut handles = vec![];

        for _ in 0..5 {
            let timestamps = Arc::clone(&timestamps);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let ts = now_millis();
                    timestamps.lock().push(ts);
                    thread::sleep(Duration::from_micros(100));
                }
            }));
        }

        // Act
        for h in handles {
            h.join().unwrap();
        }

        // Assert
        let mut all = timestamps.lock().clone();
        all.sort_unstable();

        // Verify monotonicity: each timestamp >= previous (allows duplicates)
        for w in all.windows(2) {
            assert!(
                w[1] >= w[0],
                "Timestamps must be monotonic: {} came before {}",
                w[1],
                w[0]
            );
        }
    }
}
