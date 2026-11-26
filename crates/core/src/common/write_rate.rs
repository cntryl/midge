//! One shared rate schedule for maintenance output. Call only on workers.

use crate::common::{MidgeError, MidgeResult};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
#[cfg(test)]
mod tests;

pub(crate) struct BackgroundWriteLimiter {
    bytes_per_second: u64,
    next: Mutex<Instant>,
    urgent: Arc<AtomicBool>,
    admitted_bytes: Arc<AtomicU64>,
}

impl BackgroundWriteLimiter {
    pub(crate) fn new(
        bytes_per_second: u64,
        urgent: Arc<AtomicBool>,
        admitted_bytes: Arc<AtomicU64>,
    ) -> Self {
        Self {
            bytes_per_second,
            next: Mutex::new(Instant::now()),
            urgent,
            admitted_bytes,
        }
    }

    pub(crate) fn acquire(&self, bytes: usize) -> MidgeResult<()> {
        if bytes == 0 || self.urgent.load(Ordering::Acquire) {
            return Ok(());
        }
        let nanos = (bytes as u128)
            .saturating_mul(1_000_000_000)
            .div_ceil(u128::from(self.bytes_per_second));
        let duration = Duration::from_nanos(u64::try_from(nanos).map_err(|_| {
            MidgeError::ResourceLimit("background rate interval exceeds supported duration".into())
        })?);
        let until = {
            let mut next = self
                .next
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let until = (*next)
                .max(Instant::now())
                .checked_add(duration)
                .ok_or_else(|| {
                    MidgeError::ResourceLimit(
                        "background rate deadline exceeds monotonic clock".into(),
                    )
                })?;
            *next = until;
            until
        };
        while Instant::now() < until {
            if self.urgent.load(Ordering::Acquire) {
                *self
                    .next
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Instant::now();
                return Ok(());
            }
            std::thread::sleep(
                until
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(1)),
            );
        }
        self.admitted_bytes
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
        Ok(())
    }
}

pub(crate) struct RateLimitedReader<R> {
    pub(crate) inner: R,
    pub(crate) limiter: Option<Arc<BackgroundWriteLimiter>>,
}

impl<R: std::io::Read> std::io::Read for RateLimitedReader<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        // Bound every reservation, including a caller with a very large read.
        let limit = out.len().min(64 * 1024);
        let count = self.inner.read(&mut out[..limit])?;
        if let Some(limiter) = &self.limiter {
            limiter.acquire(count).map_err(std::io::Error::other)?;
        }
        Ok(count)
    }
}
