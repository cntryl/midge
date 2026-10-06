//! Caller measurements. Inter-ACK intervals describe this worker's stream,
//! not repeated execution of a stable logical transaction intent.
use hdrhistogram::Histogram;
use serde_json::{json, Value};
use std::time::Duration;

pub(super) const CSV_FIELDS: [&str; 16] = [
    "valid",
    "attempt_samples",
    "successful_call_samples",
    "inter_ack_samples",
    "actual_sleep_ns",
    "censored_inter_ack_samples",
    "censored_inter_ack_ns",
    "attempt_p50_us",
    "attempt_p95_us",
    "attempt_p99_us",
    "successful_call_p50_us",
    "successful_call_p95_us",
    "successful_call_p99_us",
    "inter_ack_p50_us",
    "inter_ack_p95_us",
    "inter_ack_p99_us",
];

#[derive(Debug)]
pub(super) struct LatencyMetrics {
    pub(super) successful_us: Histogram<u64>,
    pub(super) inter_ack_us: Histogram<u64>,
    pub(super) actual_sleep_ns: u64,
    pub(super) censored_samples: u64,
    pub(super) censored_ns: u64,
    pub(super) valid: bool,
}

impl Default for LatencyMetrics {
    fn default() -> Self {
        Self {
            successful_us: Histogram::new(3).expect("create successful-call histogram"),
            inter_ack_us: Histogram::new(3).expect("create inter-ACK histogram"),
            actual_sleep_ns: 0,
            censored_samples: 0,
            censored_ns: 0,
            valid: true,
        }
    }
}

impl LatencyMetrics {
    pub(super) fn successful(&mut self, call: Duration, interval: Duration) {
        for (histogram, elapsed) in [
            (&mut self.successful_us, call),
            (&mut self.inter_ack_us, interval),
        ] {
            let value = u64::try_from(elapsed.as_micros())
                .unwrap_or(u64::MAX)
                .max(1);
            histogram.record(value).expect("record caller latency");
        }
    }

    fn add_checked(target: &mut u64, value: u64, valid: &mut bool) {
        if let Some(sum) = target.checked_add(value) {
            *target = sum;
        } else {
            *target = u64::MAX;
            *valid = false;
        }
    }

    pub(super) fn slept(&mut self, elapsed: Duration) {
        let Ok(nanos) = u64::try_from(elapsed.as_nanos()) else {
            self.valid = false;
            return;
        };
        Self::add_checked(&mut self.actual_sleep_ns, nanos, &mut self.valid);
    }

    pub(super) fn censor(&mut self, elapsed: Duration) {
        if elapsed.is_zero() {
            return;
        }
        let Ok(nanos) = u64::try_from(elapsed.as_nanos()) else {
            self.valid = false;
            return;
        };
        Self::add_checked(&mut self.censored_samples, 1, &mut self.valid);
        Self::add_checked(&mut self.censored_ns, nanos, &mut self.valid);
    }

    pub(super) fn merge(&mut self, other: &Self) {
        self.successful_us
            .add(&other.successful_us)
            .expect("merge successful-call histogram");
        self.inter_ack_us
            .add(&other.inter_ack_us)
            .expect("merge inter-ACK histogram");
        self.valid &= other.valid;
        Self::add_checked(
            &mut self.actual_sleep_ns,
            other.actual_sleep_ns,
            &mut self.valid,
        );
        Self::add_checked(
            &mut self.censored_samples,
            other.censored_samples,
            &mut self.valid,
        );
        Self::add_checked(&mut self.censored_ns, other.censored_ns, &mut self.valid);
    }

    pub(super) fn summary(&self, attempts: &Histogram<u64>) -> Value {
        json!({
            "schema_version": 1,
            "valid": self.valid,
            "attempt_samples": attempts.len(),
            "successful_call_samples": self.successful_us.len(),
            "inter_ack_samples": self.inter_ack_us.len(),
            "actual_sleep_ns": self.actual_sleep_ns,
            "censored_inter_ack_samples": self.censored_samples,
            "censored_inter_ack_ns": self.censored_ns,
            "attempt_p50_us": super::quantile(attempts, 0.50),
            "attempt_p95_us": super::quantile(attempts, 0.95),
            "attempt_p99_us": super::quantile(attempts, 0.99),
            "successful_call_p50_us": super::quantile(&self.successful_us, 0.50),
            "successful_call_p95_us": super::quantile(&self.successful_us, 0.95),
            "successful_call_p99_us": super::quantile(&self.successful_us, 0.99),
            "inter_ack_p50_us": super::quantile(&self.inter_ack_us, 0.50),
            "inter_ack_p95_us": super::quantile(&self.inter_ack_us, 0.95),
            "inter_ack_p99_us": super::quantile(&self.inter_ack_us, 0.99),
        })
    }
}

#[cfg(test)]
#[allow(
    unused_imports,
    reason = "Harness-free benches compile cfg(test) helpers without running unit tests"
)]
mod tests {
    use super::*;

    #[test]
    fn should_merge_worker_histograms_and_sleep_without_averaging_percentiles() {
        // Arrange
        let mut first = LatencyMetrics::default();
        first.successful(Duration::from_micros(10), Duration::from_micros(100));
        first.slept(Duration::from_millis(1));
        let mut second = LatencyMetrics::default();
        second.successful(Duration::from_micros(90), Duration::from_micros(900));
        second.slept(Duration::from_millis(2));
        second.censor(Duration::from_micros(50));
        // Act
        first.merge(&second);
        // Assert
        assert_eq!(first.successful_us.len(), 2);
        assert_eq!(first.inter_ack_us.value_at_quantile(0.99), 900);
        assert_eq!(first.actual_sleep_ns, 3_000_000);
        assert_eq!(first.censored_samples, 1);
        assert_eq!(first.censored_ns, 50_000);
        assert!(first.valid);
    }

    #[test]
    fn should_keep_accounting_invalid_when_worker_time_overflows_during_merge() {
        // Arrange
        let mut first = LatencyMetrics {
            actual_sleep_ns: u64::MAX,
            ..LatencyMetrics::default()
        };
        let mut second = LatencyMetrics::default();
        second.slept(Duration::from_nanos(1));
        // Act
        first.merge(&second);
        first.merge(&LatencyMetrics::default());
        // Assert
        assert!(!first.valid);
        assert_eq!(first.actual_sleep_ns, u64::MAX);
    }
}
