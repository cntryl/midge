//! Fixed-bin latency histograms shared by runtime metrics and metadata
//! accounting.
//!
//! Eighty bins cover nanosecond durations: sixty-four linear bins of 250 µs up
//! to 16 ms, then sixteen doubling bins up to about 17 minutes, with the last
//! bin absorbing everything beyond. The layout is fixed so a histogram costs
//! the same regardless of operation count and so snapshots from different
//! engines or revisions compare bin for bin.

use std::sync::atomic::{AtomicU64, Ordering};

/// Number of bins in every latency histogram.
pub(crate) const HISTOGRAM_BUCKETS: usize = 80;
/// Leading bins of `STEP_NS` width each.
pub(crate) const LINEAR_BUCKETS: usize = 64;
/// Width of one linear bin in nanoseconds (250 µs).
pub(crate) const STEP_NS: u64 = 250_000;

/// Inclusive upper bound in nanoseconds of bin `index`.
pub(crate) fn upper_bound(index: usize) -> u64 {
    if index < LINEAR_BUCKETS {
        u64::try_from(index + 1).expect("fixed histogram index") * STEP_NS - 1
    } else {
        (u64::try_from(LINEAR_BUCKETS).expect("fixed histogram index") * STEP_NS)
            .checked_shl(u32::try_from(index - LINEAR_BUCKETS + 1).expect("fixed shift"))
            .unwrap_or(u64::MAX)
            .saturating_sub(1)
    }
}

/// Bin that holds a duration of `ns` nanoseconds.
pub(crate) fn bucket_index(ns: u64) -> usize {
    if ns < u64::try_from(LINEAR_BUCKETS).expect("fixed histogram index") * STEP_NS {
        usize::try_from(ns / STEP_NS).expect("linear bucket index fits")
    } else {
        (LINEAR_BUCKETS..HISTOGRAM_BUCKETS)
            .find(|index| ns <= upper_bound(*index))
            .unwrap_or(HISTOGRAM_BUCKETS - 1)
    }
}

/// Percentile summary of one latency histogram.
///
/// `count` is exact. Each percentile is the inclusive upper bound of the bin
/// that holds that rank, so it rounds up to the bin edge and is `0` only when
/// the histogram is empty.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct LatencySummary {
    /// Number of recorded durations.
    pub count: u64,
    /// Upper bound of the bin holding the median.
    pub p50_ns: u64,
    /// Upper bound of the bin holding the 95th percentile.
    pub p95_ns: u64,
    /// Upper bound of the bin holding the 99th percentile.
    pub p99_ns: u64,
    /// Upper bound of the bin holding the 99.9th percentile.
    pub p999_ns: u64,
}

/// Lock-free histogram for concurrent recording from many threads.
///
/// Recording is one relaxed atomic increment; snapshots read bins one at a
/// time, so a summary taken during heavy recording can be off by the few
/// increments that landed mid-read. That is acceptable for operator metrics.
#[derive(Debug)]
pub(crate) struct AtomicLatencyHistogram {
    bins: [AtomicU64; HISTOGRAM_BUCKETS],
}

impl Default for AtomicLatencyHistogram {
    fn default() -> Self {
        Self {
            bins: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl AtomicLatencyHistogram {
    /// Record one duration in nanoseconds.
    #[inline]
    pub(crate) fn record_ns(&self, ns: u64) {
        self.bins[bucket_index(ns)].fetch_add(1, Ordering::Relaxed);
    }

    /// Record one duration.
    #[inline]
    pub(crate) fn record(&self, elapsed: std::time::Duration) {
        self.record_ns(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
    }

    /// Current counts and percentiles.
    pub(crate) fn summary(&self) -> LatencySummary {
        let counts: Vec<u64> = self
            .bins
            .iter()
            .map(|bin| bin.load(Ordering::Relaxed))
            .collect();
        summarize(&counts)
    }
}

/// Summarize bin counts laid out by [`upper_bound`].
pub(crate) fn summarize(counts: &[u64]) -> LatencySummary {
    let total: u128 = counts.iter().map(|count| u128::from(*count)).sum();
    if total == 0 {
        return LatencySummary::default();
    }
    let percentile = |numerator: u128, denominator: u128| -> u64 {
        let rank = (total * numerator).div_ceil(denominator).max(1);
        let mut seen = 0_u128;
        for (index, count) in counts.iter().enumerate() {
            seen += u128::from(*count);
            if seen >= rank {
                return upper_bound(index);
            }
        }
        upper_bound(HISTOGRAM_BUCKETS - 1)
    };
    LatencySummary {
        count: u64::try_from(total).unwrap_or(u64::MAX),
        p50_ns: percentile(50, 100),
        p95_ns: percentile(95, 100),
        p99_ns: percentile(99, 100),
        p999_ns: percentile(999, 1000),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_place_durations_in_monotonic_bins_when_spanning_linear_and_doubling_ranges() {
        // Arrange
        let linear_end = u64::try_from(LINEAR_BUCKETS).unwrap() * STEP_NS;
        let samples = [
            0,
            1,
            STEP_NS - 1,
            STEP_NS,
            linear_end - 1,
            linear_end,
            u64::MAX,
        ];

        // Act
        let indexes: Vec<usize> = samples.iter().map(|ns| bucket_index(*ns)).collect();

        // Assert
        assert_eq!(indexes[0], 0);
        assert_eq!(indexes[1], 0);
        assert_eq!(indexes[2], 0);
        assert_eq!(indexes[3], 1);
        assert_eq!(
            indexes[4],
            LINEAR_BUCKETS - 1,
            "just under 16 ms is the last linear bin"
        );
        assert_eq!(indexes[5], LINEAR_BUCKETS, "16 ms opens the doubling bins");
        assert_eq!(indexes[6], HISTOGRAM_BUCKETS - 1);
        for pair in indexes.windows(2) {
            assert!(pair[0] <= pair[1]);
        }
        for index in 0..HISTOGRAM_BUCKETS {
            assert_eq!(bucket_index(upper_bound(index)), index);
        }
    }

    #[test]
    fn should_report_bin_upper_bounds_when_summarizing_known_distribution() {
        // Arrange: 90 fast samples in bin 0, 9 in bin 4, 1 in bin 70.
        let histogram = AtomicLatencyHistogram::default();
        for _ in 0..90 {
            histogram.record_ns(10);
        }
        for _ in 0..9 {
            histogram.record_ns(4 * STEP_NS + 1);
        }
        histogram.record_ns(upper_bound(70));

        // Act
        let summary = histogram.summary();

        // Assert
        assert_eq!(summary.count, 100);
        assert_eq!(summary.p50_ns, upper_bound(0));
        assert_eq!(summary.p95_ns, upper_bound(4));
        assert_eq!(summary.p99_ns, upper_bound(4));
        assert_eq!(summary.p999_ns, upper_bound(70));
    }

    #[test]
    fn should_return_zero_summary_when_histogram_is_empty() {
        // Arrange
        let histogram = AtomicLatencyHistogram::default();

        // Act
        let summary = histogram.summary();

        // Assert
        assert_eq!(summary, LatencySummary::default());
    }
}
