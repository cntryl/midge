//! Bounded admission-pressure history alongside the disk reservation ledger.

use super::actor::ReservationResult;
use std::time::Instant;

/// The local operation whose admission most recently failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(usize)]
pub enum StorageAdmissionKind {
    Wal,
    TransactionSpill,
    Flush,
    Compaction,
    FlushHeadroom,
    StartupResidue,
}

/// Why a local operation could not reserve its working space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageAdmissionReason {
    LocalCapacity,
    CloudUpload,
    Compaction,
}

/// Oldest rejected admission class that has not subsequently succeeded.
///
/// This records observed admission failures, not a queue of caller requests.
/// A caller can retry or abandon its operation; successful admission of the
/// same class clears its observation without hiding failures of other classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct StorageAdmissionBlock {
    pub operation: StorageAdmissionKind,
    pub reason: StorageAdmissionReason,
    pub requested_bytes: u64,
    pub free_bytes_at_rejection: u64,
    pub age_millis: u64,
    pub attempts: u64,
}

#[derive(Clone, Copy)]
struct RejectedAdmission {
    snapshot: StorageAdmissionBlock,
    since: Instant,
}

#[derive(Default)]
pub(super) struct AdmissionPressure {
    rejected: [Option<RejectedAdmission>; 6],
    pub rejections_total: u64,
}

impl AdmissionPressure {
    pub fn observe(
        &mut self,
        operation: StorageAdmissionKind,
        requested_bytes: u64,
        free_bytes: u64,
        result: ReservationResult,
        now: Instant,
    ) {
        let reason = match result {
            ReservationResult::Ok => {
                self.rejected[operation as usize] = None;
                return;
            }
            ReservationResult::RejectNoSpace => StorageAdmissionReason::LocalCapacity,
            ReservationResult::WaitForCloudUpload => StorageAdmissionReason::CloudUpload,
            ReservationResult::WaitForCompaction => StorageAdmissionReason::Compaction,
        };
        self.rejections_total = self.rejections_total.saturating_add(1);
        let previous = self.rejected[operation as usize];
        self.rejected[operation as usize] = Some(RejectedAdmission {
            snapshot: StorageAdmissionBlock {
                operation,
                reason,
                requested_bytes,
                free_bytes_at_rejection: free_bytes,
                age_millis: 0,
                attempts: previous.map_or(1, |entry| entry.snapshot.attempts.saturating_add(1)),
            },
            since: previous.map_or(now, |entry| entry.since),
        });
    }

    pub fn snapshot(&self, now: Instant) -> Option<StorageAdmissionBlock> {
        self.rejected
            .iter()
            .flatten()
            .min_by_key(|entry| entry.since)
            .map(|entry| {
                let mut snapshot = entry.snapshot;
                snapshot.age_millis =
                    u64::try_from(now.saturating_duration_since(entry.since).as_millis())
                        .unwrap_or(u64::MAX);
                snapshot
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn should_preserve_oldest_pressure_when_other_admissions_recover() {
        // Arrange
        let mut pressure = AdmissionPressure::default();
        let now = Instant::now();
        pressure.observe(
            StorageAdmissionKind::Wal,
            80,
            20,
            ReservationResult::RejectNoSpace,
            now,
        );
        pressure.observe(
            StorageAdmissionKind::Flush,
            120,
            20,
            ReservationResult::WaitForCloudUpload,
            now + Duration::from_millis(10),
        );

        // Act
        pressure.observe(
            StorageAdmissionKind::Wal,
            80,
            200,
            ReservationResult::Ok,
            now + Duration::from_millis(50),
        );
        pressure.observe(
            StorageAdmissionKind::Flush,
            120,
            80,
            ReservationResult::WaitForCloudUpload,
            now + Duration::from_millis(60),
        );
        let snapshot = pressure.snapshot(now + Duration::from_millis(100)).unwrap();

        // Assert
        assert_eq!(snapshot.operation, StorageAdmissionKind::Flush);
        assert_eq!(snapshot.reason, StorageAdmissionReason::CloudUpload);
        assert_eq!(snapshot.age_millis, 90);
        assert_eq!(snapshot.attempts, 2);
        assert_eq!(pressure.rejections_total, 3);
        pressure.observe(
            StorageAdmissionKind::Flush,
            120,
            200,
            ReservationResult::Ok,
            now,
        );
        assert!(pressure.snapshot(now).is_none());
    }
}
