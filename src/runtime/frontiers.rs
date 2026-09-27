//! WAL durability frontiers that only move forward.

/// The highest sequences known synced, durable locally, and durable in cloud
/// storage.
///
/// Every advance is monotonic, so a receipt for older data cannot lower a
/// frontier. The only way down is the named recovery reset.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WalFrontiers {
    last_synced: u64,
    local_durable: u64,
    cloud_durable: u64,
}

impl WalFrontiers {
    /// Frontiers after recovery replayed the WAL through `sequence`: that
    /// much is durable locally and, until startup re-proves it, in the cloud.
    #[must_use]
    pub(crate) fn recovered_at(sequence: u64) -> Self {
        Self {
            last_synced: 0,
            local_durable: sequence,
            cloud_durable: sequence,
        }
    }

    /// Last sequence the WAL writer synced.
    #[must_use]
    pub fn last_synced(&self) -> u64 {
        self.last_synced
    }

    /// Highest sequence fsynced locally.
    #[must_use]
    pub fn local_durable(&self) -> u64 {
        self.local_durable
    }

    /// Highest sequence confirmed durable in cloud storage.
    #[must_use]
    pub fn cloud_durable(&self) -> u64 {
        self.cloud_durable
    }

    /// Record a local sync through `sequence`. An older sync leaves both
    /// frontiers where they are.
    pub(crate) fn advance_synced_to(&mut self, sequence: u64) {
        self.last_synced = self.last_synced.max(sequence);
        self.local_durable = self.local_durable.max(sequence);
    }

    /// Record local durability through `sequence` without a writer sync.
    pub(crate) fn advance_local_to(&mut self, sequence: u64) {
        self.local_durable = self.local_durable.max(sequence);
    }

    /// Record cloud durability through `sequence`.
    pub(crate) fn advance_cloud_to(&mut self, sequence: u64) {
        self.cloud_durable = self.cloud_durable.max(sequence);
    }

    /// Recovery only: restart the cloud frontier at what the manifest proves,
    /// before startup re-advances it through validated remote segments.
    pub(crate) fn reset_cloud_for_recovery(&mut self, sequence: u64) {
        self.cloud_durable = sequence;
    }

    #[cfg(test)]
    pub(crate) fn set_last_synced_for_test(&mut self, sequence: u64) {
        self.last_synced = sequence;
    }

    #[cfg(test)]
    pub(crate) fn set_local_durable_for_test(&mut self, sequence: u64) {
        self.local_durable = sequence;
    }

    #[cfg(test)]
    pub(crate) fn set_cloud_durable_for_test(&mut self, sequence: u64) {
        self.cloud_durable = sequence;
    }
}

#[cfg(test)]
mod tests {
    use super::WalFrontiers;

    #[test]
    fn should_not_lower_frontiers_when_an_older_sequence_arrives() {
        // Arrange
        let mut frontiers = WalFrontiers::default();
        frontiers.advance_synced_to(20);
        frontiers.advance_cloud_to(15);

        // Act
        frontiers.advance_synced_to(10);
        frontiers.advance_local_to(5);
        frontiers.advance_cloud_to(3);

        // Assert
        assert_eq!(frontiers.last_synced(), 20);
        assert_eq!(frontiers.local_durable(), 20);
        assert_eq!(frontiers.cloud_durable(), 15);
    }

    #[test]
    fn should_lower_cloud_frontier_only_through_recovery_reset() {
        // Arrange
        let mut frontiers = WalFrontiers::default();
        frontiers.advance_cloud_to(15);

        // Act
        frontiers.reset_cloud_for_recovery(7);

        // Assert
        assert_eq!(frontiers.cloud_durable(), 7);
    }
}
