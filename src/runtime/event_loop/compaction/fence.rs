//! The compaction-publication fence.
//!
//! A compaction whose manifest authority switch completed, or whose
//! publication intent may have reached durable storage, but which could not
//! be fully settled leaves restart recovery as the only safe reconciler.
//! Further compaction could consume that output and make recovery
//! ambiguous, so once degraded the fence refuses every new compaction for
//! the life of the process. Only reopening, which replays the durable
//! intent, constructs a fresh fence.

/// Owns the one-way "publication unsettled" state that gates compaction.
#[derive(Debug, Default)]
pub(in crate::runtime::event_loop) struct CompactionPublicationFence {
    degraded: bool,
}

impl CompactionPublicationFence {
    /// Record that a publication could not be settled. Irreversible until
    /// reopen.
    pub(in crate::runtime::event_loop) fn degrade(&mut self) {
        self.degraded = true;
    }

    pub(in crate::runtime::event_loop) fn is_degraded(&self) -> bool {
        self.degraded
    }

    /// Refuse a new compaction while an earlier publication is unsettled.
    pub(in crate::runtime::event_loop) fn admit_compaction(
        &self,
    ) -> crate::common::MidgeResult<()> {
        if self.degraded {
            return Err(crate::common::MidgeError::Fenced(
                "compaction publication is unsettled; refusing another compaction until recovery"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_admit_compaction_when_publication_has_not_degraded() {
        // Arrange
        let fence = CompactionPublicationFence::default();

        // Act
        let admitted = fence.admit_compaction();

        // Assert
        assert!(admitted.is_ok());
        assert!(!fence.is_degraded());
    }

    #[test]
    fn should_refuse_compaction_as_fenced_when_publication_degrades() {
        // Arrange
        let mut fence = CompactionPublicationFence::default();

        // Act
        fence.degrade();
        let admitted = fence.admit_compaction();

        // Assert
        assert!(fence.is_degraded());
        assert!(matches!(
            admitted,
            Err(crate::common::MidgeError::Fenced(_))
        ));
    }

    #[test]
    fn should_stay_degraded_when_degraded_again() {
        // Arrange
        let mut fence = CompactionPublicationFence::default();
        fence.degrade();

        // Act
        fence.degrade();

        // Assert
        assert!(fence.is_degraded(), "only reopen may clear the fence");
        assert!(fence.admit_compaction().is_err());
    }
}
