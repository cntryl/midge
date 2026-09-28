//! A publication in flight, tied to the manifest-publication gate it holds.
//!
//! The gate serializes manifest-authority changes. A pending publication may
//! exist only while its owner holds that gate, and settling the publication
//! must release exactly that owner. Keeping both behind this type means no
//! path can clear the pending state and forget the gate, or release another
//! owner's gate.

use crate::runtime::event_loop::coordination::{ManifestPublicationGate, ManifestPublicationOwner};

/// Holds at most one in-flight publication together with its gate owner.
#[derive(Debug)]
pub(in crate::runtime::event_loop) struct PublicationSlot<P> {
    pending: Option<(ManifestPublicationOwner, P)>,
}

impl<P> Default for PublicationSlot<P> {
    fn default() -> Self {
        Self { pending: None }
    }
}

impl<P> PublicationSlot<P> {
    /// Record `pending` as in flight. The caller must already hold `gate`
    /// as `owner`, and no other publication may be in flight.
    pub(in crate::runtime::event_loop) fn install(
        &mut self,
        gate: &ManifestPublicationGate,
        owner: ManifestPublicationOwner,
        pending: P,
    ) -> crate::common::MidgeResult<()> {
        if self.pending.is_some() {
            return Err(crate::common::MidgeError::Internal(
                "a publication is already in flight".into(),
            ));
        }
        if !gate.is_owned_by(&owner) {
            return Err(crate::common::MidgeError::Internal(
                "a publication may be installed only by the gate's owner".into(),
            ));
        }
        self.pending = Some((owner, pending));
        Ok(())
    }

    pub(in crate::runtime::event_loop) fn is_active(&self) -> bool {
        self.pending.is_some()
    }

    pub(in crate::runtime::event_loop) fn get(&self) -> Option<&P> {
        self.pending.as_ref().map(|(_, pending)| pending)
    }

    pub(in crate::runtime::event_loop) fn get_mut(&mut self) -> Option<&mut P> {
        self.pending.as_mut().map(|(_, pending)| pending)
    }

    /// Settle the in-flight publication: take it and release its owner's
    /// hold on `gate` in one step. Returns `None` and leaves `gate` untouched
    /// when nothing is in flight.
    pub(in crate::runtime::event_loop) fn finish(
        &mut self,
        gate: &mut ManifestPublicationGate,
    ) -> Option<P> {
        let (owner, pending) = self.pending.take()?;
        gate.release(&owner);
        Some(pending)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::event_loop::coordination::{
        ManifestPublicationGate, ManifestPublicationOwner,
    };

    fn compaction_owner(request_id: u64) -> ManifestPublicationOwner {
        ManifestPublicationOwner::Compaction {
            request_id,
            output_generation: 1,
        }
    }

    #[test]
    fn should_release_gate_when_finishing_installed_publication() {
        // Arrange
        let mut gate = ManifestPublicationGate::default();
        let owner = compaction_owner(7);
        assert!(gate.try_acquire(owner.clone()));
        let mut slot = PublicationSlot::default();
        slot.install(&gate, owner, "pending")
            .expect("gate held by owner");

        // Act
        let finished = slot.finish(&mut gate);

        // Assert
        assert_eq!(finished, Some("pending"));
        assert!(!slot.is_active());
        assert!(!gate.is_active(), "finishing must release the gate");
    }

    #[test]
    fn should_refuse_install_when_gate_is_not_held_by_publication_owner() {
        // Arrange
        let mut gate = ManifestPublicationGate::default();
        assert!(gate.try_acquire(ManifestPublicationOwner::WalPrune));
        let mut slot = PublicationSlot::default();

        // Act
        let installed = slot.install(&gate, compaction_owner(7), "pending");

        // Assert
        assert!(installed.is_err());
        assert!(!slot.is_active());
    }

    #[test]
    fn should_leave_other_owner_gate_alone_when_nothing_is_pending() {
        // Arrange
        let mut gate = ManifestPublicationGate::default();
        assert!(gate.try_acquire(ManifestPublicationOwner::WalPrune));
        let mut slot = PublicationSlot::<&str>::default();

        // Act
        let finished = slot.finish(&mut gate);

        // Assert
        assert_eq!(finished, None);
        assert!(
            gate.is_active(),
            "an empty slot must not release another owner"
        );
    }
}
