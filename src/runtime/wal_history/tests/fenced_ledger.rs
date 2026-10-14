//! Ledger repair and publication through the shipping filesystem writer fence.

use super::{wal, WalHistory, WalHistoryFence, POWER_LOSS_SEEDS};
use crate::common::MidgeError;
use crate::io::Fs;
use crate::lease::{fs_leader_store::FsLeaderStore, LeaderStore};
use crate::simulation::crash_fs::{CrashDevice, Fault, FaultEffect, FaultError, FaultId, FaultOp};
use crate::wal::ledger_store::{COMPACT_AFTER_EDITS, LEDGER_FILE};
use std::sync::Arc;
use std::time::Duration;

struct FencedHistory {
    history: WalHistory,
    epoch: u64,
}

fn open_fenced(device: &CrashDevice, holder: &str) -> FencedHistory {
    let root: Arc<dyn Fs> = Arc::new(device.fs());
    let store = Arc::new(FsLeaderStore::new(
        root,
        crate::lease::LeaseClock::default(),
    ));
    let lease = store
        .acquire_leadership(holder)
        .expect("acquire filesystem lease");
    let fence = WalHistoryFence::new(
        store,
        holder.to_string(),
        lease.epoch,
        Duration::from_secs(1),
    );
    let history = WalHistory::open_with_fence(wal(device), 1, lease.epoch, &fence)
        .expect("open history under its acquired lease");
    FencedHistory {
        history,
        epoch: lease.epoch,
    }
}

fn ledger_fault(device: &CrashDevice, op: FaultOp, effect: FaultEffect) -> FaultId {
    device.inject(Fault {
        op,
        path: format!("{}/{LEDGER_FILE}", crate::wal::WAL_DIR),
        nth: 1,
        effect,
        error: FaultError::Io,
    })
}

fn assert_reserved_after_power_loss(device: &CrashDevice, allowed: &[u64]) {
    for seed in 0..POWER_LOSS_SEEDS {
        let crashed = device.fork();
        crashed.power_loss(seed);
        let reopened = open_fenced(&crashed, "restarted");
        let reserved = reopened.history.ledger().seq_reserved_through();
        assert!(
            allowed.contains(&reserved),
            "seed {seed}: recovered reservation {reserved}, expected one of {allowed:?}"
        );
    }
}

#[test]
fn should_preserve_acknowledged_reservation_when_fenced_append_is_repaired_after_sync_failure() {
    for effect in [FaultEffect::Before, FaultEffect::After] {
        // Arrange
        let device = CrashDevice::new();
        let mut owner = open_fenced(&device, "owner");
        owner
            .history
            .reserve(100)
            .expect("reserve acknowledged floor");
        let fault = ledger_fault(&device, FaultOp::Sync, effect);
        let failed = owner.history.reserve(200);

        // Act
        let retried = owner.history.reserve(300);

        // Assert
        assert!(
            device.fired(fault),
            "{effect:?}: append sync fault did not fire"
        );
        assert!(
            matches!(failed, Err(MidgeError::Io(_))),
            "{effect:?}: {failed:?}"
        );
        retried.expect("repair then acknowledge the new reservation");
        assert_eq!(owner.history.ledger().seq_reserved_through(), 300);
        assert_reserved_after_power_loss(&device, &[300]);
    }
}

#[test]
fn should_refuse_new_reservation_when_fenced_repair_publication_fails() {
    for sync_effect in [FaultEffect::Before, FaultEffect::After] {
        for rename_effect in [FaultEffect::Before, FaultEffect::After] {
            // Arrange
            let device = CrashDevice::new();
            let mut owner = open_fenced(&device, "owner");
            owner
                .history
                .reserve(100)
                .expect("reserve acknowledged floor");
            let sync_fault = ledger_fault(&device, FaultOp::Sync, sync_effect);
            let failed_append = owner.history.reserve(200);
            let rename_fault = ledger_fault(&device, FaultOp::Rename, rename_effect);

            // Act
            let rejected = owner.history.reserve(300);

            // Assert
            assert!(device.fired(sync_fault));
            assert!(device.fired(rename_fault));
            assert!(
                matches!(failed_append, Err(MidgeError::Io(_))),
                "{failed_append:?}"
            );
            assert!(matches!(rejected, Err(MidgeError::Io(_))), "{rejected:?}");
            assert_eq!(owner.history.ledger().seq_reserved_through(), 100);
            // The failed 200 reservation may have reached disk. The 300
            // reservation never passed repair and must not appear after restart.
            assert_reserved_after_power_loss(&device, &[100, 200]);
        }
    }
}

#[test]
fn should_preserve_acknowledged_reservation_when_fenced_automatic_compaction_is_interrupted() {
    let threshold = u64::try_from(COMPACT_AFTER_EDITS).expect("compaction threshold fits u64");
    for effect in [FaultEffect::Before, FaultEffect::After] {
        // Arrange
        let device = CrashDevice::new();
        let mut owner = open_fenced(&device, "owner");
        for through in 1..threshold {
            owner
                .history
                .reserve(through)
                .expect("reserve before compaction");
        }
        let fault = ledger_fault(&device, FaultOp::Rename, effect);

        // Act
        let acknowledged = owner.history.reserve(threshold);

        // Assert: the append is durable even when its optional compaction
        // publication fails, both before and after the rename took effect.
        assert!(
            device.fired(fault),
            "{effect:?}: compaction fault did not fire"
        );
        acknowledged.expect("compaction failure must not reject the synced append");
        assert_eq!(owner.history.ledger().seq_reserved_through(), threshold);
        assert_reserved_after_power_loss(&device, &[threshold]);

        // Act: the next effect repairs the journal before extending it.
        owner
            .history
            .reserve(threshold + 1)
            .expect("repair and reserve after compaction failure");

        // Assert
        assert_eq!(owner.history.ledger().seq_reserved_through(), threshold + 1);
        assert_reserved_after_power_loss(&device, &[threshold + 1]);
    }
}

#[test]
fn should_reject_stale_repair_without_changing_successor_reserved_history() {
    for effect in [FaultEffect::Before, FaultEffect::After] {
        // Arrange
        let device = CrashDevice::new();
        let mut predecessor = open_fenced(&device, "predecessor");
        predecessor
            .history
            .reserve(100)
            .expect("reserve predecessor floor");
        let fault = ledger_fault(&device, FaultOp::Sync, effect);
        let failed = predecessor.history.reserve(200);
        let mut successor = open_fenced(&device, "successor");
        successor
            .history
            .reserve(300)
            .expect("reserve successor floor");
        let successor_files = device.files();

        // Act
        let rejected = predecessor.history.reserve(400);

        // Assert
        assert!(device.fired(fault));
        assert!(matches!(failed, Err(MidgeError::Io(_))), "{failed:?}");
        assert!(successor.epoch > predecessor.epoch);
        assert!(
            matches!(rejected, Err(MidgeError::Fenced(_))),
            "{rejected:?}"
        );
        assert_eq!(
            device.files(),
            successor_files,
            "stale repair changed successor files"
        );
        assert_eq!(successor.history.ledger().seq_reserved_through(), 300);
        assert_reserved_after_power_loss(&device, &[300]);
    }
}
