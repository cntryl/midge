//! Engine campaigns (#831, #838).
//!
//! Seeded workloads drive an engine on a simulated device through clean
//! restarts, process crashes, power losses and injected faults. Cloud
//! campaigns add a simulated object store, which can fail, lose answers and
//! land requests late, and, in provider mode, takeovers by a process with a
//! fresh cache. After
//! every reopen the oracle judges every key the workload can touch, in every
//! family, against the history of commits, and a WAL invariant checks that
//! no segment name is durably given twice. What recovery serves becomes the
//! verified base for the next history: the engine may not lose a value it has
//! already returned after a crash.
//!
//! A finding carries a crash image: the device files and store objects of
//! the last recovery point before it, with the history, the names given so
//! far and the remaining workload, so replaying it needs nothing else. Images are written
//! below `MIDGE_CAMPAIGN_IMAGES` when that variable names a directory, and
//! load back for replay.
//!
//! Control campaigns, which must find nothing, run a seed budget: the
//! default per pull request, and `MIDGE_CAMPAIGN_PROFILE=nightly` for the
//! nightly run, which also moves its seeds to `MIDGE_CAMPAIGN_SEED_BASE`.
//! Regression campaigns keep fixed seeds and require the invariant to hold.
//! `MIDGE_CAMPAIGN_REPLAY` can select a saved image for the crash control.

#[path = "wal_campaigns/config.rs"]
mod config;
#[path = "wal_campaigns/harness.rs"]
mod harness;
#[path = "wal_campaigns/images.rs"]
mod images;
#[path = "wal_campaigns/search.rs"]
mod search;
#[path = "wal_campaigns/workload.rs"]
mod workload;

use cntryl_midge::__internal::simulation::{Fault, FaultEffect, FaultError, FaultOp};
use config::{Config, Mode, StoreFaults, RENEWING_LEASE_TTL_MILLIS};
use harness::{Campaign, Stop};
use images::{assert_clean, load_image};
use search::campaign;
use std::path::Path;
use workload::{control_seeds, Durability, Op, Profile, DEFAULT, DETECTION_SEEDS, KEYS};

#[test]
fn should_find_no_violation_when_synced_writes_survive_crashes() {
    // Arrange: lease renewals interleave with the crashes.
    if let Some(directory) = std::env::var_os("MIDGE_CAMPAIGN_REPLAY") {
        let image = load_image(Path::new(&directory));
        let recovered = Campaign::replay(&image);
        assert!(recovered.is_ok(), "saved recovery image: {recovered:#?}");
        return;
    }
    let workloads = control_seeds(32);
    let config = Config {
        lease_ttl_millis: RENEWING_LEASE_TTL_MILLIS,
        ..Config::default()
    };

    // Act
    let finding = campaign(Profile::CrashControl, workloads, 24, config);

    // Assert
    assert_clean("crash-control", finding);
}

#[test]
fn should_find_no_violation_when_buffered_writes_retire_through_power_loss() {
    // Arrange: a power loss can cut a rotation between creating its segment
    // and recording it in the ledger; the open sweeps that empty orphan and
    // the next rotation creates the same name again. The ledger never named
    // the first file, but the reuse check counts names on the device.
    let workloads = control_seeds(32);
    let config = Config {
        segment_reuse: false,
        ..Config::default()
    };

    // Act
    let finding = campaign(Profile::RetentionControl, workloads, 30, config);

    // Assert
    assert_clean("retention-control", finding);
}

#[test]
fn should_keep_created_families_when_process_crash_precedes_power_loss() {
    // Arrange: the review's lead is that a manifest journal created but not
    // dir-synced before a process crash loses its name at the next power
    // loss.
    let workloads = control_seeds(32);

    // Act
    let finding = campaign(Profile::FamilyCrashes, workloads, 24, Config::default());

    // Assert
    assert_clean("family-crashes", finding);
}

#[test]
fn should_keep_best_effort_deleted_keys_absent_when_recovering_completed_flushes() {
    // Arrange
    let config = Config {
        segment_reuse: false,
        ..Config::default()
    };

    // Act
    let finding = campaign(Profile::BestEffortDelete, DETECTION_SEEDS, 16, config);

    // Assert: a completed flush makes the delete recoverable.
    assert_clean("best-effort-delete", finding);
}

/// Formerly the detection of `failed-local-unlink`: a failed removal of a
/// flushed WAL segment left its file in `wal/`, and the next open listed the
/// directory and replayed it, bringing a deleted key back. Retirement now
/// makes the ledger's new log start durable before it unlinks, so a file
/// whose removal fails is an orphan recovery never replays and the next open
/// sweeps (#842).
#[test]
fn should_keep_deleted_keys_absent_when_wal_removals_fail() {
    // Arrange
    let workloads = control_seeds(32);

    // Act
    let finding = campaign(Profile::FailedUnlink, workloads, 30, Config::default());

    // Assert
    assert_clean("failed-unlink-control", finding);
}

/// Formerly the detection of `restarted-segment-id`: once flushes retired
/// every sealed segment, a restart allocated its segment id from what the
/// WAL directory still listed and durably gave a retired name again. The
/// segment ledger now allocates every id past each one it ever named (#842).
#[test]
fn should_not_reuse_segment_names_when_flushes_retire_every_sealed_segment() {
    // Arrange
    let workloads = control_seeds(32);

    // Act
    let finding = campaign(Profile::SegmentRetirement, workloads, 16, Config::default());

    // Assert
    assert_clean("segment-retirement-control", finding);
}

/// Formerly the detection of `recovered-tail-not-durable`: recovery replayed
/// a buffered WAL tail from the page cache and served it without making it
/// durable, so a later power loss lost it. The event loop now seals the
/// recovered tail durably before it serves anything (#843).
#[test]
fn should_find_no_violation_when_buffered_tails_meet_power_loss() {
    // Arrange
    let workloads = control_seeds(32);

    // Act
    let finding = campaign(Profile::BufferedCrashes, workloads, 12, Config::default());

    // Assert
    assert_clean("buffered-crashes-control", finding);
}

/// #811: a completed lease release unlinks its acquisition lock without a
/// directory sync, so a power loss before any later one brings the lock back
/// and refuses every open. Every open republishes the segment ledger and
/// syncs `wal/` after taking its lease (#842), and on the simulated device a
/// directory sync commits every earlier namespace change, the unlink
/// included. The release a clean shutdown completes has no open after it.
#[test]
fn should_allow_successor_when_power_loss_follows_completed_lease_release() {
    // Arrange: the operator's workaround is off.
    let config = Config {
        clear_stale_lease_lock: false,
        ..Config::default()
    };
    let workload = [
        Op::Put {
            family: 0,
            key: 1,
            durability: Durability::Sync,
        },
        Op::PowerLossAfterShutdown,
    ];

    // Act
    let found = Campaign::run(0, &workload, config)
        .err()
        .map(|finding| *finding);

    // Assert
    assert_clean("completed-lease-release", found);
}

/// Registered case `recovered-tail-not-durable`, found by the buffered-crash
/// campaign: recovery replayed a buffered WAL tail from the page cache and
/// served it, but never made it durable, so a later power loss lost data the
/// engine had already returned. Recovery now seals the tail with a footer and
/// fsyncs it before serving (#843).
#[test]
fn should_keep_recovered_tail_when_power_is_lost_after_a_process_crash() {
    // Arrange: a buffered put, then the process dies and a restarted process
    // recovers and serves the replayed tail.
    let mut campaign = Campaign::new(Config::default(), 0);
    campaign.recover(Stop::Clean).expect("first open");
    campaign
        .write(0, 1, false, Durability::Buffered)
        .expect("buffered put");
    campaign.crash_process();
    campaign
        .recover(Stop::ProcessCrash)
        .expect("recover from the process crash");
    let served = campaign.served(&(DEFAULT.to_string(), vec![1]));

    // Act: power is lost with nothing unsynced reaching the device.
    campaign.lose_power_with(|_| 0);
    let recovered = campaign.recover(Stop::PowerLoss);

    // Assert
    assert!(served, "recovery did not serve the buffered tail");
    assert!(
        recovered.is_ok(),
        "recovered WAL tail was served, then lost after power loss: {recovered:?}"
    );
}

/// Registered RED case `lost-buffered-delete`, found by the retention
/// control's nightly budget: a buffered delete is flushed into an SST, a power
/// loss drops its unsynced WAL record, and compaction then drops the flushed
/// tombstone together with the put it shadowed, so recovery replays the put.
///
/// A completed flush wakes its caller before the event loop syncs the WAL to
/// seal the active segment for pruning, so whether the delete's record is
/// still unsynced when power is lost is a race the event loop often wins. The
/// WAL's next sync fails and drops its writeback, so either way the device
/// keeps the WAL from before the delete.
#[test]
fn should_not_resurrect_flushed_delete_when_power_loss_drops_its_wal_record() {
    // Arrange: a synced put, a buffered delete of it and a completed flush,
    // then power is lost with the WAL page from before the delete.
    let mut campaign = Campaign::new(Config::default(), 0);
    campaign.recover(Stop::Clean).expect("first open");
    campaign
        .write(0, 1, false, Durability::Sync)
        .expect("synced put");
    campaign
        .write(0, 1, true, Durability::Buffered)
        .expect("buffered delete");
    campaign.inject(Fault {
        op: FaultOp::Sync,
        path: "wal".into(),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });
    campaign.flush(0).expect("flush the buffered delete");
    campaign.lose_power_with(|_| 0);
    let recovered = campaign.recover(Stop::PowerLoss);

    // Act
    campaign.apply(&Op::Compact, &[]).expect("compact");
    let after_compaction = campaign.apply(&Op::ProcessCrash, &[]);

    // Assert
    assert!(
        recovered.is_ok(),
        "recovery after the power loss: {recovered:?}"
    );
    assert!(
        after_compaction.is_ok(),
        "flushed delete resurrected after compaction: {after_compaction:?}"
    );
}

#[test]
fn should_find_no_violation_when_provider_writes_survive_takeovers() {
    // Arrange: a power loss can cut a rotation between creating its segment
    // and recording it in the ledger; the open sweeps that empty orphan and
    // the next rotation creates the same name again. The ledger never named
    // the first file, but the reuse check counts names on the device.
    let workloads = control_seeds(16);
    let config = Config {
        segment_reuse: false,
        ..Config::cloud(Mode::ProviderCloud)
    };

    // Act
    let finding = campaign(Profile::CloudControl, workloads, 24, config);

    // Assert
    assert_clean("provider-control", finding);
}

/// Found by the provider control (seed 15): a restart reserves a block of
/// sequences in the cache's WAL ledger and a flush then records the family's
/// frontier at a sequence inside that block. A fresh-cache takeover has no
/// ledger, so it starts its sequences from the SSTs and the catalog, below
/// the frontier, and the next recovery skips its acknowledged strict put as
/// already flushed.
#[test]
fn should_keep_strict_put_when_a_fresh_cache_takes_over_after_a_flush() {
    // Arrange: a restart moves the sequence past its predecessor's
    // reservation, then a flush records the family's frontier there.
    let config = Config {
        segment_reuse: false,
        ..Config::cloud(Mode::ProviderCloud)
    };
    let workload = [
        Op::Delete {
            family: 0,
            key: 4,
            durability: Durability::Sync,
        },
        Op::PowerLoss {
            seed: 10_350_570_903_621_081_399,
        },
        Op::Flush { family: 0 },
        Op::FreshCache,
        Op::Put {
            family: 0,
            key: 1,
            durability: Durability::Sync,
        },
        Op::PowerLoss {
            seed: 14_639_653_375_325_880_093,
        },
    ];

    // Act: the cache that wrote the put recovers it after a power loss.
    let finding = Campaign::run(0, &workload, config)
        .err()
        .map(|finding| *finding);

    // Assert
    assert_clean("fresh-cache-below-frontier", finding);
}

#[test]
fn should_find_no_violation_when_simulated_cloud_writes_survive_crashes() {
    // Arrange: a power loss can cut a rotation between creating its segment
    // and recording it in the ledger; the open sweeps that empty orphan and
    // the next rotation creates the same name again. The ledger never named
    // the first file, but the reuse check counts names on the device.
    let workloads = control_seeds(16);
    let config = Config {
        segment_reuse: false,
        ..Config::cloud(Mode::SimulatedCloud)
    };

    // Act
    let finding = campaign(Profile::CloudControl, workloads, 24, config);

    // Assert
    assert_clean("simulated-cloud-control", finding);
}

/// Formerly the detection of `out-of-order-publication` (#841): the catalog
/// published later segments past one whose uploads failed, so a takeover
/// recovered later writes without earlier ones. Publication now waits for a
/// contiguous run, and failed uploads keep retrying.
#[test]
fn should_keep_earlier_writes_when_a_segment_exhausts_its_upload_attempts() {
    // Arrange: segment 1's first three upload attempts fail, and segments
    // seal every two asynchronous writes, so later segments upload past it.
    // A successful clean shutdown promises every acknowledged write before
    // a fresh-cache takeover, including the segment whose uploads fail.
    let config = Config {
        segment_reuse: false,
        store_faults: StoreFaults::FailSegment {
            segment: 1,
            attempts: 3,
        },
        ..Config::cloud(Mode::ProviderCloud)
    };
    let mut campaign = Campaign::new(config, 0);
    campaign
        .recover(Stop::Clean)
        .expect("open provider campaign");
    for key in 0..KEYS {
        assert!(
            campaign
                .write(0, key, false, Durability::Buffered)
                .expect("cloud_async admission"),
            "every seeded cloud_async write must be acknowledged"
        );
    }

    // Act
    campaign
        .apply(&Op::CleanRestart, &[])
        .expect("shutdown must publish every ACK");
    let recovered = campaign.apply(&Op::FreshCache, &[]);

    // Assert
    assert_eq!(campaign.failed_segment_uploads(1), 3);
    assert!(
        recovered.is_ok(),
        "published writes must survive takeover: {recovered:?}"
    );
}

#[test]
fn should_find_no_violation_when_cloud_uploads_fail() {
    // Arrange: WAL and SST uploads and deletes fail before or after their
    // effect, lose their answer or land late.
    let workloads = control_seeds(16);
    let config = Config {
        segment_reuse: false,
        store_faults: StoreFaults::Seeded,
        ..Config::cloud(Mode::ProviderCloud)
    };

    // Act
    let finding = campaign(Profile::CloudFaults, workloads, 12, config);

    // Assert
    assert_clean("cloud-faults", finding);
}
