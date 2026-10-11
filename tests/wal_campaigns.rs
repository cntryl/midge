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
//! Detection campaigns keep fixed seeds, so they prove the same findings on
//! every run. `MIDGE_CAMPAIGN_REPLAY` names a saved image to replay.

#[path = "wal_campaigns/config.rs"]
mod config;
#[path = "wal_campaigns/harness.rs"]
mod harness;
#[path = "wal_campaigns/images.rs"]
mod images;
#[path = "wal_campaigns/known.rs"]
mod known;
#[path = "wal_campaigns/search.rs"]
mod search;
#[path = "wal_campaigns/workload.rs"]
mod workload;

use config::{Config, Mode, StoreFaults, RENEWING_LEASE_TTL_MILLIS};
use harness::{Campaign, FindingKind, Stop, LEASE_LOCK};
use images::{assert_clean, assert_reproduced, load_image};
use known::{loses_required_value, resurrects};
use search::campaign;
use std::path::Path;
use workload::{control_seeds, Durability, Op, Profile, DEFAULT, DETECTION_SEEDS, KEYS};

#[test]
fn should_find_no_violation_when_synced_writes_survive_crashes() {
    // Arrange: lease renewals interleave with the crashes.
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
    // Arrange: segment-name reuse after restarts is the known
    // `restarted-segment-id`, reported by its own campaign.
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
fn should_reproduce_best_effort_delete_when_campaigning() {
    // Arrange
    let config = Config {
        segment_reuse: false,
        ..Config::default()
    };

    // Act
    let finding = campaign(Profile::BestEffortDelete, DETECTION_SEEDS, 16, config);

    // Assert: a key came back after a best-effort delete.
    assert_reproduced("best-effort-delete", finding, |finding| {
        resurrects(&finding.kind)
            && finding.workload.iter().any(|op| {
                matches!(
                    op,
                    Op::Delete {
                        durability: Durability::BestEffort,
                        ..
                    }
                )
            })
    });
}

#[test]
fn should_reproduce_failed_local_unlink_when_campaigning() {
    // Arrange
    let config = Config {
        segment_reuse: false,
        ..Config::default()
    };

    // Act
    let finding = campaign(Profile::FailedUnlink, DETECTION_SEEDS, 30, config);

    // Assert: a deleted key came back after a WAL removal failed.
    assert_reproduced("failed-local-unlink", finding, |finding| {
        resurrects(&finding.kind) && finding.faults_fired > 0
    });
}

#[test]
fn should_reproduce_restarted_segment_id_when_campaigning() {
    // Arrange
    let workloads = DETECTION_SEEDS;

    // Act
    let finding = campaign(Profile::SegmentRetirement, workloads, 16, Config::default());

    // Assert
    assert_reproduced(
        "restarted-segment-id",
        finding,
        |finding| matches!(&finding.kind, FindingKind::SegmentReuse(name) if name.starts_with("wal/")),
    );
}

#[test]
fn should_reproduce_recovered_tail_loss_when_campaigning() {
    // Arrange
    let workloads = DETECTION_SEEDS;

    // Act
    let finding = campaign(Profile::BufferedCrashes, workloads, 12, Config::default());

    // Assert: a value recovery served after a process crash was lost.
    assert_reproduced("recovered-tail-not-durable", finding, |finding| {
        loses_required_value(&finding.kind)
            && finding.workload.contains(&Op::ProcessCrash)
            && finding
                .workload
                .iter()
                .any(|op| matches!(op, Op::PowerLoss { .. }))
    });
}

#[test]
fn should_reproduce_stale_lease_lock_when_power_is_lost() {
    // Arrange: #811, found by power losses; the operator's workaround is off.
    let config = Config {
        clear_stale_lease_lock: false,
        ..Config::default()
    };

    // Act
    let found = DETECTION_SEEDS
        .map(|seed| (seed, Profile::CrashControl.workload(seed, 24, Mode::Local)))
        .find_map(|(seed, workload)| Campaign::run(seed, &workload, config).err())
        .map(|finding| *finding);

    // Assert
    assert_reproduced(
        "stale-lease-lock",
        found,
        |finding| matches!(&finding.kind, FindingKind::Refused(error) if error.contains(LEASE_LOCK)),
    );
}

/// Replay the image `MIDGE_CAMPAIGN_REPLAY` names, as a campaign saved it
/// below `MIDGE_CAMPAIGN_IMAGES`; CI uploads those as artifacts. Without the
/// variable there is nothing to replay.
#[test]
fn should_replay_saved_image_when_one_is_named() {
    // Arrange
    let Some(directory) = std::env::var_os("MIDGE_CAMPAIGN_REPLAY") else {
        return;
    };
    let image = load_image(Path::new(&directory));

    // Act
    let replayed = Campaign::replay(&image);

    // Assert
    assert!(replayed.is_ok(), "the image reproduces {replayed:#?}");
}

/// Registered RED case `recovered-tail-not-durable`, found by the
/// buffered-crash campaign: recovery replays a buffered WAL tail from the page
/// cache and serves it, but never makes it durable, so a later power loss
/// loses data the engine already returned.
#[test]
#[ignore = "#808 stage 2: recovered-tail-not-durable"]
fn should_keep_recovered_tail_when_power_is_lost_after_a_process_crash() {
    // Arrange: a buffered put, then the process dies and a restarted process
    // recovers and serves the replayed tail.
    let mut campaign = Campaign::new(Config::default(), 0);
    campaign.recover(Stop::Clean).expect("first open");
    campaign.write(0, 1, false, Durability::Buffered);
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
#[test]
#[ignore = "#808 stage 3: lost-buffered-delete"]
fn should_not_resurrect_flushed_delete_when_power_loss_drops_its_wal_record() {
    // Arrange: a synced put, a buffered delete of it and a completed flush,
    // then power is lost with the WAL page from before the delete.
    let mut campaign = Campaign::new(Config::default(), 0);
    campaign.recover(Stop::Clean).expect("first open");
    campaign.write(0, 1, false, Durability::Sync);
    campaign.write(0, 1, true, Durability::Buffered);
    campaign
        .apply(&Op::Flush { family: 0 }, &[])
        .expect("flush");
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
    // Arrange: segment-name reuse after restarts is the known
    // `restarted-segment-id`, reported by its own campaign.
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

#[test]
fn should_find_no_violation_when_simulated_cloud_writes_survive_crashes() {
    // Arrange: segment-name reuse after restarts is the known
    // `restarted-segment-id`, reported by its own campaign.
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
    // The store settles before a fresh-cache takeover.
    let config = Config {
        segment_reuse: false,
        store_faults: StoreFaults::FailSegment {
            segment: 1,
            attempts: 3,
        },
        ..Config::cloud(Mode::ProviderCloud)
    };
    let mut workload: Vec<Op> = (0..KEYS)
        .map(|key| Op::Put {
            family: 0,
            key,
            durability: Durability::Buffered,
        })
        .collect();
    workload.extend([Op::Settle, Op::FreshCache]);

    // Act
    let finding = Campaign::run(0, &workload, config)
        .err()
        .map(|finding| *finding);

    // Assert
    assert_clean("segment-upload-attempts", finding);
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
