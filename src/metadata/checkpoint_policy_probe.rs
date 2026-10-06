//! Experimental metadata-only cadence probe. No engine publication path calls
//! this module and its synthetic SST metadata does not establish data durability.
use super::accounting::{Medium, Origin, Owner};
use super::store::ManifestStore;
use super::{FileMeta, Manifest, ManifestEdit, ManifestPersistence};
use crate::common::MidgeResult;
use crate::config::RecoveryPolicy;
use crate::io::{Fs, FsError, FsPath, RealFs};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;

/// Executes the existing real manifest journal/snapshot implementation under
/// an experimental caller cadence. Returns counters, not a production policy.
///
/// # Panics
/// Panics if bounded fixture indices cannot be represented by the host.
///
/// # Errors
/// Returns actual filesystem, framing, accounting or reconstruction failures.
pub fn run_checkpoint_policy_probe(
    root: &Path,
    files: usize,
    cycles: usize,
    interval: usize,
    journal_cap: u64,
) -> MidgeResult<Value> {
    if files == 0
        || files > 4_096
        || cycles == 0
        || cycles > 65_536
        || interval == 0
        || journal_cap == 0
    {
        return Err(crate::MidgeError::InvalidArgument(
            "invalid bounded policy probe size".into(),
        ));
    }
    if std::fs::read_dir(root)?.next().is_some() {
        return Err(crate::MidgeError::InvalidArgument(
            "policy probe requires an empty private directory".into(),
        ));
    }
    let fs: Arc<dyn Fs> = Arc::new(RealFs::new(root).map_err(FsError::into_midge)?);
    let owner = Owner::new();
    let store =
        ManifestStore::new_with_accounting(Arc::clone(&fs), owner.clone(), Medium::Persistent);
    let mut manifest = seed_manifest(files);
    store
        .save_snapshot_for(Origin::Ddl, &manifest)?
        .adopt_into(&mut manifest);
    let before = owner.handle().snapshot();
    let mut journal_peak = 0;
    let mut replay_ns = 0_u128;
    let mut verified_replays = 0;
    let started = Instant::now();
    for cycle in 1..=cycles {
        let edit = ManifestEdit::BumpWalSeq {
            seq: u64::try_from(cycle + 1).expect("bounded cycle"),
        };
        let id = store.append_for(Origin::OrdinaryLocalFlush, &edit)?;
        manifest.apply_edit(&edit);
        manifest.note_applied_journal_edit(id);
        let bytes = fs
            .metadata(&FsPath::new("manifest.journal"))
            .map_err(FsError::into_midge)?
            .len;
        journal_peak = journal_peak.max(bytes);
        // Real reload at every edit verifies all deferred horizons; this is
        // deliberately excluded from measured publication/cadence elapsed.
        let replay_started = Instant::now();
        let recovered =
            ManifestPersistence::load_with_fs_and_policy_typed(&fs, RecoveryPolicy::Strict)?;
        replay_ns += replay_started.elapsed().as_nanos();
        if json!(recovered) != json!(manifest) {
            return Err(crate::MidgeError::Internal(
                "probe manifest reconstruction mismatch".into(),
            ));
        }
        verified_replays += 1;
        if cycle % interval == 0 || bytes >= journal_cap || cycle == cycles {
            store
                .save_snapshot_for(Origin::OrdinaryLocalFlush, &manifest)?
                .adopt_into(&mut manifest);
        }
    }
    let total_ns = started.elapsed().as_nanos();
    let after = owner.handle().snapshot();
    let a = &after
        .bucket(Origin::OrdinaryLocalFlush, Medium::Persistent)
        .counters;
    let b = &before
        .bucket(Origin::OrdinaryLocalFlush, Medium::Persistent)
        .counters;
    Ok(json!({
        "schema_version": 1, "scope": "synthetic_fixed_cardinality_metadata_real_fs",
        "files": files, "cycles": cycles, "snapshot_interval": interval, "journal_trigger_bytes": journal_cap,
        "journal_peak_bytes": journal_peak, "journal_cap_scope": "trigger_after_single_durable_edit_not_hard_preallocation_limit",
        "snapshot_issued_bytes": a.issued_bytes[0] - b.issued_bytes[0],
        "journal_issued_bytes": a.issued_bytes[1] - b.issued_bytes[1],
        "checkpoints": a.checkpoint_complete_count - b.checkpoint_complete_count,
        "checkpoint_elapsed_ns": a.checkpoint_elapsed_ns - b.checkpoint_elapsed_ns,
        "verified_replays": verified_replays, "replay_elapsed_ns": replay_ns,
        "total_elapsed_ns": total_ns, "cadence_elapsed_ns": total_ns.saturating_sub(replay_ns),
        "owner_integrity": after,
        "production_policy_accepted": false,
    }))
}

fn seed_manifest(files: usize) -> Manifest {
    let mut manifest = Manifest::default();
    let cf = ManifestEdit::CreateColumnFamily {
        id: 1,
        name: "probe".into(),
        created_at: 0,
    };
    manifest.apply_edit(&cf);
    for index in 0..files {
        manifest.files.push(FileMeta {
            name: format!("cf-1-sst-{index:08}.sst"),
            level: 1,
            size_bytes: 1_048_576,
            content_crc32c: Some(0),
            cf_id: 1,
            sst_seq: u64::try_from(index).expect("bounded index"),
            smallest_key: Some(index.to_be_bytes().to_vec()),
            largest_key: Some(index.to_be_bytes().to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            key_bounds_complete: true,
            sublevel: 0,
            read_count: Arc::new(AtomicU64::new(0)),
        });
    }
    manifest
}
