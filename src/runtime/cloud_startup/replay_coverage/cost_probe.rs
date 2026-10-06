//! Same real SST bytes, varied probe order/budget/proof-release schedule.
//! Filesystem-backed remote adapter; no production-provider latency claim.
use super::ReplayCoverage;
use crate::io::{Fs, FsError, RealFs};
use crate::metadata::{FileMeta, Manifest};
use crate::sst::FsSstFactoryIo;
use crate::wal::{WalOpKind, WalRecord};
use crate::{Bytes, MidgeError, MidgeResult};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Default)]
struct Reads {
    calls: AtomicU64,
    bytes: AtomicU64,
    failed: AtomicU64,
}
impl crate::io::traits::ReadObserver for Reads {
    fn remote_range_started(&self) {}
    fn remote_range_completed(&self, bytes: u64, _elapsed: Duration, failed: bool) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.failed.fetch_add(u64::from(failed), Ordering::Relaxed);
    }
}

/// Build a deterministic real-SST fixture and exercise unchanged coverage.
///
/// # Errors
/// Returns real SST/filesystem/coverage errors or an exact-proof mismatch.
pub fn run_recovery_cost_probe(
    root: &Path,
    files: usize,
    entries: usize,
    budget: usize,
    interleaved: bool,
    release_interval: usize,
) -> MidgeResult<Value> {
    if files == 0 || files > 64 || entries == 0 || entries > 1_024 || budget == 0 {
        return Err(MidgeError::InvalidArgument(
            "invalid bounded recovery probe size".into(),
        ));
    }
    if std::fs::read_dir(root)?.next().is_some() {
        return Err(MidgeError::InvalidArgument(
            "recovery probe requires an empty private directory".into(),
        ));
    }
    let local: Arc<dyn Fs> = Arc::new(RealFs::new(root).map_err(FsError::into_midge)?);
    let (manifest, fixture_bytes) = seed_ssts(root, files, entries, &local)?;
    let cloud = Arc::new(crate::storage::filesystem::FileSystem::new(
        root.join("cloud"),
    )?);
    let reads = Arc::new(Reads::default());
    let remote: Arc<dyn Fs> = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
        local,
        cloud,
        Duration::from_secs(5),
    ));
    let remote = remote.with_read_observer(reads.clone()).ok_or_else(|| {
        MidgeError::Internal("remote adapter cannot retain counting observer".into())
    })?;
    let coverage = ReplayCoverage::new(manifest, remote, budget);
    let started = Instant::now();
    let mut releases = 0;
    for index in 0..files * entries {
        let (file, entry) = if interleaved {
            (index % files, index / files)
        } else {
            (index / entries, index % entries)
        };
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from(key(file, entry)),
            Some(Bytes::from_static(b"value")),
            7,
            1,
        );
        if !coverage.contains(&record) {
            return Err(MidgeError::Internal(
                "real-SST exact coverage probe failed".into(),
            ));
        }
        if release_interval != 0 && (index + 1) % release_interval == 0 {
            coverage.release_reader();
            releases += 1;
        }
    }
    let elapsed = started.elapsed().as_nanos();
    coverage.release_reader();
    if coverage.read_budget.used() != 0 {
        return Err(MidgeError::Internal(
            "probe retained resource charge".into(),
        ));
    }
    Ok(json!({
        "schema_version": 1, "scope": "real_sst_exact_coverage_filesystem_remote_adapter",
        "fixture_xxh3_128": format!("{:032x}", xxhash_rust::xxh3::xxh3_128(&fixture_bytes)),
        "files": files, "entries_per_file": entries, "budget_bytes": budget, "interleaved": interleaved,
        "proof_release_interval": release_interval, "explicit_proof_releases": releases,
        "probes": coverage.probes.get(), "all_exact_proofs_passed": true,
        "manifest_nodes_visited": coverage.manifest_scanned.get(), "manifest_candidates": coverage.manifest_candidates.get(),
        "reader_opens": coverage.reader_opens.get(), "reader_evictions": coverage.reader_evictions.get(),
        "verified_sst_bytes": coverage.verified_bytes.get(), "decoded_entries": coverage.decode_steps.get(),
        "key_reconstruction_allocations": coverage.reconstruction_allocations.get(),
        "block_hits": coverage.block_hits.get(), "block_misses": coverage.block_misses.get(),
        "charged_peak_bytes": coverage.read_budget.peak(), "charged_final_bytes": coverage.read_budget.used(),
        "coverage_elapsed_ns": coverage.elapsed_ns.get(), "wall_elapsed_ns": elapsed,
        "remote_range_calls": reads.calls.load(Ordering::Relaxed), "remote_range_bytes": reads.bytes.load(Ordering::Relaxed),
        "remote_range_failures": reads.failed.load(Ordering::Relaxed), "production_optimization_accepted": false,
    }))
}

fn key(file: usize, entry: usize) -> Vec<u8> {
    format!("file-{file:03}-key-{entry:08}").into_bytes()
}

fn seed_ssts(
    root: &Path,
    files: usize,
    entries: usize,
    local: &Arc<dyn Fs>,
) -> MidgeResult<(Manifest, Vec<u8>)> {
    let factory = FsSstFactoryIo::new(Arc::clone(local), 64 * 1_024)
        .with_compression_policy(crate::codec::CompressionPolicy::None);
    std::fs::create_dir_all(root.join("cloud/sst"))?;
    let mut manifest = Manifest::default();
    let mut fixture_bytes = Vec::new();
    for file in 0..files {
        let mut writer = factory.create_for_flush(
            crate::common::resource_budget::ResourceBudget::new(8 * 1_024 * 1_024),
        )?;
        for entry in 0..entries {
            writer.add_sorted_with_meta(
                &key(file, entry),
                Some(b"value"),
                7,
                crate::types::EntryType::Put,
                None,
            )?;
        }
        let bytes = writer.finish_bytes()?;
        let name =
            crate::cloud_layout::file_name(0, 0, u64::try_from(file + 1).expect("bounded file"));
        std::fs::write(root.join("cloud/sst").join(&name), &bytes)?;
        fixture_bytes.extend_from_slice(&bytes);
        manifest.files.push(FileMeta {
            name,
            level: 0,
            size_bytes: u64::try_from(bytes.len()).expect("bounded SST"),
            content_crc32c: Some(crc32c::crc32c(&bytes)),
            cf_id: 0,
            sst_seq: u64::try_from(file + 1).expect("bounded file"),
            smallest_key: Some(key(file, 0)),
            largest_key: Some(key(file, entries - 1)),
            smallest_seq: Some(7),
            largest_seq: Some(7),
            key_bounds_complete: true,
            sublevel: 0,
            read_count: Arc::new(AtomicU64::new(0)),
        });
    }
    Ok((manifest, fixture_bytes))
}
