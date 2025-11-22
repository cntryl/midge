//! Centralized file naming and sequence allocation for Midge LSM storage.
//!
//! Requirements implemented:
//! - WAL seq: global, monotonic, padded
//! - SST seq: per-CF, monotonic, padded
//! - Filenames encode only ordering (no metadata leaks)
//! - Manifest persists allocator state (but manifest is optional for recovery)

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

use crate::api::column_family::ColumnFamilyId;
use dashmap::DashMap;

/// Fixed width for padded sequence numbers.
/// Ensures lexicographic sort == numeric sort across all storage backends.
const PAD_WIDTH: usize = 16;

fn pad(n: u64) -> String {
    format!("{:0width$}", n, width = PAD_WIDTH)
}

/// Global WAL sequence allocator.
static NEXT_WAL_SEQ: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new(1));

/// Per-CF SST sequence allocators (key: cf_id).
static NEXT_SST_SEQS: LazyLock<DashMap<u32, AtomicU64>> = LazyLock::new(|| DashMap::new());

/// Initialize sequence allocators from manifest.
pub fn initialize_sequences(manifest: &crate::core::manifest::Manifest) {
    NEXT_WAL_SEQ.store(manifest.next_wal_seq, Ordering::Relaxed);

    NEXT_SST_SEQS.clear();
    for (&cf_id, &next_seq) in &manifest.next_sst_seqs {
        NEXT_SST_SEQS.insert(cf_id, AtomicU64::new(next_seq));
    }
}

/// Allocate next WAL seq.
pub fn allocate_wal_seq() -> u64 {
    NEXT_WAL_SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Allocate next SST seq for CF.
pub fn allocate_sst_seq(cf_id: ColumnFamilyId) -> u64 {
    let id = cf_id.as_u32();

    if let Some(entry) = NEXT_SST_SEQS.get(&id) {
        return entry.fetch_add(1, Ordering::Relaxed);
    }

    // First allocation for this CF.
    NEXT_SST_SEQS.insert(id, AtomicU64::new(2)); // next will be 2
    1
}

/// Snapshot WAL seq for manifest.
pub fn current_next_wal_seq() -> u64 {
    NEXT_WAL_SEQ.load(Ordering::Relaxed)
}

/// Snapshot all SST seqs for manifest.
pub fn current_next_sst_seqs() -> std::collections::HashMap<u32, u64> {
    NEXT_SST_SEQS
        .iter()
        .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
        .collect()
}

/// ---- Naming helpers --------------------------------------------------------

pub fn wal_filename(wal_seq: u64) -> String {
    format!("{}.wal", pad(wal_seq))
}

pub fn wal_path(db_path: &Path, wal_seq: u64) -> PathBuf {
    db_path.join(wal_filename(wal_seq))
}

pub fn sst_filename(sst_seq: u64) -> String {
    format!("{}.sst", pad(sst_seq))
}

pub fn sst_cf_dir(db_path: &Path, cf_id: ColumnFamilyId) -> PathBuf {
    db_path.join(cf_id.as_u32().to_string())
}

pub fn sst_path(db_path: &Path, cf_id: ColumnFamilyId, sst_seq: u64) -> PathBuf {
    sst_cf_dir(db_path, cf_id).join(sst_filename(sst_seq))
}

pub fn manifest_path(db_path: &Path) -> PathBuf {
    db_path.join("manifest.json")
}

/// ---- Parsing ---------------------------------------------------------------

pub fn parse_wal_seq(filename: &str) -> Option<u64> {
    filename.strip_suffix(".wal")?.parse().ok()
}

pub fn parse_sst_seq(filename: &str) -> Option<u64> {
    filename.strip_suffix(".sst")?.parse().ok()
}

pub fn parse_cf_id_from_dir(dirname: &str) -> Option<u32> {
    dirname.parse().ok()
}

/// ---- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn wal_naming() {
        let dir = TempDir::new().unwrap();
        assert_eq!(wal_filename(1), format!("{:016}.wal", 1));
        assert_eq!(
            wal_path(dir.path(), 1),
            dir.path().join(format!("{:016}.wal", 1))
        );

        assert_eq!(parse_wal_seq("0000000000000005.wal"), Some(5));
        assert!(parse_wal_seq("x.wal").is_none());
    }

    #[test]
    fn sst_naming() {
        let dir = TempDir::new().unwrap();
        let cf = ColumnFamilyId::new(7);

        assert_eq!(sst_filename(3), format!("{:016}.sst", 3));
        assert_eq!(
            sst_path(dir.path(), cf, 3),
            dir.path().join("7").join(format!("{:016}.sst", 3))
        );

        assert_eq!(parse_sst_seq("0000000000000042.sst"), Some(42));
        assert_eq!(parse_cf_id_from_dir("7"), Some(7));
    }

    #[test]
    fn seq_alloc() {
        NEXT_WAL_SEQ.store(1, Ordering::Relaxed);
        NEXT_SST_SEQS.clear();

        assert_eq!(allocate_wal_seq(), 1);
        assert_eq!(allocate_wal_seq(), 2);
        assert_eq!(current_next_wal_seq(), 3);

        let cf = ColumnFamilyId::new(1);
        assert_eq!(allocate_sst_seq(cf), 1);
        assert_eq!(allocate_sst_seq(cf), 2);
        assert_eq!(allocate_sst_seq(cf), 3);

        let map = current_next_sst_seqs();
        assert_eq!(map.get(&1), Some(&4));
    }
}
