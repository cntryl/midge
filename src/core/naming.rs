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
static NEXT_SST_SEQS: LazyLock<DashMap<u32, AtomicU64>> = LazyLock::new(DashMap::new);

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
        .map(|e| (*e.key(), e.value().load(Ordering::Relaxed)))
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
    fn should_generate_wal_filename_when_sequence_provided() {
        // Arrange
        let seq = 1;

        // Act
        let filename = wal_filename(seq);

        // Assert
        assert_eq!(filename, format!("{:016}.wal", 1));
    }

    #[test]
    fn should_generate_wal_path_when_sequence_provided() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let seq = 1;

        // Act
        let path = wal_path(dir.path(), seq);

        // Assert
        assert_eq!(path, dir.path().join(format!("{:016}.wal", 1)));
    }

    #[test]
    fn should_parse_wal_sequence_when_valid_filename_provided() {
        // Arrange
        let filename = "0000000000000005.wal";

        // Act
        let seq = parse_wal_seq(filename);

        // Assert
        assert_eq!(seq, Some(5));
    }

    #[test]
    fn should_return_none_when_parsing_invalid_wal_filename() {
        // Arrange
        let filename = "x.wal";

        // Act
        let seq = parse_wal_seq(filename);

        // Assert
        assert!(seq.is_none());
    }

    #[test]
    fn should_generate_sst_filename_when_sequence_provided() {
        // Arrange
        let seq = 3;

        // Act
        let filename = sst_filename(seq);

        // Assert
        assert_eq!(filename, format!("{:016}.sst", 3));
    }

    #[test]
    fn should_generate_sst_path_when_sequence_and_cf_provided() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let cf = ColumnFamilyId::new(7);
        let seq = 3;

        // Act
        let path = sst_path(dir.path(), cf, seq);

        // Assert
        assert_eq!(path, dir.path().join("7").join(format!("{:016}.sst", 3)));
    }

    #[test]
    fn should_parse_sst_sequence_when_valid_filename_provided() {
        // Arrange
        let filename = "0000000000000042.sst";

        // Act
        let seq = parse_sst_seq(filename);

        // Assert
        assert_eq!(seq, Some(42));
    }

    #[test]
    fn should_parse_cf_id_when_valid_directory_name_provided() {
        // Arrange
        let dirname = "7";

        // Act
        let cf_id = parse_cf_id_from_dir(dirname);

        // Assert
        assert_eq!(cf_id, Some(7));
    }

    #[test]
    fn should_allocate_incrementing_wal_sequences() {
        // Arrange
        NEXT_WAL_SEQ.store(1, Ordering::Relaxed);

        // Act
        let seq1 = allocate_wal_seq();
        let seq2 = allocate_wal_seq();
        let next = current_next_wal_seq();

        // Assert
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(next, 3);
    }

    #[test]
    fn should_allocate_incrementing_sst_sequences_per_column_family() {
        // Arrange
        NEXT_SST_SEQS.clear();
        let cf = ColumnFamilyId::new(1);

        // Act
        let seq1 = allocate_sst_seq(cf);
        let seq2 = allocate_sst_seq(cf);
        let seq3 = allocate_sst_seq(cf);
        let map = current_next_sst_seqs();

        // Assert
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(seq3, 3);
        assert_eq!(map.get(&1), Some(&4));
    }
}
