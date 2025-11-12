use std::sync::Arc;

/// Compaction strategy for organizing SSTables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompactionStyle {
    /// Leveled compaction: each level is a fraction of the size of the next.
    /// Provides better read performance and space amplification.
    Leveled,

    /// Size-tiered compaction: compact files of similar size together.
    /// Provides better write performance but higher space amplification.
    SizeTiered,
}

/// Compression algorithm for SSTable data blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompressionType {
    /// No compression.
    None,

    /// LZ4 compression (very fast, moderate compression ratio).
    Lz4,

    /// Zstd compression (slower, high compression ratio).
    Zstd,
}

impl From<CompressionType> for crate::common::codec::CompressionType {
    fn from(ct: CompressionType) -> Self {
        match ct {
            CompressionType::None => crate::common::codec::CompressionType::None,
            CompressionType::Lz4 => crate::common::codec::CompressionType::Lz4,
            CompressionType::Zstd => crate::common::codec::CompressionType::Zstd3, // Use balanced Zstd level
        }
    }
}

/// A unique identifier for a column family within a database.
///
/// Column family IDs are assigned sequentially starting from 0.
/// The default column family always has ID 0.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct ColumnFamilyId(pub u32);

/// The default column family ID, always present in every database.
pub const DEFAULT_CF_ID: ColumnFamilyId = ColumnFamilyId(0);
pub const DEFAULT_CF_NAME: &str = "default";

impl ColumnFamilyId {
    /// Create a new column family ID.
    #[inline]
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    /// Get the raw u32 value.
    #[inline]
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl From<u32> for ColumnFamilyId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl From<ColumnFamilyId> for u32 {
    fn from(id: ColumnFamilyId) -> Self {
        id.0
    }
}

/// A handle to a column family, used for read/write operations.
///
/// This is a lightweight reference that can be cloned cheaply.
/// The handle remains valid even if the column family is dropped,
/// but operations will fail with an error.
#[derive(Debug, Clone)]
pub struct ColumnFamilyHandle {
    pub(crate) id: ColumnFamilyId,
    pub(crate) name: Arc<str>,
}

impl ColumnFamilyHandle {
    /// Create a new column family handle.
    pub(crate) fn new(id: ColumnFamilyId, name: String) -> Self {
        Self {
            id,
            name: Arc::from(name.as_str()),
        }
    }

    /// Get the column family ID.
    #[inline]
    pub fn id(&self) -> ColumnFamilyId {
        self.id
    }

    /// Get the column family name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Configuration options for a column family.
///
/// Each column family can have independent settings for memtable size,
/// compaction strategy, compression, and other parameters.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnFamilyConfig {
    /// Maximum size of the active memtable in bytes.
    /// When exceeded, the memtable is frozen and a flush is triggered.
    /// Default: 64 MB
    pub memtable_max_bytes: usize,

    /// Maximum number of immutable memtables waiting to be flushed.
    /// If this limit is reached, writes will be stalled.
    /// Default: 2
    pub max_immutable_memtables: usize,

    /// Compaction strategy for this column family.
    /// Default: Leveled
    pub compaction_style: CompactionStyle,

    /// Target size for SSTable files in bytes.
    /// Compaction will aim to produce files around this size.
    /// Default: 64 MB
    pub target_file_size: usize,

    /// Maximum level for the LSM tree (L0, L1, ..., Lmax).
    /// Default: 7
    pub max_level: usize,

    /// Size multiplier between levels in leveled compaction.
    /// Level N+1 will be approximately this many times larger than level N.
    /// Default: 10
    pub level_size_multiplier: usize,

    /// Number of bits per key for bloom filters.
    /// Higher values reduce false positive rate but use more memory.
    /// Default: 10 (approximately 1% false positive rate)
    pub bloom_bits_per_key: u32,

    /// Compression algorithm for SSTable data blocks.
    /// Default: None
    pub compression: CompressionType,

    /// Optional per-CF block cache size in bytes.
    /// If None, uses the shared block cache.
    /// Default: None (shared cache)
    pub block_cache_size: Option<usize>,

    /// Optional time-to-live in seconds.
    /// Entries older than this will be removed during compaction.
    /// If None, entries never expire.
    /// Default: None
    pub ttl_seconds: Option<u64>,
}

impl Default for ColumnFamilyConfig {
    fn default() -> Self {
        Self {
            memtable_max_bytes: 64 * 1024 * 1024, // 64 MB
            max_immutable_memtables: 2,
            compaction_style: CompactionStyle::Leveled,
            target_file_size: 64 * 1024 * 1024, // 64 MB
            max_level: 7,
            level_size_multiplier: 10,
            bloom_bits_per_key: 10,
            compression: CompressionType::None,
            block_cache_size: None,
            ttl_seconds: None,
        }
    }
}

// ---------------- Testing ----------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_column_family_id_and_convert_to_u32() {
        // Arrange
        let id = ColumnFamilyId::new(5);

        // Act
        let value = id.as_u32();

        // Assert
        assert_eq!(value, 5);
    }

    #[test]
    fn should_convert_u32_to_column_family_id() {
        // Arrange
        let id: ColumnFamilyId = 10u32.into();

        // Act
        let value = u32::from(id);

        // Assert
        assert_eq!(value, 10);
    }

    #[test]
    fn should_have_default_column_family_id_of_zero() {
        // Arrange
        let default_id = DEFAULT_CF_ID;

        // Act
        let value = default_id.as_u32();

        // Assert
        assert_eq!(value, 0);
    }

    #[test]
    fn should_create_column_family_handle_with_id_and_name() {
        // Arrange
        let id = ColumnFamilyId::new(1);
        let name = "test_cf".to_string();

        // Act
        let handle = ColumnFamilyHandle::new(id, name);

        // Assert
        assert_eq!(handle.id().as_u32(), 1);
        assert_eq!(handle.name(), "test_cf");
    }

    #[test]
    fn should_create_column_family_config_with_sensible_defaults() {
        // Arrange

        // Act
        let config = ColumnFamilyConfig::default();

        // Assert
        assert_eq!(config.memtable_max_bytes, 64 * 1024 * 1024);
        assert_eq!(config.max_immutable_memtables, 2);
        assert_eq!(config.compaction_style, CompactionStyle::Leveled);
        assert_eq!(config.bloom_bits_per_key, 10);
        assert_eq!(config.compression, CompressionType::None);
        assert_eq!(config.block_cache_size, None);
        assert_eq!(config.ttl_seconds, None);
    }

    #[test]
    fn should_clone_column_family_handle_preserving_id_and_name() {
        // Arrange
        let handle = ColumnFamilyHandle::new(ColumnFamilyId::new(2), "clone_test".to_string());

        // Act
        let cloned = handle.clone();

        // Assert
        assert_eq!(handle.id(), cloned.id());
        assert_eq!(handle.name(), cloned.name());
    }
}
