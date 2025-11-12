//! Column family configuration types.
//!
//! These configuration enums define storage engine behavior for column families.

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

impl From<CompressionType> for crate::codec::CompressionType {
    fn from(ct: CompressionType) -> Self {
        match ct {
            CompressionType::None => crate::codec::CompressionType::None,
            CompressionType::Lz4 => crate::codec::CompressionType::Lz4,
            CompressionType::Zstd => crate::codec::CompressionType::Zstd3, // Use balanced Zstd level
        }
    }
}
