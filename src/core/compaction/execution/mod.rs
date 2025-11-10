/// Compaction execution implementation.
///
/// This module contains the low-level machinery for executing compaction operations:
/// - Collecting versions from multiple SST files
/// - Filtering tombstones based on snapshot visibility  
/// - Applying compaction filters
/// - Writing compacted SST files
///
/// The high-level compaction strategy (when to compact, which files to select)
/// is handled by the parent `compaction` module.

pub(crate) mod collection;
pub(crate) mod filtering;
pub(crate) mod merging;
pub(crate) mod output_writer;
pub(crate) mod types;

// Re-export public API for backward compatibility
pub use types::CompactionVersion;

pub(crate) use collection::{collect_compaction_versions, sort_versions_for_output};
pub(crate) use filtering::apply_compaction_filter;
pub(crate) use merging::{deduplicate_versions, filter_safe_tombstones};
pub(crate) use output_writer::{write_compacted_sst, SstWriterContext};
