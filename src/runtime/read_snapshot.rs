//! Read snapshot - immutable view for parallel read execution
//!
//! Captures immutable references to memtables and SST metadata
//! at a specific sequence number, allowing safe parallel reads.

use crate::runtime::range_cover::RangeCoverSweep;
use crate::runtime::read_resources::ReadResources;
use crate::runtime::sst_read_view::{LevelRangeCandidates, RangeCandidates, SstReadView};
use cntryl_midge_core::common::{MidgeError, MidgeResult};
use cntryl_midge_core::io::Fs;
#[cfg(test)]
use cntryl_midge_core::types::EntryType;
use cntryl_midge_core::types::{KeyState, RangeTombstone};
use cntryl_midge_durability::metadata::FileMeta;
use cntryl_midge_table::memtable::SkipListMemtable;
use cntryl_midge_table::sst::fs::reader_io::SstStateScan;
use cntryl_midge_table::sst::fs::SstFileIo;
use cntryl_midge_table::sst::traits::SstStateReader;
#[cfg(test)]
use std::collections::BTreeMap;
use std::sync::Arc;

/// Immutable snapshot of readable state for a column family
///
/// This struct captures all necessary state to perform read operations
/// without holding references to mutable runtime state.
pub struct ReadSnapshot {
    sources: Arc<ReadSnapshotSources>,
    /// Wall-clock captured when this snapshot was created. TTL visibility is
    /// evaluated against this value for the whole snapshot.
    pub read_time_millis: u64,
}

/// Shared immutable sources. Capturing a new TTL read time only clones an Arc.
#[doc(hidden)]
pub struct ReadSnapshotSources {
    /// CF ID
    pub cf_id: cntryl_midge_core::types::ColumnFamilyId,
    /// Active memtable snapshot
    pub memtable: Arc<SkipListMemtable>,
    /// Immutable memtables, oldest to newest as frozen; point reads iterate
    /// them in reverse so newer versions are seen first.
    pub immutable_memtables: Vec<Arc<SkipListMemtable>>,
    /// Immutable indexed SST catalog for this column family.
    pub(crate) sst_view: Arc<SstReadView>,
    /// SST filesystem handle (rooted at `db_path`)
    pub sst_fs: Arc<dyn Fs>,
    /// SST path prefix relative to the fs root (typically "sst")
    pub sst_path_prefix: std::path::PathBuf,
    /// In-memory mode flag (skip SST reads when true)
    pub memory_mode: bool,
    read_resources: Option<Arc<ReadResources>>,
    diagnostics: Arc<crate::diagnostics::RuntimeDiagnostics>,
}

impl std::ops::Deref for ReadSnapshot {
    type Target = ReadSnapshotSources;

    fn deref(&self) -> &Self::Target {
        &self.sources
    }
}

mod merge;
pub(crate) mod point_read;
mod scan;
mod snapshot;

pub(crate) use scan::SnapshotScan;

impl std::fmt::Debug for ReadSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadSnapshot")
            .field("cf_id", &self.cf_id)
            .field("memtable", &"<memtable>")
            .field("immutable_memtables_len", &self.immutable_memtables.len())
            .field("sst_files_len", &self.sst_view.file_count())
            .field("sst_fs", &"<dyn Fs>")
            .field("sst_path_prefix", &self.sst_path_prefix)
            .field("memory_mode", &self.memory_mode)
            .field("read_time_millis", &self.read_time_millis)
            .field("read_resources", &self.read_resources.is_some())
            .finish_non_exhaustive()
    }
}
