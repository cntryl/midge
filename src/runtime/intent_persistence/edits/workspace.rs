//! Borrowed preflight for the typed allocations made by intent replay.

use super::{codec, frame_checksum, IntentLogEntry, MidgeError, MidgeResult};
use crate::common::OperationDeadline;
use crate::metadata::codec::workspace::{
    Byte, HeapFootprint, MetadataFile, Name, Sequence, VectorLayout,
};
use serde::de::IgnoredAny;
use serde::Deserialize;
use std::mem::size_of;

mod append;
#[cfg(test)]
mod tests;
use append::Append;

pub(crate) struct IntentDecodeWorkspace {
    /// Peak owned typed decode payload. Raw immutable input is counted by its
    /// capture owner; later serialization scratch is also separately charged.
    pub(crate) bytes: usize,
    /// All appended entries, including superseded prefixes.
    pub(crate) entries: usize,
    /// Final vector capacity and a conservative retained owned-field bound.
    /// Superseded dynamic fields remain counted without a per-entry cache.
    pub(crate) retained_state_bytes: usize,
    /// Peak scratch for one publication entry's normalized named encoding.
    pub(crate) max_single_normalization_scratch: usize,
    /// Appended publication variants only, including superseded entries.
    pub(crate) publication_entries: usize,
}

#[derive(Deserialize)]
struct Edit {
    sequence: u64,
    previous_checksum: u32,
    keep: usize,
    append: Append,
}

#[derive(Deserialize)]
struct RuntimeFile {
    name: Name,
    #[serde(rename = "level")]
    _level: IgnoredAny,
    #[serde(rename = "size_bytes")]
    _size_bytes: IgnoredAny,
    #[serde(default, rename = "content_crc32c")]
    _content_crc32c: IgnoredAny,
    #[serde(rename = "cf_id")]
    _cf_id: IgnoredAny,
    smallest_key: Option<Sequence<Byte>>,
    largest_key: Option<Sequence<Byte>>,
    #[serde(default, rename = "smallest_seq")]
    _smallest_seq: IgnoredAny,
    #[serde(default, rename = "largest_seq")]
    _largest_seq: IgnoredAny,
    #[serde(default, rename = "key_bounds_complete")]
    _key_bounds_complete: IgnoredAny,
    #[serde(default, rename = "earliest_expiration_millis")]
    _earliest_expiration_millis: IgnoredAny,
}

impl HeapFootprint for RuntimeFile {
    const ITEM_BYTES: usize = size_of::<crate::runtime::FileMeta>();

    fn heap_bytes(&self) -> Option<usize> {
        self.name
            .heap_bytes()?
            .checked_add(
                self.smallest_key
                    .as_ref()
                    .map_or(Some(0), HeapFootprint::heap_bytes)?,
            )?
            .checked_add(
                self.largest_key
                    .as_ref()
                    .map_or(Some(0), HeapFootprint::heap_bytes)?,
            )
    }
}

#[derive(Deserialize)]
enum Entry {
    SeqnoAllocated {
        #[serde(rename = "seqno")]
        _seqno: IgnoredAny,
        #[serde(rename = "cf_id")]
        _cf_id: IgnoredAny,
    },
    FlushPlanned {
        #[serde(rename = "cf_id")]
        _cf_id: IgnoredAny,
        #[serde(rename = "seqno_range")]
        _seqno_range: IgnoredAny,
    },
    CompactionPlanned {
        input_files: Sequence<Name>,
        #[serde(rename = "output_level")]
        _output_level: IgnoredAny,
    },
    FlushPublish {
        #[serde(rename = "phase")]
        _phase: IgnoredAny,
        #[serde(rename = "cf_id")]
        _cf_id: IgnoredAny,
        #[serde(rename = "sequence")]
        _sequence: IgnoredAny,
        file_meta: RuntimeFile,
    },
    CompactionPublish {
        #[serde(rename = "phase")]
        _phase: IgnoredAny,
        #[serde(rename = "cf_id")]
        _cf_id: IgnoredAny,
        removed: Sequence<Name>,
        added: Sequence<RuntimeFile>,
    },
    MoveSstsPublish {
        #[serde(rename = "phase")]
        _phase: IgnoredAny,
        captured: Sequence<MetadataFile>,
        #[serde(rename = "target_level")]
        _target_level: IgnoredAny,
    },
    SstAdded {
        file_meta: RuntimeFile,
    },
    CompactionApplied {
        removed: Sequence<Name>,
        added: Sequence<RuntimeFile>,
    },
    WalSynced {
        #[serde(rename = "segment_id")]
        _segment_id: IgnoredAny,
        #[serde(rename = "seqno")]
        _seqno: IgnoredAny,
    },
    CloudUploadComplete {
        resource: Name,
        #[serde(rename = "seqno")]
        _seqno: IgnoredAny,
    },
}

impl HeapFootprint for Entry {
    const ITEM_BYTES: usize = size_of::<IntentLogEntry>();

    fn heap_bytes(&self) -> Option<usize> {
        match self {
            Self::SeqnoAllocated { .. } | Self::FlushPlanned { .. } | Self::WalSynced { .. } => {
                Some(0)
            }
            Self::CompactionPlanned { input_files, .. } => input_files.heap_bytes(),
            Self::FlushPublish { file_meta, .. } | Self::SstAdded { file_meta } => {
                file_meta.heap_bytes()
            }
            Self::CompactionPublish { removed, added, .. }
            | Self::CompactionApplied { removed, added } => {
                removed.heap_bytes()?.checked_add(added.heap_bytes()?)
            }
            Self::MoveSstsPublish { captured, .. } => captured.heap_bytes(),
            Self::CloudUploadComplete { resource, .. } => resource.heap_bytes(),
        }
    }
}

impl Entry {
    fn is_publication(&self) -> bool {
        matches!(
            self,
            Self::FlushPublish { .. }
                | Self::CompactionPublish { .. }
                | Self::MoveSstsPublish { .. }
        )
    }
}

pub(super) fn decoded_workspace_bound(
    bytes: &[u8],
    deadline: &OperationDeadline,
) -> MidgeResult<IntentDecodeWorkspace> {
    let mut state = ReplayFootprint::default();
    let mut position = 0;
    check_deadline(deadline)?;
    while let Some(frame) = codec::frame_len(&bytes[position..])? {
        check_deadline(deadline)?;
        let end = position.checked_add(frame).ok_or_else(overflow)?;
        let encoded = &bytes[position..end];
        let edit: Edit = codec::decode(codec::INTENT, encoded)?;
        check_deadline(deadline)?;
        state.apply(&edit)?;
        state.checksum = frame_checksum(encoded);
        position = end;
    }
    check_deadline(deadline)?;
    Ok(IntentDecodeWorkspace {
        bytes: state.peak,
        entries: state.entries,
        retained_state_bytes: state
            .vector
            .capacity
            .checked_mul(Entry::ITEM_BYTES)
            .and_then(|bytes| bytes.checked_add(state.dynamic_bytes))
            .ok_or_else(overflow)?,
        max_single_normalization_scratch: state.max_single_normalization_scratch,
        publication_entries: state.publication_entries,
    })
}

#[derive(Default)]
struct ReplayFootprint {
    sequence: u64,
    checksum: u32,
    len: usize,
    vector: VectorLayout,
    dynamic_bytes: usize,
    entries: usize,
    publication_entries: usize,
    max_single_normalization_scratch: usize,
    peak: usize,
}

impl ReplayFootprint {
    fn apply(&mut self, edit: &Edit) -> MidgeResult<()> {
        if self.sequence.checked_add(1) != Some(edit.sequence)
            || self.checksum != edit.previous_checksum
            || edit.keep > self.len
        {
            return Err(MidgeError::Corruption(
                "intent predecessor or prefix mismatch".into(),
            ));
        }
        let old_state = self
            .vector
            .capacity
            .checked_mul(Entry::ITEM_BYTES)
            .ok_or_else(overflow)?;
        // Superseded entries' heap bytes are deliberately retained in this
        // scalar upper bound, never in a decoded cache or per-entry index.
        self.dynamic_bytes = self
            .dynamic_bytes
            .checked_add(edit.append.dynamic_bytes)
            .ok_or_else(overflow)?;
        let decode_peak = old_state
            .checked_add(edit.append.peak_bytes)
            .and_then(|bytes| bytes.checked_add(self.dynamic_bytes))
            .ok_or_else(overflow)?;
        self.len = edit
            .keep
            .checked_add(edit.append.len)
            .ok_or_else(overflow)?;
        let state_overlap = self
            .vector
            .grow(self.len, Entry::ITEM_BYTES)
            .ok_or_else(overflow)?;
        // Vec::extend reserves before the IntoIter append allocation drops.
        // A state grow may temporarily retain both its old and new buffer.
        let apply_peak = state_overlap
            .checked_add(edit.append.capacity_bytes)
            .and_then(|bytes| bytes.checked_add(self.dynamic_bytes))
            .ok_or_else(overflow)?;
        self.peak = self.peak.max(decode_peak).max(apply_peak);
        self.entries = self
            .entries
            .checked_add(edit.append.len)
            .ok_or_else(overflow)?;
        self.publication_entries = self
            .publication_entries
            .checked_add(edit.append.publication_entries)
            .ok_or_else(overflow)?;
        self.max_single_normalization_scratch = self
            .max_single_normalization_scratch
            .max(edit.append.max_single_normalization_scratch);
        self.sequence = edit.sequence;
        Ok(())
    }
}

fn check_deadline(deadline: &OperationDeadline) -> MidgeResult<()> {
    if deadline.is_expired() {
        return Err(MidgeError::Timeout(
            "intent workspace preflight exceeded original operation deadline".into(),
        ));
    }
    Ok(())
}

fn overflow() -> MidgeError {
    MidgeError::ResourceLimit("intent decoded workspace overflow".into())
}
