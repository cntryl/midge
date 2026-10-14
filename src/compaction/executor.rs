//! Compaction execution: lazy version merging and output
//!
//! This module implements a K-way compaction pipeline:
//!   1. Open one block-at-a-time logical-version cursor per input SST.
//!   2. Merge their heads into a sorted stream (key ascending, seq descending).
//!   3. Deduplicate one key at a time (newest version first).
//!   4. Preserve raw TTL values; read snapshots alone interpret expiration.
//!   5. Feed the result directly to the `SstFactory` writer.
//!
//! This keeps one merge head per input and avoids materializing either an input
//! SST, the aggregate plan, or a second deduplicated output vector.

use crate::common::MidgeResult;
use crate::sst::traits::RawSstVersion;

mod merge;
mod partition;

pub(crate) use merge::{
    collect_compaction_stream_inputs, merge_repair_inputs_to_run, TombstoneGcPolicy,
};
pub(crate) use partition::write_partitioned_compaction_outputs;

/// A single logical version of a key observed during compaction.
///
/// Compaction consumers treat this as the "flattened" key history:
///   - `seq` is strictly monotonic per write.
///   - Higher `seq` means "newer".
///   - Tombstones represent deletions.
///   - TTL is expressed as an absolute expiry timestamp (milliseconds since epoch).
pub type CompactionVersion = RawSstVersion;

fn tombstone_is_obsolete(sequence: u64, snapshot_horizon: Option<u64>) -> bool {
    snapshot_horizon.is_none_or(|horizon| sequence <= horizon)
}

pub(super) fn ensure_compaction_not_aborted(
    abort_check: Option<&dyn Fn() -> bool>,
) -> MidgeResult<()> {
    if abort_check.is_some_and(|check| check()) {
        return Err(crate::common::MidgeError::Aborted(
            "compaction aborted due to ingest epoch change".to_string(),
        ));
    }
    Ok(())
}
