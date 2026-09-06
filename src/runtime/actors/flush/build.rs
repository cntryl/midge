//! Bounded construction from an immutable memtable; output stays local until publication.

use super::{FlushActor, FlushBuildTask};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult};
use crate::sst::SstFactory;

#[derive(Default)]
struct Bounds {
    first: Option<(Vec<u8>, ResourceReservation)>,
    last: Option<(Vec<u8>, ResourceReservation)>,
}

impl Bounds {
    fn include(&mut self, key: &[u8], budget: &ResourceBudget) -> MidgeResult<()> {
        for (bound, replace) in [
            (&mut self.first, std::cmp::Ordering::Greater),
            (&mut self.last, std::cmp::Ordering::Less),
        ] {
            if bound
                .as_ref()
                .is_none_or(|(old, _)| old.as_slice().cmp(key) == replace)
            {
                *bound = None;
                let charge = budget.reserve(key.len(), "flush output boundary key")?;
                *bound = Some((key.to_vec(), charge));
            }
        }
        Ok(())
    }
}

pub(super) fn write(
    factory: &crate::sst::FsSstFactoryIo,
    task: &mut FlushBuildTask,
    budget: &ResourceBudget,
) -> MidgeResult<crate::runtime::FileMeta> {
    if !factory.compaction_scratch_cleanup_verified() {
        return Err(MidgeError::ResourceLimit(
            "prior flush scratch cleanup is unconfirmed; retain disk admission until recovery"
                .into(),
        ));
    }
    if let Some(parent) = task.staging_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Streaming scratch and final output can coexist. Admission must precede
    // creation of the writer, which can write scratch on its first full block.
    task.reservation = FlushActor::reserve_flush(
        task.hybrid_storage.as_ref(),
        task.identity.cf_id,
        crate::sst::size_bound::flush_staging_bytes(task.memtable.encoded_size_upper_bound()),
    )?;
    let mut writer = factory.create_for_flush(budget.clone())?;
    crate::failpoints::fail_point!("midge::flush_worker::after_scratch_creation");
    let mut bounds = Bounds::default();
    let mut smallest_seq = u64::MAX;
    let mut largest_seq = 0;
    task.memtable
        .visit_frozen_versions(budget, |key, value, sequence, expiration, op_type| {
            bounds.include(key, budget)?;
            smallest_seq = smallest_seq.min(sequence);
            largest_seq = largest_seq.max(sequence);
            writer.add_sorted_with_meta(key, value, sequence, op_type, expiration)
        })?;
    task.memtable.visit_frozen_ranges(|range| {
        bounds.include(&range.start, budget)?;
        bounds.include(&range.end, budget)?;
        smallest_seq = smallest_seq.min(range.seq);
        largest_seq = largest_seq.max(range.seq);
        writer.add_range_tombstone(&range.start, &range.end, range.seq)
    })?;
    let Some((smallest_key, _first_charge)) = bounds.first else {
        return Err(MidgeError::Corruption(
            "attempted to flush an empty immutable memtable".into(),
        ));
    };
    let (largest_key, _last_charge) = bounds.last.expect("first key implies last key");
    crate::sst::fs::finish_writer_to_path(writer, &task.staging_path)?;
    let (size_bytes, checksum) = crate::sst::fs::file_identity(&task.staging_path)?;
    Ok(crate::runtime::FileMeta {
        name: String::new(),
        level: 0,
        size_bytes,
        content_crc32c: Some(checksum),
        cf_id: task.identity.cf_id,
        smallest_key: Some(smallest_key),
        largest_key: Some(largest_key),
        smallest_seq: Some(smallest_seq),
        largest_seq: Some(largest_seq),
        key_bounds_complete: true,
    })
}
