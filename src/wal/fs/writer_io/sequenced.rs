use super::{FsWalWriterIo, WalWriter};
use crate::common::MidgeError;
use crate::wal::append_completion::{WalAppendPrefix, WalPendingAppend};
use crate::wal::sequenced::SequencedWalUnit;
use crate::wal::traits::WalAppendError;
use crate::wal::writer_authority::PendingWriterEffect;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

impl FsWalWriterIo {
    pub(super) fn submit_owned_units(
        &self,
        prefix: WalAppendPrefix,
        units: Vec<SequencedWalUnit>,
        timeout: Duration,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> Result<WalPendingAppend, WalAppendError> {
        self.validate_sequenced_submission(prefix, &units)?;
        crate::failpoints::fail_point!("midge::wal::timeout_before_enqueue", |_| Err(
            WalAppendError::unchanged(MidgeError::Timeout(
                "injected timeout before WAL enqueue".into()
            ))
        ));
        let retained = units
            .iter()
            .try_fold(0usize, |bytes, unit| {
                bytes
                    .checked_add(unit.retained_memory_bound()?)
                    .ok_or_else(|| {
                        MidgeError::ResourceLimit("WAL submission memory bound overflow".into())
                    })
            })
            .map_err(WalAppendError::unchanged)?;
        let reservation = self
            .raw_submission_memory
            .reserve(retained, "writer-owned WAL encode")
            .map_err(WalAppendError::unchanged)?;
        let started = std::time::Instant::now();
        let (effect, root) =
            PendingWriterEffect::admit(self.write_authority.read().clone(), group, timeout)
                .map_err(WalAppendError::unchanged)?;
        let facts = units
            .iter()
            .map(|unit| (unit.last_sequence(), unit.cf_id()))
            .collect::<Vec<_>>();
        let expected_start = self.current_pos();
        let (sender, response) = std::sync::mpsc::sync_channel(1);
        {
            let mut queue = self.queue.lock();
            let mut state = self.sync_state.lock();
            if let Some(error) = state.failure() {
                return Err(WalAppendError::unchanged(error));
            }
            if queue.len() >= super::super::writer_runner::MAX_QUEUE_DEPTH
                || state.submitted_writes != state.completed_writes
                || self.raw_submission_pending.load(Ordering::Acquire)
            {
                return Err(WalAppendError::unchanged(MidgeError::WriteStall(
                    "WAL append prefix is busy".into(),
                )));
            }
            if started.elapsed() >= timeout {
                return Err(WalAppendError::unchanged(MidgeError::Timeout(
                    "WAL encode admission expired before submission".into(),
                )));
            }
            let id = state.submitted_writes.checked_add(1).ok_or_else(|| {
                WalAppendError::unchanged(MidgeError::ResourceLimit(
                    "WAL append identity exhausted".into(),
                ))
            })?;
            self.raw_submission_pending.store(true, Ordering::Release);
            state.submitted_writes = id;
            queue.push(super::super::writer_runner::QueuedWrite::sequenced(
                units,
                (prefix, sender),
                effect,
                id,
                reservation,
            ));
        }
        self.queue_cond.notify_one();
        let failures = Arc::clone(&self.sync_state);
        let tail = Arc::clone(&self.tail);
        let pending = Arc::clone(&self.raw_submission_pending);
        Ok(WalPendingAppend::new(
            response,
            Arc::new(move || failures.lock().failure()),
            root,
            Box::new(move |completion| {
                if completion.prefix != prefix
                    || completion.start_offset != expected_start
                    || completion.last_record_offset < completion.start_offset
                    || completion.last_record_offset >= completion.end_offset
                {
                    return Err(MidgeError::Internal(
                        "WAL append receipt does not match its admitted prefix".into(),
                    ));
                }
                let mut tail = tail.lock();
                for &(sequence, family) in &facts {
                    tail.count(sequence, family);
                }
                pending.store(false, Ordering::Release);
                Ok(())
            }),
        ))
    }

    fn validate_sequenced_submission(
        &self,
        prefix: WalAppendPrefix,
        units: &[SequencedWalUnit],
    ) -> Result<(), WalAppendError> {
        if let Some(error) = self.writer_failure() {
            return Err(WalAppendError::unchanged(error));
        }
        if units.is_empty()
            || self.sealed.load(Ordering::Acquire)
            || prefix.writer_epoch != self.header.epoch
            || prefix.segment_id != self.header.id
            || prefix.first_sequence > prefix.last_sequence
            || units
                .last()
                .is_none_or(|unit| unit.last_sequence() != prefix.last_sequence)
        {
            return Err(WalAppendError::unchanged(MidgeError::InvalidArgument(
                "sequenced WAL submission does not match the open segment".into(),
            )));
        }
        self.tail
            .lock()
            .admit_all(
                &self.header,
                units.iter().map(SequencedWalUnit::last_sequence),
            )
            .map_err(|error| WalAppendError::unchanged(MidgeError::InvalidArgument(error)))
    }
}
