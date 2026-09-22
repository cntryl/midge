//! Private output sinks for the shared SST block pipeline.
//!
//! Sinks own where encoded bytes go and the resources needed to keep them
//! there. They deliberately do not know about blocks, compression, indexes,
//! or footer layout; that policy lives in `pipeline`.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult};

/// Destination for bytes emitted by the shared SST block pipeline.
///
/// The associated guard types keep budget reservations alive until a block or
/// entry has been fully incorporated into the pipeline. This lets a failed
/// encode leave the current block and its reservations unchanged.
pub(super) trait BlockSink {
    type Finish;
    type EntryReservation;
    type FlushReservation;

    fn offset(&self) -> MidgeResult<u64>;

    fn append(&mut self, bytes: &[u8]) -> MidgeResult<()>;

    fn advance_after_append(&mut self, bytes: u64) -> MidgeResult<()>;

    fn reserve_entry(&self, retained_bytes: usize) -> MidgeResult<Self::EntryReservation>;

    fn retain_entry(&mut self, reservation: Self::EntryReservation);

    fn prepare_block(
        &self,
        block_bytes: usize,
        first_key: Option<&[u8]>,
        key_count: usize,
    ) -> MidgeResult<Self::FlushReservation>;

    fn finish_block(&mut self, reservation: Self::FlushReservation);

    fn finish(self) -> MidgeResult<Self::Finish>;
}

/// In-memory destination used by the compatibility/test writer path.
pub(super) struct VecBlockSink {
    bytes: Vec<u8>,
}

impl VecBlockSink {
    pub(super) fn new() -> Self {
        Self { bytes: Vec::new() }
    }
}

impl BlockSink for VecBlockSink {
    type Finish = Vec<u8>;
    type EntryReservation = ();
    type FlushReservation = ();

    fn offset(&self) -> MidgeResult<u64> {
        u64::try_from(self.bytes.len()).map_err(|_| {
            MidgeError::ResourceLimit("SST output offset exceeds the supported range".to_string())
        })
    }

    fn append(&mut self, bytes: &[u8]) -> MidgeResult<()> {
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn advance_after_append(&mut self, _bytes: u64) -> MidgeResult<()> {
        Ok(())
    }

    fn reserve_entry(&self, _retained_bytes: usize) -> MidgeResult<Self::EntryReservation> {
        Ok(())
    }

    fn retain_entry(&mut self, _reservation: Self::EntryReservation) {}

    fn prepare_block(
        &self,
        _block_bytes: usize,
        _first_key: Option<&[u8]>,
        _key_count: usize,
    ) -> MidgeResult<Self::FlushReservation> {
        Ok(())
    }

    fn finish_block(&mut self, _reservation: Self::FlushReservation) {}

    fn finish(self) -> MidgeResult<Self::Finish> {
        Ok(self.bytes)
    }
}

/// Scratch-file destination used by flush and compaction writers.
pub(super) struct ScratchBlockSink {
    scratch: super::super::scratch::TrackedScratch,
    offset: u64,
    budget: Option<ResourceBudget>,
    current_reservations: Vec<ResourceReservation>,
    persistent_reservations: Vec<ResourceReservation>,
}

pub(super) struct ScratchFlushReservation {
    _compression_workspace: Option<ResourceReservation>,
    persistent: Option<ResourceReservation>,
}

impl ScratchBlockSink {
    pub(super) fn new(
        budget: Option<ResourceBudget>,
        outstanding: Arc<std::sync::atomic::AtomicUsize>,
        directory: Option<&Path>,
    ) -> MidgeResult<Self> {
        Ok(Self {
            scratch: super::super::scratch::TrackedScratch::new(outstanding, directory)
                .map_err(MidgeError::Io)?,
            offset: 0,
            budget,
            current_reservations: Vec::new(),
            persistent_reservations: Vec::new(),
        })
    }

    pub(super) fn reserve_finalization(
        &self,
        retained_bytes: usize,
    ) -> MidgeResult<Option<ResourceReservation>> {
        self.budget
            .as_ref()
            .map(|budget| budget.reserve(retained_bytes, "SST finalization buffers"))
            .transpose()
    }
}

impl BlockSink for ScratchBlockSink {
    type Finish = super::super::scratch::TrackedScratch;
    type EntryReservation = Option<ResourceReservation>;
    type FlushReservation = ScratchFlushReservation;

    fn offset(&self) -> MidgeResult<u64> {
        Ok(self.offset)
    }

    fn append(&mut self, bytes: &[u8]) -> MidgeResult<()> {
        self.scratch
            .as_file_mut()
            .write_all(bytes)
            .map_err(MidgeError::Io)
    }

    fn advance_after_append(&mut self, bytes: u64) -> MidgeResult<()> {
        self.offset = self.offset.checked_add(bytes).ok_or_else(|| {
            MidgeError::ResourceLimit("SST stream offset exceeds the supported range".to_string())
        })?;
        Ok(())
    }

    fn reserve_entry(&self, retained_bytes: usize) -> MidgeResult<Self::EntryReservation> {
        self.budget
            .as_ref()
            .map(|budget| budget.reserve(retained_bytes, "SST current block entry"))
            .transpose()
    }

    fn retain_entry(&mut self, reservation: Self::EntryReservation) {
        if let Some(reservation) = reservation {
            self.current_reservations.push(reservation);
        }
    }

    fn prepare_block(
        &self,
        block_bytes: usize,
        first_key: Option<&[u8]>,
        key_count: usize,
    ) -> MidgeResult<Self::FlushReservation> {
        let compression_workspace_bytes = block_bytes.saturating_mul(2).saturating_add(4096);
        let compression_workspace = self
            .budget
            .as_ref()
            .map(|budget| budget.reserve(compression_workspace_bytes, "SST compression workspace"))
            .transpose()?;
        let persistent_bytes = first_key
            .map_or(0, |key| {
                key.len()
                    .saturating_add(
                        std::mem::size_of::<(Vec<u8>, crate::sst::types::BlockHandle)>(),
                    )
            })
            .saturating_add(key_count.saturating_mul(16));
        let persistent = self
            .budget
            .as_ref()
            .map(|budget| budget.reserve(persistent_bytes, "SST index and bloom metadata"))
            .transpose()?;
        Ok(ScratchFlushReservation {
            _compression_workspace: compression_workspace,
            persistent,
        })
    }

    fn finish_block(&mut self, reservation: Self::FlushReservation) {
        self.current_reservations.clear();
        if let Some(reservation) = reservation.persistent {
            self.persistent_reservations.push(reservation);
        }
    }

    fn finish(self) -> MidgeResult<Self::Finish> {
        Ok(self.scratch)
    }
}
