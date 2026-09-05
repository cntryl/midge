//! Storage Budget Actor - manages disk space and backpressure
//!
//! Responsibilities:
//! - Track disk usage across WAL, SSTs, compaction, and pending uploads
//! - Enforce watermarks (high, critical, emergency)
//! - Reserve space before flush/compaction
//! - Coordinate cloud uploads and local eviction
//! - Signal backpressure to the engine

use super::policy::StorageBudgetPolicy;
use super::state::DiskState;
use std::collections::{HashMap, VecDeque};

/// Opaque identity for one flush or compaction space reservation.
///
/// A terminal operation must consume this exact token. This prevents a
/// delayed completion from releasing an unrelated reservation that happened
/// to be queued first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[must_use]
pub struct StorageReservationToken(u64);

#[derive(Debug, Clone, Copy)]
struct CompactionReservation {
    input_bytes: u64,
    estimated_output_bytes: u64,
}

/// Result of a space reservation request
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservationResult {
    /// Space reserved successfully
    Ok,
    /// Wait for cloud uploads to complete
    WaitForCloudUpload,
    /// Wait for compaction to free space
    WaitForCompaction,
    /// No space available; reject write or flush
    RejectNoSpace,
}

/// Storage Budget Actor
pub struct StorageBudgetActor {
    policy: StorageBudgetPolicy,
    disk_state: DiskState,
    /// Queue of SST IDs waiting for upload (FIFO for LRU/FIFO strategies)
    pending_evictions: VecDeque<(u64, u64)>, // (sst_id, size)
    /// Flush reservations indexed by their operation identity.
    flush_reservations: HashMap<StorageReservationToken, u64>,
    /// Reusable staging capacity for the largest accepted, unbuilt generation.
    /// This token shares the ordinary reservation ledger with active flushes.
    flush_headroom: Option<StorageReservationToken>,
    flush_headroom_target: u64,
    /// Compaction reservations indexed by their operation identity.
    compaction_reservations: HashMap<StorageReservationToken, CompactionReservation>,
    /// Next opaque reservation identity.
    next_reservation_id: u64,
    /// Cloud inputs can be absent locally; only confirmed local deletion
    /// releases their resident bytes in ephemeral mode.
    ephemeral_sst_cache: bool,
    /// Last reported watermark state
    last_watermark_state: WatermarkState,
    pub(super) admission_pressure: super::pressure::AdmissionPressure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatermarkState {
    Normal,
    High,
    Critical,
    Emergency,
}

impl StorageBudgetActor {
    pub fn new(policy: StorageBudgetPolicy) -> Self {
        Self {
            policy,
            disk_state: DiskState::new(),
            pending_evictions: VecDeque::new(),
            flush_reservations: HashMap::new(),
            flush_headroom: None,
            flush_headroom_target: 0,
            compaction_reservations: HashMap::new(),
            next_reservation_id: 1,
            ephemeral_sst_cache: false,
            last_watermark_state: WatermarkState::Normal,
            admission_pressure: super::pressure::AdmissionPressure::default(),
        }
    }

    pub(super) fn usage_snapshot(&self) -> super::state::LocalStorageUsage {
        let headroom = self
            .flush_headroom
            .and_then(|token| self.flush_reservations.get(&token))
            .copied()
            .unwrap_or(0);
        super::state::LocalStorageUsage {
            wal_bytes: self.disk_state.wal_bytes,
            transaction_spill_bytes: self.disk_state.scratch_bytes,
            resident_sst_bytes: self.disk_state.sst_bytes,
            startup_residue_bytes: self.disk_state.startup_residue_bytes,
            flush_staging_reserved_bytes: self.disk_state.new_sst_reserve.saturating_sub(headroom),
            flush_headroom_reserved_bytes: headroom,
            compaction_staging_reserved_bytes: self.disk_state.compaction_reserve,
            wal_headroom_reserved_bytes: self.disk_state.wal_reserve,
            reservations: self
                .flush_reservations
                .len()
                .saturating_sub(usize::from(self.flush_headroom.is_some()))
                .saturating_add(self.compaction_reservations.len()),
        }
    }

    fn next_token(&mut self) -> StorageReservationToken {
        let token = StorageReservationToken(self.next_reservation_id);
        self.next_reservation_id = self.next_reservation_id.wrapping_add(1).max(1);
        token
    }

    pub fn enable_ephemeral_sst_cache(&mut self, max_local_bytes: u64) {
        self.policy.max_local_bytes = max_local_bytes;
        self.ephemeral_sst_cache = true;
        self.check_watermarks();
    }

    pub fn reconcile_local_disk_usage(&mut self, sst_bytes: u64, wal_bytes: u64) {
        self.disk_state.sst_bytes = sst_bytes;
        self.disk_state.wal_bytes = wal_bytes;
        self.check_watermarks();
    }

    pub fn release_local_sst_bytes(&mut self, bytes: u64) {
        self.disk_state.sst_bytes = self.disk_state.sst_bytes.saturating_sub(bytes);
        self.replenish_flush_headroom();
        self.check_watermarks();
    }

    pub(crate) fn reconcile_startup_scratch_residue(
        &mut self,
        bytes: u64,
    ) -> Result<(), ReservationResult> {
        if !self.ephemeral_sst_cache {
            return Ok(());
        }
        self.disk_state.startup_residue_bytes = bytes;
        self.check_watermarks();
        if self.disk_state.total_committed() > self.policy.max_local_bytes {
            return Err(ReservationResult::RejectNoSpace);
        }
        Ok(())
    }

    /// Reserve caller-owned transaction spill files before creating them.
    pub(crate) fn admit_local_scratch_bytes(
        &mut self,
        bytes: u64,
    ) -> Result<(), ReservationResult> {
        if !self.ephemeral_sst_cache {
            return Ok(());
        }
        self.resize_flush_headroom(self.flush_headroom_target)?;
        if bytes > self.disk_state.free_bytes(self.policy.max_local_bytes) {
            return Err(ReservationResult::RejectNoSpace);
        }
        self.disk_state.scratch_bytes = self.disk_state.scratch_bytes.saturating_add(bytes);
        self.check_watermarks();
        Ok(())
    }

    pub(crate) fn release_local_scratch_bytes(&mut self, bytes: u64) {
        self.disk_state.scratch_bytes = self.disk_state.scratch_bytes.saturating_sub(bytes);
        self.check_watermarks();
    }

    /// Charge WAL bytes before encoding or submitting their append. A rejected
    /// append cannot consume local disk while cloud uploads are unavailable.
    pub(crate) fn admit_local_wal_bytes(&mut self, bytes: u64) -> Result<(), ReservationResult> {
        if !self.ephemeral_sst_cache {
            return Ok(());
        }
        self.resize_flush_headroom(self.flush_headroom_target)?;
        if bytes > self.disk_state.free_bytes(self.policy.max_local_bytes) {
            return Err(ReservationResult::RejectNoSpace);
        }
        self.disk_state.wal_bytes = self.disk_state.wal_bytes.saturating_add(bytes);
        self.check_watermarks();
        Ok(())
    }

    pub(crate) fn release_local_wal_bytes(&mut self, bytes: u64) {
        self.disk_state.wal_bytes = self.disk_state.wal_bytes.saturating_sub(bytes);
        self.replenish_flush_headroom();
        self.check_watermarks();
    }

    pub(crate) fn set_flush_headroom(&mut self, bytes: u64) -> Result<(), ReservationResult> {
        if !self.ephemeral_sst_cache {
            return Ok(());
        }
        self.resize_flush_headroom(bytes)?;
        self.flush_headroom_target = bytes;
        Ok(())
    }

    fn resize_flush_headroom(&mut self, target: u64) -> Result<(), ReservationResult> {
        // A flush already holding the worker slot returns its reservation
        // before the next generation is built, so that capacity is reusable.
        let executing = self
            .flush_reservations
            .iter()
            .filter(|(token, _)| Some(**token) != self.flush_headroom)
            .map(|(_, bytes)| *bytes)
            .max()
            .unwrap_or(0);
        let required = target.saturating_sub(executing);
        let held = self
            .flush_headroom
            .and_then(|token| self.flush_reservations.get(&token))
            .copied()
            .unwrap_or(0);
        if held == required {
            return Ok(());
        }
        if required.saturating_sub(held) > self.disk_state.free_bytes(self.policy.max_local_bytes) {
            return Err(ReservationResult::RejectNoSpace);
        }
        if let Some(token) = self.flush_headroom.take() {
            self.flush_reservations.remove(&token);
        }
        self.disk_state.new_sst_reserve = self
            .disk_state
            .new_sst_reserve
            .saturating_sub(held)
            .saturating_add(required);
        if required > 0 {
            let token = self.next_token();
            self.flush_reservations.insert(token, required);
            self.flush_headroom = Some(token);
        }
        Ok(())
    }

    fn replenish_flush_headroom(&mut self) {
        // A completed output can briefly coexist with the next allowance.
        // Do not overbook disk while waiting for confirmed cache retirement.
        let _ = self.resize_flush_headroom(self.flush_headroom_target);
    }

    /// Reserve output space for one flush and return its operation token.
    pub fn reserve_for_flush_with_token(
        &mut self,
        est_size: u64,
    ) -> Result<StorageReservationToken, ReservationResult> {
        if self.ephemeral_sst_cache {
            let held = self
                .flush_headroom
                .and_then(|token| self.flush_reservations.get(&token))
                .copied()
                .unwrap_or(0);
            let transferred = held.min(est_size);
            if est_size.saturating_sub(transferred)
                > self.disk_state.free_bytes(self.policy.max_local_bytes)
            {
                return Err(ReservationResult::RejectNoSpace);
            }
            if let Some(token) = self.flush_headroom {
                if held == transferred {
                    self.flush_reservations.remove(&token);
                    self.flush_headroom = None;
                } else {
                    self.flush_reservations.insert(token, held - transferred);
                }
            }
            let token = self.next_token();
            self.flush_reservations.insert(token, est_size);
            self.disk_state.new_sst_reserve = self
                .disk_state
                .new_sst_reserve
                .saturating_add(est_size - transferred);
            self.check_watermarks();
            return Ok(token);
        }
        let usage_percent = self.disk_state.usage_percent(self.policy.max_local_bytes);

        // Emergency: reject all new writes
        if self.policy.is_emergency_watermark(usage_percent) {
            tracing::warn!("Emergency watermark hit; rejecting flush");
            return Err(ReservationResult::RejectNoSpace);
        }

        // Critical: wait for cloud uploads
        if self.policy.is_critical_watermark(usage_percent) {
            tracing::warn!("Critical watermark hit; requesting cloud uploads");
            return Err(ReservationResult::WaitForCloudUpload);
        }

        // High: may need to wait for compaction
        if self.policy.is_high_watermark(usage_percent) {
            let free = self.disk_state.free_bytes(self.policy.max_local_bytes);
            if free < est_size {
                tracing::info!("High watermark; waiting for compaction to free space");
                return Err(ReservationResult::WaitForCompaction);
            }
        }

        // Admission must include this output even below the high watermark.
        // Otherwise one large flush can overrun an almost-empty local disk.
        if est_size > self.disk_state.free_bytes(self.policy.max_local_bytes) {
            return Err(ReservationResult::RejectNoSpace);
        }

        // Reserve space for the new SST
        let token = self.next_token();
        self.disk_state.new_sst_reserve = self.disk_state.new_sst_reserve.saturating_add(est_size);
        self.flush_reservations.insert(token, est_size);
        self.check_watermarks();

        Ok(token)
    }

    /// Complete exactly one flush reservation. Returns false for a duplicate
    /// or unknown token and intentionally leaves accounting unchanged.
    pub fn complete_flush_for(&mut self, token: StorageReservationToken, actual_size: u64) -> bool {
        let Some(reserved_size) = self.flush_reservations.remove(&token) else {
            tracing::warn!(?token, "ignoring unmatched flush completion reservation");
            return false;
        };
        self.disk_state.new_sst_reserve = self
            .disk_state
            .new_sst_reserve
            .saturating_sub(reserved_size);
        self.disk_state.sst_bytes = self.disk_state.sst_bytes.saturating_add(actual_size);
        self.replenish_flush_headroom();
        self.check_watermarks();
        tracing::info!(
            ?token,
            actual_size,
            "Flush completed; SST added to local storage"
        );
        true
    }

    /// Release exactly one unpublished flush reservation.
    pub fn abort_flush_for(&mut self, token: StorageReservationToken) -> bool {
        let Some(reserved_size) = self.flush_reservations.remove(&token) else {
            tracing::warn!(?token, "ignoring unmatched flush reservation release");
            return false;
        };
        self.disk_state.new_sst_reserve = self
            .disk_state
            .new_sst_reserve
            .saturating_sub(reserved_size);
        self.replenish_flush_headroom();
        self.check_watermarks();
        true
    }

    /// Reserve a fixed reusable compaction staging window. Remote input size
    /// and total output count do not contribute to resident disk usage.
    pub(crate) fn reserve_compaction_staging_with_token(
        &mut self,
        bytes: u64,
    ) -> Result<StorageReservationToken, ReservationResult> {
        self.resize_flush_headroom(self.flush_headroom_target)?;
        if bytes > self.disk_state.free_bytes(self.policy.max_local_bytes) {
            return Err(ReservationResult::RejectNoSpace);
        }
        let token = self.next_token();
        self.disk_state.compaction_reserve =
            self.disk_state.compaction_reserve.saturating_add(bytes);
        self.compaction_reservations.insert(
            token,
            CompactionReservation {
                input_bytes: 0,
                estimated_output_bytes: bytes,
            },
        );
        self.check_watermarks();
        Ok(token)
    }

    /// Plan a compaction and return the reservation that must settle it.
    pub fn plan_compaction_with_token(&mut self, input_sizes: &[u64]) -> StorageReservationToken {
        let total_input: u64 = input_sizes.iter().sum();
        // Estimate output as ~90% of input (conservative)
        let estimated_output = total_input.saturating_mul(9) / 10;
        let token = self.next_token();
        self.disk_state.compaction_reserve = self
            .disk_state
            .compaction_reserve
            .saturating_add(estimated_output);
        self.compaction_reservations.insert(
            token,
            CompactionReservation {
                input_bytes: total_input,
                estimated_output_bytes: estimated_output,
            },
        );
        self.check_watermarks();
        tracing::info!(
            ?token,
            input_bytes = total_input,
            reserve_bytes = estimated_output,
            "Compaction planned"
        );
        token
    }

    /// Complete exactly one compaction reservation.
    pub fn complete_compaction_for(
        &mut self,
        token: StorageReservationToken,
        output_sizes: &[u64],
    ) -> bool {
        let Some(reservation) = self.compaction_reservations.remove(&token) else {
            tracing::warn!(
                ?token,
                "ignoring unmatched compaction completion reservation"
            );
            return false;
        };
        let total_output: u64 = output_sizes.iter().sum();
        self.disk_state.compaction_reserve = self
            .disk_state
            .compaction_reserve
            .saturating_sub(reservation.estimated_output_bytes);
        self.disk_state.sst_bytes = self
            .disk_state
            .sst_bytes
            .saturating_sub(if self.ephemeral_sst_cache {
                0
            } else {
                reservation.input_bytes
            })
            .saturating_add(total_output);
        self.check_watermarks();
        tracing::info!(?token, output_bytes = total_output, "Compaction completed");
        true
    }

    /// Settle a compaction whose local manifest authority switched but whose
    /// later publication steps failed. Inputs remain physically retained, so
    /// account the new outputs without subtracting their bytes. This may
    /// conservatively overcount after a later recovery cleanup, but it must
    /// never undercount live disk use while both generations remain present.
    pub fn retain_compaction_inputs_for(
        &mut self,
        token: StorageReservationToken,
        output_sizes: &[u64],
    ) -> bool {
        let Some(reservation) = self.compaction_reservations.remove(&token) else {
            tracing::warn!(
                ?token,
                "ignoring unmatched retained-input compaction reservation"
            );
            return false;
        };
        let total_output: u64 = output_sizes.iter().sum();
        self.disk_state.compaction_reserve = self
            .disk_state
            .compaction_reserve
            .saturating_sub(reservation.estimated_output_bytes);
        self.disk_state.sst_bytes = self.disk_state.sst_bytes.saturating_add(total_output);
        self.check_watermarks();
        tracing::warn!(
            ?token,
            input_bytes = reservation.input_bytes,
            output_bytes = total_output,
            "compaction publication incomplete; retaining and accounting both input and output generations"
        );
        true
    }

    /// Release exactly one uncompleted compaction reservation.
    pub fn abort_compaction_for(&mut self, token: StorageReservationToken) -> bool {
        let Some(reservation) = self.compaction_reservations.remove(&token) else {
            tracing::warn!(?token, "ignoring unmatched compaction reservation release");
            return false;
        };
        self.disk_state.compaction_reserve = self
            .disk_state
            .compaction_reserve
            .saturating_sub(reservation.estimated_output_bytes);
        self.check_watermarks();
        true
    }

    /// Complete a cloud upload
    #[cfg(test)]
    fn complete_cloud_upload(&mut self, sst_id: u64, actual_size: u64) {
        // When cloud upload completes, the SST is stable in cloud.
        // We can now consider it for local eviction.
        self.pending_evictions.push_back((sst_id, actual_size));
        tracing::info!("Cloud upload completed; SST marked for potential eviction");
    }

    /// Check watermarks and emit warnings if state changed
    fn check_watermarks(&mut self) {
        let usage_percent = self.disk_state.usage_percent(self.policy.max_local_bytes);
        let new_state = if self.policy.is_emergency_watermark(usage_percent) {
            WatermarkState::Emergency
        } else if self.policy.is_critical_watermark(usage_percent) {
            WatermarkState::Critical
        } else if self.policy.is_high_watermark(usage_percent) {
            WatermarkState::High
        } else {
            WatermarkState::Normal
        };

        if new_state != self.last_watermark_state {
            tracing::warn!(
                new_state = ?new_state,
                usage_percent = usage_percent,
                "Watermark transition"
            );
            self.last_watermark_state = new_state;
        }
    }

    /// Get current disk state (snapshot)
    pub fn disk_state(&self) -> DiskState {
        self.disk_state.clone()
    }

    pub fn max_local_bytes(&self) -> u64 {
        self.policy.max_local_bytes
    }

    pub(super) fn pending_eviction_count(&self) -> usize {
        self.pending_evictions.len()
    }

    /// Get pending eviction queue
    #[cfg(test)]
    pub fn pending_evictions(&self) -> Vec<(u64, u64)> {
        self.pending_evictions.iter().copied().collect()
    }

    /// Pop the next SST for eviction
    #[cfg(test)]
    pub fn next_eviction(&mut self) -> Option<(u64, u64)> {
        self.pending_evictions.pop_front()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn should_reuse_flush_headroom_when_wal_consumes_remaining_disk_capacity() {
        // Arrange
        let mut actor = super::StorageBudgetActor::new(super::StorageBudgetPolicy::new(1000));
        actor.enable_ephemeral_sst_cache(1000);
        actor.set_flush_headroom(400).expect("pending generation");
        actor.admit_local_wal_bytes(600).expect("WAL admission");
        // Act
        let token = actor
            .reserve_for_flush_with_token(400)
            .expect("prepaid flush at full capacity");
        assert_eq!(actor.disk_state().total_committed(), 1000);
        actor.complete_flush_for(token, 100);
        // Assert
        assert!(
            actor.admit_local_wal_bytes(1).is_err(),
            "publication must retain room for the next pending flush"
        );
        actor.release_local_sst_bytes(100);
        let next = actor
            .reserve_for_flush_with_token(400)
            .expect("reuse retired staging");
        assert_eq!(actor.disk_state().total_committed(), 1000);
        actor.set_flush_headroom(0).expect("no pending generations");
        actor.abort_flush_for(next);
        assert_eq!(actor.disk_state().total_committed(), 600);
    }

    #[test]
    fn should_share_dynamic_flush_headroom_when_compaction_and_wal_coexist() {
        // Arrange
        let mut actor = super::StorageBudgetActor::new(super::StorageBudgetPolicy::new(1000));
        actor.enable_ephemeral_sst_cache(1000);
        actor
            .set_flush_headroom(100)
            .expect("small pending generation");
        let compaction = actor
            .reserve_compaction_staging_with_token(700)
            .expect("compaction");
        // Act
        actor
            .admit_local_wal_bytes(150)
            .expect("small WAL still fits beside compaction");
        let flush = actor
            .reserve_for_flush_with_token(100)
            .expect("pending flush can always run");
        // Assert
        assert_eq!(actor.disk_state().total_committed(), 950);
        assert!(
            actor.set_flush_headroom(300).is_err(),
            "oversized next generation cannot displace existing owners"
        );
        assert_eq!(actor.disk_state().total_committed(), 950);
        actor
            .set_flush_headroom(0)
            .expect("drained pending generations");
        actor.abort_flush_for(flush);
        actor.abort_compaction_for(compaction);
        assert_eq!(actor.disk_state().total_committed(), 150);
    }

    use super::*;

    #[test]
    fn should_reconcile_retained_startup_residue_without_releasing_live_scratch() {
        // Arrange
        let mut actor = StorageBudgetActor::new(StorageBudgetPolicy::new(1_000));
        actor.enable_ephemeral_sst_cache(1_000);
        actor
            .admit_local_scratch_bytes(200)
            .expect("live spill admission");

        // Act
        actor
            .reconcile_startup_scratch_residue(300)
            .expect("initial residue");
        actor
            .reconcile_startup_scratch_residue(300)
            .expect("repeated readback");

        // Assert
        assert_eq!(actor.disk_state().total_committed(), 500);
        assert_eq!(
            actor.reconcile_startup_scratch_residue(801),
            Err(ReservationResult::RejectNoSpace)
        );
        assert_eq!(actor.disk_state().total_committed(), 1_001);
        actor
            .reconcile_startup_scratch_residue(0)
            .expect("residue removed");
        assert_eq!(actor.disk_state().total_committed(), 200);
    }

    #[test]
    fn should_enforce_shared_local_capacity_when_multiple_workloads_reserve_space() {
        // Arrange
        let mut actor = StorageBudgetActor::new(StorageBudgetPolicy::new(1_000));
        actor.enable_ephemeral_sst_cache(1_000);
        actor.admit_local_wal_bytes(200).expect("WAL reservation");
        actor
            .admit_local_scratch_bytes(300)
            .expect("spill reservation");

        // Act
        let denied = actor.reserve_compaction_staging_with_token(501);
        let allowed = actor
            .reserve_compaction_staging_with_token(500)
            .expect("remaining staging window");

        // Assert
        assert_eq!(denied, Err(ReservationResult::RejectNoSpace));
        assert_eq!(actor.disk_state().total_committed(), 1_000);
        assert!(actor.admit_local_scratch_bytes(1).is_err());
        assert!(actor.abort_compaction_for(allowed));
        actor.release_local_scratch_bytes(300);
        assert_eq!(actor.disk_state().total_committed(), 200);
    }

    #[test]
    fn should_keep_resident_bytes_charged_when_compaction_inputs_are_cloud_only() {
        // Arrange
        let mut actor = StorageBudgetActor::new(StorageBudgetPolicy::new(1_000));
        actor.enable_ephemeral_sst_cache(1_000);
        actor.reconcile_local_disk_usage(100, 40);
        let token = actor.plan_compaction_with_token(&[200]);

        // Act
        assert!(actor.complete_compaction_for(token, &[50]));

        // Assert
        assert_eq!(actor.disk_state().sst_bytes, 150);
        assert_eq!(actor.disk_state().wal_bytes, 40);
        actor.release_local_sst_bytes(50);
        assert_eq!(actor.disk_state().total_committed(), 140);
    }

    #[test]
    fn should_keep_reservations_charged_when_reconciling_physical_local_files() {
        // Arrange
        let mut actor = StorageBudgetActor::new(StorageBudgetPolicy::new(1_000));
        let token = actor
            .reserve_for_flush_with_token(100)
            .expect("flush reservation");

        // Act
        actor.reconcile_local_disk_usage(200, 50);

        // Assert
        assert_eq!(actor.disk_state().total_committed(), 350);
        assert!(actor.abort_flush_for(token));
        assert_eq!(actor.disk_state().total_committed(), 250);
    }

    #[test]
    fn should_reject_flush_when_projected_usage_exceeds_local_capacity() {
        // Arrange
        let mut actor = StorageBudgetActor::new(StorageBudgetPolicy::new(1_000));

        // Act
        let result = actor.reserve_for_flush_with_token(1_001);

        // Assert
        assert_eq!(result, Err(ReservationResult::RejectNoSpace));
        assert_eq!(actor.disk_state().total_committed(), 0);
    }

    #[test]
    fn should_preserve_existing_reservation_when_another_flush_would_exceed_capacity() {
        // Arrange
        let mut actor = StorageBudgetActor::new(StorageBudgetPolicy::new(1_000));
        let first = actor
            .reserve_for_flush_with_token(800)
            .expect("first reservation");

        // Act
        let result = actor.reserve_for_flush_with_token(300);

        // Assert
        assert!(result.is_err());
        assert_eq!(actor.disk_state().new_sst_reserve, 800);
        assert!(actor.abort_flush_for(first));
    }

    #[test]
    fn should_reserve_space_when_below_high_watermark() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024); // 1 MB
        let mut actor = StorageBudgetActor::new(policy);

        // Act
        let result = actor.reserve_for_flush_with_token(100_000);

        // Assert
        assert!(result.is_ok());
        assert_eq!(actor.disk_state().new_sst_reserve, 100_000);
    }

    #[test]
    fn should_queue_evictions_on_cloud_upload() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024); // 1 MB
        let mut actor = StorageBudgetActor::new(policy);

        // Act
        actor.complete_cloud_upload(42, 50_000);

        // Assert
        let evictions = actor.pending_evictions();
        assert_eq!(evictions.len(), 1);
        assert_eq!(evictions[0], (42, 50_000));
    }

    // ===== E2E Disk Pressure Stress Tests (Section 9) =====

    #[test]
    fn should_handle_gradual_disk_pressure_buildup() {
        // Arrange: Simulate gradual writes filling disk
        let policy = StorageBudgetPolicy::new(1024 * 1024); // 1 MB
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Gradually add SSTs (no watermark transitions yet)
        for _ in 0..3 {
            let token = actor
                .reserve_for_flush_with_token(250_000)
                .expect("flush reservation should succeed");
            assert!(actor.complete_flush_for(token, 240_000));
        }

        // Assert: Should have ~720KB of SST data, all fits comfortably (720KB < 1MB)
        let state = actor.disk_state();
        assert!(state.sst_bytes >= 600_000 && state.sst_bytes <= 800_000);
        assert!(state.total_committed() < 1024 * 1024); // Still below max
    }

    #[test]
    fn should_wait_for_cloud_upload_at_critical_watermark() {
        // Arrange: 1MB disk, critical watermark at 95%
        // ((total as f64 / 1048576 as f64) * 100.0) as u32 >= 95
        // Need: total >= 1048576 * 0.95 = 995549.2 bytes
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Fill to 996K bytes (995549 / 1048576 * 100 = 95%, but truncates to 94%)
        // So actually need 999K to get 95.2%
        actor.disk_state.sst_bytes = 999_000;

        let result = actor.reserve_for_flush_with_token(10_000);

        // Assert: Should ask to wait for cloud uploads
        assert_eq!(result, Err(ReservationResult::WaitForCloudUpload));
    }

    #[test]
    fn should_wait_for_compaction_at_high_watermark() {
        // Arrange: 1MB disk, high watermark at 90%
        // Need: total >= 1048576 * 0.90 = 943718.4 bytes
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Fill to 944K (944000 / 1048576 * 100 = 90.02%)
        actor.disk_state.sst_bytes = 944_000;

        // Request a flush that won't fit in remaining space
        let result = actor.reserve_for_flush_with_token(110_000); // Remaining ~104KB, won't fit 110KB

        // Assert: Should request compaction
        assert_eq!(result, Err(ReservationResult::WaitForCompaction));
    }

    #[test]
    fn should_reject_writes_at_emergency_watermark() {
        // Arrange: 1MB disk, emergency at 98%
        // Need: total >= 1048576 * 0.98 = 1027643.48 bytes
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Fill to 1028K (1028000 / 1048576 * 100 = 98.0%)
        actor.disk_state.sst_bytes = 1_028_000;

        let result = actor.reserve_for_flush_with_token(10_000);

        // Assert: Should reject writes
        assert_eq!(result, Err(ReservationResult::RejectNoSpace));
    }

    #[test]
    fn should_track_flush_completion() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Reserve, complete, verify accounting
        let token = actor
            .reserve_for_flush_with_token(50_000)
            .expect("flush reservation should succeed");

        let before_complete = actor.disk_state();
        assert_eq!(before_complete.new_sst_reserve, 50_000);
        assert_eq!(before_complete.sst_bytes, 0);

        assert!(actor.complete_flush_for(token, 45_000));

        // Assert: Reserve → SST conversion
        let after_complete = actor.disk_state();
        assert_eq!(after_complete.new_sst_reserve, 0);
        assert_eq!(after_complete.sst_bytes, 45_000);
    }

    #[test]
    fn should_release_full_flush_reservation_when_actual_output_exceeds_estimate() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        let token = actor
            .reserve_for_flush_with_token(10)
            .expect("flush reservation should succeed");

        // Act
        assert!(actor.complete_flush_for(token, 20));

        // Assert
        let state = actor.disk_state();
        assert_eq!(state.new_sst_reserve, 0);
        assert_eq!(state.sst_bytes, 20);
    }

    #[test]
    fn should_release_flush_reservation_when_flush_fails() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        let token = actor
            .reserve_for_flush_with_token(100)
            .expect("flush reservation should succeed");

        // Act
        assert!(actor.abort_flush_for(token));

        // Assert
        assert_eq!(actor.disk_state().new_sst_reserve, 0);
        assert_eq!(actor.disk_state().sst_bytes, 0);
    }

    #[test]
    fn should_finalize_compaction_publication_when_output_replaces_inputs() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        actor.disk_state.sst_bytes = 300;

        // Act
        let token = actor.plan_compaction_with_token(&[100, 100]);
        assert!(actor.complete_compaction_for(token, &[50]));

        // Assert
        let state = actor.disk_state();
        assert_eq!(state.compaction_reserve, 0);
        assert_eq!(state.sst_bytes, 150);
    }

    #[test]
    fn should_account_both_generations_when_post_manifest_compaction_step_fails() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        actor.disk_state.sst_bytes = 300;
        let token = actor.plan_compaction_with_token(&[100, 100]);

        // Act
        assert!(actor.retain_compaction_inputs_for(token, &[50]));

        // Assert
        let state = actor.disk_state();
        assert_eq!(state.compaction_reserve, 0);
        assert_eq!(state.sst_bytes, 350);
    }

    #[test]
    fn should_release_compaction_reservation_when_compaction_is_aborted() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        let token = actor.plan_compaction_with_token(&[100, 100]);

        // Act
        assert!(actor.abort_compaction_for(token));

        // Assert
        assert_eq!(actor.disk_state().compaction_reserve, 0);
        assert_eq!(actor.disk_state().sst_bytes, 0);
    }

    #[test]
    fn should_settle_only_the_matching_flush_reservation_when_completions_reorder() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        let first = actor
            .reserve_for_flush_with_token(100)
            .expect("first flush reservation should succeed");
        let second = actor
            .reserve_for_flush_with_token(200)
            .expect("second flush reservation should succeed");

        // Act
        assert!(actor.complete_flush_for(second, 150));

        // Assert
        let state = actor.disk_state();
        assert_eq!(state.new_sst_reserve, 100);
        assert_eq!(state.sst_bytes, 150);
        assert!(actor.abort_flush_for(first));
        assert_eq!(actor.disk_state().new_sst_reserve, 0);
    }

    #[test]
    fn should_release_only_the_matching_compaction_reservation_when_terminal_paths_reorder() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        actor.disk_state.sst_bytes = 1_000;
        let first = actor.plan_compaction_with_token(&[100]);
        let second = actor.plan_compaction_with_token(&[200]);

        // Act
        assert!(actor.complete_compaction_for(second, &[150]));

        // Assert
        let state = actor.disk_state();
        assert_eq!(state.compaction_reserve, 90);
        assert_eq!(state.sst_bytes, 950);
        assert!(actor.abort_compaction_for(first));
        assert_eq!(actor.disk_state().compaction_reserve, 0);
    }

    #[test]
    fn should_ignore_duplicate_token_terminal_events_without_changing_accounting() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);
        let token = actor
            .reserve_for_flush_with_token(100)
            .expect("flush reservation should succeed");

        // Act
        assert!(actor.complete_flush_for(token, 90));
        let settled = actor.disk_state();

        // Assert
        assert!(!actor.complete_flush_for(token, 90));
        assert!(!actor.abort_flush_for(token));
        assert_eq!(actor.disk_state().new_sst_reserve, settled.new_sst_reserve);
        assert_eq!(actor.disk_state().sst_bytes, settled.sst_bytes);
    }

    #[test]
    fn should_recover_from_emergency_after_cloud_upload() {
        // Arrange
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Reach emergency watermark (1028K = 98.0%)
        actor.disk_state.sst_bytes = 1_028_000;
        let result1 = actor.reserve_for_flush_with_token(10_000);
        assert_eq!(result1, Err(ReservationResult::RejectNoSpace));

        // Simulate cloud upload freeing 300KB
        actor.complete_cloud_upload(1, 300_000);
        actor.disk_state.sst_bytes = 728_000; // 69.4% usage

        // Try reservation again
        let result2 = actor.reserve_for_flush_with_token(50_000);

        // Assert: Should recover to Normal watermark (<90%)
        assert!(result2.is_ok());
    }

    #[test]
    fn should_handle_concurrent_operations_under_pressure() {
        // Arrange: Test complex scenario with simultaneous flushes and compaction
        let policy = StorageBudgetPolicy::new(5 * 1024 * 1024); // 5 MB for more flexibility
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Build up with mixed operations
        for i in 0..10 {
            // Attempt flush (each 150KB reserve)
            let flush_result = actor.reserve_for_flush_with_token(150_000);

            // Complete flush if successful
            if let Ok(token) = flush_result {
                assert!(actor.complete_flush_for(token, 140_000));
            }

            // Every 3 flushes, simulate a compaction
            if i % 3 == 2 && i > 0 {
                let estimated_input =
                    300_000 + (u64::try_from(i).expect("loop index fits in u64") * 50_000);
                let token =
                    actor.plan_compaction_with_token(&[estimated_input / 2, estimated_input / 2]);
                assert!(actor.complete_compaction_for(token, &[estimated_input / 2]));
            }
        }

        // Assert: 10 flushes each add 140_000 to sst_bytes; compactions at
        // i=2,5,8 replace their input bytes with a smaller output, netting
        // out to this exact figure (hand-traced from the loop above).
        let state = actor.disk_state();
        assert_eq!(state.sst_bytes, 575_000);
        assert_eq!(state.compaction_reserve, 0, "all compactions settled");
        assert_eq!(state.new_sst_reserve, 0, "all flushes settled");
        assert_eq!(state.total_committed(), 575_000);
    }

    #[test]
    fn should_track_pending_evictions_during_pressure() {
        // Arrange: Simulate scenario where cloud uploads queue evictions
        let policy = StorageBudgetPolicy::new(1024 * 1024);
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Create several SSTs and upload them
        for sst_id in 1_u64..=5 {
            actor.complete_cloud_upload(sst_id, 100_000);
        }

        // Assert: All uploads should be queued for eviction
        let evictions = actor.pending_evictions();
        assert_eq!(evictions.len(), 5);

        // Verify FIFO order
        for (idx, (sst_id, size)) in evictions.iter().enumerate() {
            assert_eq!(*sst_id, (idx + 1) as u64);
            assert_eq!(*size, 100_000);
        }

        // Verify we can pop them in order
        for expected_sst_id in 1_u64..=5 {
            let (sst_id, size) = actor.next_eviction().unwrap();
            assert_eq!(sst_id, expected_sst_id);
            assert_eq!(size, 100_000);
        }

        // Verify queue is empty
        assert!(actor.next_eviction().is_none());
    }

    #[test]
    fn should_correctly_calculate_metrics_under_sustained_load() {
        // Arrange: Sustained high-load scenario
        let policy = StorageBudgetPolicy::new(10 * 1024 * 1024); // 10 MB
        let mut actor = StorageBudgetActor::new(policy);

        // Act: Simulate sustained write load
        let mut total_flushed = 0u64;
        for i in 0..50 {
            let flush_size = 100_000 + (i % 50_000);

            if let Ok(token) = actor.reserve_for_flush_with_token(flush_size) {
                let actual_size = flush_size - 10_000; // 10KB overhead
                assert!(actor.complete_flush_for(token, actual_size));
                total_flushed += actual_size;
            }
        }

        // Assert: Metrics should accurately reflect all operations
        let state = actor.disk_state();
        assert_eq!(
            state.sst_bytes, total_flushed,
            "SST bytes should match total flushed"
        );
        assert!(
            state.total_committed() >= total_flushed,
            "Total committed should be >= SST bytes"
        );
    }
}
