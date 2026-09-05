//! Keep accepted cloud transactions inside a flushable memtable generation.

use super::EventLoop;
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::TransactionOp;
use crate::sst::size_bound::{flush_staging_bytes, point_bytes, range_bytes, FIXED_SST_BYTES};
use crate::sst::Memtable;
use std::collections::HashMap;

impl EventLoop {
    pub(super) fn cloud_flush_staging_window(&self) -> Option<u64> {
        self.hybrid_storage
            .as_ref()
            .filter(|storage| storage.ephemeral_sst_cache_enabled())
            // The other half remains available to compaction staging.
            .map(|storage| storage.budget_snapshot().max_local_bytes / 2)
    }

    pub(super) fn prepare_cloud_transaction_memtables(
        &mut self,
        ops: &[TransactionOp],
    ) -> MidgeResult<()> {
        let Some(window) = self.cloud_flush_staging_window() else {
            return Ok(());
        };
        let mut growth = HashMap::new();
        for op in ops {
            add_operation_growth(&mut growth, op);
        }
        self.admit_cloud_memtable_growth(&growth, window)
    }

    pub(super) fn prepare_cloud_spilled_transaction_memtables(
        &mut self,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
    ) -> MidgeResult<()> {
        let Some(window) = self.cloud_flush_staging_window() else {
            return Ok(());
        };
        let mut growth = HashMap::new();
        source.for_each(|_, op| {
            add_operation_growth(&mut growth, &op);
            Ok(())
        })?;
        self.admit_cloud_memtable_growth(&growth, window)
    }

    fn admit_cloud_memtable_growth(
        &mut self,
        growth: &HashMap<u32, usize>,
        window: u64,
    ) -> MidgeResult<()> {
        let mut freeze = Vec::new();
        // Validate every family before changing any generation. In particular,
        // an indivisible transaction must fail before its first WAL frame.
        for (&cf_id, &additional) in growth {
            let standalone = flush_staging_bytes(FIXED_SST_BYTES.saturating_add(additional));
            if standalone > window {
                return Err(MidgeError::NoSpace(format!(
                    "column family {cf_id} transaction requires {standalone} flush staging bytes, exceeding its {window}-byte local window"
                )));
            }
            let cf = self.state.get_cf(cf_id).ok_or_else(|| {
                MidgeError::InvalidArgument(format!("column family {cf_id} does not exist"))
            })?;
            let projected = flush_staging_bytes(
                cf.memtable
                    .encoded_size_upper_bound()
                    .saturating_add(additional),
            );
            if projected > window && cf.memtable.size_bytes() > 0 {
                if self.state.is_immutable_memtable_queue_full(cf_id)
                    || self.state.l0_slot_usage(cf_id) >= self.state.l0_hard_ceiling()
                {
                    return Err(MidgeError::WriteStall(format!(
                        "column family {cf_id} must publish an immutable before admitting another cloud generation"
                    )));
                }
                freeze.push(cf_id);
            }
        }
        let required = self.pending_cloud_flush_headroom(growth, window);
        self.hybrid_storage
            .as_ref()
            .expect("cloud staging storage")
            .set_flush_headroom(required)?;
        for cf_id in freeze {
            self.freeze_active_memtable(cf_id)?;
        }
        self.schedule_next_flush_worker();
        Ok(())
    }

    fn pending_cloud_flush_headroom(&self, growth: &HashMap<u32, usize>, window: u64) -> u64 {
        self.state
            .column_families
            .iter()
            .flat_map(|(&cf_id, cf)| {
                let additional = growth.get(&cf_id).copied().unwrap_or(0);
                let active = if cf.memtable.size_bytes() == 0 && additional == 0 {
                    0
                } else {
                    let projected = flush_staging_bytes(
                        cf.memtable
                            .encoded_size_upper_bound()
                            .saturating_add(additional),
                    );
                    if projected > window && cf.memtable.size_bytes() > 0 {
                        flush_staging_bytes(cf.memtable.encoded_size_upper_bound()).max(
                            flush_staging_bytes(FIXED_SST_BYTES.saturating_add(additional)),
                        )
                    } else {
                        projected
                    }
                };
                std::iter::once(active).chain(
                    cf.immutable_flushes
                        .iter()
                        .filter(|flush| flush.built.is_none())
                        .map(|flush| {
                            flush_staging_bytes(flush.memtable.encoded_size_upper_bound())
                        }),
                )
            })
            .max()
            .unwrap_or(0)
    }

    pub(super) fn refresh_cloud_flush_headroom(&self) -> MidgeResult<()> {
        let Some(window) = self.cloud_flush_staging_window() else {
            return Ok(());
        };
        let required = self.pending_cloud_flush_headroom(&HashMap::new(), window);
        self.hybrid_storage
            .as_ref()
            .expect("cloud staging storage")
            .set_flush_headroom(required)
    }

    pub(super) fn freeze_cloud_memtables_near_staging_limit(&mut self) -> MidgeResult<usize> {
        let Some(window) = self.cloud_flush_staging_window() else {
            return Ok(0);
        };
        self.refresh_cloud_flush_headroom()?;
        let combined_staging = self
            .state
            .column_families
            .values()
            .filter(|cf| cf.memtable.size_bytes() > 0)
            .map(|cf| flush_staging_bytes(cf.memtable.encoded_size_upper_bound()))
            .fold(0_u64, u64::saturating_add);
        if combined_staging < window / 2 {
            return Ok(0);
        }
        let candidates: Vec<_> = self
            .state
            .column_families
            .iter()
            .filter(|(_, cf)| cf.memtable.size_bytes() > 0)
            .map(|(&cf_id, _)| cf_id)
            .collect();
        let mut count = 0;
        let mut failure = None;
        for cf_id in candidates {
            match self.freeze_active_memtable(cf_id) {
                Ok(frozen) => count += usize::from(frozen.is_some()),
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        failure.map_or(Ok(count), Err)
    }
}

fn add_operation_growth(growth: &mut HashMap<u32, usize>, op: &TransactionOp) {
    let (cf_id, bytes) = match op {
        TransactionOp::Put {
            cf_id, key, value, ..
        } => (*cf_id, point_bytes(key.len(), value.len())),
        TransactionOp::Delete { cf_id, key } => (*cf_id, point_bytes(key.len(), 0)),
        TransactionOp::DeleteRange {
            cf_id,
            start_key,
            end_key,
        } => {
            if start_key >= end_key {
                return;
            }
            (*cf_id, range_bytes(start_key.len(), end_key.len()))
        }
    };
    let current = growth.entry(cf_id).or_default();
    *current = current.saturating_add(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn fixture() -> MidgeResult<(
        tempfile::TempDir,
        EventLoop,
        Arc<crate::storage::HybridStorage>,
    )> {
        let directory = tempfile::tempdir()?;
        let setup = crate::storage::test_support::build_cloud_backed_filesystem_simulation(
            directory.path(),
            Some(256 * 1024),
        )?;
        setup.hybrid_storage.enable_ephemeral_sst_cache(256 * 1024);
        let state = crate::runtime::RuntimeState::new(directory.path().to_path_buf(), false);
        let config = crate::runtime::RuntimeConfig {
            hybrid_storage: Some(Arc::clone(&setup.hybrid_storage)),
            ..crate::runtime::RuntimeConfig::default()
        };
        let event_loop = EventLoop::new(
            state,
            false,
            Arc::new(crate::runtime::ResponseRouter::new()),
            config,
            None,
        )?;
        Ok((directory, event_loop, setup.hybrid_storage))
    }

    #[test]
    fn should_preserve_flush_capacity_before_admitting_cloud_transaction_wal() -> MidgeResult<()> {
        // Arrange
        let (_directory, mut event_loop, hybrid) = fixture()?;
        let ops = [TransactionOp::Put {
            cf_id: 0,
            key: bytes::Bytes::from_static(b"key"),
            value: bytes::Bytes::from(vec![1; 2048]),
            ttl_seconds: None,
            insert_only: false,
        }];
        // Act
        event_loop.prepare_cloud_transaction_memtables(&ops)?;
        let append = hybrid.admit_local_wal_bytes(256 * 1024 - 1);
        // Assert
        assert!(
            matches!(append, Err(MidgeError::NoSpace(_))),
            "WAL must not consume the last flush staging bytes"
        );
        assert!(hybrid.budget_snapshot().total_committed_bytes > 0);
        Ok(())
    }

    #[test]
    fn should_freeze_small_cloud_families_when_combined_staging_pressure_is_high() -> MidgeResult<()>
    {
        // Arrange
        let (_directory, mut event_loop, _hybrid) = fixture()?;
        let window = event_loop
            .cloud_flush_staging_window()
            .expect("cloud window");
        for index in 0..4 {
            let cf_id = event_loop.state.create_cf(format!("small-{index}"))?;
            let cf = event_loop.state.get_cf(cf_id).expect("family");
            cf.memtable
                .put_with_seq(b"key".to_vec(), vec![1; 2048], 1, None)?;
            assert!(flush_staging_bytes(cf.memtable.encoded_size_upper_bound()) < window / 2);
        }
        // Act
        let frozen = event_loop.freeze_cloud_memtables_near_staging_limit()?;
        // Assert
        assert_eq!(
            frozen, 4,
            "shared WAL pressure must drain individually small families"
        );
        Ok(())
    }

    #[test]
    fn should_report_near_limit_freeze_error_when_flush_identity_space_is_exhausted(
    ) -> MidgeResult<()> {
        // Arrange
        let (_directory, mut event_loop, _hybrid) = fixture()?;
        event_loop.state.next_flush_id = u64::MAX;
        event_loop
            .state
            .get_cf(0)
            .expect("family")
            .memtable
            .put_with_seq(b"key".to_vec(), vec![1; 16 * 1024], 1, None)?;
        // Act
        let result = event_loop.freeze_cloud_memtables_near_staging_limit();
        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        assert!(event_loop
            .state
            .get_cf(0)
            .expect("family")
            .memtable
            .get_bytes(b"key")?
            .is_some());
        Ok(())
    }

    #[test]
    fn should_preserve_active_memtable_when_flush_identity_space_is_exhausted() -> MidgeResult<()> {
        // Arrange
        let (_directory, mut event_loop, _hybrid) = fixture()?;
        event_loop.state.next_flush_id = u64::MAX;
        let cf = event_loop.state.get_cf(0).expect("family");
        cf.memtable
            .put_with_seq(b"key".to_vec(), b"accepted".to_vec(), 1, None)?;
        // Act
        let result = event_loop.freeze_active_memtable(0);
        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        assert_eq!(
            event_loop
                .state
                .get_cf(0)
                .expect("family")
                .memtable
                .get_bytes(b"key")?,
            Some(bytes::Bytes::from_static(b"accepted"))
        );
        Ok(())
    }
}
