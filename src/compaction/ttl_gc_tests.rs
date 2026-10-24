use super::*;
use crate::io::{Fs, MockFs};
use crate::sst::{FsSstFactoryIo, SstFactory};
use crate::types::EntryType;
use std::sync::Arc;

#[test]
fn should_require_captured_expiration_authority_when_reclaiming_versions() -> crate::MidgeResult<()>
{
    for (time, eligible, retained, horizon, reclaimed) in [
        (99, true, true, None, false),
        (100, true, true, None, true),
        (100, false, true, None, false),
        (100, true, false, Some(50), false),
        (100, true, true, Some(1), false),
        (100, true, true, Some(2), true),
    ] {
        // Arrange
        let fs: Arc<dyn Fs> = Arc::new(MockFs::new());
        let factory = FsSstFactoryIo::new(fs, 4096);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", Some(b"older"), 1, EntryType::Put, None)?;
        writer.add_with_meta(b"key", Some(b"latest"), 2, EntryType::Put, Some(100))?;
        writer.add_with_meta(b"future", Some(b"keep"), 3, EntryType::Put, Some(200))?;
        writer.finish_to_path(Path::new("source.sst"))?;
        let mut plan = CompactionPlan::new(0, 0, 1).with_output_seq(912);
        plan.add_test_source("source.sst");
        plan.compaction_time_millis = Some(time);
        plan.point_tombstone_gc_eligible = eligible;
        plan.retained_snapshot_sources = retained;
        plan.snapshot_horizon = horizon;

        // Act
        let outputs = execute_compaction(&plan, &factory, Path::new(""), None)?;

        // Assert: raw contents, independent of current wall-clock TTL reads.
        assert_eq!(outputs.len(), 1);
        let rows = factory
            .open(Path::new(&outputs[0]))?
            .scan_range_raw_state(None, None)?;
        assert!(rows.iter().any(|(key, _)| key.as_ref() == b"future"));
        let latest = rows.iter().find(|(key, _)| key.as_ref() == b"key");
        if reclaimed {
            assert!(latest.is_none());
        } else {
            assert!(
                matches!(latest, Some((_, crate::types::KeyState::Value(value, 2, Some(100), _))) if value.as_ref() == b"latest")
            );
        }
    }
    Ok(())
}
