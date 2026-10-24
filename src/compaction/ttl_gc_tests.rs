use super::*;
use crate::io::{Fs, MockFs};
use crate::sst::{FsSstFactoryIo, SstFactory};
use crate::types::EntryType;
use std::sync::Arc;

#[test]
fn should_preserve_raw_expiration_when_compaction_precedes_clock_correction(
) -> crate::MidgeResult<()> {
    for horizon in [None, Some(1), Some(2), Some(50)] {
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
        plan.point_tombstone_gc_eligible = true;
        plan.snapshot_horizon = horizon;

        // Act
        let outputs = execute_compaction(&plan, &factory, Path::new(""), None)?;

        // Assert: an expired latest value remains the masking record. A
        // corrected clock can read it; expiry never reveals the older value.
        assert_eq!(outputs.len(), 1);
        let reader = factory.open(Path::new(&outputs[0]))?;
        let rows = reader.scan_range_raw_state(None, None)?;
        assert!(rows.iter().any(|(key, _)| key.as_ref() == b"future"));
        let latest = rows.iter().find(|(key, _)| key.as_ref() == b"key");
        assert!(
            matches!(latest, Some((_, crate::types::KeyState::Value(value, 2, Some(100), _))) if value.as_ref() == b"latest")
        );
        assert!(matches!(
            reader.get_state_at_with_time(b"key", u64::MAX, 100)?,
            crate::types::KeyState::Tombstone(2)
        ));
        assert!(
            matches!(reader.get_state_at_with_time(b"key", u64::MAX, 99)?,
            crate::types::KeyState::Value(value, 2, Some(100), _) if value.as_ref() == b"latest")
        );
    }
    Ok(())
}
