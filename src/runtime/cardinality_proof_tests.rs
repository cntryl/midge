use super::actors::CompactionActor;
use super::RuntimeState;
use crate::compaction::{Compactor, LeveledCompactionConfig};
use crate::metadata::FileMeta;
use crate::sst::SkipListMemtable;
use std::collections::HashSet;
use std::sync::Arc;

const COLUMN_FAMILIES: usize = 3;
const LEVELS: usize = 4;
const MANIFEST_BITS: usize = COLUMN_FAMILIES * LEVELS;

fn proof_config() -> LeveledCompactionConfig {
    LeveledCompactionConfig {
        l0_compaction_threshold: u64::MAX,
        l0_file_count_threshold: 1,
        max_compaction_input_files: 64,
        level_multiplier: 1,
        l1_target_size: 1,
        max_levels: LEVELS,
    }
}

fn proof_file(cf_id: u32, level: u32, ordinal: usize) -> FileMeta {
    FileMeta {
        name: format!("proof-cf-{cf_id}-level-{level}-{ordinal}.sst"),
        level,
        size_bytes: if level == 0 { 1 } else { 2 },
        cf_id,
        sst_seq: u64::try_from(ordinal).expect("proof ordinal"),
        smallest_key: Some(b"a".to_vec()),
        largest_key: Some(b"z".to_vec()),
        key_bounds_complete: true,
        ..Default::default()
    }
}

fn proof_state(mask: usize) -> RuntimeState {
    let mut state = RuntimeState::new("/tmp/midge-cardinality-proof".into(), true);
    for cf_index in 1..COLUMN_FAMILIES {
        let expected = u32::try_from(cf_index).expect("proof column family ID");
        let actual = state
            .create_cf(format!("proof-cf-{cf_index}"))
            .expect("create proof column family");
        assert_eq!(actual, expected);
    }
    state.l0_compaction_trigger = 1;
    state.max_immutable_memtables = 2;
    for bit in 0..MANIFEST_BITS {
        if mask & (1 << bit) == 0 {
            continue;
        }
        let cf_id = u32::try_from(bit / LEVELS).expect("proof column family ID");
        let level = u32::try_from(bit % LEVELS).expect("proof level");
        state.manifest.files.push(proof_file(cf_id, level, bit));
    }
    state
}

fn assert_admission_stops_at_hard_ceiling(state: &mut RuntimeState, cf_id: u32) {
    let hard_ceiling = state.l0_hard_ceiling();
    let mut generation = 0_u64;
    while !state.l0_write_slot_unavailable(cf_id) {
        assert!(state.l0_slot_usage(cf_id) < hard_ceiling);
        generation = generation.saturating_add(1);
        let immutable = Arc::new(SkipListMemtable::new());
        immutable
            .put_with_seq(
                format!("reserved-{generation}").into_bytes(),
                b"value".to_vec(),
                generation,
                None,
            )
            .expect("reserve proof immutable generation");
        state
            .track_new_immutable_flush(cf_id, immutable, generation)
            .expect("track proof immutable generation");
        assert!(state.l0_slot_usage(cf_id) <= hard_ceiling);
    }
    assert_eq!(state.l0_slot_usage(cf_id), hard_ceiling);
    assert!(state.l0_write_slot_unavailable(cf_id));

    let cf = state.get_cf_mut(cf_id).expect("proof column family");
    cf.immutable_memtables.clear();
    cf.immutable_flushes.clear();
}

fn debt_rank(files: &[FileMeta]) -> usize {
    files
        .iter()
        .map(|file| LEVELS.saturating_sub(1 + file.level as usize))
        .sum()
}

fn apply_plan(state: &mut RuntimeState, plan: &crate::compaction::CompactionPlan, step: usize) {
    let input_names = plan.input_files.iter().cloned().collect::<HashSet<_>>();
    let output_size = state
        .manifest
        .files
        .iter()
        .filter(|file| input_names.contains(&file.name))
        .map(|file| file.size_bytes)
        .sum();
    state
        .manifest
        .files
        .retain(|file| !input_names.contains(&file.name));
    state.manifest.files.push(FileMeta {
        name: format!("proof-output-mask-{}-step-{step}.sst", state.sequence),
        level: plan.target_level,
        size_bytes: output_size,
        cf_id: plan.cf_id,
        sst_seq: u64::try_from(step).expect("proof step"),
        smallest_key: Some(b"a".to_vec()),
        largest_key: Some(b"z".to_vec()),
        key_bounds_complete: true,
        ..Default::default()
    });
    state.sequence = state.sequence.saturating_add(1);
}

#[test]
fn should_prove_every_small_abstract_manifest_invariant() {
    let config = proof_config();
    let compactor = Compactor::with_config(config.clone());
    let factory: Arc<dyn crate::sst::SstFactory> = Arc::new(crate::sst::FsSstFactoryIo::new(
        Arc::new(crate::io::MockFs::new()),
        4096,
    ));

    for mask in 0..(1 << MANIFEST_BITS) {
        // Arrange
        let mut state = proof_state(mask);
        for cf_id in 0..u32::try_from(COLUMN_FAMILIES).expect("proof CF count") {
            assert_admission_stops_at_hard_ceiling(&mut state, cf_id);
        }
        let mut actor = CompactionActor::new_with_config(Arc::clone(&factory), config.clone());
        let mut steps = 0;

        // Act
        while let Some(plan) = actor
            .check_manual_compaction(&state)
            .expect("proof compaction planning")
        {
            let before = debt_rank(&state.manifest.files);
            apply_plan(&mut state, &plan, steps);
            let after = debt_rank(&state.manifest.files);
            assert!(
                after < before,
                "debt rank did not decrease for mask {mask:#x}, step {steps}: {before} -> {after}"
            );
            steps += 1;
            assert!(steps <= MANIFEST_BITS * LEVELS);
        }

        // Assert
        for cf_id in 0..u32::try_from(COLUMN_FAMILIES).expect("proof CF count") {
            assert!(
                compactor
                    .compaction_debt_is_clear(&state.manifest.files, cf_id)
                    .expect("proof debt predicate"),
                "debt remained for mask {mask:#x}, column family {cf_id}"
            );
        }
    }
}

#[test]
fn should_round_robin_every_critical_column_family_without_starvation() {
    // Arrange
    let config = proof_config();
    let factory: Arc<dyn crate::sst::SstFactory> = Arc::new(crate::sst::FsSstFactoryIo::new(
        Arc::new(crate::io::MockFs::new()),
        4096,
    ));
    let mut actor = CompactionActor::new_with_config(factory, config);
    let mut state = proof_state(0);
    state.set_compaction_enabled(true);
    state.max_immutable_memtables = 0;
    for cf_id in 0..u32::try_from(COLUMN_FAMILIES).expect("proof CF count") {
        state
            .manifest
            .files
            .extend((0..2).map(|ordinal| proof_file(cf_id, 0, ordinal)));
    }

    // Act
    let scheduled = (0..COLUMN_FAMILIES * 2)
        .map(|_| {
            actor
                .check_compaction(&state)
                .expect("critical proof planning")
                .expect("critical proof plan")
                .cf_id
        })
        .collect::<Vec<_>>();

    // Assert
    assert_eq!(scheduled, vec![0, 1, 2, 0, 1, 2]);
}
