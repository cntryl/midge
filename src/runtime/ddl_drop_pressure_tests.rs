use super::*;

fn pressured_state() -> (RuntimeState, ManifestEdit, usize) {
    let mut state = RuntimeState::new(std::path::PathBuf::from("unused-memory-path"), true);
    let create = create_edit(&state, "pressure").expect("create family edit");
    apply_local_edit(&mut state, &create).expect("apply family creation");
    let ManifestEdit::CreateColumnFamily { id, .. } = create else {
        unreachable!("create edit must create a family");
    };
    let retained = &state.get_cf(0).expect("default family").memtable;
    retained
        .put_with_seq(b"retained".to_vec(), vec![1; 128], 1, None)
        .expect("write retained data");
    let retained_bytes = retained.size_bytes();
    state
        .get_cf(id)
        .expect("pressure family")
        .memtable
        .put_with_seq(b"pressure".to_vec(), vec![2; 1024], 2, None)
        .expect("write pressure data");
    state.recompute_total_memtable_bytes();
    let drop = drop_edit(&state, id, true).expect("prepare discard");
    (state, drop, retained_bytes)
}

#[test]
fn should_keep_other_family_memory_charged_when_local_drop_is_reapplied() {
    // Arrange
    let (mut state, drop, retained_bytes) = pressured_state();

    // Act
    apply_local_edit(&mut state, &drop).expect("apply local drop");
    apply_local_edit(&mut state, &drop).expect("reapply local drop");

    // Assert
    assert_eq!(state.total_memtable_bytes, retained_bytes);
}

#[test]
fn should_keep_other_family_memory_charged_when_remote_drop_is_reapplied() {
    // Arrange
    let (mut state, drop, retained_bytes) = pressured_state();

    // Act
    apply_remote_committed_visibility(&mut state, &drop).expect("apply committed remote drop");
    apply_remote_committed_visibility(&mut state, &drop).expect("reapply committed remote drop");

    // Assert
    assert_eq!(state.total_memtable_bytes, retained_bytes);
}
