// Stubs for WAL vs manifest divergence tests

#[test]
fn should_prefer_wal_replay_given_manifest_lagging_latest_commit_when_recovering_after_crash() {
    // TODO: implement
    panic!("TODO: implement should_prefer_wal_replay_given_manifest_lagging_latest_commit_when_recovering_after_crash");
}

#[test]
fn should_rollback_manifest_view_given_manifest_ahead_of_persisted_ssts_when_detecting_inconsistent_state_on_restart() {
    // TODO: implement
    panic!("TODO: implement should_rollback_manifest_view_given_manifest_ahead_of_persisted_ssts_when_detecting_inconsistent_state_on_restart");
}

#[test]
fn should_refuse_to_open_database_given_irreconcilable_manifest_and_wal_states_when_corruption_detected() {
    // TODO: implement
    panic!("TODO: implement should_refuse_to_open_database_given_irreconcilable_manifest_and_wal_states_when_corruption_detected");
}

#[test]
fn should_rebuild_manifest_from_ssts_given_missing_manifest_and_clean_wal_tail_when_starting_after_disk_issue() {
    // TODO: implement
    panic!("TODO: implement should_rebuild_manifest_from_ssts_given_missing_manifest_and_clean_wal_tail_when_starting_after_disk_issue");
}
