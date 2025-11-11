mod common;

#[test]
fn should_block_backup_start_given_active_compaction_when_requested() {
    panic!("TODO: implement test - backup start blocked if active compaction running");
}

#[test]
fn should_fail_cf_drop_given_inflight_flush() {
    panic!("TODO: implement test - column family drop fails if flush in progress");
}

#[test]
fn should_allow_backup_readonly_mode_given_active_writes() {
    panic!("TODO: implement test - readonly backup allowed during active writes");
}

#[test]
fn should_handle_config_reload_during_compaction_without_panic() {
    panic!("TODO: implement test - config reload during compaction does not panic");
}

#[test]
fn should_return_current_cf_list_given_admin_query_when_changes_in_progress() {
    panic!("TODO: implement test - admin API returns consistent CF list during modifications");
}
