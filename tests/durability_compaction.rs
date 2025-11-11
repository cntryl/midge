mod common;

#[test]
fn should_commit_new_ssts_and_manifest_together_given_compaction_successful() {
    panic!("TODO: implement test - atomic commit of new SSTs and manifest after compaction");
}

#[test]
fn should_cleanup_partial_output_given_compaction_failure() {
    panic!("TODO: implement test - partial compaction output cleaned up on failure");
}

#[test]
fn should_delete_old_sst_files_only_after_manifest_persisted() {
    panic!("TODO: implement test - old SST deletion only after manifest persist completes");
}

#[test]
fn should_fsync_new_ssts_before_updating_manifest() {
    panic!("TODO: implement test - new SSTs must be fsynced before manifest update");
}

#[test]
fn should_recover_consistent_state_given_crash_mid_compaction_when_restart() {
    panic!("TODO: implement test - crash during compaction leaves database in consistent state");
}

#[test]
fn should_preserve_source_ssts_when_compaction_output_not_fsynced() {
    panic!("TODO: implement test - source SSTs preserved if compaction outputs not durable");
}
