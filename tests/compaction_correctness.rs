mod common;

#[test]
fn should_produce_identical_output_given_same_input_runs_when_compacting() {
    panic!("TODO: implement test - deterministic compaction produces bit-identical output");
}

#[test]
fn should_remove_deleted_keys_given_tombstones_when_compaction_runs() {
    panic!("TODO: implement test - tombstones suppress older versions during compaction");
}

#[test]
fn should_keep_write_amplification_under_target_given_mixed_workload() {
    panic!("TODO: implement test - write amplification remains within configured bounds");
}
