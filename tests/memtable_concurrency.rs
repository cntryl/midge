mod common;

#[test]
fn should_generate_strictly_increasing_sequence_numbers_given_parallel_writes() {
    panic!("TODO: implement test - sequence numbers strictly increasing under concurrent load");
}

#[test]
fn should_route_new_writes_to_new_memtable_given_freeze_in_progress_when_full() {
    panic!("TODO: implement test - writes route to new memtable atomically during freeze");
}

#[test]
fn should_return_latest_value_given_concurrent_puts_to_same_key_when_read() {
    panic!("TODO: implement test - concurrent puts return latest value consistently");
}

#[test]
fn should_trigger_flush_given_memtable_exceeds_threshold_when_background_thread_runs() {
    panic!("TODO: implement test - memtable flush triggered when size threshold exceeded");
}
