mod common;

#[test]
fn should_return_error_given_iterator_used_after_close() {
    panic!("TODO: implement test - iterator returns error when used after close");
}

#[test]
fn should_continue_iteration_given_compaction_in_progress_when_scan() {
    panic!("TODO: implement test - active iterator remains valid during background compaction");
}

#[test]
fn should_rewind_iterator_to_start_given_reset_called() {
    panic!("TODO: implement test - iterator rewinds to start position on reset");
}

#[test]
fn should_resume_iteration_given_checkpoint_sequence() {
    panic!("TODO: implement test - iterator can resume from checkpoint sequence");
}

#[test]
fn should_iterate_in_reverse_given_reverse_iterator_enabled_when_scan() {
    panic!("TODO: implement test - reverse iterator produces keys in descending order");
}
