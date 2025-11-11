mod common;

#[test]
fn should_read_uncommitted_value_given_put_in_same_transaction_when_read() {
    panic!("TODO: implement test - transaction sees its own uncommitted writes");
}

#[test]
fn should_not_see_uncommitted_write_given_other_transaction_when_read() {
    panic!("TODO: implement test - uncommitted data invisible to other transactions");
}

#[test]
fn should_rollback_all_operations_given_transaction_abort_called() {
    panic!("TODO: implement test - transaction abort rolls back all operations atomically");
}

#[test]
fn should_detect_conflict_given_concurrent_updates_to_same_key_when_commit() {
    panic!("TODO: implement test - write-write conflicts detected on commit");
}

#[test]
fn should_return_old_value_given_snapshot_created_before_write() {
    panic!("TODO: implement test - snapshot isolation returns old value");
}
