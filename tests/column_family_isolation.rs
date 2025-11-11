mod common;

#[test]
fn should_not_return_key_from_different_cf_given_same_user_key_when_read() {
    panic!("TODO: implement test - column families isolate data correctly");
}

#[test]
fn should_compact_cf_independently_given_multiple_cfs_when_threshold_exceeded() {
    panic!("TODO: implement test - CFs compact independently");
}

#[test]
fn should_recreate_cf_with_same_name_given_previous_drop_when_reopen() {
    panic!("TODO: implement test - CF can be dropped and recreated safely");
}
