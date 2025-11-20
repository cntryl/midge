mod common;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::test_temp_dir;

// Phase 1 Merge Operator Error Path Tests (stubs) - will be implemented.

#[test]
#[ignore]
fn should_fail_merge_given_operator_returns_error() { /* TODO */ }

#[test]
#[ignore]
fn should_fail_merge_given_unregistered_operator_when_merging() { /* TODO */ }

#[test]
#[ignore]
fn should_abort_flush_given_merge_error_during_compaction() { /* TODO */ }

#[test]
#[ignore]
fn should_abort_wal_replay_given_merge_error_when_recovering() { /* TODO */ }
