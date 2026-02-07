//! Testing utilities.
//!
//! Keep `testkit` organized: types/config live in `config`, mocks in `storage_mock`,
//! compatibility helpers in `engine_compat`, and assertions in `assertions`.

pub mod bench;
pub mod kv;
pub mod stress;
pub mod ycsb;
pub mod zipfian;

mod assertions;
mod backpressure;
mod config;
mod misc;
mod storage_mock;

pub use assertions::{assert_get_equals, assert_key_absent, bulk_put};
pub use backpressure::open_engine_with_memory_budget_bytes;
pub use config::{
    all_storage_modes, all_storage_modes_new, compaction_test_opts, disk_storage_modes,
    durability_opts, durable_storage_modes, filesystem_storage_modes, for_each_storage_mode,
    manual_compaction_test_opts, memory_opts, memory_storage_modes, opts_for_mode, test_temp_dir,
    MidgeOptions, StorageMode,
};
pub use misc::{
    open_with_mode, populate_multi_level_data, test_helpers, with_engine_restart,
    DurabilityTestContext,
};
pub use storage_mock::MockStorage;
