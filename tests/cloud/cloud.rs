//! Cloud module integration tests
//!
//! This module includes all cloud-related tests organized in the cloud/ subdirectory.

// Cloud test modules
mod cloud {
    pub(crate) mod async_upload;
    pub(crate) mod cloud_sst;
    pub(crate) mod cloud_sst_manager;
    pub(crate) mod cloud_wal;
    pub(crate) mod cloud_wal_batch_manager;
    pub(crate) mod cloud_wal_debug;
    pub(crate) mod cloud_wal_integration;
    pub(crate) mod cloud_wal_mock;
    pub(crate) mod cloud_wal_reader;
    pub(crate) mod cloud_wal_segment;
    pub(crate) mod cloud_wal_writer;
    pub(crate) mod crash_recovery;
    pub(crate) mod wal_pruning;
}
