//! Public API types
//!
//! User-facing types for queries, mutations, snapshots, transactions,
//! column families, write batches, and merge operators.

pub mod column_family;
pub mod kv_store;
pub mod merge_operator;
pub mod mutation;
pub mod query;
pub mod snapshot;
pub mod write_batch;
pub mod write_options;

// Re-export all public API types
pub use column_family::{
    ColumnFamilyConfig, ColumnFamilyHandle, ColumnFamilyId,
    DEFAULT_CF_ID, DEFAULT_CF_NAME,
};
pub use kv_store::{DynKvStore, KvStore, KvTransaction};
pub use merge_operator::{
    BytesAppendOperator, DynMergeOperator, IntegerAddOperator, MergeOperator, StringAppendOperator,
};
pub use mutation::{Mutation, MutationOp};
pub use query::Query;
pub use snapshot::Snapshot;
// Transaction is now internal - use EngineTransaction exported from lib.rs
pub use write_batch::WriteBatch;
pub use write_options::WriteOptions;
