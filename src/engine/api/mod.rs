//! Engine API - Public interfaces
pub mod errors;
pub mod iterator;
pub mod kv;
pub mod merge_operator;
pub mod options;
pub mod query;
pub mod results;
pub mod traits;
pub mod transaction;
pub mod types;
pub mod write_options;

pub use errors::{ApiError, ApiResult};
pub use iterator::{Direction, Iterator, IteratorBuilder};
pub use kv::{Key, KvPair, OptionalValue, Value};
pub use merge_operator::MergeOperator;
pub use options::{Durability, Goal, MemoryBudget, OpenOptions, WorkloadProfile};
pub use query::Query;
pub use results::{CasResult, InsertResult};
pub use traits::{Engine as EngineT, KvIterator, Transaction as TransactionT, Ttl, TxMode};
pub use transaction::{
    IsolationLevel, Transaction, TransactionMode, TransactionState, WriteIntent,
};
pub use write_options::{DurabilityPolicy, WriteOptions};

// Re-export caller-visible acknowledgment semantics.
pub use crate::common::AckPolicy;
