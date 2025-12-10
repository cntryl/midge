//! Midge - High-performance embedded LSM-tree database
//!
//! Clean architectural layers:
//! - `common` - Foundational types with zero dependencies
//! - `engine` - Main KV store and operations
//! - `runtime` - Actor-based background task coordination
//! - `metadata` - Manifest and version management
//! - `wal` - Write-ahead logging abstraction
//! - `sst` - Sorted string tables (memtable → immutable SST)
//! - `storage` - Storage backend abstraction (fs, cloud, hybrid)
//! - `compaction` - Compaction planning and execution
//! - `iterators` - Iterator implementations
//! - `metrics` - Performance monitoring
//! - `testkit` - Testing utilities

#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::unwrap_used))]

// Foundation - no dependencies
pub mod common;

// Storage abstraction
pub mod storage;

// Data structures
pub mod iterators;

// Logging
pub mod wal;

// SSTs and memtables
pub mod sst;

// Metadata management
pub mod metadata;

// Main engine
pub mod engine;

// Background processing
pub mod runtime;

// Data organization
pub mod compaction;

// Observability
pub mod metrics;

// Testing
pub mod testkit;

// Re-export key types for consumers
pub use common::{MidgeError, MidgeResult};

// Main engine API
pub use engine::{
    MidgeEngine, 
    ColumnFamilyHandle, 
    ColumnFamilyId,
    open_engine,
};

// Engine API types
pub use engine::api::{
    // Query and iteration
    Query, 
    Iterator, 
    Direction,
    
    // Write operations
    WriteBatch, 
    WriteOptions,
    
    // Transactions
    KvTransaction, 
    IsolationLevel,
    Transaction,
    
    // Results
    CasResult, 
    InsertResult,
    
    // Snapshots
    Snapshot,
    
    // Column families
    ColumnFamily,
    
    // Configuration (smart defaults)
    OpenOptions,
    Goal,
    Durability,
    MemoryBudget,
    WorkloadProfile,
    
    // Key-value types
    Key,
    Value,
    KvPair,
    
    // Errors
    ApiError,
    ApiResult,
};

// Observability
pub use metrics::PerformanceMetrics;

// Testing utilities
pub use testkit::{MidgeOptions, StorageMode, MockStorage};
