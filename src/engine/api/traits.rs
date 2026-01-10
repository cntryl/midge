//! Authoritative trait-based API for Midge
//!
//! Minimal, explicit, and AI-proof traits following the authoritative design principles:
//! - Single Transaction type (mode-gated at runtime)
//! - begin_tx requires ColumnFamilyId (no cross-CF transactions)
//! - commit ALWAYS requires WriteOptions
//! - put/insert support optional TTL
//! - No batch API (transactions ARE the batch)

use super::WriteOptions;
use crate::common::MidgeResult;
use crate::engine::ColumnFamilyId;
use std::ops::Range;

pub type Bytes = bytes::Bytes;

/// Transaction mode controls read/write capabilities
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxMode {
    ReadOnly,
    ReadWrite,
}

impl From<super::TransactionMode> for TxMode {
    fn from(mode: super::TransactionMode) -> Self {
        match mode {
            super::TransactionMode::ReadOnly => TxMode::ReadOnly,
            super::TransactionMode::ReadWrite => TxMode::ReadWrite,
        }
    }
}

impl From<TxMode> for super::TransactionMode {
    fn from(mode: TxMode) -> Self {
        match mode {
            TxMode::ReadOnly => super::TransactionMode::ReadOnly,
            TxMode::ReadWrite => super::TransactionMode::ReadWrite,
        }
    }
}

/// TTL (time-to-live) specification for write operations
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ttl {
    /// Duration in seconds from commit time
    pub seconds: u64,
}

impl Ttl {
    pub fn seconds(seconds: u64) -> Self {
        Self { seconds }
    }
}

/// Key-value pair returned by scans
pub type KvPair = (Bytes, Bytes);

/// Iterator for scan results
pub trait KvIterator {
    fn next(&mut self) -> MidgeResult<Option<KvPair>>;
}

/// Concrete iterator implementation wrapping the internal iterator
pub struct MidgeKvIterator {
    inner: super::Iterator,
}

impl KvIterator for MidgeKvIterator {
    fn next(&mut self) -> MidgeResult<Option<KvPair>> {
        Ok(self
            .inner
            .next()
            .map(|(k, v)| (Bytes::from(k), Bytes::from(v))))
    }
}

/// Transaction trait - single type with mode-based capability control
///
/// All reads and writes MUST execute within a transaction. Transactions are:
/// - Bound to exactly one ColumnFamily at creation time
/// - Cannot be reused after commit or rollback
/// - Provide snapshot isolation with repeatable reads
pub trait Transaction {
    type Iter: KvIterator;

    // === Introspection ===

    /// Get the transaction mode
    fn mode(&self) -> TxMode;

    /// Get the column family this transaction is bound to
    fn column_family_id(&self) -> ColumnFamilyId;

    /// Check if transaction is closed (committed or rolled back)
    fn is_closed(&self) -> bool;

    // === Reads (allowed in all modes) ===

    /// Get a value for the given key
    ///
    /// Returns None if key doesn't exist or has been deleted.
    /// Provides read-your-own-writes semantics.
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>>;

    /// Scan half-open range [start, end)
    ///
    /// Returns an iterator over all key-value pairs in the range.
    fn scan(&self, start: &[u8], end: &[u8]) -> MidgeResult<Self::Iter>;

    /// Scan using a Range<Bytes> for convenience
    fn scan_range(&self, range: Range<Bytes>) -> MidgeResult<Self::Iter> {
        self.scan(&range.start, &range.end)
    }

    // === Writes (only allowed in ReadWrite mode) ===

    /// Put (upsert) a key-value pair with optional TTL
    ///
    /// Overwrites any existing value. TTL is attached at write time and is immutable.
    /// Returns TxReadOnly error if called on ReadOnly transaction.
    fn put(&mut self, key: Bytes, value: Bytes, ttl: Option<Ttl>) -> MidgeResult<()>;

    /// Insert a key-value pair (error if key already exists) with optional TTL
    ///
    /// Returns KeyAlreadyExists if key exists.
    /// Returns TxReadOnly error if called on ReadOnly transaction.
    fn insert(&mut self, key: Bytes, value: Bytes, ttl: Option<Ttl>) -> MidgeResult<()>;

    /// Delete a key
    ///
    /// Idempotent - no error if key doesn't exist.
    /// Returns TxReadOnly error if called on ReadOnly transaction.
    fn delete(&mut self, key: &[u8]) -> MidgeResult<()>;

    /// Delete all keys in range [start, end)
    ///
    /// Returns TxReadOnly error if called on ReadOnly transaction.
    fn delete_range(&mut self, start: &[u8], end: &[u8]) -> MidgeResult<()>;

    // === Lifecycle ===

    /// Commit the transaction with explicit write options
    ///
    /// WriteOptions MUST always be supplied - no defaults.
    /// For ReadOnly transactions, this is either a no-op or returns TxReadOnly.
    fn commit(self: Box<Self>, opts: WriteOptions) -> MidgeResult<()>;

    /// Rollback the transaction
    ///
    /// Discards all pending writes. Safe to call on any transaction state.
    fn rollback(self: Box<Self>) -> MidgeResult<()>;
}

/// Engine trait - main database interface
///
/// All operations execute through transactions. No direct put/get/delete helpers.
pub trait Engine {
    type Tx: Transaction;

    // === Identification ===

    /// Get the default column family ID
    fn default_column_family_id(&self) -> ColumnFamilyId;

    // === Column Family Management ===

    /// Create a new column family
    ///
    /// Returns the ID of the newly created column family.
    fn create_column_family(&self, name: &str) -> MidgeResult<ColumnFamilyId>;

    /// Drop a column family
    ///
    /// All data in the column family will be deleted.
    fn drop_column_family(&self, cf: ColumnFamilyId) -> MidgeResult<()>;

    // === Transaction Management ===

    /// Begin a new transaction bound to the specified column family
    ///
    /// The transaction is permanently bound to this CF and cannot cross CF boundaries.
    /// Mode controls whether writes are allowed (ReadWrite) or forbidden (ReadOnly).
    fn begin_tx(&self, cf: ColumnFamilyId, mode: TxMode) -> MidgeResult<Box<Self::Tx>>;

    // === Maintenance ===

    /// Force flush of memtables to disk
    fn flush(&self) -> MidgeResult<()>;

    /// Compact all SSTables
    fn compact_all(&self) -> MidgeResult<()>;

    /// Shutdown the engine
    ///
    /// Waits for all background operations to complete.
    fn shutdown(self) -> MidgeResult<()>
    where
        Self: Sized;
}
