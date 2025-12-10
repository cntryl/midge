//! Column Family API
//!
//! Column families provide logical partitioning of data within a database.
//! Each column family has its own memtable, SST files, and compaction settings.

pub use super::super::ColumnFamilyHandle as ColumnFamily;
