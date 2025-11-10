//! Engine coordination subsystems.
//!
//! This module contains coordination logic for background tasks:
//! - `flush_manager` - Memtable flushing coordination

mod flush_manager;

// Re-export coordination modules if needed
// Currently all methods are implemented as impl blocks on MidgeEngine
