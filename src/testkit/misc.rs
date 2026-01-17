//! Miscellaneous test helpers.
//!
//! Prefer adding new helpers to a more specific module (config/mock/assertions/etc).

use super::MidgeOptions;

/// Open a Midge engine with the given options.
///
/// This is the canonical test helper for opening engines in integration tests.
/// The `mode` parameter is for diagnostics only; the actual configuration comes from `opts`.
///
/// # Arguments
/// * `opts` - Configuration options for the engine (from `opts_for_mode`)
/// * `_mode` - Mode string (for logging/diagnostics only, not used)
///
/// # Returns
/// The opened `Engine` instance.
///
/// # Panics
/// Panics if the engine fails to open.
pub fn open_with_mode(opts: MidgeOptions, _mode: &str) -> crate::Engine {
    crate::Engine::open_with_options(opts).expect("failed to open engine")
}

/// Durability test context (stub for compatibility).
pub struct DurabilityTestContext {
    _private: (),
}

impl DurabilityTestContext {
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for DurabilityTestContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Populate multi-level data for compaction tests.
///
/// NOTE: Currently a stub retained for compatibility.
pub fn populate_multi_level_data(
    _engine: &crate::Engine,
    _cf: &crate::ColumnFamilyHandle,
    _levels: usize,
) -> crate::MidgeResult<()> {
    Ok(())
}

/// Test helpers module.
pub mod test_helpers {
    use std::time::Duration;

    /// Wait for a signal with default timeout.
    pub fn wait_for_signal_default<T>(rx: std::sync::mpsc::Receiver<T>) -> Option<T> {
        rx.recv_timeout(Duration::from_secs(5)).ok()
    }
}

/// Helper for testing engine restart scenarios.
pub fn with_engine_restart<F1, F2>(opts: MidgeOptions, before_restart: F1, after_restart: F2)
where
    F1: FnOnce(&crate::Engine),
    F2: FnOnce(&crate::Engine),
{
    {
        let engine = crate::Engine::open_with_options(opts.clone()).expect("open");
        before_restart(&engine);
        drop(engine);
    }

    {
        let engine = crate::Engine::open_with_options(opts).expect("reopen");
        after_restart(&engine);
    }
}
