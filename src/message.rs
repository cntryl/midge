//! Minimal message types for benchmark suites.

/// Tiny message kinds used for dispatch/match overhead measurements.
#[derive(Clone, Copy, Debug)]
pub enum MessageKind {
    Noop,
    StartupPing,
    GetRuntimeConfig,
}
