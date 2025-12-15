//! Filesystem abstraction for the engine (small, start-only-with-traits)
//!
//! This module implements the minimal trait surface from the FS design
//! specification: typed path IDs, durability enum, error types, and the
//! `EngineFs` and WAL/SST writer/reader traits. Implementations (RealFs,
//! FastFs, ChaosFs) will be added later.

pub mod traits;

pub use traits::*;

// Submodules (stubs / implementations)
pub mod chaos;
pub mod real;
pub mod mock;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traits_are_send_sync() {
        // Ensure the trait objects are Send + Sync where expected.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<&dyn EngineFs>();
    }
}
