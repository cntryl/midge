//! Adapters for exposing MidgeEngine through external trait interfaces.
//!
//! This module contains adapter types that wrap the engine and implement
//! external trait interfaces using composition rather than inheritance.
//!
//! Current adapters:
//! - `KvStoreAdapter` - Implements the `KvStore` trait for external API compatibility

pub mod kv_store;

pub use kv_store::KvStoreAdapter;
