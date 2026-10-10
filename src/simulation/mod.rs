//! Deterministic fault scheduling for the shipping storage interfaces.

pub mod counterexample;
pub mod filesystem;
pub mod object_store;
pub mod oracle;
pub mod scheduler;

#[cfg(test)]
mod campaign_tests;
