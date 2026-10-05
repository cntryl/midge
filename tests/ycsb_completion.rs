//! #745 regression target: controls are in the exact shared duration-client loop.
#![cfg(feature = "internal-testing")]

#[path = "../benches/bench_support/config.rs"]
mod config;
#[path = "../benches/bench_support/ycsb.rs"]
mod ycsb;
