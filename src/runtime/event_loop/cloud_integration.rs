//! Cloud / hybrid storage integration
//!
//! Handles `CloudAsync` WAL flush, cloud upload ack/fail events,
//! and hybrid storage polling/push-channel draining.

use super::EventLoop;
use std::convert::TryFrom;

mod ack;
mod metadata;
mod prune;
mod sealing;

impl EventLoop {
    pub(super) fn elapsed_micros_to_u64(duration: std::time::Duration) -> u64 {
        u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests;
