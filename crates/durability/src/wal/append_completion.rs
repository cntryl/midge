//! Typed append admission and physical completion for the loop-owned pipeline.

use super::traits::WalAppendError;
use cntryl_midge_core::common::MidgeError;
use std::cell::Cell;
use std::sync::{mpsc, Arc};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct WalAppendPrefix {
    pub barrier_id: u64,
    pub writer_epoch: u64,
    pub segment_id: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct WalAppendCompletion {
    pub prefix: WalAppendPrefix,
    pub start_offset: u64,
    pub(crate) last_record_offset: u64,
    pub end_offset: u64,
    pub(crate) elapsed: Duration,
}

pub type AppendResult = Result<WalAppendCompletion, WalAppendError>;
type AcknowledgeAppend = Box<dyn Fn(&WalAppendCompletion) -> Result<(), MidgeError> + Send + Sync>;

/// The owner must retain its lexical root until this callback is terminal.
/// A deadline observed by the caller does not cancel admitted physical work.
#[doc(hidden)]
pub struct WalPendingAppend {
    response: mpsc::Receiver<AppendResult>,
    failure: Arc<dyn Fn() -> Option<MidgeError> + Send + Sync>,
    _root: Option<crate::lease::WriterEffectGroup>,
    settled: Cell<bool>,
    acknowledge: AcknowledgeAppend,
}

impl WalPendingAppend {
    pub(crate) fn new(
        response: mpsc::Receiver<AppendResult>,
        failure: Arc<dyn Fn() -> Option<MidgeError> + Send + Sync>,
        root: Option<crate::lease::WriterEffectGroup>,
        acknowledge: AcknowledgeAppend,
    ) -> Self {
        Self {
            response,
            failure,
            _root: root,
            settled: Cell::new(false),
            acknowledge,
        }
    }

    pub fn try_complete(&self) -> Option<AppendResult> {
        if self.settled.get() {
            return None;
        }
        let result = match self.response.try_recv() {
            Ok(result) => result,
            Err(mpsc::TryRecvError::Empty) => return None,
            Err(mpsc::TryRecvError::Disconnected) => {
                Err(WalAppendError::unknown((self.failure)().unwrap_or_else(
                    || MidgeError::Internal("WAL writer exited before append completion".into()),
                )))
            }
        };
        self.settled.set(true);
        Some(result.and_then(|completion| {
            (self.acknowledge)(&completion).map_err(WalAppendError::unknown)?;
            Ok(completion)
        }))
    }
}
