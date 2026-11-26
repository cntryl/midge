//! Typed completion for an asynchronously appended segment footer.

use cntryl_midge_core::common::{MidgeError, MidgeResult};
use std::cell::Cell;
use std::sync::{mpsc, Arc};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct WalFooterPrefix {
    pub operation_id: u64,
    pub writer_epoch: u64,
    pub segment_id: u64,
    pub(crate) start_offset: u64,
    pub end_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct WalFooterCompletion {
    pub prefix: WalFooterPrefix,
    pub(crate) elapsed: Duration,
}

/// The event loop retains this receipt until the footer write reaches terminal
/// completion. Dropping the caller's wait never cancels an admitted write.
#[doc(hidden)]
pub struct WalPendingFooter {
    expected: WalFooterPrefix,
    response: mpsc::Receiver<MidgeResult<WalFooterCompletion>>,
    failure: Arc<dyn Fn() -> Option<MidgeError> + Send + Sync>,
    _root: Option<crate::lease::WriterEffectGroup>,
    settled: Cell<bool>,
}

impl WalPendingFooter {
    pub(crate) fn new(
        response: mpsc::Receiver<MidgeResult<WalFooterCompletion>>,
        expected: WalFooterPrefix,
        failure: Arc<dyn Fn() -> Option<MidgeError> + Send + Sync>,
        root: Option<crate::lease::WriterEffectGroup>,
    ) -> Self {
        Self {
            expected,
            response,
            failure,
            _root: root,
            settled: Cell::new(false),
        }
    }

    pub fn try_complete(&self) -> Option<MidgeResult<WalFooterCompletion>> {
        if self.settled.get() {
            return None;
        }
        let result = match self.response.try_recv() {
            Ok(result) => Some(result),
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                Some(Err((self.failure)().unwrap_or_else(|| {
                    MidgeError::Internal("WAL writer exited before footer completion".into())
                })))
            }
        };
        if result.is_some() {
            self.settled.set(true);
        }
        result.map(|completion| {
            completion.and_then(|completion| {
                if completion.prefix == self.expected {
                    Ok(completion)
                } else {
                    Err(MidgeError::Internal(
                        "WAL footer completion did not match its admitted identity".into(),
                    ))
                }
            })
        })
    }
}
