//! Scoped observations of actual engine startup ownership transitions.

/// Facts emitted by the startup resource owner, without advancing liveness.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupEvent {
    LeaseAcquired { epoch: u64 },
    RuntimePrepared,
    RuntimeAdmitted,
    RuntimeAborted,
    CleanupFinished { successful: bool },
}

/// An explicitly passed observer for a single startup attempt.
#[doc(hidden)]
pub trait StartupObserver: Send + Sync + std::fmt::Debug {
    fn observe(&self, event: StartupEvent);
}
