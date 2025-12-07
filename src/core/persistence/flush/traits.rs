//! Flush observer trait for extensible flush notifications.
//!
//! This trait provides a clean abstraction for components that need to respond
//! to flush lifecycle events. It replaces ad-hoc callbacks with a single
//! composable interface.

use crate::core::manifest::Manifest;
use crate::error::MidgeError;

/// Type alias for manifest update callback to reduce complexity.
pub type ManifestCallback = Box<dyn Fn(&Manifest) + Send + Sync>;

/// Observer trait for flush lifecycle events.
///
/// Implement this trait to receive notifications when:
/// - A flush completes successfully (manifest updated)
/// - A background error occurs during flush
///
/// This provides a clean extension point for:
/// - Engine manifest cache updates
/// - Metrics collection
/// - Cloud upload coordination
/// - Test instrumentation
pub trait FlushOutput: Send + Sync {
    /// Called when a flush job completes successfully.
    ///
    /// The manifest has been updated and persisted at this point.
    /// Implementations should be fast and non-blocking.
    fn on_flush_complete(&self, manifest: &Manifest);

    /// Called when a background error occurs during flush.
    ///
    /// This allows observers to handle errors appropriately:
    /// - Update error state for writes
    /// - Log or alert
    /// - Trigger recovery procedures
    fn on_background_error(&self, error: &MidgeError);
}

/// A no-op implementation that ignores all events.
///
/// Useful for testing or when no observers are needed.
pub struct NullFlushOutput;

impl FlushOutput for NullFlushOutput {
    fn on_flush_complete(&self, _manifest: &Manifest) {}
    fn on_background_error(&self, _error: &MidgeError) {}
}

/// Adapter that bridges the old callback-style API to the new trait.
///
/// This provides backward compatibility during migration.
pub struct CallbackFlushOutput {
    manifest_callback: Option<ManifestCallback>,
    error_holder: Option<std::sync::Arc<parking_lot::RwLock<Option<MidgeError>>>>,
}

impl CallbackFlushOutput {
    /// Create a new adapter from optional callbacks.
    pub fn new(
        manifest_callback: Option<ManifestCallback>,
        error_holder: Option<std::sync::Arc<parking_lot::RwLock<Option<MidgeError>>>>,
    ) -> Self {
        Self {
            manifest_callback,
            error_holder,
        }
    }
}

impl FlushOutput for CallbackFlushOutput {
    fn on_flush_complete(&self, manifest: &Manifest) {
        if let Some(ref cb) = self.manifest_callback {
            cb(manifest);
        }
    }

    fn on_background_error(&self, error: &MidgeError) {
        if let Some(ref holder) = self.error_holder {
            *holder.write() = Some(MidgeError::internal(error.to_string()));
        }
    }
}

/// Clears background error when flush succeeds.
///
/// This helper is used after successful flush to allow writes to resume.
pub fn clear_background_error(
    error_holder: &Option<std::sync::Arc<parking_lot::RwLock<Option<MidgeError>>>>,
) {
    if let Some(ref holder) = error_holder {
        *holder.write() = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn should_notify_on_flush_complete() {
        // Arrange
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();
        let cb: Box<dyn Fn(&Manifest) + Send + Sync> =
            Box::new(move |_| called_clone.store(true, Ordering::SeqCst));
        let output = CallbackFlushOutput::new(Some(cb), None);
        let manifest = Manifest::default();

        // Act
        output.on_flush_complete(&manifest);

        // Assert
        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn should_set_background_error() {
        // Arrange
        let holder = Arc::new(parking_lot::RwLock::new(None));
        let output = CallbackFlushOutput::new(None, Some(holder.clone()));
        let error = MidgeError::internal("test error");

        // Act
        output.on_background_error(&error);

        // Assert
        assert!(holder.read().is_some());
    }

    #[test]
    fn should_clear_background_error() {
        // Arrange
        let holder = Arc::new(parking_lot::RwLock::new(Some(MidgeError::internal(
            "error",
        ))));
        let holder_opt = Some(holder.clone());

        // Act
        clear_background_error(&holder_opt);

        // Assert
        assert!(holder.read().is_none());
    }

    #[test]
    fn should_handle_null_output_without_panic() {
        // Arrange
        let output = NullFlushOutput;
        let manifest = Manifest::default();
        let error = MidgeError::internal("test");

        // Act
        output.on_flush_complete(&manifest);
        output.on_background_error(&error);

        // Assert
        // (no panic = success)
    }
}
