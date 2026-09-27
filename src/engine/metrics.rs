use crate::common::{MidgeError, MidgeResult};
use crate::runtime::{next_request_id, RuntimeHandle, RuntimeMsg, RuntimeResponse};
use crate::types::{
    ReadAmpMetricsSnapshot, RecoveryMetricsSnapshot, RuntimeMetricsSnapshot, StorageLayoutSnapshot,
};
use std::time::Duration;

/// Runtime-backed observability façade for an open engine.
#[derive(Clone)]
pub struct EngineMetrics {
    runtime_handle: RuntimeHandle,
}

impl EngineMetrics {
    pub(super) fn new(runtime_handle: RuntimeHandle) -> Self {
        Self { runtime_handle }
    }

    /// Return the current read-amplification snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime cannot provide a snapshot.
    pub fn get_read_amp_metrics(&self) -> MidgeResult<ReadAmpMetricsSnapshot> {
        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::GetReadAmpMetrics {
                request_id: next_request_id()?,
            })?;
        match response {
            RuntimeResponse::ReadAmpMetricsSnapshot { snapshot, .. } => Ok(snapshot),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response from GetReadAmpMetrics".to_string(),
            )),
        }
    }

    /// Return metrics captured during startup recovery.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime cannot provide a snapshot.
    pub fn get_recovery_metrics(&self) -> MidgeResult<RecoveryMetricsSnapshot> {
        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::GetRecoveryMetrics {
                request_id: next_request_id()?,
            })?;
        match response {
            RuntimeResponse::RecoveryMetricsSnapshot { snapshot, .. } => Ok(snapshot),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response from GetRecoveryMetrics".to_string(),
            )),
        }
    }

    /// Return the current SST layout and pinned snapshot state.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime cannot provide a storage-layout snapshot.
    pub fn get_storage_layout(&self) -> MidgeResult<StorageLayoutSnapshot> {
        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::GetStorageLayout {
                request_id: next_request_id()?,
            })?;
        match response {
            RuntimeResponse::StorageLayoutSnapshot { snapshot, .. } => Ok(snapshot),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response from GetStorageLayout".to_string(),
            )),
        }
    }

    /// Return an operator-facing runtime metrics and health snapshot.
    ///
    /// Hybrid engines include local working-storage charges and admission
    /// pressure in `local_storage`, plus observed remote range request costs.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime cannot provide a snapshot.
    pub fn get_runtime_metrics(&self) -> MidgeResult<RuntimeMetricsSnapshot> {
        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::GetRuntimeMetrics {
                request_id: next_request_id()?,
            })?;
        Self::parse_runtime_metrics(response)
    }

    /// Return runtime metrics without blocking beyond `timeout`.
    ///
    /// # Errors
    ///
    /// Returns a runtime error or [`MidgeError::Timeout`] when the deadline expires.
    pub fn get_runtime_metrics_with_timeout(
        &self,
        timeout: Duration,
    ) -> MidgeResult<RuntimeMetricsSnapshot> {
        Self::validate_timeout(timeout)?;
        let response = self.runtime_handle.send_and_wait_timeout(
            RuntimeMsg::GetRuntimeMetrics {
                request_id: next_request_id()?,
            },
            timeout,
        )?;
        match response {
            Some(response) => Self::parse_runtime_metrics(response),
            None => Err(MidgeError::Timeout(
                "get_runtime_metrics_with_timeout exceeded the deadline".to_string(),
            )),
        }
    }

    fn validate_timeout(timeout: Duration) -> MidgeResult<()> {
        if timeout.is_zero() {
            return Err(MidgeError::Timeout(
                "get_runtime_metrics_with_timeout deadline is zero".to_string(),
            ));
        }
        Ok(())
    }

    fn parse_runtime_metrics(response: RuntimeResponse) -> MidgeResult<RuntimeMetricsSnapshot> {
        match response {
            RuntimeResponse::RuntimeMetricsSnapshot { snapshot, .. } => Ok(*snapshot),
            RuntimeResponse::Error { error, .. } => Err(error),
            other => Err(MidgeError::Internal(format!(
                "Unexpected response from GetRuntimeMetrics: {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EngineMetrics;
    use crate::common::MidgeError;
    use std::time::Duration;

    #[test]
    fn should_reject_zero_timeout_without_constructing_engine() {
        // Arrange
        let timeout = Duration::ZERO;

        // Act
        let result = EngineMetrics::validate_timeout(timeout);

        // Assert
        assert!(matches!(result, Err(MidgeError::Timeout(_))));
    }
}
