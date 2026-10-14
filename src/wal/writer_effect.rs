//! Capture a result from one storage effect admitted by a writer authority.

use crate::common::{MidgeError, MidgeResult};

pub(crate) struct WriterEffectError {
    pub(crate) error: MidgeError,
    /// A failed authority check proves no effect only before admission.
    pub(crate) started: bool,
}

pub(crate) fn with_writer_effect<T>(
    authority: impl FnOnce(&mut dyn FnMut()) -> MidgeResult<()>,
    operation: impl FnOnce() -> T,
) -> Result<T, WriterEffectError> {
    let mut operation = Some(operation);
    let mut result = None;
    let status = authority(&mut || {
        if let Some(operation) = operation.take() {
            result = Some(operation());
        }
    });
    status.map_err(|error| WriterEffectError {
        error,
        started: result.is_some(),
    })?;
    result.ok_or_else(|| WriterEffectError {
        error: MidgeError::Internal(
            "writer authority completed without running its storage effect".into(),
        ),
        started: false,
    })
}
