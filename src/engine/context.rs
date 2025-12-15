//! Engine execution context

/// Execution context for operations
#[derive(Clone, Debug, Default)]
pub struct Context {
    pub operation_id: u64,
}

impl Context {
    pub fn new(operation_id: u64) -> Self {
        Self { operation_id }
    }
}
