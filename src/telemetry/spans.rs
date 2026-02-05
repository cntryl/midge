//! Tracing span definitions and utilities

use std::fmt;

/// Span kinds for semantic identification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanKind {
    /// API call (put, get, delete, etc.)
    ApiCall,
    /// WAL operation
    WalOperation,
    /// Memtable operation
    MemtableOperation,
    /// Flush operation
    FlushOperation,
    /// Compaction operation
    CompactionOperation,
    /// SST operation (create, load, read)
    SstOperation,
    /// Cloud I/O (upload, download)
    CloudIo,
    /// Internal actor message
    ActorMessage,
}

impl fmt::Display for SpanKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpanKind::ApiCall => write!(f, "api_call"),
            SpanKind::WalOperation => write!(f, "wal_operation"),
            SpanKind::MemtableOperation => write!(f, "memtable_operation"),
            SpanKind::FlushOperation => write!(f, "flush_operation"),
            SpanKind::CompactionOperation => write!(f, "compaction_operation"),
            SpanKind::SstOperation => write!(f, "sst_operation"),
            SpanKind::CloudIo => write!(f, "cloud_io"),
            SpanKind::ActorMessage => write!(f, "actor_message"),
        }
    }
}

/// Semantic operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    Put,
    Get,
    Delete,
    Range,
    Merge,
    Flush,
    Compaction,
    Upload,
    Download,
}

impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperationType::Put => write!(f, "put"),
            OperationType::Get => write!(f, "get"),
            OperationType::Delete => write!(f, "delete"),
            OperationType::Range => write!(f, "range"),
            OperationType::Merge => write!(f, "merge"),
            OperationType::Flush => write!(f, "flush"),
            OperationType::Compaction => write!(f, "compaction"),
            OperationType::Upload => write!(f, "upload"),
            OperationType::Download => write!(f, "download"),
        }
    }
}

/// Span guard for structured tracing
pub struct MidgeSpan {
    name: String,
    attributes: Vec<(&'static str, String)>,
}

impl MidgeSpan {
    /// Create a new span
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            attributes: Vec::new(),
        }
    }

    /// Add a semantic attribute
    pub fn with_attr(mut self, key: &'static str, value: impl ToString) -> Self {
        self.attributes.push((key, value.to_string()));
        self
    }

    /// Add operation type
    pub fn with_operation(self, op: OperationType) -> Self {
        self.with_attr("db.operation", op.to_string())
    }

    /// Add column family
    pub fn with_cf(self, cf_id: u32) -> Self {
        self.with_attr("db.column_family", cf_id.to_string())
    }

    /// Add key size
    pub fn with_key_size(self, size: usize) -> Self {
        self.with_attr("db.key_size", size.to_string())
    }

    /// Add value size
    pub fn with_value_size(self, size: usize) -> Self {
        self.with_attr("db.value_size", size.to_string())
    }

    /// Add storage mode
    pub fn with_storage_mode(self, mode: &str) -> Self {
        self.with_attr("storage.mode", mode)
    }

    /// Add result status
    pub fn with_status(self, status: &str) -> Self {
        self.with_attr("rpc.status_code", status)
    }
}

impl fmt::Debug for MidgeSpan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MidgeSpan")
            .field("name", &self.name)
            .field("attributes", &self.attributes)
            .finish()
    }
}

/// Create a traced span for an operation
/// This is a no-op if telemetry is disabled
#[macro_export]
macro_rules! trace_span {
    ($name:expr) => {{
        #[cfg(feature = "telemetry")]
        {
            tracing::debug_span!($name)
        }
        #[cfg(not(feature = "telemetry"))]
        {
            let _ = ();
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_span_with_attributes() {
        // Arrange

        // Act
        let span = MidgeSpan::new("put_operation")
            .with_operation(OperationType::Put)
            .with_cf(0)
            .with_key_size(32)
            .with_value_size(1024);

        let attr_len = span.attributes.len();

        // Assert
        assert_eq!(span.name, "put_operation");
        assert_eq!(attr_len, 4);
    }

    #[test]
    fn should_format_operation_types() {
        assert_eq!(OperationType::Put.to_string(), "put");
        assert_eq!(OperationType::Delete.to_string(), "delete");
        assert_eq!(OperationType::Flush.to_string(), "flush");
    }

    #[test]
    fn should_format_span_kinds() {
        assert_eq!(SpanKind::ApiCall.to_string(), "api_call");
        assert_eq!(SpanKind::WalOperation.to_string(), "wal_operation");
    }
}
