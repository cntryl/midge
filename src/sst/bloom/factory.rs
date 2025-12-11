//! Bloom filter factory for polymorphic creation

use super::{BloomReader, BloomWriter};
use crate::common::MidgeResult;

/// Factory trait for creating bloom filters
pub trait BloomFilterFactory: Send + Sync {
    /// Create a new writer with estimated keys and false positive rate
    fn create_writer(&self, estimated_keys: usize, fpr: f64) -> BloomWriter;

    /// Create a writer with default parameters
    fn create_writer_default(&self, estimated_keys: usize) -> BloomWriter {
        self.create_writer(estimated_keys, 0.01)
    }

    /// Deserialize a filter from bytes
    fn deserialize(&self, data: &[u8]) -> MidgeResult<BloomReader>;
}

/// Default bloom filter factory
#[derive(Debug, Clone)]
pub struct BloomFactory;

impl BloomFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BloomFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl BloomFilterFactory for BloomFactory {
    fn create_writer(&self, estimated_keys: usize, fpr: f64) -> BloomWriter {
        BloomWriter::new(estimated_keys, fpr)
    }

    fn deserialize(&self, data: &[u8]) -> MidgeResult<BloomReader> {
        BloomReader::deserialize(data)
    }
}

#[cfg(test)]
mod tests {
    use super::super::writer::BloomFilterOps;
    use super::*;

    #[test]
    fn should_create_writer() {
        // Arrange
        let factory = BloomFactory::new();

        // Act
        let writer = factory.create_writer(100, 0.01);

        // Assert
        assert_eq!(writer.key_count(), 0);
    }

    #[test]
    fn should_create_writer_with_defaults() {
        // Arrange
        let factory = BloomFactory::new();

        // Act
        let writer = factory.create_writer_default(100);

        // Assert
        assert_eq!(writer.key_count(), 0);
    }

    #[test]
    fn should_deserialize_filter() {
        // Arrange
        let factory = BloomFactory::new();
        let mut writer = factory.create_writer(100, 0.01);
        writer.insert(b"test");
        let serialized = writer.serialize();

        // Act
        let result = factory.deserialize(&serialized);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_handle_factory_polymorphism() {
        // Arrange
        let factory: Box<dyn BloomFilterFactory> = Box::new(BloomFactory::new());

        // Act
        let writer = factory.create_writer(100, 0.01);

        // Assert
        assert_eq!(writer.key_count(), 0);
    }
}
