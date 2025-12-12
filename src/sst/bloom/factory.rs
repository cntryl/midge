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
    use super::super::writer::{BloomFilterOps, BloomTestResult};
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

    // ===== New comprehensive invariant tests =====

    #[test]
    fn should_handle_factory_default_creation() {
        // Arrange
        let factory = BloomFactory::default();

        // Act
        let writer = factory.create_writer(100, 0.01);

        // Assert
        assert_eq!(writer.key_count(), 0);
        assert!(writer.size_bytes() > 0);
    }

    #[test]
    fn should_create_writers_with_different_capacities() {
        // Arrange
        let factory = BloomFactory::new();

        // Act
        let writer_small = factory.create_writer(10, 0.01);
        let writer_large = factory.create_writer(10000, 0.01);

        // Assert
        assert!(writer_large.size_bytes() > writer_small.size_bytes());
    }

    #[test]
    fn should_create_writers_with_varying_fpr() {
        // Arrange
        let factory = BloomFactory::new();

        // Act
        let writer_strict = factory.create_writer(100, 0.001);  // Stricter FPR
        let writer_loose = factory.create_writer(100, 0.1);    // Looser FPR

        // Assert
        assert!(writer_strict.size_bytes() > writer_loose.size_bytes());
    }

    #[test]
    fn should_preserve_fpr_in_default_creation() {
        // Arrange
        let factory = BloomFactory::new();

        // Act
        let writer_default = factory.create_writer_default(100);
        let writer_explicit = factory.create_writer(100, 0.01);

        // Assert (both should have same size since default is 0.01)
        assert_eq!(writer_default.size_bytes(), writer_explicit.size_bytes());
    }

    #[test]
    fn should_deserialize_valid_data_after_factory_creation() {
        // Arrange
        let factory = BloomFactory::new();
        let mut writer = factory.create_writer(100, 0.01);
        writer.insert(b"test");
        let serialized = writer.serialize();

        // Act
        let result = factory.deserialize(&serialized);

        // Assert
        assert!(result.is_ok());
        let reader = result.unwrap();
        assert_eq!(reader.key_count(), 1);
    }

    #[test]
    fn should_handle_multiple_factory_instances() {
        // Arrange
        let factory1 = BloomFactory::new();
        let factory2 = BloomFactory::new();

        // Act
        let mut writer1 = factory1.create_writer(100, 0.01);
        writer1.insert(b"key");
        let serialized1 = writer1.serialize();

        let reader = factory2.deserialize(&serialized1);

        // Assert
        assert!(reader.is_ok());
    }

    #[test]
    fn should_preserve_state_through_factory_cycle() {
        // Arrange
        let factory = BloomFactory::new();
        let mut writer = factory.create_writer(100, 0.01);
        for i in 0..50 {
            writer.insert(format!("key{}", i).as_bytes());
        }

        // Act
        let serialized = writer.serialize();
        let reader = factory.deserialize(&serialized).unwrap();

        // Assert
        assert_eq!(reader.key_count(), 50);
        for i in 0..50 {
            assert_eq!(
                reader.contains(format!("key{}", i).as_bytes()),
                BloomTestResult::MightBePresent
            );
        }
    }

    #[test]
    fn should_handle_factory_polymorphism_with_deserialization() {
        // Arrange
        let factory: Box<dyn BloomFilterFactory> = Box::new(BloomFactory::new());
        let mut writer = factory.create_writer(100, 0.01);
        writer.insert(b"polymorphic");
        let serialized = writer.serialize();

        // Act
        let result = factory.deserialize(&serialized);

        // Assert
        assert!(result.is_ok());
        let reader = result.unwrap();
        assert_eq!(reader.contains(b"polymorphic"), BloomTestResult::MightBePresent);
    }

    #[test]
    fn should_reject_invalid_deserialization_data() {
        // Arrange
        let factory = BloomFactory::new();
        let invalid_data = vec![1, 2, 3];

        // Act
        let result = factory.deserialize(&invalid_data);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_handle_edge_case_capacity() {
        // Arrange
        let factory = BloomFactory::new();

        // Act
        let writer_zero = factory.create_writer(0, 0.01);
        let writer_one = factory.create_writer(1, 0.01);

        // Assert
        assert_eq!(writer_zero.size_bytes(), 0);
        assert!(writer_one.size_bytes() > 0);
    }

    #[test]
    fn should_handle_extreme_fpr_values() {
        // Arrange
        let factory = BloomFactory::new();

        // Act
        let writer_extreme_low = factory.create_writer(100, 0.0001);
        let writer_extreme_high = factory.create_writer(100, 0.99);

        // Assert
        assert!(writer_extreme_low.size_bytes() > writer_extreme_high.size_bytes());
    }
}
