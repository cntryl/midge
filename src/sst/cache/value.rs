//! Cached block value with metadata

use bytes::Bytes;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// A cached block value with metadata
#[derive(Clone, Debug)]
pub struct CacheValue {
    /// The actual block data
    pub data: Arc<Bytes>,
    /// Insertion timestamp (nanoseconds since epoch)
    pub inserted_at: u64,
    /// Access count for frequency tracking
    pub access_count: Arc<AtomicU64>,
}

impl CacheValue {
    /// Create a new cached value
    pub fn new(data: Bytes) -> Self {
        Self {
            data: Arc::new(data),
            inserted_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            access_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get the size of the cached data in bytes
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.data.len()
    }

    /// Increment access count and return the new value
    #[inline(always)]
    #[must_use]
    pub fn increment_access(&self) -> u64 {
        self.access_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }

    /// Get current access count
    #[must_use]
    pub fn access_count(&self) -> u64 {
        self.access_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_value_with_data() {
        // Arrange
        let data = Bytes::from(&b"test data"[..]);

        // Act
        let value = CacheValue::new(data.clone());

        // Assert
        assert_eq!(value.data.as_ref(), &data);
    }

    #[test]
    fn should_report_correct_size_bytes() {
        // Arrange
        let data = Bytes::from(&b"hello"[..]);
        let value = CacheValue::new(data);

        // Act
        let size = value.size_bytes();

        // Assert
        assert_eq!(size, 5);
    }

    #[test]
    fn should_handle_empty_data() {
        // Arrange
        let data = Bytes::from(&b""[..]);
        let value = CacheValue::new(data);

        // Act
        let size = value.size_bytes();

        // Assert
        assert_eq!(size, 0);
    }

    #[test]
    fn should_handle_large_data() {
        // Arrange
        let large_data = vec![42u8; 10000];
        let data = Bytes::from(large_data);
        let value = CacheValue::new(data);

        // Act
        let size = value.size_bytes();

        // Assert
        assert_eq!(size, 10000);
    }

    #[test]
    fn should_initialize_access_count_to_zero() {
        // Arrange
        let data = Bytes::from(&b"test"[..]);
        let value = CacheValue::new(data);

        // Act
        let count = value.access_count();

        // Assert
        assert_eq!(count, 0);
    }

    #[test]
    fn should_increment_access_count() {
        // Arrange
        let data = Bytes::from(&b"test"[..]);
        let value = CacheValue::new(data);

        // Act
        let count1 = value.increment_access();

        // Assert
        assert_eq!(count1, 1);
        assert_eq!(value.access_count(), 1);
    }

    #[test]
    fn should_monotonically_increase_access_count() {
        // Arrange
        let data = Bytes::from(&b"test"[..]);
        let value = CacheValue::new(data);

        // Act
        let mut counts = Vec::new();
        for _ in 0..5 {
            let count = value.increment_access();
            counts.push((count, value.access_count()));
        }

        // Assert
        for (i, (returned, stored)) in counts.into_iter().enumerate() {
            let expected = (i as u64) + 1;
            assert_eq!(returned, expected);
            assert_eq!(stored, expected);
        }
    }

    #[test]
    fn should_handle_multiple_increments_concurrently() {
        // Arrange
        let data = Bytes::from(&b"test"[..]);
        let value = std::sync::Arc::new(CacheValue::new(data));

        // Act - simulate concurrent increments
        let mut handles = vec![];
        for _ in 0..10 {
            let v = std::sync::Arc::clone(&value);
            let handle = std::thread::spawn(move || {
                for _ in 0..10 {
                    let _ = v.increment_access();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert (10 threads * 10 increments = 100)
        assert_eq!(value.access_count(), 100);
    }

    #[test]
    fn should_record_insertion_timestamp() {
        // Arrange
        let before_time = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap_or(u64::MAX);

        // Act
        let data = Bytes::from(&b"test"[..]);
        let value = CacheValue::new(data);

        let after_time = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap_or(u64::MAX);

        // Assert
        assert!(value.inserted_at >= before_time);
        assert!(value.inserted_at <= after_time);
    }

    #[test]
    fn should_be_cloneable() {
        // Arrange
        let data = Bytes::from(&b"test"[..]);
        let value1 = CacheValue::new(data);
        let _ = value1.increment_access();

        // Act
        let value2 = value1.clone();
        let _ = value2.increment_access();

        // Assert (both share the same Arc, so access count is shared)
        assert_eq!(value1.access_count(), 2);
        assert_eq!(value2.access_count(), 2);
    }

    #[test]
    fn should_preserve_data_through_clone() {
        // Arrange
        let data = Bytes::from(&b"important data"[..]);
        let value1 = CacheValue::new(data.clone());

        // Act
        let value2 = value1.clone();

        // Assert
        assert_eq!(value1.data.as_ref(), &data);
        assert_eq!(value2.data.as_ref(), &data);
        assert_eq!(value1.size_bytes(), value2.size_bytes());
    }

    #[test]
    fn should_share_access_count_across_clones() {
        // Arrange
        let data = Bytes::from(&b"test"[..]);
        let value1 = CacheValue::new(data);

        // Act
        let value2 = value1.clone();
        let _ = value1.increment_access();

        // Assert (clones share the same Arc for access_count)
        assert_eq!(value2.access_count(), 1);
    }
}
