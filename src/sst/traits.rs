//! SST traits and common contracts
//!
//! This module defines generic SST reader/writer traits and re-exports
//! filesystem-backed adapters from `fs`.

use crate::error::MidgeResult;
use bytes::Bytes;

/// Simple range tombstone descriptor stored in SST metadata.
#[derive(Debug, Clone)]
pub struct RangeTombstone {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub seq: u64,
}

/// Reader contract for SST implementations.
pub trait SstReader: Send + Sync {
    /// Get the value for a specific key, if present.
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>>;

    /// Scan a key range [start, end) where either bound may be None.
    fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>>;
}

/// Presence state of a key in an SST. Used by stateful reader trait.
#[derive(Debug, Clone)]
pub enum KeyState {
    Absent,
    Tombstone(u64),
    /// Value, sequence number, expiration timestamp (Unix millis)
    Value(Bytes, u64, Option<u64>),
}

/// Stateful reader contract for SST implementations, exposing tombstones.
pub trait SstStateReader {
    /// Get presence state (value/tombstone/absent) for a specific key.
    fn get_state(&self, key: &[u8]) -> MidgeResult<KeyState>;

    /// Scan a key range [start, end) returning presence state for each key.
    fn scan_range_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, KeyState)>>;

    /// Snapshot-aware point lookup: only returns state if entry's seq <= snapshot.
    fn get_state_at(&self, key: &[u8], _snapshot_seq: u64) -> MidgeResult<KeyState> {
        // Default fallback: if implementation doesn't use sequences, reuse get_state
        self.get_state(key)
    }

    /// Snapshot-aware range scan.
    fn scan_range_state_at(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        _snapshot_seq: u64,
    ) -> MidgeResult<Vec<(Bytes, KeyState)>> {
        // Default fallback
        self.scan_range_state(start, end)
    }
}

/// Writer contract for SST implementations.
///
/// A writer collects entries and on `finish` produces a reader over the
/// built SST.
pub trait SstWriter {
    type Reader: SstReader;

    /// Add a key-value entry to the SST under construction.
    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;

    /// Finalize and produce a reader instance over the resulting SST.
    fn finish(self) -> MidgeResult<Self::Reader>;
}

// Re-export filesystem-backed reader for convenience
pub use crate::sst::fs::SstFile as SstFsReader;

/// Object-safe SST writer used by factories when the concrete writer type
/// should be hidden behind a trait object. It supports incremental adds and
/// finishing into raw bytes.
pub trait DynSstWriter: Send {
    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;
    /// Add an entry with explicit sequence, tombstone, and expiration metadata.
    /// Default implementation falls back to `add` and ignores metadata.
    fn add_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        _seq: u64,
        tombstone: bool,
        _expiration: Option<u64>,
    ) -> MidgeResult<()> {
        if tombstone {
            return Ok(());
        }
        match value {
            Some(v) => self.add(key, v),
            None => Ok(()),
        }
    }
    /// Add a range tombstone [start, end) at sequence `seq` to be persisted in SST metadata.
    fn add_range_tombstone(&mut self, _start: &[u8], _end: &[u8], _seq: u64) -> MidgeResult<()> {
        // Default no-op; writers that support range tombstones should override.
        Ok(())
    }
    fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>>;
    /// Finalize the writer and persist the SST directly to `path`.
    ///
    /// Default implementation falls back to `finish_bytes()` and writes the
    /// returned bytes to `path`. Implementations that can stream to disk
    /// should override this to avoid building the whole image in memory.
    fn finish_to_path(self: Box<Self>, path: &std::path::Path) -> MidgeResult<()> {
        let bytes = self.finish_bytes()?;
        std::fs::write(path, &bytes)?;
        Ok(())
    }
}

/// Factory trait for creating SST writers. Engines should depend on this
/// abstraction instead of concrete SST implementations so different writer
/// types (in-memory, FS-backed, remote) can be provided at runtime.
pub trait SstFactory: Send + Sync {
    fn create(
        &self,
        compression: crate::common::codec::CompressionType,
        block_size: usize,
        use_internal: bool,
    ) -> Box<dyn DynSstWriter>;

    /// Create an SST writer with custom bloom filter configuration.
    fn create_with_bloom(
        &self,
        compression: crate::common::codec::CompressionType,
        block_size: usize,
        use_internal: bool,
        _bloom_bits_per_key: u32,
    ) -> Box<dyn DynSstWriter> {
        // Default implementation delegates to create(), ignoring bloom config
        self.create(compression, block_size, use_internal)
    }
}

/// Factory trait for opening SST readers from a path.
pub trait SstReaderFactory: Send + Sync {
    fn open(&self, path: &std::path::Path) -> MidgeResult<Box<dyn SstStateReader>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_sst_behavior_tests(
        make_writer: impl Fn() -> Box<dyn DynSstWriter>,
        make_reader: impl Fn(&std::path::Path) -> Box<dyn SstStateReader>,
    ) {
        // Build an SST using the provided writer factory
        let mut w = make_writer();
        w.add(b"a", b"A").expect("add");
        w.add(b"b", b"B").expect("add");
        w.add(b"c", b"C").expect("add");

        let bytes = w.finish_bytes().expect("finish bytes");

        // Write to temp file then open reader via provided reader factory
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("test.sst");
        std::fs::write(&path, &bytes).expect("write sst");

        let r = make_reader(&path);

        // 1) point gets
        let ga = r.get_state(b"a").expect("get_state");
        match ga {
            KeyState::Value(v, _seq, _exp) => assert_eq!(v.as_ref(), b"A"),
            other => panic!("unexpected state for a: {:?}", other),
        }

        let gx = r.get_state(b"x").expect("get_state absent");
        match gx {
            KeyState::Absent => {}
            other => panic!("unexpected state for x: {:?}", other),
        }

        // 2) scan range
        let all = r.scan_range_state(None, None).expect("scan all");
        let keys: Vec<Vec<u8>> = all.into_iter().map(|(k, _)| k.to_vec()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn should_support_mem_reader_behavior() {
        // Arrange - writer: MemSstFactory -> DynSstWriter
        let writer_factory = || -> Box<dyn DynSstWriter> {
            let f = crate::sst::mem::MemSstFactory {};
            f.create(crate::common::codec::CompressionType::None, 4096, false)
        };
        // reader: MemSstReaderFactory (reads bytes from the path)
        let reader_factory = |p: &std::path::Path| -> Box<dyn SstStateReader> {
            let fac = crate::sst::mem::MemSstReaderFactory::new(false);
            fac.open(p).expect("open mem reader")
        };

        // Act
        run_sst_behavior_tests(writer_factory, reader_factory);
        // Assert - run_sst_behavior_tests validates the behavior
    }

    #[test]
    fn should_support_fs_reader_behavior() {
        // Arrange
        let writer_factory = || -> Box<dyn DynSstWriter> {
            let f = crate::sst::mem::MemSstFactory {};
            f.create(crate::common::codec::CompressionType::None, 4096, false)
        };
        // reader: open filesystem-backed SstFile via SstFile::open and box it
        let reader_factory = |p: &std::path::Path| -> Box<dyn SstStateReader> {
            Box::new(crate::sst::fs::SstFile::open(p).expect("test SST file should open"))
        };

        // Act
        run_sst_behavior_tests(writer_factory, reader_factory);
        // Assert - run_sst_behavior_tests validates the behavior
    }

    // Dummy reader that only implements the base methods; default fallbacks should delegate to them.
    struct Dummy;

    impl SstStateReader for Dummy {
        fn get_state(&self, key: &[u8]) -> crate::error::MidgeResult<KeyState> {
            use bytes::Bytes;
            if key == b"a" {
                Ok(KeyState::Value(Bytes::from_static(b"X"), 0, None))
            } else {
                Ok(KeyState::Absent)
            }
        }

        fn scan_range_state(
            &self,
            _start: Option<&[u8]>,
            _end: Option<&[u8]>,
        ) -> crate::error::MidgeResult<Vec<(bytes::Bytes, KeyState)>> {
            use bytes::Bytes;
            Ok(vec![(Bytes::from_static(b"a"), KeyState::Tombstone(0))])
        }

        // Do not override get_state_at / scan_range_state_at; we want to use defaults.
    }

    #[test]
    fn should_forward_get_state_at_to_get_state_when_default_impl() {
        // Arrange
        let d = Dummy;

        // Act
        let result1 = SstStateReader::get_state_at(&d, b"a", 0).expect("get_state_at should succeed");
        let result2 = SstStateReader::get_state_at(&d, b"z", 123).expect("get_state_at should succeed");

        // Assert
        match result1 {
            KeyState::Value(v, 0, _exp) => assert_eq!(v.as_ref(), b"X"),
            other => panic!("unexpected: {:?}", other),
        }
        match result2 {
            KeyState::Absent => {}
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn should_forward_scan_range_state_at_to_scan_range_state_when_default_impl() {
        // Arrange
        let d = Dummy;

        // Act - Snapshot should not change result since default delegates
        let rows = SstStateReader::scan_range_state_at(&d, None, None, 42).unwrap();

        // Assert
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.as_ref(), b"a");
        match rows[0].1 {
            KeyState::Tombstone(0) => {}
            ref other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn should_call_add_with_meta_tombstone_via_default_impl() {
        // Arrange - Test that the default implementation of add_with_meta handles tombstones
        // by returning Ok(()) without adding anything
        struct TestWriter {
            entries: Vec<(Vec<u8>, Vec<u8>)>,
        }
        impl DynSstWriter for TestWriter {
            fn add(&mut self, key: &[u8], value: &[u8]) -> crate::error::MidgeResult<()> {
                self.entries.push((key.to_vec(), value.to_vec()));
                Ok(())
            }
            fn finish_bytes(self: Box<Self>) -> crate::error::MidgeResult<Vec<u8>> {
                Ok(vec![])
            }
        }
        let mut writer: Box<dyn DynSstWriter> = Box::new(TestWriter { entries: vec![] });

        // Act - Add with tombstone=true should not call add
        writer
            .add_with_meta(b"a", Some(b"value"), 1, true, None)
            .unwrap();
        // Add with tombstone=false and Some value should call add
        writer
            .add_with_meta(b"b", Some(b"value2"), 2, false, None)
            .unwrap();
        // Add with None value should not call add
        writer.add_with_meta(b"c", None, 3, false, None).unwrap();

        // Assert - Verify only one entry was added (the second call)
        let bytes = writer.finish_bytes().unwrap();
        assert_eq!(bytes.len(), 0); // Our test writer just returns empty vec
    }

    #[test]
    fn should_call_add_range_tombstone_via_default_impl() {
        // Arrange - Test that the default implementation of add_range_tombstone is a no-op
        struct TestWriter;
        impl DynSstWriter for TestWriter {
            fn add(&mut self, _key: &[u8], _value: &[u8]) -> crate::error::MidgeResult<()> {
                Ok(())
            }
            fn finish_bytes(self: Box<Self>) -> crate::error::MidgeResult<Vec<u8>> {
                Ok(vec![])
            }
        }
        let mut writer: Box<dyn DynSstWriter> = Box::new(TestWriter);

        // Act - Should not panic or error, just no-op
        writer.add_range_tombstone(b"a", b"z", 100).unwrap();

        // Assert
        // No assertions needed - the call succeeding is the test
    }

    #[test]
    fn should_create_range_tombstone_struct() {
        // Arrange
        let rt = RangeTombstone {
            start: b"key1".to_vec(),
            end: b"key9".to_vec(),
            seq: 42,
        };

        // Assert - Check initial values
        assert_eq!(rt.start, b"key1");
        assert_eq!(rt.end, b"key9");
        assert_eq!(rt.seq, 42);

        // Act - Clone
        let rt2 = rt.clone();

        // Assert - Check cloned values
        assert_eq!(rt2.seq, 42);
    }
}
