//! In-memory mock filesystem for tests and benches (FastFs-like behavior)
//!
//! Provides a simple `MockFs` implementing `EngineFs` using in-memory
//! maps. `commit()` and `finish()` are no-ops for `Durability::Unsafe` and
//! `Durability::Durable` in this minimal implementation (can be extended
//! to simulate latency or failures later).

use super::*;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type WalKey = (CfId, WalId);
type SstKey = (CfId, SstId);

#[derive(Clone, Default)]
pub struct MockFs {
    wal_store: Arc<Mutex<HashMap<WalKey, Arc<Mutex<Vec<Bytes>>>>>>,
    sst_store: Arc<Mutex<HashMap<SstKey, Vec<Bytes>>>>,
    manifest_store: Arc<Mutex<HashMap<CfId, Bytes>>>,
}

impl MockFs {
    pub fn new() -> Self {
        Self::default()
    }
}

struct MockWalWriter {
    bucket: Arc<Mutex<Vec<Bytes>>>,
}

impl WalWriter for MockWalWriter {
    fn append(&mut self, record: Bytes) -> FsResult<()> {
        self.bucket.lock().unwrap().push(record);
        Ok(())
    }

    fn commit(&mut self, _dur: Durability) -> FsResult<()> {
        // For mock, commit is a no-op
        Ok(())
    }

    fn close(self: Box<Self>) -> FsResult<()> {
        Ok(())
    }
}

struct MockWalReader {
    bucket: Arc<Mutex<Vec<Bytes>>>,
}

impl WalReader for MockWalReader {
    fn read_all(&mut self) -> FsResult<Vec<Bytes>> {
        Ok(self.bucket.lock().unwrap().clone())
    }
}

struct MockSstWriter {
    temp: Vec<Bytes>,
    sst_store: Arc<Mutex<HashMap<SstKey, Vec<Bytes>>>>,
    key: SstKey,
}

impl SstWriter for MockSstWriter {
    fn write_block(&mut self, block: Bytes) -> FsResult<()> {
        self.temp.push(block);
        Ok(())
    }

    fn finish(self: Box<Self>, _dur: Durability) -> FsResult<()> {
        let mut map = self.sst_store.lock().unwrap();
        map.insert(self.key, self.temp);
        Ok(())
    }
}

struct MockSstReader {
    data: Vec<Bytes>,
}

impl SstReader for MockSstReader {
    fn read_block(&mut self, offset: u64, _len: u64) -> FsResult<Bytes> {
        // naive: offset is index, len is ignored; return clone
        let idx = offset as usize;
        if idx >= self.data.len() {
            return Err(FsError::NotFound("block".into()));
        }
        Ok(self.data[idx].clone())
    }

    fn len(&self) -> FsResult<u64> {
        Ok(self.data.len() as u64)
    }
}

impl EngineFs for MockFs {
    fn wal_open(&self, cf: CfId, wal: WalId) -> FsResult<Box<dyn WalWriter>> {
        let key = (cf, wal);
        let mut store = self.wal_store.lock().unwrap();
        let entry = store
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(Vec::new())));
        Ok(Box::new(MockWalWriter {
            bucket: Arc::clone(entry),
        }))
    }

    fn wal_read(&self, cf: CfId, wal: WalId) -> FsResult<Box<dyn WalReader>> {
        let key = (cf, wal);
        let store = self.wal_store.lock().unwrap();
        if let Some(v) = store.get(&key) {
            Ok(Box::new(MockWalReader {
                bucket: Arc::clone(v),
            }))
        } else {
            Err(FsError::NotFound("wal".into()))
        }
    }

    fn wal_list(&self, cf: CfId) -> FsResult<Vec<WalId>> {
        let store = self.wal_store.lock().unwrap();
        let mut out = Vec::new();
        for ((c, w), _) in store.iter() {
            if *c == cf {
                out.push(*w);
            }
        }
        Ok(out)
    }

    fn wal_delete(&self, cf: CfId, wal: WalId) -> FsResult<()> {
        let mut store = self.wal_store.lock().unwrap();
        store.remove(&(cf, wal));
        Ok(())
    }

    fn sst_create(&self, cf: CfId, sst: SstId) -> FsResult<Box<dyn SstWriter>> {
        let key = (cf, sst);
        Ok(Box::new(MockSstWriter {
            temp: Vec::new(),
            sst_store: Arc::clone(&self.sst_store),
            key,
        }))
    }

    fn sst_open(&self, cf: CfId, sst: SstId) -> FsResult<Box<dyn SstReader>> {
        let key = (cf, sst);
        let store = self.sst_store.lock().unwrap();
        if let Some(v) = store.get(&key) {
            Ok(Box::new(MockSstReader { data: v.clone() }))
        } else {
            Err(FsError::NotFound("sst".into()))
        }
    }

    fn sst_list(&self, cf: CfId) -> FsResult<Vec<SstId>> {
        let store = self.sst_store.lock().unwrap();
        let mut out = Vec::new();
        for ((c, s), _) in store.iter() {
            if *c == cf {
                out.push(*s);
            }
        }
        Ok(out)
    }

    fn sst_delete(&self, cf: CfId, sst: SstId) -> FsResult<()> {
        let mut store = self.sst_store.lock().unwrap();
        store.remove(&(cf, sst));
        Ok(())
    }

    fn manifest_read(&self, cf: CfId) -> FsResult<Bytes> {
        let store = self.manifest_store.lock().unwrap();
        if let Some(b) = store.get(&cf) {
            Ok(b.clone())
        } else {
            Err(FsError::NotFound("manifest".into()))
        }
    }

    fn manifest_replace_atomic(
        &self,
        cf: CfId,
        new_contents: Bytes,
        _dur: Durability,
    ) -> FsResult<()> {
        let mut store = self.manifest_store.lock().unwrap();
        store.insert(cf, new_contents);
        Ok(())
    }

    fn sync_dir_if_supported(&self, _cf: CfId) -> FsResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_write_and_read_roundtrip() {
        let fs = MockFs::new();
        let cf = CfId(1);
        let wal = WalId(1);

        // open writer and append
        let mut w = fs.wal_open(cf, wal).unwrap();
        w.append(Bytes::from("rec1")).unwrap();
        w.append(Bytes::from("rec2")).unwrap();
        w.commit(Durability::Unsafe).unwrap();

        // read back
        let mut r = fs.wal_read(cf, wal).unwrap();
        let v = r.read_all().unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0], Bytes::from("rec1"));
    }

    #[test]
    fn sst_write_and_read_roundtrip() {
        let fs = MockFs::new();
        let cf = CfId(2);
        let sst = SstId(1);

        let mut w = fs.sst_create(cf, sst).unwrap();
        w.write_block(Bytes::from("blk1")).unwrap();
        w.write_block(Bytes::from("blk2")).unwrap();
        w.finish(Durability::Unsafe).unwrap();

        let mut r = fs.sst_open(cf, sst).unwrap();
        let b = r.read_block(0, 0).unwrap();
        assert_eq!(b, Bytes::from("blk1"));
    }
}
