//! Real filesystem implementation (stub)
//!
//! This is a minimal stub for `RealFs` that will later be implemented to
//! perform proper durable writes on the platform. For now it returns
//! `FsError::Unsupported` for operations that would require actual IO.

use super::*;
use bytes::Bytes;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// Real filesystem implementation (minimal, focusing on WAL + manifest).
pub struct RealFs {
    root: PathBuf,
}

impl RealFs {
    /// Construct a new `RealFs` rooted at `path`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn cf_dir(&self, cf: CfId) -> PathBuf {
        self.root.join(format!("cf-{}", cf.0))
    }

    fn wal_path(&self, cf: CfId, wal: WalId) -> PathBuf {
        self.cf_dir(cf).join(format!("{:014}.wal", wal.0))
    }

    fn manifest_path(&self, cf: CfId) -> PathBuf {
        self.cf_dir(cf).join("manifest")
    }

    fn sst_path(&self, cf: CfId, sst: SstId) -> PathBuf {
        self.cf_dir(cf).join(format!("{:014}.sst", sst.0))
    }

    fn sst_temp_path(&self, cf: CfId, sst: SstId) -> PathBuf {
        self.cf_dir(cf).join(format!("{:014}.tmp", sst.0))
    }

    fn ensure_cf_dir(&self, cf: CfId) -> FsResult<()> {
        let dir = self.cf_dir(cf);
        fs::create_dir_all(&dir).map_err(|e| FsError::Io(e.to_string()))
    }
}

struct RealWalWriter {
    file: File,
}

impl WalWriter for RealWalWriter {
    fn append(&mut self, record: Bytes) -> FsResult<()> {
        // length prefix (u32 LE) + payload
        let len = record.len() as u32;
        self.file
            .write_all(&len.to_le_bytes())
            .map_err(|e| FsError::Io(e.to_string()))?;
        self.file
            .write_all(&record)
            .map_err(|e| FsError::Io(e.to_string()))?;
        Ok(())
    }

    fn commit(&mut self, dur: Durability) -> FsResult<()> {
        if let Durability::Durable = dur {
            self.file.sync_all().map_err(|e| FsError::Io(e.to_string()))?;
        }
        Ok(())
    }

    fn close(self: Box<Self>) -> FsResult<()> {
        // dropping will close the file
        Ok(())
    }
}

struct RealWalReader {
    file: File,
}

impl WalReader for RealWalReader {
    fn read_all(&mut self) -> FsResult<Vec<Bytes>> {
        let mut out = Vec::new();
        let mut buf = Vec::new();
        self.file.seek(SeekFrom::Start(0)).map_err(|e| FsError::Io(e.to_string()))?;
        self.file.read_to_end(&mut buf).map_err(|e| FsError::Io(e.to_string()))?;

        let mut cursor: usize = 0;
        while cursor + 4 <= buf.len() {
            let size = u32::from_le_bytes(buf[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if cursor + size > buf.len() {
                return Err(FsError::Corruption("truncated wal record".into()));
            }
            out.push(Bytes::from(buf[cursor..cursor + size].to_vec()));
            cursor += size;
        }

        Ok(out)
    }
}

struct RealSstWriter {
    temp_file: File,
    final_path: PathBuf,
    dir: PathBuf,
}

impl SstWriter for RealSstWriter {
    fn write_block(&mut self, block: Bytes) -> FsResult<()> {
        // length prefix (u32 LE) + payload
        let len = block.len() as u32;
        self.temp_file
            .write_all(&len.to_le_bytes())
            .map_err(|e| FsError::Io(e.to_string()))?;
        self.temp_file
            .write_all(&block)
            .map_err(|e| FsError::Io(e.to_string()))?;
        Ok(())
    }

    fn finish(self: Box<Self>, dur: Durability) -> FsResult<()> {
        if let Durability::Durable = dur {
            self.temp_file.sync_all().map_err(|e| FsError::Io(e.to_string()))?;
        }
        drop(self.temp_file); // close temp file
        fs::rename(&self.final_path.with_extension("tmp"), &self.final_path)
            .map_err(|e| FsError::Io(e.to_string()))?;
        // sync directory
        if let Durability::Durable = dur {
            match OpenOptions::new().read(true).open(&self.dir) {
                Ok(d) => d.sync_all().map_err(|e| FsError::Io(e.to_string())),
                Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => Ok(()),
                Err(e) => Err(FsError::Io(e.to_string())),
            }?;
        }
        Ok(())
    }
}

struct RealSstReader {
    file: File,
}

impl SstReader for RealSstReader {
    fn read_block(&mut self, offset: u64, len: u64) -> FsResult<Bytes> {
        self.file.seek(SeekFrom::Start(offset)).map_err(|e| FsError::Io(e.to_string()))?;
        let mut buf = vec![0; len as usize];
        self.file.read_exact(&mut buf).map_err(|e| FsError::Io(e.to_string()))?;
        Ok(Bytes::from(buf))
    }

    fn len(&self) -> FsResult<u64> {
        let meta = self.file.metadata().map_err(|e| FsError::Io(e.to_string()))?;
        Ok(meta.len())
    }
}

impl EngineFs for RealFs {
    fn wal_open(&self, cf: CfId, wal: WalId) -> FsResult<Box<dyn WalWriter>> {
        self.ensure_cf_dir(cf)?;
        let p = self.wal_path(cf, wal);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&p)
            .map_err(|e| FsError::Io(e.to_string()))?;
        Ok(Box::new(RealWalWriter { file }))
    }

    fn wal_read(&self, cf: CfId, wal: WalId) -> FsResult<Box<dyn WalReader>> {
        let p = self.wal_path(cf, wal);
        let file = File::open(&p).map_err(|e| FsError::Io(e.to_string()))?;
        Ok(Box::new(RealWalReader { file }))
    }

    fn wal_list(&self, cf: CfId) -> FsResult<Vec<WalId>> {
        let dir = self.cf_dir(cf);
        let mut out = Vec::new();
        match fs::read_dir(&dir) {
            Ok(entries) => {
                for entry in entries {
                    let e = entry.map_err(|e| FsError::Io(e.to_string()))?;
                    if let Some(name) = e.path().file_name().and_then(|s| s.to_str()) {
                        if name.ends_with(".wal") {
                            if let Ok(id) = name.trim_end_matches(".wal").parse::<u64>() {
                                out.push(WalId(id));
                            }
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Directory doesn't exist, no WALs
            }
            Err(e) => return Err(FsError::Io(e.to_string())),
        }
        Ok(out)
    }

    fn wal_delete(&self, cf: CfId, wal: WalId) -> FsResult<()> {
        let p = self.wal_path(cf, wal);
        fs::remove_file(&p).map_err(|e| FsError::Io(e.to_string()))
    }

    fn sst_create(&self, cf: CfId, sst: SstId) -> FsResult<Box<dyn SstWriter>> {
        self.ensure_cf_dir(cf)?;
        let temp_path = self.sst_temp_path(cf, sst);
        let final_path = self.sst_path(cf, sst);
        let temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|e| FsError::Io(e.to_string()))?;
        Ok(Box::new(RealSstWriter {
            temp_file,
            final_path,
            dir: self.cf_dir(cf),
        }))
    }

    fn sst_open(&self, cf: CfId, sst: SstId) -> FsResult<Box<dyn SstReader>> {
        let path = self.sst_path(cf, sst);
        let file = File::open(&path).map_err(|e| FsError::Io(e.to_string()))?;
        Ok(Box::new(RealSstReader { file }))
    }

    fn sst_list(&self, cf: CfId) -> FsResult<Vec<SstId>> {
        let dir = self.cf_dir(cf);
        let mut out = Vec::new();
        match fs::read_dir(&dir) {
            Ok(entries) => {
                for entry in entries {
                    let e = entry.map_err(|e| FsError::Io(e.to_string()))?;
                    if let Some(name) = e.path().file_name().and_then(|s| s.to_str()) {
                        if name.ends_with(".sst") {
                            if let Ok(id) = name.trim_end_matches(".sst").parse::<u64>() {
                                out.push(SstId(id));
                            }
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Directory doesn't exist, no SSTs
            }
            Err(e) => return Err(FsError::Io(e.to_string())),
        }
        Ok(out)
    }

    fn sst_delete(&self, cf: CfId, sst: SstId) -> FsResult<()> {
        let path = self.sst_path(cf, sst);
        fs::remove_file(&path).map_err(|e| FsError::Io(e.to_string()))
    }

    fn manifest_read(&self, cf: CfId) -> FsResult<Bytes> {
        let p = self.manifest_path(cf);
        let mut f = File::open(&p).map_err(|e| FsError::Io(e.to_string()))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).map_err(|e| FsError::Io(e.to_string()))?;
        Ok(Bytes::from(buf))
    }

    fn manifest_replace_atomic(&self, cf: CfId, new_contents: Bytes, _dur: Durability) -> FsResult<()> {
        self.ensure_cf_dir(cf)?;
        let tmp = self.cf_dir(cf).join("manifest.tmp");
        let finalp = self.manifest_path(cf);

        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| FsError::Io(e.to_string()))?;
            f.write_all(&new_contents).map_err(|e| FsError::Io(e.to_string()))?;
            f.sync_all().map_err(|e| FsError::Io(e.to_string()))?;
        }
        fs::rename(&tmp, &finalp).map_err(|e| FsError::Io(e.to_string()))?;
        // sync directory
        self.sync_dir_if_supported(cf)
    }

    fn sync_dir_if_supported(&self, cf: CfId) -> FsResult<()> {
        let dir = self.cf_dir(cf);
        match OpenOptions::new().read(true).open(&dir) {
            Ok(d) => d.sync_all().map_err(|e| FsError::Io(e.to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => Ok(()),
            Err(e) => Err(FsError::Io(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use bytes::Bytes;

    #[test]
    fn realfs_new_compiles() {
        let td = TempDir::new().unwrap();
        let _ = RealFs::new(td.path());
    }

    #[test]
    fn wal_roundtrip() {
        let td = TempDir::new().unwrap();
        let fs = RealFs::new(td.path());
        let cf = CfId(1);
        let wal = WalId(42);

        let mut w = fs.wal_open(cf, wal).expect("open");
        w.append(Bytes::from("r1")).unwrap();
        w.append(Bytes::from("r2")).unwrap();
        w.commit(Durability::Durable).unwrap();

        let mut r = fs.wal_read(cf, wal).expect("read");
        let v = r.read_all().unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0], Bytes::from("r1"));
        assert_eq!(v[1], Bytes::from("r2"));
    }

    #[test]
    fn manifest_replace_and_read() {
        let td = TempDir::new().unwrap();
        let fs = RealFs::new(td.path());
        let cf = CfId(2);

        fs.manifest_replace_atomic(cf, Bytes::from("hello"), Durability::Durable).unwrap();
        let got = fs.manifest_read(cf).unwrap();
        assert_eq!(got, Bytes::from("hello"));
    }

    #[test]
    fn should_create_sst_writer_when_called() {
        // Arrange
        let td = TempDir::new().unwrap();
        let fs = RealFs::new(td.path());
        let cf = CfId(1);
        let sst = SstId(100);

        // Act
        let writer = fs.sst_create(cf, sst);

        // Assert
        assert!(writer.is_ok());
    }

    #[test]
    fn should_write_block_to_sst_when_appending() {
        // Arrange
        let td = TempDir::new().unwrap();
        let fs = RealFs::new(td.path());
        let cf = CfId(1);
        let sst = SstId(100);
        let mut writer = fs.sst_create(cf, sst).unwrap();

        // Act
        let result = writer.write_block(Bytes::from("test block"));

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_finish_sst_when_called() {
        // Arrange
        let td = TempDir::new().unwrap();
        let fs = RealFs::new(td.path());
        let cf = CfId(1);
        let sst = SstId(100);
        let mut writer = fs.sst_create(cf, sst).unwrap();
        writer.write_block(Bytes::from("data")).unwrap();

        // Act
        let result = writer.finish(Durability::Unsafe);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_open_sst_reader_when_file_exists() {
        // Arrange
        let td = TempDir::new().unwrap();
        let fs = RealFs::new(td.path());
        let cf = CfId(1);
        let sst = SstId(100);
        let mut writer = fs.sst_create(cf, sst).unwrap();
        writer.write_block(Bytes::from("data")).unwrap();
        writer.finish(Durability::Unsafe).unwrap();

        // Act
        let reader = fs.sst_open(cf, sst);

        // Assert
        assert!(reader.is_ok());
    }

    #[test]
    fn should_read_block_from_sst_when_offset_provided() {
        // Arrange
        let td = TempDir::new().unwrap();
        let fs = RealFs::new(td.path());
        let cf = CfId(1);
        let sst = SstId(100);
        let mut writer = fs.sst_create(cf, sst).unwrap();
        writer.write_block(Bytes::from("block1")).unwrap();
        writer.finish(Durability::Unsafe).unwrap();
        let mut reader = fs.sst_open(cf, sst).unwrap();

        // Act
        let block = reader.read_block(0, 10);

        // Assert
        assert!(block.is_ok());
        assert_eq!(block.unwrap(), Bytes::from(&[6,0,0,0, 98,108,111,99,107,49][..]));
    }

    #[test]
    fn should_return_correct_length_for_sst() {
        // Arrange
        let td = TempDir::new().unwrap();
        let fs = RealFs::new(td.path());
        let cf = CfId(1);
        let sst = SstId(100);
        let mut writer = fs.sst_create(cf, sst).unwrap();
        writer.write_block(Bytes::from("block1")).unwrap();
        writer.write_block(Bytes::from("block2")).unwrap();
        writer.finish(Durability::Unsafe).unwrap();
        let reader = fs.sst_open(cf, sst).unwrap();

        // Act
        let len = reader.len();

        // Assert
        assert!(len.is_ok());
        assert_eq!(len.unwrap(), 20); // 4+6 + 4+6
    }

    #[test]
    fn should_list_empty_ssts_when_none_exist() {
        // Arrange
        let td = TempDir::new().unwrap();
        let fs = RealFs::new(td.path());
        let cf = CfId(2);

        // Act
        let ssts = fs.sst_list(cf);

        // Assert
        assert!(ssts.is_ok());
        assert_eq!(ssts.unwrap(), Vec::<SstId>::new());
    }

    #[test]
    fn should_list_single_sst_when_one_exists() {
        // Arrange
        let td = TempDir::new().unwrap();
        let fs = RealFs::new(td.path());
        let cf = CfId(2);
        let mut writer = fs.sst_create(cf, SstId(10)).unwrap();
        writer.write_block(Bytes::from("data")).unwrap();
        writer.finish(Durability::Unsafe).unwrap();

        // Act
        let ssts = fs.sst_list(cf);

        // Assert
        assert!(ssts.is_ok());
        assert_eq!(ssts.unwrap(), vec![SstId(10)]);
    }

    #[test]
    fn should_delete_sst_when_file_exists() {
        // Arrange
        let td = TempDir::new().unwrap();
        let fs = RealFs::new(td.path());
        let cf = CfId(3);
        let sst = SstId(5);
        let mut writer = fs.sst_create(cf, sst).unwrap();
        writer.write_block(Bytes::from("test")).unwrap();
        writer.finish(Durability::Unsafe).unwrap();

        // Act
        let result = fs.sst_delete(cf, sst);

        // Assert
        assert!(result.is_ok());
    }
}
