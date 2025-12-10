//! Filesystem-backed SST reader

use bytes::Bytes;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::common::{MidgeError, MidgeResult};
use crate::sst::encoding;
use crate::sst::traits::SstReader;
use crate::sst::types::{BlockHandle, Footer};

/// SST file reader
pub struct SstFile {
    path: std::path::PathBuf,
    footer: Option<Footer>,
    cached_file: std::sync::Mutex<Option<File>>,
}

impl SstFile {
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            footer: None,
            cached_file: std::sync::Mutex::new(None),
        }
    }

    pub fn open(path: &Path) -> MidgeResult<Self> {
        let mut reader = Self::new(path);
        reader.load_metadata()?;
        Ok(reader)
    }

    fn load_metadata(&mut self) -> MidgeResult<()> {
        let mut file = File::open(&self.path)?;
        let file_size = std::fs::metadata(&self.path)?.len();

        if file_size < 48 {
            return Err(MidgeError::Corruption("SST file too small".into()));
        }

        // Read footer (48 bytes from end)
        file.seek(SeekFrom::End(-48))?;
        let mut footer_data = [0u8; 48];
        file.read_exact(&mut footer_data)?;

        self.footer = Some(Footer::decode(&footer_data)?);
        *self.cached_file.lock().expect("cached_file lock poisoned") = Some(file);

        Ok(())
    }

    fn read_block(&self, handle: &BlockHandle) -> MidgeResult<Vec<u8>> {
        let mut file_guard = self.cached_file.lock().expect("cached_file lock poisoned");
        if file_guard.is_none() {
            *file_guard = Some(File::open(&self.path)?);
        }

        if let Some(file) = file_guard.as_mut() {
            file.seek(SeekFrom::Start(handle.offset))?;

            let mut buffer = vec![0u8; handle.size as usize];
            file.read_exact(&mut buffer)?;

            // First 4 bytes are length prefix
            if buffer.len() < 4 {
                return Err(MidgeError::Corruption("Block too short".into()));
            }

            let len = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
            if len + 4 > buffer.len() {
                return Err(MidgeError::Corruption("Block data truncated".into()));
            }

            Ok(buffer[4..4 + len].to_vec())
        } else {
            Err(MidgeError::Corruption("Cannot open SST file".into()))
        }
    }

    fn scan_index(&self) -> MidgeResult<Vec<(Vec<u8>, BlockHandle)>> {
        let footer = self
            .footer
            .as_ref()
            .ok_or_else(|| MidgeError::Corruption("No footer".into()))?;

        let index_data = self.read_block(&footer.index_handle)?;
        let mut result = Vec::new();
        let mut offset = 0;

        while offset < index_data.len() {
            if offset + 20 > index_data.len() {
                break;
            }

            let key_len = u32::from_le_bytes([
                index_data[offset],
                index_data[offset + 1],
                index_data[offset + 2],
                index_data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + key_len > index_data.len() {
                break;
            }

            let key = index_data[offset..offset + key_len].to_vec();
            offset += key_len;

            if offset + 16 > index_data.len() {
                break;
            }

            let block_offset = u64::from_le_bytes([
                index_data[offset],
                index_data[offset + 1],
                index_data[offset + 2],
                index_data[offset + 3],
                index_data[offset + 4],
                index_data[offset + 5],
                index_data[offset + 6],
                index_data[offset + 7],
            ]);
            offset += 8;

            let block_size = u64::from_le_bytes([
                index_data[offset],
                index_data[offset + 1],
                index_data[offset + 2],
                index_data[offset + 3],
                index_data[offset + 4],
                index_data[offset + 5],
                index_data[offset + 6],
                index_data[offset + 7],
            ]);
            offset += 8;

            result.push((key, BlockHandle::new(block_offset, block_size)));
        }

        Ok(result)
    }

    fn scan_block(&self, handle: &BlockHandle) -> MidgeResult<Vec<(Vec<u8>, Option<Bytes>)>> {
        let block_data = self.read_block(handle)?;
        let mut result = Vec::new();
        let mut offset = 0;

        while offset < block_data.len() {
            if let Ok((entry, next_offset)) = encoding::decode(&block_data, offset) {
                // Reconstruct key from shared_len + key_delta
                // For now, assume no shared prefix (simplified)
                let key = if entry.shared_len == 0 {
                    entry.key_delta
                } else {
                    // Would need to track previous key for prefix decompression
                    entry.key_delta
                };

                let value = entry.value.map(|v| Bytes::from(v));
                result.push((key, value));
                offset = next_offset;
            } else {
                break;
            }
        }

        Ok(result)
    }
}

impl crate::sst::SstReader for SstFile {
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        let index = self.scan_index()?;

        // Find block containing key using binary search on first key of each block
        let mut block_handle = None;
        for (idx, (first_key, handle)) in index.iter().enumerate() {
            if key <= first_key.as_slice() || idx == index.len() - 1 {
                block_handle = Some(handle);
                break;
            }
        }

        if let Some(handle) = block_handle {
            let entries = self.scan_block(handle)?;
            for (entry_key, value) in entries {
                if entry_key == key {
                    return Ok(value);
                }
            }
        }

        Ok(None)
    }

    fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let index = self.scan_index()?;
        let mut result = Vec::new();

        for (_first_key, handle) in index {
            let entries = self.scan_block(&handle)?;
            for (key, value) in entries {
                // Check range bounds
                if let Some(s) = start {
                    if key.as_slice() < s {
                        continue;
                    }
                }
                if let Some(e) = end {
                    if key.as_slice() >= e {
                        continue;
                    }
                }

                if let Some(val) = value {
                    result.push((Bytes::from(key), val));
                }
            }
        }

        Ok(result)
    }
}

impl crate::sst::SstStateReader for SstFile {
    fn get_state(&self, key: &[u8]) -> MidgeResult<crate::sst::types::KeyState> {
        match self.get(key)? {
            Some(value) => Ok(crate::sst::types::KeyState::Value(value, 0, None, 0)),
            None => Ok(crate::sst::types::KeyState::Absent),
        }
    }

    fn scan_range_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, crate::sst::types::KeyState)>> {
        let pairs = self.scan_range(start, end)?;
        Ok(pairs
            .into_iter()
            .map(|(k, v)| (k.clone(), crate::sst::types::KeyState::Value(v, 0, None, 0)))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::traits::DynSstWriter;
    use std::fs;

    #[test]
    fn should_retrieve_written_entries_when_reading_sst_file() -> MidgeResult<()> {
        // Arrange
        let temp_dir = std::env::temp_dir().join("midge_sst_reader_test");
        fs::create_dir_all(&temp_dir)?;

        let mut writer = crate::sst::fs::FsSstWriter::new(&temp_dir, 4096)?;
        writer.add(b"key1", b"value1")?;
        writer.add(b"key2", b"value2")?;
        let bytes = Box::new(writer).finish_bytes()?;

        let sst_path = temp_dir.join("test.sst");
        fs::write(&sst_path, &bytes)?;

        // Act
        let reader = SstFile::open(&sst_path)?;
        let val1 = reader.get(b"key1")?;

        // Assert
        assert_eq!(val1.map(|v| v.to_vec()), Some(b"value1".to_vec()));

        Ok(())
    }
}
