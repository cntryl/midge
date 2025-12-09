//! Filesystem WAL reader implementation

use crate::common::MidgeResult;
use crate::wal::traits::{WalReader, WalReaderDyn};
use crate::wal::types::{WalPos, WalRecord};
use crate::wal::encoding;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::Path;

/// Filesystem-backed WAL reader
pub struct FsWalReader {
    file: File,
    current_pos: u64,
}

impl FsWalReader {
    /// Create a new filesystem WAL reader
    pub fn new(dir: &Path) -> MidgeResult<Self> {
        let file_path = dir.join("wal.log");
        let file = File::open(&file_path)
            .map_err(|e| crate::common::MidgeError::Io(e))?;
        
        Ok(Self {
            file,
            current_pos: 0,
        })
    }
}

impl WalReader for FsWalReader {
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        // Seek to position
        self.file.seek(std::io::SeekFrom::Start(pos))
            .map_err(|e| crate::common::MidgeError::Io(e))?;
        
        // Read length prefix
        let mut len_buf = [0u8; 4];
        match self.file.read_exact(&mut len_buf) {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(crate::common::MidgeError::Io(e)),
        }
        
        let len = u32::from_le_bytes(len_buf) as usize;
        
        // Read record bytes
        let mut record_buf = vec![0u8; len];
        self.file.read_exact(&mut record_buf)
            .map_err(|e| crate::common::MidgeError::Io(e))?;
        
        // Decode record
        let record = encoding::decode(&record_buf[..])?;
        self.current_pos = pos + 4 + len as u64;
        
        Ok(Some(record))
    }
    
    fn replay<F>(&mut self, start: WalPos, mut cb: F) -> MidgeResult<()>
    where
        F: FnMut(&WalRecord) -> MidgeResult<()>,
    {
        // Seek to start position
        self.file.seek(std::io::SeekFrom::Start(start))
            .map_err(|e| crate::common::MidgeError::Io(e))?;
        
        let mut pos = start;
        
        loop {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            match self.file.read_exact(&mut len_buf) {
                Ok(_) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(crate::common::MidgeError::Io(e)),
            }
            
            let len = u32::from_le_bytes(len_buf) as usize;
            
            // Read record bytes
            let mut record_buf = vec![0u8; len];
            match self.file.read_exact(&mut record_buf) {
                Ok(_) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(crate::common::MidgeError::Corruption(
                        "Incomplete record at end of WAL".to_string(),
                    ));
                },
                Err(e) => return Err(crate::common::MidgeError::Io(e)),
            }
            
            // Decode and invoke callback
            let record = encoding::decode(&record_buf[..])?;
            cb(&record)?;
            
            pos += 4 + len as u64;
        }
        
        self.current_pos = pos;
        Ok(())
    }
    
    fn close(&mut self) -> MidgeResult<()> {
        // File is automatically closed when dropped
        Ok(())
    }
}

/// Object-safe implementation for WalReaderDyn
impl WalReaderDyn for FsWalReader {
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        WalReader::read_at(self, pos)
    }
    
    fn replay_boxed(
        &mut self,
        start: WalPos,
        cb: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
    ) -> MidgeResult<()> {
        self.replay(start, cb)
    }
    
    fn close(&mut self) -> MidgeResult<()> {
        WalReader::close(self)
    }
}

