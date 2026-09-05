//! Bounded read-ahead over a stable read-only file handle.

use super::{Durability, File, FsError, FsResult};
use bytes::{Bytes, BytesMut};

fn read_only_error() -> FsError {
    FsError::Unsupported("buffered immutable read handles are read only".into())
}

#[derive(Default)]
struct ReadWindow {
    offset: u64,
    bytes: Bytes,
}

pub(crate) struct BufferedReadFile<'a> {
    inner: Box<dyn File + 'a>,
    file_len: u64,
    range_buffer_bytes: usize,
    window: parking_lot::Mutex<ReadWindow>,
}

impl<'a> BufferedReadFile<'a> {
    pub(crate) fn new(inner: Box<dyn File + 'a>, range_buffer_bytes: usize) -> FsResult<Self> {
        if range_buffer_bytes == 0 {
            return Err(FsError::Io("read window must be positive".into()));
        }
        let file_len = inner.len()?;
        Ok(Self {
            inner,
            file_len,
            range_buffer_bytes,
            window: parking_lot::Mutex::new(ReadWindow::default()),
        })
    }

    fn window_slice(&self, offset: u64, remaining: usize) -> FsResult<Bytes> {
        let window_size = self.range_buffer_bytes as u64;
        let aligned_offset = (offset / window_size) * window_size;
        let mut window = self.window.lock();
        if window.bytes.is_empty() || window.offset != aligned_offset {
            let length = window_size.min(self.file_len - aligned_offset);
            let bytes = self.inner.read_at(aligned_offset, length)?;
            if bytes.len() as u64 != length {
                return Err(FsError::Corruption(
                    "immutable source returned a short immutable range".into(),
                ));
            }
            *window = ReadWindow {
                offset: aligned_offset,
                bytes,
            };
        }
        let start = usize::try_from(offset - window.offset)
            .map_err(|_| FsError::Io("file read offset exceeds addressable memory".into()))?;
        let length = remaining.min(window.bytes.len() - start);
        Ok(window.bytes.slice(start..start + length))
    }
}

impl File for BufferedReadFile<'_> {
    fn read_at(&self, offset: u64, length: u64) -> FsResult<Bytes> {
        let length = usize::try_from(length.min(self.file_len.saturating_sub(offset)))
            .map_err(|_| FsError::Io("file read exceeds addressable memory".into()))?;
        if length == 0 {
            return Ok(Bytes::new());
        }
        let first = self.window_slice(offset, length)?;
        if first.len() == length {
            return Ok(first);
        }
        // The caller owns its bounded frame buffer. Independently cap each
        // provider request and the retained read-ahead window.
        let mut output = BytesMut::with_capacity(length);
        output.extend_from_slice(&first);
        while output.len() < length {
            let next = self.window_slice(offset + output.len() as u64, length - output.len())?;
            output.extend_from_slice(&next);
        }
        Ok(output.freeze())
    }

    fn len(&self) -> FsResult<u64> {
        Ok(self.file_len)
    }
    fn write_at(&mut self, _offset: u64, _data: Bytes) -> FsResult<()> {
        Err(read_only_error())
    }
    fn append(&mut self, _data: Bytes) -> FsResult<u64> {
        Err(read_only_error())
    }
    fn sync(&mut self, _durability: Durability) -> FsResult<()> {
        Err(read_only_error())
    }
    fn close(self: Box<Self>) -> FsResult<()> {
        self.inner.close()
    }
}
