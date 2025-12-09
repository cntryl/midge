/// Safe, Vec-backed arena for temporary write buffers.
///
/// This avoids unsafe code by using Rust's `Vec<u8>` for allocation while
/// providing a small API similar to the previous arena: capacity reservation,
/// resize and slice access. We round up capacity requests to a page-sized
/// multiple to keep allocations reasonably aligned and sized for OS I/O.
pub struct Arena {
    buf: Vec<u8>,
}

impl Arena {
    #[inline]
    fn page_size() -> usize {
        4096
    }

    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        if cap == 0 {
            return Self::new();
        }
        let page = Self::page_size();
        let cap_rounded = cap.div_ceil(page) * page;
        let v = Vec::with_capacity(cap_rounded);
        // length is already 0 after with_capacity; no need to resize/initialize
        Self { buf: v }
    }

    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub fn reserve(&mut self, additional: usize) {
        self.buf.reserve(additional);
    }

    pub fn resize(&mut self, new_len: usize, val: u8) {
        self.buf.resize(new_len, val);
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf[..]
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..]
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}
