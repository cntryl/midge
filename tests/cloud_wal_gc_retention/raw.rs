//! Inspect persisted V4 blocks rather than inferring GC from read absence.
use cntryl_midge::__internal::{
    codec::decompress_block_with_trailer,
    sst::{
        encoding,
        types::{decode_range_tombstones, Footer},
    },
    types::EntryType,
};
use std::path::Path;

fn u64_at(bytes: &[u8], offset: usize) -> usize {
    usize::try_from(u64::from_le_bytes(
        bytes[offset..offset + 8].try_into().unwrap(),
    ))
    .unwrap()
}

fn block(bytes: &[u8], offset: usize, size: usize) -> bytes::Bytes {
    decompress_block_with_trailer(&bytes[offset + 4..offset + size]).unwrap()
}

pub(super) fn tombstone_sequences(root: &Path, engine: &cntryl_midge::Engine, cf: u32) -> Vec<u64> {
    let mut sequences = Vec::new();
    // Read only authoritative outputs; obsolete inputs can be deleted
    // asynchronously after compact_all returns.
    for file in engine
        .metrics()
        .get_storage_layout()
        .unwrap()
        .levels
        .iter()
        .flat_map(|level| &level.files)
        .filter(|file| file.cf_id == cf)
    {
        let bytes = std::fs::read(root.join("cloud_store/sst").join(&file.name)).unwrap();
        let footer = Footer::decode(&bytes[bytes.len() - 84..]).unwrap();
        let index = block(
            &bytes,
            usize::try_from(footer.index_handle.offset).unwrap(),
            usize::try_from(footer.index_handle.size).unwrap(),
        );
        let mut pos = 0;
        while pos < index.len() {
            let key_len =
                usize::try_from(u32::from_le_bytes(index[pos..pos + 4].try_into().unwrap()))
                    .unwrap();
            pos += 4 + key_len;
            let data = block(&bytes, u64_at(&index, pos), u64_at(&index, pos + 8));
            pos += 16;
            let mut cursor = 0;
            while cursor < data.len() {
                let (entry, next) = encoding::decode(&data, cursor).unwrap();
                if entry.entry_type == EntryType::Delete {
                    sequences.push(entry.sequence);
                }
                cursor = next;
            }
        }
        let metadata = block(
            &bytes,
            usize::try_from(footer.meta_index_handle.offset).unwrap(),
            usize::try_from(footer.meta_index_handle.size).unwrap(),
        );
        // V4 metadata stores its optional range block handle at bytes 8..24.
        let size = u64_at(&metadata, 16);
        if size != 0 {
            let ranges = block(&bytes, u64_at(&metadata, 8), size);
            sequences.extend(
                decode_range_tombstones(&ranges)
                    .unwrap()
                    .iter()
                    .map(|t| t.seq),
            );
        }
    }
    sequences.sort_unstable();
    sequences.dedup();
    sequences
}
