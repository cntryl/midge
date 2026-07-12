use super::format::{
    field_len_bytes, read_crc_exact, read_crc_vec, read_frame_header, read_u32_at, read_u64_at,
    u64_to_usize, usize_to_u64, write_frame_parts,
};
use super::{
    IntentLookup, OrdinalOp, TransactionOp, NO_RANGE_CHILD, RANGE_HEADER_LEN, RANGE_MAGIC,
    RANGE_TABLE_ENTRY_LEN, RANGE_VERSION,
};
use crate::common::{MidgeError, MidgeResult};
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug)]
struct RangeNodeMeta {
    op_index: usize,
    left: u64,
    right: u64,
    max_end: Bytes,
}

fn build_range_nodes(ops: &[OrdinalOp]) -> MidgeResult<Vec<RangeNodeMeta>> {
    let range_indices = ops
        .iter()
        .enumerate()
        .filter_map(|(index, op)| {
            matches!(op.op, TransactionOp::DeleteRange { .. }).then_some(index)
        })
        .collect::<Vec<_>>();
    let mut nodes = Vec::with_capacity(range_indices.len());
    let _ = build_range_subtree(&range_indices, ops, &mut nodes)?;
    Ok(nodes)
}

fn build_range_subtree(
    indices: &[usize],
    ops: &[OrdinalOp],
    nodes: &mut Vec<RangeNodeMeta>,
) -> MidgeResult<Option<u64>> {
    if indices.is_empty() {
        return Ok(None);
    }
    let middle = indices.len() / 2;
    let op_index = indices[middle];
    let (_, end_key) = range_keys(&ops[op_index].op).ok_or_else(|| {
        MidgeError::Internal("range index selected a non-range operation".to_string())
    })?;
    let node_index = nodes.len();
    nodes.push(RangeNodeMeta {
        op_index,
        left: NO_RANGE_CHILD,
        right: NO_RANGE_CHILD,
        max_end: end_key.clone(),
    });
    let left = build_range_subtree(&indices[..middle], ops, nodes)?;
    let right = build_range_subtree(&indices[middle + 1..], ops, nodes)?;
    let mut max_end = end_key.clone();
    for child in [left, right].into_iter().flatten() {
        let child_index = u64_to_usize(child)?;
        if nodes[child_index].max_end > max_end {
            max_end = nodes[child_index].max_end.clone();
        }
    }
    nodes[node_index] = RangeNodeMeta {
        op_index,
        left: left.unwrap_or(NO_RANGE_CHILD),
        right: right.unwrap_or(NO_RANGE_CHILD),
        max_end,
    };
    Ok(Some(usize_to_u64(node_index)?))
}

fn range_keys(op: &TransactionOp) -> Option<(&Bytes, &Bytes)> {
    match op {
        TransactionOp::DeleteRange {
            start_key, end_key, ..
        } => Some((start_key, end_key)),
        TransactionOp::Put { .. } | TransactionOp::Delete { .. } => None,
    }
}

pub(super) fn write_range_index_file(path: &Path, ops: &[OrdinalOp]) -> MidgeResult<()> {
    let nodes = build_range_nodes(ops)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&[0; RANGE_HEADER_LEN])?;
    let table_bytes = nodes
        .len()
        .checked_mul(RANGE_TABLE_ENTRY_LEN)
        .ok_or_else(|| {
            MidgeError::ResourceLimit("transaction range index table overflow".to_string())
        })?;
    write_zeroes(&mut file, table_bytes)?;
    let node_section_offset = file.stream_position()?;
    let mut node_offsets = Vec::with_capacity(nodes.len());
    for node in &nodes {
        node_offsets.push(file.stream_position()?);
        write_range_node_frame(&mut file, node, ops)?;
    }
    let end = file.stream_position()?;
    file.seek(SeekFrom::Start(RANGE_HEADER_LEN as u64))?;
    for offset in node_offsets {
        let bytes = offset.to_le_bytes();
        file.write_all(&bytes)?;
        file.write_all(&crc32c::crc32c(&bytes).to_le_bytes())?;
    }
    let header = encode_range_header(nodes.len(), node_section_offset)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    file.seek(SeekFrom::Start(end))?;
    file.sync_all()?;
    Ok(())
}

fn write_zeroes(file: &mut File, mut bytes: usize) -> MidgeResult<()> {
    const ZEROES: [u8; 4096] = [0; 4096];
    while bytes != 0 {
        let chunk = bytes.min(ZEROES.len());
        file.write_all(&ZEROES[..chunk])?;
        bytes -= chunk;
    }
    Ok(())
}

fn encode_range_header(
    node_count: usize,
    node_section_offset: u64,
) -> MidgeResult<[u8; RANGE_HEADER_LEN]> {
    let mut header = [0_u8; RANGE_HEADER_LEN];
    header[..8].copy_from_slice(RANGE_MAGIC);
    header[8..12].copy_from_slice(&RANGE_VERSION.to_le_bytes());
    header[12..20].copy_from_slice(&usize_to_u64(node_count)?.to_le_bytes());
    header[20..28].copy_from_slice(&node_section_offset.to_le_bytes());
    let crc = crc32c::crc32c(&header[..28]);
    header[28..32].copy_from_slice(&crc.to_le_bytes());
    Ok(header)
}

fn write_range_node_frame(
    file: &mut File,
    node: &RangeNodeMeta,
    ops: &[OrdinalOp],
) -> MidgeResult<()> {
    let ordinal_op = &ops[node.op_index];
    let (start_key, end_key) = range_keys(&ordinal_op.op).ok_or_else(|| {
        MidgeError::Internal("range node references a non-range operation".to_string())
    })?;
    let ordinal = ordinal_op.ordinal.to_le_bytes();
    let left = node.left.to_le_bytes();
    let right = node.right.to_le_bytes();
    let start_len = field_len_bytes(start_key.len())?;
    let end_len = field_len_bytes(end_key.len())?;
    let max_end_len = field_len_bytes(node.max_end.len())?;
    write_frame_parts(
        file,
        &[
            &ordinal,
            &left,
            &right,
            &start_len,
            start_key,
            &end_len,
            end_key,
            &max_end_len,
            &node.max_end,
        ],
    )
}

pub(super) struct RangeHeader {
    node_count: usize,
    pub(super) node_section_offset: u64,
}

struct RangeNode {
    ordinal: u64,
    left: u64,
    right: u64,
    start_key: Bytes,
    end_key: Bytes,
    max_end: Bytes,
}

pub(super) fn read_range_header(file: &mut File) -> MidgeResult<RangeHeader> {
    let mut header = [0_u8; RANGE_HEADER_LEN];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;
    if &header[..8] != RANGE_MAGIC {
        return Err(MidgeError::Corruption(
            "transaction range index has invalid magic".to_string(),
        ));
    }
    let version = read_u32_at(&header, 8)?;
    if version != RANGE_VERSION {
        return Err(MidgeError::Corruption(format!(
            "unsupported transaction range index version {version}"
        )));
    }
    if crc32c::crc32c(&header[..28]) != read_u32_at(&header, 28)? {
        return Err(MidgeError::Corruption(
            "transaction range index header checksum mismatch".to_string(),
        ));
    }
    let node_count = u64_to_usize(read_u64_at(&header, 12)?)?;
    let node_section_offset = read_u64_at(&header, 20)?;
    let expected_offset = RANGE_HEADER_LEN
        .checked_add(
            node_count
                .checked_mul(RANGE_TABLE_ENTRY_LEN)
                .ok_or_else(|| {
                    MidgeError::Corruption("transaction range table length overflow".to_string())
                })?,
        )
        .ok_or_else(|| {
            MidgeError::Corruption("transaction range node offset overflow".to_string())
        })?;
    if node_section_offset != usize_to_u64(expected_offset)? {
        return Err(MidgeError::Corruption(
            "transaction range node section offset is invalid".to_string(),
        ));
    }
    Ok(RangeHeader {
        node_count,
        node_section_offset,
    })
}

fn read_range_node(
    file: &mut File,
    header: &RangeHeader,
    node_index: u64,
) -> MidgeResult<RangeNode> {
    let index = u64_to_usize(node_index)?;
    if index >= header.node_count {
        return Err(MidgeError::Corruption(format!(
            "transaction range child {node_index} is out of bounds"
        )));
    }
    let table_offset = RANGE_HEADER_LEN
        .checked_add(index.checked_mul(RANGE_TABLE_ENTRY_LEN).ok_or_else(|| {
            MidgeError::Corruption("transaction range table index overflow".to_string())
        })?)
        .ok_or_else(|| {
            MidgeError::Corruption("transaction range table offset overflow".to_string())
        })?;
    file.seek(SeekFrom::Start(usize_to_u64(table_offset)?))?;
    let mut entry = [0_u8; RANGE_TABLE_ENTRY_LEN];
    file.read_exact(&mut entry)?;
    let expected_crc = read_u32_at(&entry, 8)?;
    if crc32c::crc32c(&entry[..8]) != expected_crc {
        return Err(MidgeError::Corruption(
            "transaction range offset checksum mismatch".to_string(),
        ));
    }
    let node_offset = read_u64_at(&entry, 0)?;
    if node_offset < header.node_section_offset {
        return Err(MidgeError::Corruption(
            "transaction range node offset is out of bounds".to_string(),
        ));
    }
    file.seek(SeekFrom::Start(node_offset))?;
    read_range_node_frame(file)
}

fn read_range_node_frame(file: &mut File) -> MidgeResult<RangeNode> {
    let (payload_len, expected_crc) = read_frame_header(file)?;
    if payload_len < 36 {
        return Err(MidgeError::Corruption(
            "transaction range node is truncated".to_string(),
        ));
    }
    let mut crc = 0_u32;
    let mut fixed = [0_u8; 24];
    read_crc_exact(file, &mut fixed, &mut crc)?;
    let ordinal = read_u64_at(&fixed, 0)?;
    let left = read_u64_at(&fixed, 8)?;
    let right = read_u64_at(&fixed, 16)?;
    let mut consumed = 24_usize;
    let start_key = read_crc_field(file, payload_len, &mut consumed, &mut crc)?;
    let end_key = read_crc_field(file, payload_len, &mut consumed, &mut crc)?;
    let max_end = read_crc_field(file, payload_len, &mut consumed, &mut crc)?;
    if consumed != payload_len {
        return Err(MidgeError::Corruption(
            "transaction range node has trailing bytes".to_string(),
        ));
    }
    if crc != expected_crc {
        return Err(MidgeError::Corruption(
            "transaction range node checksum mismatch".to_string(),
        ));
    }
    Ok(RangeNode {
        ordinal,
        left,
        right,
        start_key: Bytes::from(start_key),
        end_key: Bytes::from(end_key),
        max_end: Bytes::from(max_end),
    })
}

fn read_crc_field(
    file: &mut File,
    payload_len: usize,
    consumed: &mut usize,
    crc: &mut u32,
) -> MidgeResult<Vec<u8>> {
    let mut length_bytes = [0_u8; 4];
    read_crc_exact(file, &mut length_bytes, crc)?;
    *consumed = consumed.saturating_add(4);
    let length = read_u32_at(&length_bytes, 0)? as usize;
    if consumed.saturating_add(length) > payload_len {
        return Err(MidgeError::Corruption(
            "transaction range field exceeds its frame".to_string(),
        ));
    }
    let value = read_crc_vec(file, length, crc)?;
    *consumed = consumed.saturating_add(length);
    Ok(value)
}

pub(super) fn lookup_range_index(
    path: &Path,
    key: &[u8],
    ordinal_ceiling: u64,
    latest: &mut Option<(u64, IntentLookup)>,
) -> MidgeResult<()> {
    let mut file = File::open(path)?;
    let header = read_range_header(&mut file)?;
    if header.node_count == 0 {
        return Ok(());
    }
    let mut lookup = RangeLookup {
        file: &mut file,
        header: &header,
        key,
        ordinal_ceiling,
        latest,
    };
    lookup_range_subtree(&mut lookup, 0, None, header.node_count)
}

struct RangeLookup<'a> {
    file: &'a mut File,
    header: &'a RangeHeader,
    key: &'a [u8],
    ordinal_ceiling: u64,
    latest: &'a mut Option<(u64, IntentLookup)>,
}

fn lookup_range_subtree(
    lookup: &mut RangeLookup<'_>,
    node_index: u64,
    loaded: Option<RangeNode>,
    remaining_nodes: usize,
) -> MidgeResult<()> {
    if remaining_nodes == 0 {
        return Err(MidgeError::Corruption(
            "transaction range index contains a child cycle".to_string(),
        ));
    }
    let node = match loaded {
        Some(node) => node,
        None => read_range_node(lookup.file, lookup.header, node_index)?,
    };
    if node.left != NO_RANGE_CHILD {
        let left = read_range_node(lookup.file, lookup.header, node.left)?;
        if lookup.key < left.max_end.as_ref() {
            lookup_range_subtree(lookup, node.left, Some(left), remaining_nodes - 1)?;
        }
    }
    if node.ordinal < lookup.ordinal_ceiling
        && node.start_key.as_ref() <= lookup.key
        && lookup.key < node.end_key.as_ref()
        && lookup
            .latest
            .as_ref()
            .is_none_or(|(ordinal, _)| node.ordinal > *ordinal)
    {
        *lookup.latest = Some((node.ordinal, IntentLookup::Deleted));
    }
    if node.right != NO_RANGE_CHILD && node.start_key.as_ref() <= lookup.key {
        lookup_range_subtree(lookup, node.right, None, remaining_nodes - 1)?;
    }
    Ok(())
}

pub(super) fn validate_range_index(path: &Path) -> MidgeResult<()> {
    let mut file = File::open(path)?;
    let header = read_range_header(&mut file)?;
    for index in 0..header.node_count {
        let node = read_range_node(&mut file, &header, usize_to_u64(index)?)?;
        for child in [node.left, node.right] {
            if child != NO_RANGE_CHILD && u64_to_usize(child)? >= header.node_count {
                return Err(MidgeError::Corruption(
                    "transaction range child is out of bounds".to_string(),
                ));
            }
        }
        if node.max_end < node.end_key {
            return Err(MidgeError::Corruption(
                "transaction range subtree maximum is invalid".to_string(),
            ));
        }
    }
    Ok(())
}
