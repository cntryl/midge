//! Borrowed shape preflight for manifest decode, journal replay and validation.

use super::workspace::{HeapFootprint, MetadataFile, Name, Sequence, VectorLayout};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use serde::de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::marker::PhantomData;
use std::mem::size_of;

#[cfg(test)]
mod tests;

impl HeapFootprint for u32 {
    const ITEM_BYTES: usize = size_of::<Self>();
    fn heap_bytes(&self) -> Option<usize> {
        Some(0)
    }
}

impl HeapFootprint for u64 {
    const ITEM_BYTES: usize = size_of::<Self>();
    fn heap_bytes(&self) -> Option<usize> {
        Some(0)
    }
}

struct Entries<K, V> {
    len: usize,
    bytes: usize,
    marker: PhantomData<(K, V)>,
}

impl<K, V> Default for Entries<K, V> {
    fn default() -> Self {
        Self {
            len: 0,
            bytes: 0,
            marker: PhantomData,
        }
    }
}

impl<K: HeapFootprint, V: HeapFootprint> HeapFootprint for Entries<K, V> {
    const ITEM_BYTES: usize = size_of::<std::collections::HashMap<(), ()>>();
    fn heap_bytes(&self) -> Option<usize> {
        Some(self.bytes)
    }
}

impl<'de, K, V> Deserialize<'de> for Entries<K, V>
where
    K: Deserialize<'de> + HeapFootprint,
    V: Deserialize<'de> + HeapFootprint,
{
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Pairs<K, V>(PhantomData<(K, V)>);
        impl<'de, K, V> Visitor<'de> for Pairs<K, V>
        where
            K: Deserialize<'de> + HeapFootprint,
            V: Deserialize<'de> + HeapFootprint,
        {
            type Value = Entries<K, V>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a manifest map")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let item = K::ITEM_BYTES
                    .checked_add(V::ITEM_BYTES)
                    .ok_or_else(|| de::Error::custom("map entry size overflow"))?;
                let hinted = map
                    .size_hint()
                    .unwrap_or(0)
                    .min((1024 * 1024) / item.max(1));
                let mut len = 0_usize;
                let mut dynamic = 0_usize;
                while let Some((key, value)) = map.next_entry::<K, V>()? {
                    len = len
                        .checked_add(1)
                        .ok_or_else(|| de::Error::custom("map count overflow"))?;
                    dynamic = dynamic
                        .checked_add(
                            key.heap_bytes()
                                .ok_or_else(|| de::Error::custom("map key overflow"))?,
                        )
                        .and_then(|bytes| bytes.checked_add(value.heap_bytes()?))
                        .ok_or_else(|| de::Error::custom("map field overflow"))?;
                }
                // Pair padding, bucket slack/control bytes and both old/new
                // tables during a grow are bounded independently of payloads.
                let slots = len.max(hinted);
                let storage = if slots == 0 {
                    0
                } else {
                    slots
                        .checked_mul(item.max(1))
                        .and_then(|bytes| bytes.checked_mul(8))
                        .and_then(|bytes| bytes.checked_add(256))
                        .ok_or_else(|| de::Error::custom("map storage overflow"))?
                };
                Ok(Entries {
                    len,
                    bytes: storage
                        .checked_add(dynamic)
                        .ok_or_else(|| de::Error::custom("map footprint overflow"))?,
                    marker: PhantomData,
                })
            }
        }
        decoder.deserialize_map(Pairs(PhantomData))
    }
}

#[derive(Deserialize)]
struct FrontierRow {
    #[serde(rename = "cf_id")]
    _cf_id: IgnoredAny,
    #[serde(rename = "through_sequence")]
    _through_sequence: IgnoredAny,
}

impl HeapFootprint for FrontierRow {
    const ITEM_BYTES: usize = size_of::<(u32, u64)>();
    fn heap_bytes(&self) -> Option<usize> {
        Some(0)
    }
}

#[derive(Deserialize)]
struct Family {
    #[serde(rename = "id")]
    _id: IgnoredAny,
    name: Name,
    #[serde(default, rename = "created_at")]
    _created_at: IgnoredAny,
    #[serde(default, rename = "deleted_at")]
    _deleted_at: IgnoredAny,
    #[serde(default, rename = "drop_sequence")]
    _drop_sequence: IgnoredAny,
    #[serde(default)]
    dropped_sst_names: Sequence<Name>,
    #[serde(default, rename = "reclaimed")]
    _reclaimed: IgnoredAny,
}

impl HeapFootprint for Family {
    const ITEM_BYTES: usize = size_of::<crate::metadata::ColumnFamilyMeta>();
    fn heap_bytes(&self) -> Option<usize> {
        self.name
            .heap_bytes()?
            .checked_add(self.dropped_sst_names.heap_bytes()?)
    }
}

#[derive(Deserialize)]
struct Snapshot {
    flushed_through: Sequence<FrontierRow>,
    #[serde(default)]
    files: Sequence<MetadataFile>,
    #[serde(default)]
    retired_ssts: Entries<Name, MetadataFile>,
    #[serde(default)]
    column_families: Sequence<Family>,
    #[serde(default)]
    next_sst_seqs: Entries<u32, u64>,
    #[serde(default, rename = "edit_checkpoint_id")]
    _edit_checkpoint_id: IgnoredAny,
}

impl Snapshot {
    fn authority_bytes(&self) -> Option<usize> {
        self.column_families
            .heap_bytes()?
            .checked_add(self.flushed_through.heap_bytes()?)?
            .checked_add(self.flushed_through.len.checked_mul(256)?)
    }
    fn bytes(&self) -> Option<usize> {
        let collections = self
            .files
            .len
            .checked_add(self.retired_ssts.len)?
            .checked_add(self.column_families.len)?
            .checked_add(self.next_sst_seqs.len)?
            .checked_add(self.flushed_through.len)?;
        // Frontier conversion plus validation sets/maps coexist with the
        // decoded vectors. This includes their inline tuple/control storage.
        self.files
            .heap_bytes()?
            .checked_add(self.retired_ssts.heap_bytes()?)?
            .checked_add(self.column_families.heap_bytes()?)?
            .checked_add(self.next_sst_seqs.heap_bytes()?)?
            .checked_add(self.flushed_through.heap_bytes()?)?
            .checked_add(collections.checked_mul(256)?)
    }
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "edit_id")]
    _edit_id: IgnoredAny,
    edit: Edit,
}

#[derive(Deserialize)]
enum Edit {
    AddSst(MetadataFile),
    RemoveSst {
        name: Name,
    },
    MoveSsts {
        captured: Sequence<MetadataFile>,
        #[serde(rename = "target_level")]
        _target_level: IgnoredAny,
    },
    CompleteSstDelete {
        file: MetadataFile,
    },
    CreateColumnFamily {
        #[serde(rename = "id")]
        _id: IgnoredAny,
        name: Name,
        #[serde(rename = "created_at")]
        _created_at: IgnoredAny,
    },
    DropColumnFamilyAt {
        #[serde(rename = "id")]
        _id: IgnoredAny,
        #[serde(rename = "drop_sequence")]
        _drop_sequence: IgnoredAny,
        #[serde(default)]
        dropped_sst_names: Sequence<Name>,
    },
    ReclaimColumnFamily {
        #[serde(rename = "id")]
        _id: IgnoredAny,
        #[serde(default)]
        names: Sequence<Name>,
    },
    AdvanceFlushFrontier {
        #[serde(rename = "cf_id")]
        _cf_id: IgnoredAny,
        #[serde(rename = "freeze_sequence")]
        _freeze_sequence: IgnoredAny,
    },
    BumpNextSstSeq {
        #[serde(rename = "cf_id")]
        _cf_id: IgnoredAny,
        #[serde(rename = "next_seq")]
        _next_seq: IgnoredAny,
    },
    Batch(BatchEdits),
}

struct BatchEdits {
    bytes: usize,
    authority: usize,
}

impl<'de> Deserialize<'de> for BatchEdits {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Edits;
        impl<'de> Visitor<'de> for Edits {
            type Value = BatchEdits;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a batch of manifest edits")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut edits: A) -> Result<Self::Value, A::Error> {
                let mut layout =
                    VectorLayout::with_hint(edits.size_hint().unwrap_or(0), Edit::ITEM_BYTES)
                        .ok_or_else(|| de::Error::custom("manifest batch capacity overflow"))?;
                let mut len = 0_usize;
                let mut dynamic = 0_usize;
                let mut authority = 0_usize;
                while let Some(edit) = edits.next_element::<Edit>()? {
                    len = len
                        .checked_add(1)
                        .ok_or_else(|| de::Error::custom("manifest batch count overflow"))?;
                    layout
                        .grow(len, Edit::ITEM_BYTES)
                        .ok_or_else(|| de::Error::custom("manifest batch capacity overflow"))?;
                    dynamic = dynamic
                        .checked_add(
                            edit.heap_bytes().ok_or_else(|| {
                                de::Error::custom("manifest edit footprint overflow")
                            })?,
                        )
                        .ok_or_else(|| de::Error::custom("manifest batch footprint overflow"))?;
                    authority = authority
                        .checked_add(edit.authority_bytes().ok_or_else(|| {
                            de::Error::custom("manifest authority footprint overflow")
                        })?)
                        .ok_or_else(|| {
                            de::Error::custom("manifest authority footprint overflow")
                        })?;
                }
                Ok(BatchEdits {
                    bytes: layout
                        .peak_bytes
                        .checked_add(dynamic)
                        .ok_or_else(|| de::Error::custom("manifest batch footprint overflow"))?,
                    authority,
                })
            }
        }
        decoder.deserialize_seq(Edits)
    }
}

impl Edit {
    fn authority_bytes(&self) -> Option<usize> {
        match self {
            Self::CreateColumnFamily { name, .. } => name
                .heap_bytes()?
                .checked_add(4 * Family::ITEM_BYTES)?
                .checked_add(256),
            Self::Batch(edits) => Some(edits.authority),
            // SST inventories, names and counters are discarded by reduction;
            // other lifecycle/frontier edits cannot introduce a new family id.
            _ => Some(0),
        }
    }
}

impl HeapFootprint for Edit {
    const ITEM_BYTES: usize = size_of::<crate::metadata::ManifestEdit>();
    fn heap_bytes(&self) -> Option<usize> {
        match self {
            Self::AddSst(file) | Self::CompleteSstDelete { file } => file.heap_bytes(),
            Self::RemoveSst { name } | Self::CreateColumnFamily { name, .. } => name.heap_bytes(),
            Self::MoveSsts { captured, .. } => captured.heap_bytes(),
            Self::DropColumnFamilyAt {
                dropped_sst_names, ..
            } => dropped_sst_names.heap_bytes(),
            Self::ReclaimColumnFamily { names, .. } => names.heap_bytes(),
            Self::AdvanceFlushFrontier { .. } | Self::BumpNextSstSeq { .. } => Some(0),
            Self::Batch(edits) => Some(edits.bytes),
        }
    }
}

pub(crate) struct ManifestDecodeWorkspace {
    pub(crate) bytes: usize,
    pub(crate) authority: usize,
}

/// Raw control bytes and intent replay/normalization are accounted by caller.
pub(crate) fn decoded_workspace_bound(
    snapshot: &[u8],
    journal: &[u8],
    deadline: &OperationDeadline,
) -> MidgeResult<ManifestDecodeWorkspace> {
    check(deadline)?;
    let snapshot: Snapshot = super::decode(super::MANIFEST, snapshot)?;
    let base = snapshot.bytes().ok_or_else(overflow)?;
    let mut vector = VectorLayout::default();
    let mut count = 0_usize;
    let mut dynamic = 0_usize;
    let mut added_authority = 0_usize;
    let mut position = 0_usize;
    while let Some(header) = journal.get(position..position.saturating_add(5)) {
        check(deadline)?;
        if header[0] == 0 && journal[position..].iter().all(|byte| *byte == 0) {
            break;
        }
        let length = u32::from_le_bytes(header[1..5].try_into().expect("journal length")) as usize;
        let end = position
            .checked_add(9)
            .and_then(|start| start.checked_add(length))
            .ok_or_else(overflow)?;
        let Some(record) = journal.get(position..end) else {
            break;
        };
        let payload = &record[5..5 + length];
        let crc = u32::from_be_bytes(record[5 + length..].try_into().expect("journal CRC"));
        if crc32c::crc32c(payload) != crc {
            return Err(MidgeError::Corruption(
                "manifest journal CRC mismatch during preflight".into(),
            ));
        }
        if header[0] != 9 {
            let envelope: Envelope = super::decode(super::JOURNAL, payload)?;
            dynamic = dynamic
                .checked_add(envelope.edit.heap_bytes().ok_or_else(overflow)?)
                .ok_or_else(overflow)?;
            added_authority = added_authority
                .checked_add(envelope.edit.authority_bytes().ok_or_else(overflow)?)
                .ok_or_else(overflow)?;
            count = count.checked_add(1).ok_or_else(overflow)?;
            vector
                .grow(count, size_of::<(u64, crate::metadata::ManifestEdit)>())
                .ok_or_else(overflow)?;
        }
        position = end;
    }
    check(deadline)?;
    // Snapshot authority, a move's full input clone and lifecycle/validation
    // views may coexist. Journal-owned child vectors/file fields additionally
    // cover validator maps, cloned steps and manifest collection grows. The
    // outer replay Vec may overlap its final collected representation.
    let bytes = base
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(dynamic.checked_mul(8)?))
        .and_then(|bytes| bytes.checked_add(vector.peak_bytes.checked_mul(2)?))
        .ok_or_else(overflow)?;
    let authority = snapshot
        .authority_bytes()
        .and_then(|bytes| bytes.checked_add(added_authority))
        .ok_or_else(overflow)?;
    Ok(ManifestDecodeWorkspace { bytes, authority })
}

fn check(deadline: &OperationDeadline) -> MidgeResult<()> {
    if deadline.is_expired() {
        return Err(MidgeError::Timeout(
            "manifest workspace preflight exhausted original budget".into(),
        ));
    }
    Ok(())
}

fn overflow() -> MidgeError {
    MidgeError::ResourceLimit("manifest decoded workspace overflow".into())
}
