//! Count the catalog's allocating containers before decoding them. The first
//! pass retains only fixed-size scalar fields and counters, never catalog rows.
use super::{
    CatalogDecodeError, CatalogEntry, VersionProbe, WalPublicationCatalog, FORMAT_VERSION,
};
use crate::types::SharedGenerationId;
use crate::wal::format::ChainLink;
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::marker::PhantomData;

trait NestedAllocation {
    fn nested_bytes(&self) -> usize;
}

struct Counted<T> {
    len: usize,
    nested: usize,
    marker: PhantomData<T>,
}

impl<T> Counted<T> {
    fn bytes(&self, element_bytes: usize) -> usize {
        vector_peak(self.len, element_bytes).saturating_add(self.nested)
    }
}

fn vector_peak(len: usize, element_bytes: usize) -> usize {
    if len == 0 {
        return 0;
    }
    let capacity = len.checked_next_power_of_two().unwrap_or(usize::MAX).max(4);
    // Vec may temporarily retain its old allocation while growing. Account
    // for the new capacity and the previous half-capacity allocation.
    capacity
        .saturating_add(capacity / 2)
        .saturating_mul(element_bytes)
}

impl<'de, T: Deserialize<'de> + NestedAllocation> Deserialize<'de> for Counted<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Counter<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de> + NestedAllocation> Visitor<'de> for Counter<T> {
            type Value = Counted<T>;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a catalog array")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut count = Counted {
                    len: 0,
                    nested: 0,
                    marker: PhantomData,
                };
                while let Some(value) = sequence.next_element::<T>()? {
                    count.len = count.len.saturating_add(1);
                    count.nested = count.nested.saturating_add(value.nested_bytes());
                }
                Ok(count)
            }
        }
        deserializer.deserialize_seq(Counter(PhantomData))
    }
}

impl NestedAllocation for (u32, u64) {
    fn nested_bytes(&self) -> usize {
        0
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Facts {
    #[serde(rename = "link")]
    _link: ChainLink,
    #[serde(rename = "record_count")]
    _record_count: u64,
    #[serde(rename = "data_len")]
    _data_len: u64,
    max_seq_per_cf: Counted<(u32, u64)>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    #[serde(rename = "epoch")]
    _epoch: u64,
    #[serde(rename = "prev")]
    _prev: ChainLink,
    facts: Facts,
    #[serde(rename = "size_bytes")]
    _size_bytes: u64,
    #[serde(rename = "crc32c")]
    _crc32c: u32,
}

impl NestedAllocation for Entry {
    fn nested_bytes(&self) -> usize {
        self.facts
            .max_seq_per_cf
            .bytes(std::mem::size_of::<(u32, u64)>())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Catalog {
    #[serde(rename = "format_version")]
    _format_version: u32,
    #[serde(rename = "fencing_epoch")]
    _fencing_epoch: u64,
    #[serde(rename = "anchor")]
    _anchor: ChainLink,
    #[serde(rename = "next_segment_id")]
    _next_segment_id: u64,
    #[serde(rename = "seq_floor")]
    _seq_floor: u64,
    #[serde(rename = "retired_by")]
    _retired_by: SharedGenerationId,
    segments: Counted<Entry>,
}

impl WalPublicationCatalog {
    pub(super) fn validate_format(bytes: &[u8]) -> Result<(), CatalogDecodeError> {
        let probe: VersionProbe = serde_json::from_slice(bytes)
            .map_err(|error| CatalogDecodeError::Invalid(format!("JSON: {error}")))?;
        match probe.format_version {
            Some(FORMAT_VERSION) => Ok(()),
            Some(version) => Err(CatalogDecodeError::Incompatible(version)),
            None => Err(CatalogDecodeError::Invalid(
                "the document names no format version".into(),
            )),
        }
    }

    pub(crate) fn decode_allocation_bound(bytes: &[u8]) -> Result<usize, CatalogDecodeError> {
        Self::validate_format(bytes)?;
        let counted: Catalog = serde_json::from_slice(bytes)
            .map_err(|error| CatalogDecodeError::Invalid(format!("JSON: {error}")))?;
        Ok(counted
            .segments
            .bytes(std::mem::size_of::<CatalogEntry>())
            .saturating_add(4096))
    }

    pub(crate) fn append_allocation_bound(&self, run: &[CatalogEntry]) -> usize {
        let head = self.head().id;
        let (count, nested) = run.iter().filter(|entry| entry.id() > head).fold(
            (0_usize, 0_usize),
            |(count, bytes), entry| {
                (
                    count.saturating_add(1),
                    bytes.saturating_add(
                        entry
                            .facts
                            .max_seq_per_cf
                            .len()
                            .saturating_mul(std::mem::size_of::<(u32, u64)>()),
                    ),
                )
            },
        );
        if count == 0 {
            return 0;
        }
        let required = self.segments.len().saturating_add(count);
        let growth = if required > self.segments.capacity() {
            required.saturating_mul(std::mem::size_of::<CatalogEntry>())
        } else {
            0
        };
        growth
            .saturating_add(vector_peak(count, std::mem::size_of::<CatalogEntry>()))
            .saturating_add(nested)
    }
}
