//! Nonallocating footprints for the owned fields in binary metadata.
//!
//! These visitors consume the same borrowed `MessagePack` values as the typed
//! decoder. They retain counts, not decoded strings, vectors or file metadata.
//! Bounds describe requested allocation payloads, not allocator/RSS overhead.

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::marker::PhantomData;
use std::mem::size_of;

pub(crate) trait HeapFootprint {
    const ITEM_BYTES: usize;
    fn heap_bytes(&self) -> Option<usize>;
}

/// Vec's requested capacity, including the old allocation during a grow.
#[derive(Default)]
pub(crate) struct VectorLayout {
    pub(crate) capacity: usize,
    pub(crate) peak_bytes: usize,
}

impl VectorLayout {
    pub(crate) fn with_hint(hint: usize, item_bytes: usize) -> Option<Self> {
        // serde 1.0's cautious Vec preallocation is capped at one MiB. The
        // locked decoder supplies an exact MessagePack array length.
        let capacity = hint.min((1024 * 1024) / item_bytes);
        Some(Self {
            capacity,
            peak_bytes: capacity.checked_mul(item_bytes)?,
        })
    }

    pub(crate) fn grow(&mut self, required: usize, item_bytes: usize) -> Option<usize> {
        let old_bytes = self.capacity.checked_mul(item_bytes)?;
        if required <= self.capacity {
            return Some(old_bytes);
        }
        // alloc::RawVec's amortized grow, also used by Vec::extend(IntoIter).
        let minimum = if item_bytes == 1 {
            8
        } else if item_bytes <= 1024 {
            4
        } else {
            1
        };
        self.capacity = self.capacity.checked_mul(2)?.max(required).max(minimum);
        let new_bytes = self.capacity.checked_mul(item_bytes)?;
        let overlap = old_bytes.checked_add(new_bytes)?;
        self.peak_bytes = self.peak_bytes.max(overlap);
        Some(overlap)
    }
}

pub(crate) struct Sequence<T> {
    pub(crate) len: usize,
    pub(crate) peak_bytes: usize,
    pub(crate) dynamic_bytes: usize,
    marker: PhantomData<T>,
}

impl<T> Default for Sequence<T> {
    fn default() -> Self {
        Self {
            len: 0,
            peak_bytes: 0,
            dynamic_bytes: 0,
            marker: PhantomData,
        }
    }
}

impl<T: HeapFootprint> HeapFootprint for Sequence<T> {
    const ITEM_BYTES: usize = size_of::<Vec<()>>();

    fn heap_bytes(&self) -> Option<usize> {
        // Child peaks are conservatively retained in dynamic_bytes, including
        // an individual child's transient decode reallocations.
        self.peak_bytes.checked_add(self.dynamic_bytes)
    }
}

impl<'de, T: Deserialize<'de> + HeapFootprint> Deserialize<'de> for Sequence<T> {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Items<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de> + HeapFootprint> Visitor<'de> for Items<T> {
            type Value = Sequence<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a metadata sequence")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut values: A) -> Result<Self::Value, A::Error> {
                let mut layout =
                    VectorLayout::with_hint(values.size_hint().unwrap_or(0), T::ITEM_BYTES)
                        .ok_or_else(|| de::Error::custom("metadata vector capacity overflow"))?;
                let mut len = 0_usize;
                let mut dynamic_bytes = 0_usize;
                while let Some(value) = values.next_element::<T>()? {
                    len = len
                        .checked_add(1)
                        .ok_or_else(|| de::Error::custom("metadata item count overflow"))?;
                    layout
                        .grow(len, T::ITEM_BYTES)
                        .ok_or_else(|| de::Error::custom("metadata vector capacity overflow"))?;
                    dynamic_bytes = dynamic_bytes
                        .checked_add(value.heap_bytes().ok_or_else(|| {
                            de::Error::custom("metadata field allocation overflow")
                        })?)
                        .ok_or_else(|| de::Error::custom("metadata allocation sum overflow"))?;
                }
                Ok(Sequence {
                    len,
                    peak_bytes: layout.peak_bytes,
                    dynamic_bytes,
                    marker: PhantomData,
                })
            }
        }

        decoder.deserialize_seq(Items(PhantomData))
    }
}

/// String's borrowed decoder requests exactly its UTF-8 byte length.
#[derive(Default)]
pub(crate) struct Name(usize);

impl HeapFootprint for Name {
    const ITEM_BYTES: usize = size_of::<String>();

    fn heap_bytes(&self) -> Option<usize> {
        Some(self.0)
    }
}

impl<'de> Deserialize<'de> for Name {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Text;
        impl Visitor<'_> for Text {
            type Value = Name;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a metadata string")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Name, E> {
                Ok(Name(value.len()))
            }

            fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Name, E> {
                std::str::from_utf8(value).map_err(E::custom)?;
                Ok(Name(value.len()))
            }
        }

        decoder.deserialize_string(Text)
    }
}

#[derive(Deserialize)]
pub(crate) struct Byte(#[serde(deserialize_with = "ignored_byte")] ());

fn ignored_byte<'de, D: Deserializer<'de>>(decoder: D) -> Result<(), D::Error> {
    u8::deserialize(decoder).map(|_| ())
}

impl HeapFootprint for Byte {
    const ITEM_BYTES: usize = 1;

    fn heap_bytes(&self) -> Option<usize> {
        Some(0)
    }
}

/// Manifest key bounds use an owned byte buffer instead of a Vec visitor.
#[derive(Default)]
pub(crate) struct BinaryKey(usize);

impl HeapFootprint for BinaryKey {
    const ITEM_BYTES: usize = size_of::<Vec<u8>>();

    fn heap_bytes(&self) -> Option<usize> {
        Some(self.0)
    }
}

impl<'de> Deserialize<'de> for BinaryKey {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Key;
        impl Visitor<'_> for Key {
            type Value = BinaryKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a metadata byte buffer")
            }

            fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<BinaryKey, E> {
                Ok(BinaryKey(value.len()))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<BinaryKey, E> {
                // key_bounds::Key also accepts hex strings under the binary
                // decoder. Validate them without allocating hex::decode output.
                if !value.len().is_multiple_of(2)
                    || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
                {
                    return Err(E::custom("invalid hex key bound"));
                }
                Ok(BinaryKey(value.len() / 2))
            }
        }

        decoder.deserialize_byte_buf(Key)
    }
}

#[derive(Deserialize)]
pub(crate) struct MetadataFile {
    name: Name,
    #[serde(rename = "level")]
    _level: de::IgnoredAny,
    #[serde(rename = "size_bytes")]
    _size_bytes: de::IgnoredAny,
    #[serde(default, rename = "content_crc32c")]
    _content_crc32c: de::IgnoredAny,
    #[serde(default, rename = "cf_id")]
    _cf_id: de::IgnoredAny,
    #[serde(default, rename = "sst_seq")]
    _sst_seq: de::IgnoredAny,
    #[serde(default)]
    smallest_key: Option<BinaryKey>,
    #[serde(default)]
    largest_key: Option<BinaryKey>,
    #[serde(default, rename = "smallest_seq")]
    _smallest_seq: de::IgnoredAny,
    #[serde(default, rename = "largest_seq")]
    _largest_seq: de::IgnoredAny,
    #[serde(default, rename = "key_bounds_complete")]
    _key_bounds_complete: de::IgnoredAny,
    #[serde(default, rename = "earliest_expiration_millis")]
    _earliest_expiration_millis: de::IgnoredAny,
    #[serde(default, rename = "sublevel")]
    _sublevel: de::IgnoredAny,
}

impl HeapFootprint for MetadataFile {
    const ITEM_BYTES: usize = size_of::<crate::metadata::FileMeta>();

    fn heap_bytes(&self) -> Option<usize> {
        // #[serde(skip)] read_count still allocates a default Arc<AtomicU64>.
        let read_count = 2 * size_of::<usize>() + size_of::<std::sync::atomic::AtomicU64>();
        self.name
            .heap_bytes()?
            .checked_add(optional_heap(self.smallest_key.as_ref())?)?
            .checked_add(optional_heap(self.largest_key.as_ref())?)?
            .checked_add(read_count)
    }
}

fn optional_heap<T: HeapFootprint>(value: Option<&T>) -> Option<usize> {
    value.map_or(Some(0), HeapFootprint::heap_bytes)
}
