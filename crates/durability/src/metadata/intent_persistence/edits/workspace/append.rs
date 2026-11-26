//! Scalar summary of an intent append, without retaining decoded entries.

use super::{Entry, HeapFootprint, VectorLayout};
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

#[derive(Default)]
pub(super) struct Append {
    pub(super) len: usize,
    pub(super) capacity_bytes: usize,
    pub(super) peak_bytes: usize,
    pub(super) dynamic_bytes: usize,
    pub(super) publication_entries: usize,
    pub(super) max_single_normalization_scratch: usize,
}

impl Append {
    fn record(&mut self, entry: &Entry, layout: &mut VectorLayout) -> Option<()> {
        self.len = self.len.checked_add(1)?;
        layout.grow(self.len, Entry::ITEM_BYTES)?;
        let dynamic = entry.heap_bytes()?;
        self.dynamic_bytes = self.dynamic_bytes.checked_add(dynamic)?;
        if entry.is_publication() {
            self.publication_entries = self.publication_entries.checked_add(1)?;
            // For these current wire variants/layouts, named payload <= 2F,
            // F = inline enum + owned-field footprint: inline space covers
            // field/tag names and scalar maxima; runtime key bytes encode at
            // most twice, and names/binary keys at most once plus headers.
            // The locked serde/Rust Vec grows with old+new <= 3*payload;
            // retaining payload+frame needs <= 3*payload+17. Since F >= the
            // enum size, 8F covers both peaks. This is a current-toolchain
            // bound, not a general guarantee about future serde/Vec layouts.
            // Normalization serializes one entry at a time after replay.
            let scratch = Entry::ITEM_BYTES.checked_add(dynamic)?.checked_mul(8)?;
            self.max_single_normalization_scratch =
                self.max_single_normalization_scratch.max(scratch);
        }
        Some(())
    }
}

impl<'de> Deserialize<'de> for Append {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Entries;
        impl<'de> Visitor<'de> for Entries {
            type Value = Append;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an intent append sequence")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut entries: A) -> Result<Append, A::Error> {
                let mut layout =
                    VectorLayout::with_hint(entries.size_hint().unwrap_or(0), Entry::ITEM_BYTES)
                        .ok_or_else(|| de::Error::custom("intent append capacity overflow"))?;
                let mut append = Append::default();
                while let Some(entry) = entries.next_element::<Entry>()? {
                    append
                        .record(&entry, &mut layout)
                        .ok_or_else(|| de::Error::custom("intent append footprint overflow"))?;
                }
                append.capacity_bytes = layout
                    .capacity
                    .checked_mul(Entry::ITEM_BYTES)
                    .ok_or_else(|| de::Error::custom("intent append capacity overflow"))?;
                append.peak_bytes = layout.peak_bytes;
                Ok(append)
            }
        }
        decoder.deserialize_seq(Entries)
    }
}
