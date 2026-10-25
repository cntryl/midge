# SST V5 decoder candidate (#918, #952)

The implementation adds canonical varint lengths, optional TTL bytes and
16-entry restart intervals. The restart offset high bit records whether its
full user key continues the previous interval. This preserves all versions
when an old snapshot seeks a group spanning multiple restarts. Point reads
count restart-key probes and decoded entries in `SstPointReadStats`.

The active unique-key regression writes a real 512-entry SST in one 64 KiB
block and requires no more than 23 entry decodes at representative positions.
The repeated-version regression separately bounds 128 versions by that group
plus the preceding interval and search probes. Production reverse scans decode
one interval at a time. The per-block hash index remains disabled pending a
measured comparison against the restart path; adding a cache allocation
without evidence would not satisfy #918 R3.

The named #935 qualification rows are `sst-v5-point-unique-64k` (650-entry
representative block: target <=24 entry decodes),
`sst-v5-point-repeated-versions` (report group length separately),
`sst-v5-reverse-limit-1` (no full-block key arena), and
`sst-v5-small-kv-footprint` (engine-produced SST bytes per logical byte).
Before/after timing, system footprint, cache-only hash evaluation, #937 row
deltas and broad engine/cloud campaigns remain pending qualification.

The existing cargo-fuzz package now has `sst_data_block_decode`,
`sst_index_decode`, and `sst_filter_decode` targets. They call the production
restart/entry, sparse-index and block-bloom parsers. The prior footer/range and
manifest/journal targets retain their paths. Each decoder target has at least
eight exact seeds from real files produced by the active Rust corpus
regression, including long keys, empty values, duplicate versions, TTL,
tombstones, inserts and range tombstones. Manifest and journal inputs are
production binary frames. Seed equality is checked during Cargo tests.

The fuzz budget is a qualification gate, not a compile claim. Run each target
for at least 60 seconds as a smoke gate, then 30 minutes for the release budget,
using the existing `cargo fuzz run TARGET -- -max_total_time=SECONDS`
facilities. No budget run is claimed by this implementation-first candidate.

The redundant lease `unsafe impl Send/Sync` declarations are removed. Generic
compile-time use sites require both concrete lease types to derive the auto
traits from their fields.
