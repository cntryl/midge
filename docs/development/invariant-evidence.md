# Exact Invariant Evidence

This reference inventory covers all 65 IDs in the [invariant catalog](invariant-catalog.md), including the groups omitted from the earlier WAL preservation selection. It indexes production symbols, active Rust regressions, actual assertion or behavior-helper call markers, fixture assumptions and explicit gaps.

The refresh uses committed prerequisite `2494eff5e9b2c727e4f9656e83a6e8f54e2d9888` plus the exact [source postimages](invariant-evidence-source-sha256.json) read while sibling implementation slices were being integrated. It is a construction and reference audit. This batch has not run the linked regressions or mutation experiments, and it carries no crash-campaign, benchmark, native-provider or release qualification claim.

The [JSON inventory](invariant-evidence.json) retains historical hosted qualification objects unchanged and scoped to their recorded revisions. Those outcomes do not qualify the current branch. Unsupported references to removed test targets were removed instead of being presented as surviving evidence. PROG-1 and PROG-2 remain named assurance gaps.

Local, memory and CloudSimulated fixtures remain distinct from Sqrzl or live providers. Cloud reads require current lease authority after #923; local retained reads and still-running runtime diagnostics have separate availability contracts. Concurrent captured memtable effects under #906 become visible only at the loop-owned ordered cut.

## AUTH-1

Production: [src/runtime/event_loop/fencing.rs:15](../../src/runtime/event_loop/fencing.rs#L15) `RuntimeFence::check_health`; [src/runtime/event_loop/fencing.rs:43](../../src/runtime/event_loop/fencing.rs#L43) `RuntimeFence::validate_within`; [src/lease/heartbeat.rs:142](../../src/lease/heartbeat.rs#L142) `run_watchdog_worker`.

Active regression references (not executed by this refresh):

- [src/runtime/event_loop/fencing.rs:245](../../src/runtime/event_loop/fencing.rs#L245) `should_reject_expired_validity_when_cached_health_is_true`: Expires actual LeaseValidity while cached health is true; authority check returns Fenced. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 267.
- [src/lease/cloud/tests.rs:2605](../../src/lease/cloud/tests.rs#L2605) `should_reject_metadata_pointer_cas_that_lands_after_successor_acquires`: Holds predecessor conditional pointer CAS, releases predecessor and acquires successor; stale publication fails and successor committed pointer remains exact. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2668, 2672.
- [tests/cloud_provider_engine_qualification/authority_publication.rs:56](../../tests/cloud_provider_engine_qualification/authority_publication.rs#L56) `should_preserve_acknowledged_history_when_predecessor_wal_resumes_after_takeover_and_cleanup`: Native S3 predecessor loses real lease validity; a higher epoch successor publishes and reclaims WAL/SST state before held immutable WAL completion. Strict predecessor write is fenced and no subsequent control mutation is forwarded. Scope: Three bounded native S3 schedules against pinned local Sqrzl: before upstream PUT, after upstream HTTP 200, and lost successful response. Genuine lease expiry; shortened predecessor shutdown drain budget only. Successor is quiesced before predecessor release to isolate catalog mutation. No exhaustive simulation or live-provider qualification claim. Assertion/helper call markers: 488, 496, 503, 519.

Preconditions: A real validity object and conditional provider mock are required. The delayed-CAS schedule has positive request/ownership prerequisites.

Uncovered boundary: These are bounded expiry/takeover schedules. A previously submitted provider mutation remains outcome-ambiguous; the tests do not claim transport cancellation proves no commit.

Catalog review: Authority and loop publication remain direct pipeline dependencies; read-retention conditions distinguish local storage, cloud lease authority and terminal lifecycle fences. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## AUTH-2

Production: [src/runtime/event_loop/fencing.rs:43](../../src/runtime/event_loop/fencing.rs#L43) `RuntimeFence::validate_within`; [src/runtime/event_loop/write_batch.rs:117](../../src/runtime/event_loop/write_batch.rs#L117) `ensure_l0_write_admission`.

Active regression references (not executed by this refresh):

- [src/runtime/event_loop/fencing.rs:154](../../src/runtime/event_loop/fencing.rs#L154) `should_not_poison_lease_health_when_leader_store_read_fails_transiently`: Transient provider validation returns Busy, preserves healthy=true, and subsequent cached health check succeeds. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 162, 163, 164.
- [src/runtime/event_loop/fencing.rs:168](../../src/runtime/event_loop/fencing.rs#L168) `should_report_timeout_without_poisoning_when_lease_validation_times_out`: Requires typed Timeout and healthy=true. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 176, 177.
- [src/runtime/event_loop/fencing.rs:181](../../src/runtime/event_loop/fencing.rs#L181) `should_poison_lease_health_when_ownership_is_lost`: Requires typed Fenced and healthy=false. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 189, 190.
- [src/engine/tests/cloud_read_fencing.rs:97](../../src/engine/tests/cloud_read_fencing.rs#L97) `should_keep_retained_reads_and_diagnostics_available_after_wal_io_fence_with_live_lease`: Local and CloudSimulated WAL sync failure leaves retained point/scan reads and runtime diagnostics usable while the cloud read lease is still valid. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 130, 134, 138, 140, 141, 142, 150, 151.
- [src/engine/tests/cloud_read_fencing.rs:5](../../src/engine/tests/cloud_read_fencing.rs#L5) `should_invalidate_cloud_reads_when_monotonic_lease_expires_before_notification`: Expired monotonic authority rejects cloud cached, missing, own-write, fresh and lazy scan reads while retained local reads and diagnostics remain available. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 32, 33, 45, 54, 56, 62, 63, 67, 73, 74, 75, 76, 77, 78, 79, 90.
- [src/engine/tests/drop_reaper.rs:28](../../src/engine/tests/drop_reaper.rs#L28) `should_fence_outliving_reads_scans_and_commit_when_drop_transaction_wait_expires`: A terminal lifecycle fence rejects outliving point/scan reads and commit after bounded Drop transaction waiting expires. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 47, 54, 55, 56, 57, 62, 63.

Preconditions: Local retained files remain readable after writer-only failure. Cloud reads additionally require the holder's current monotonic lease and healthy heartbeat; a successor may reclaim remote sources. Terminal lifecycle fencing closes reads. Diagnostic availability is scoped to a still-running runtime.

Uncovered boundary: The matrix covers retained point/scan paths, monotonic cloud expiry, local/cloud WAL sync failure and terminal Drop fencing. It does not promise cloud read availability after lease loss or after runtime teardown; native provider takeover/read-reclamation schedules remain separately qualified.

Catalog review: Authority and loop publication remain direct pipeline dependencies; read-retention conditions distinguish local storage, cloud lease authority and terminal lifecycle fences. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## AUTH-3

Production: [src/runtime/event_loop/compaction.rs:680](../../src/runtime/event_loop/compaction.rs#L680) `CompactionCoordinator::start_publication`; [src/runtime/event_loop/flush_pipeline/publication.rs:263](../../src/runtime/event_loop/flush_pipeline/publication.rs#L263) `commit_flush_metadata`; [src/runtime/actors/compaction/publication.rs:41](../../src/runtime/actors/compaction/publication.rs#L41) `CompactionPublishTask`.

Active regression references (not executed by this refresh):

- [tests/fault_injection.rs:559](../../tests/fault_injection.rs#L559) `should_ignore_orphan_sst_when_flush_intent_log_save_hits_no_space`: Actual intent-save NoSpace failure followed by reopen retains all ten accepted values and runtime sst_count=0. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 594.
- [tests/fault_injection.rs:4501](../../tests/fault_injection.rs#L4501) `should_retain_input_ssts_given_compaction_failure_before_manifest_publish`: Actual process-abort schedule resolves to the old authoritative manifest/input set and exact acknowledged rows. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 4512, 4516, 4528, 4532, 4543.
- [src/runtime/event_loop/wal_pipeline/tests.rs:258](../../src/runtime/event_loop/wal_pipeline/tests.rs#L258) `should_keep_wal_frontiers_and_visibility_unchanged_until_runtime_owner_drains_completion`: Actual physical WAL fsync completes without advancing runtime durability, visibility or memtable accounting until the owning loop drains its receipt. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 284, 292, 296, 297, 298, 299, 303, 304, 305.
- [src/runtime/event_loop/memtable_apply/tests.rs:85](../../src/runtime/event_loop/memtable_apply/tests.rs#L85) `should_publish_one_ordered_cut_when_later_apply_finishes_before_first_unit`: A later worker physically writes its unit while a lower unit is held; loop-owned applied cut and total memory accounting stay unchanged until the complete group settles. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 110, 111, 112, 113, 114, 115, 116, 117, 119, 123, 127, 129, 131, 141, 145.

Preconditions: Real writer-worker fsync and concurrent captured memtable effects are observed before loop publication. RuntimeState is not shared with workers; they return typed receipts. Existing crash/no-space publication tests retain their separate fixture scopes.

Uncovered boundary: These active behavioral pins cover WAL frontiers, ordered visibility and exact group memory bookkeeping. They are not a structural proof that every possible authoritative field has one owner; Rust ownership and source review remain part of that boundary.

Catalog review: Authority and loop publication remain direct pipeline dependencies; read-retention conditions distinguish local storage, cloud lease authority and terminal lifecycle fences. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## AUTH-4

Production: [src/runtime/event_loop/manifest.rs:338](../../src/runtime/event_loop/manifest.rs#L338) `record_ddl_authority_ambiguity`; [src/runtime/ddl.rs:615](../../src/runtime/ddl.rs#L615) `reconcile_prepared_within`; [src/metadata/manifest.rs:309](../../src/metadata/manifest.rs#L309) `Manifest::next_cf_id`.

Active regression references (not executed by this refresh):

- [src/runtime/ddl.rs:1418](../../src/runtime/ddl.rs#L1418) `should_reject_paused_create_cas_after_successor_fences_ddl_registry`: Shared actual conditional-registry schedule rejects stale create CAS after successor epoch and preserves successor visibility; drop variant is adjacent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Listed markers are the actual behavior-assertion helper calls in this function. Assertion/helper call markers: 1419.
- [tests/column_families.rs:541](../../tests/column_families.rs#L541) `should_allocate_monotonic_column_family_ids_given_deleted_column_family_when_creating`: Destructive drop/recreate has old key absent and newly written key present. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 564, 565, 574, 575.
- [src/metadata/manifest/tests.rs:268](../../src/metadata/manifest/tests.rs#L268) `should_replay_original_family_creation_without_reviving_a_dropped_family`: Replaying the original create after its permanent drop tombstone is installed is idempotent and leaves the serialized manifest and Dropped frontier unchanged. Scope: Pure manifest transition and serialized state comparison; engine restart lifecycle is separately exercised by column-family integration tests Assertion/helper call markers: 297, 301, 302.
- [tests/fault_injection.rs:9461](../../tests/fault_injection.rs#L9461) `should_abort_prior_epoch_ambiguous_ddl_when_reopening_after_crash_before_remote_cas_submission`: A new lease epoch aborts an unsubmitted prior-epoch ambiguous prepare; the family stays absent, prepare is removed and the registry contains zero invented operations. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 9484, 9488, 9497, 9502, 9512, 9513, 9524, 9525.

Preconditions: DDL crash/conditional-provider tests require failpoints; common integration modes are memory, local, and filesystem CloudSimulated.

Uncovered boundary: The drop/recreate test asserts data isolation, not a direct numeric inequality of returned CF IDs. ID nonreuse also depends on next_cf_id considering retained deleted entries. Native IAM/quota semantics are outside these fixtures.

Catalog review: Authority and loop publication remain direct pipeline dependencies; read-retention conditions distinguish local storage, cloud lease authority and terminal lifecycle fences. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## TX-1

Production: [src/engine/mod.rs:352](../../src/engine/mod.rs#L352) `Engine::begin_tx`; [src/engine/mod.rs:427](../../src/engine/mod.rs#L427) `acquire_transaction_snapshot`; [src/runtime/snapshot_pins.rs:98](../../src/runtime/snapshot_pins.rs#L98) `SnapshotPinRegistry::register`; [src/runtime/snapshot_pins.rs:223](../../src/runtime/snapshot_pins.rs#L223) `begin_acquisition`; [src/runtime/handle.rs:162](../../src/runtime/handle.rs#L162) `read_with_authority`.

Active regression references (not executed by this refresh):

- [tests/transactions.rs:3625](../../tests/transactions.rs#L3625) `should_return_old_value_given_snapshot_before_write_when_reading`: Current reader sees v2 after later commit while the preexisting transaction still sees v1. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 3655, 3659.
- [src/runtime/snapshot_pins.rs:441](../../src/runtime/snapshot_pins.rs#L441) `should_track_pinned_sst_names_when_snapshot_pin_registered`: Registry has one active pin, horizon 42, and both named SSTs pinned. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 449, 450, 451, 453, 454.
- [src/runtime/snapshot_pins.rs:733](../../src/runtime/snapshot_pins.rs#L733) `should_retain_timed_out_snapshot_pin_until_unregister`: Warning does not remove active pin, horizon, or pinned SST. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 736, 743, 744, 745, 747.
- [tests/snapshot_spill_pressure.rs:251](../../tests/snapshot_spill_pressure.rs#L251) `should_preserve_snapshot_isolation_when_spill_pressure_is_released`: Frozen old scans and point reads survive TTL expiry, sixteen acknowledged mixed-history compactions and actual spill saturation. Scope: One deterministic public-API filesystem-backed CloudSimulated schedule; 16 MiB engine budget, 2 MiB local capacity, 128 KiB transaction pool, controlled TTL time, sixteen history rounds, actual spill saturation and exact clean reopen. No native-provider, crash, whole-process RSS or exhaustive-concurrency proof. Assertion/helper call markers: 271, 304, 41, 44, 50, 55.
- [tests/cloud_provider_engine_qualification/authority_publication/read_fencing.rs:99](../../tests/cloud_provider_engine_qualification/authority_publication/read_fencing.rs#L99) `should_fence_predecessor_reads_when_successor_reclaims_remote_snapshot_files`: Actual successor compaction deletes every original remote SST after real lease expiry. Existing predecessor point reads, scan creation and pre-created forward/reverse iterators return sticky Fenced; successor ordered rows and point values are exact before and after reopen. Scope: Native S3, independent caches and real HTTP ACK/deletion observations against pinned Sqrzl. One takeover schedule and same-process successor reopen; not exhaustive simulation or independent-process recovery. Assertion/helper call markers: 119, 135, 152, 154, 156, 162, 163, 167, 195, 197, 199.
- [src/engine/tests/cloud_read_fencing.rs:5](../../src/engine/tests/cloud_read_fencing.rs#L5) `should_invalidate_cloud_reads_when_monotonic_lease_expires_before_notification`: Cloud cached/own-write/missing reads and pre-created lazy scans are fenced after monotonic expiry without a notification. Expired local snapshots remain readable; new cloud transactions are rejected. Scope: Deterministic public Engine API over Local and CloudSimulated, stopped heartbeat and expired actual LeaseValidity; no native transport or cross-owner GC claim. Assertion/helper call markers: 32, 33, 45, 54, 56, 62, 63, 67, 73, 74, 75, 76, 77, 78, 79, 90.
- [src/runtime/handle/read_authority_tests.rs:24](../../src/runtime/handle/read_authority_tests.rs#L24) `should_discard_read_outcomes_when_lease_expires_during_the_read`: Read results, missing-file errors and end-of-range outcomes are replaced by Fenced if shared lease validity expires during the read closure. Scope: Focused synchronous read-guard callback, not an actual SST I/O timing or native network cancellation fixture. Assertion/helper call markers: 36, 37, 38.

Preconditions: Public snapshot test runs common memory/local/CloudSimulated modes. Pin tests operate the real registry with supplied names; actual pinned compaction is separately mapped at SST-6. The composed spill-pressure regression uses the controlled TTL clock and actual simulated-cloud spill/GC files described in snapshot-spill-pressure-fixture.md. Cloud snapshot retention is limited to the current lease holder; read-fencing regressions cover invalidation on authority loss.

Uncovered boundary: Representative snapshots and the pin registry do not enumerate all snapshot-acquisition/GC interleavings. The API's single CF ID is a structural limit rather than cross-CF atomicity evidence. Remote reader retention across takeover is not provided; successor reclamation is permitted and predecessor cloud reads are invalidated.

Catalog review: Transaction horizons, sequence filtering, conditional validation and ordered whole-unit publication are WAL/apply dependencies; no serializable or cross-family transaction guarantee is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## TX-2

Production: [src/runtime/read_snapshot/snapshot.rs:172](../../src/runtime/read_snapshot/snapshot.rs#L172) `ReadSnapshot::get_bytes`; [src/runtime/read_snapshot/snapshot.rs:290](../../src/runtime/read_snapshot/snapshot.rs#L290) `ReadSnapshot::range_scan`; [src/engine/api/transaction.rs:901](../../src/engine/api/transaction.rs#L901) `Transaction::get`; [src/engine/api/transaction.rs:955](../../src/engine/api/transaction.rs#L955) `Transaction::scan`.

Active regression references (not executed by this refresh):

- [tests/transactions.rs:438](../../tests/transactions.rs#L438) `should_preserve_write_set_semantics_given_put_delete_put_sequence_when_reading_own_writes`: Put/delete/put intent sequence returns the final value three. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 453.
- [src/runtime/read_snapshot/tests.rs:703](../../src/runtime/read_snapshot/tests.rs#L703) `should_match_brute_force_oracle_when_scans_cross_overlapping_range_tombstones`: Forty constructed active/immutable histories compare exact eligible rows to a separate brute-force oracle at horizons 3, 5 and MAX, both directions, bounds, and optional limit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 800.

Preconditions: Oracle builds genuine memtables with deterministic generated versions/tombstones, but no SST I/O. Public own-intent test uses all common modes.

Uncovered boundary: The oracle is bounded and covers active/immutable selection; it does not independently prove every SST version tie or heavily spilled intent lookup path.

Catalog review: Transaction horizons, sequence filtering, conditional validation and ordered whole-unit publication are WAL/apply dependencies; no serializable or cross-family transaction guarantee is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## TX-3

Production: [src/runtime/read_snapshot/snapshot.rs:172](../../src/runtime/read_snapshot/snapshot.rs#L172) `ReadSnapshot::get_bytes`; [src/runtime/read_snapshot/snapshot.rs:290](../../src/runtime/read_snapshot/snapshot.rs#L290) `ReadSnapshot::range_scan`; [src/compaction/mod.rs:42](../../src/compaction/mod.rs#L42) `execute_compaction`; [src/engine/api/transaction.rs:519](../../src/engine/api/transaction.rs#L519) `Transaction::delete_range`.

Active regression references (not executed by this refresh):

- [tests/transactions.rs:4960](../../tests/transactions.rs#L4960) `should_persist_range_tombstone_when_flush_survives_restart`: Real local flush/shutdown/reopen preserves a and suppresses b/c after deletion [b,d). Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 5002, 5003, 5004.
- [tests/transactions.rs:4918](../../tests/transactions.rs#L4918) `should_use_transaction_snapshot_time_for_ttl_visibility`: After real TTL expiry the earlier snapshot still sees value and a new snapshot sees None. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 4948, 4956.
- [src/compaction/mod.rs:1046](../../src/compaction/mod.rs#L1046) `should_preserve_expired_ttl_metadata_given_compaction_then_mask_at_read_time`: Actual SST compaction retains raw value/sequence/expiration, while timed read returns Tombstone(9). Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1075, 1082.
- [tests/engine_api.rs:1975](../../tests/engine_api.rs#L1975) `should_return_correct_boundary_rows_when_reverse_scanning_with_explicit_start_and_end`: Reverse [b,e) returns exactly d,c,b. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2005.
- [tests/snapshot_spill_pressure.rs:251](../../tests/snapshot_spill_pressure.rs#L251) `should_preserve_snapshot_isolation_when_spill_pressure_is_released`: A controlled clock expires only the first eight keys for a new reader before overwrites; the original snapshot retains all thirty-two original values through compaction. Scope: One deterministic public-API filesystem-backed CloudSimulated schedule; 16 MiB engine budget, 2 MiB local capacity, 128 KiB transaction pool, controlled TTL time, sixteen history rounds, actual spill saturation and exact clean reopen. No native-provider, crash, whole-process RSS or exhaustive-concurrency proof. Assertion/helper call markers: 271, 304, 41, 44, 50, 55.

Preconditions: The restart and TTL tests use real local Engine/files; TTL test uses elapsed wall time. Reverse API test runs common modes; compaction test writes/reads real SSTs. The composed spill-pressure regression uses the controlled TTL clock and actual simulated-cloud spill/GC files described in snapshot-spill-pressure-fixture.md.

Uncovered boundary: This is a representative combination of restart, TTL, compaction and direction evidence, not an exhaustive cross-product of every memtable/SST/tombstone/TTL state. Binary-prefix evidence is also mapped at TX-7.

Catalog review: Transaction horizons, sequence filtering, conditional validation and ordered whole-unit publication are WAL/apply dependencies; no serializable or cross-family transaction guarantee is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## TX-4

Production: [src/runtime/actors/wal/transaction.rs:590](../../src/runtime/actors/wal/transaction.rs#L590) `allocate_transaction_sequences`; [src/runtime/actors/wal/transaction.rs:626](../../src/runtime/actors/wal/transaction.rs#L626) `build_transaction_wal_batch`; [src/runtime/event_loop/write_batch.rs:803](../../src/runtime/event_loop/write_batch.rs#L803) `finish_drained_write_after_publish`; [src/wal/recovery/streaming.rs:278](../../src/wal/recovery/streaming.rs#L278) `replay_wal_with_options`.

Active regression references (not executed by this refresh):

- [src/wal/recovery/tests.rs:1056](../../src/wal/recovery/tests.rs#L1056) `should_recover_only_committed_transaction_given_split_wal_records_when_commit_marker_is_missing`: Real WAL with synced TxnBegin and Put but no commit marker reports two decoded records and creates no recovered CF memtable. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1102, 1103.
- [src/runtime/event_loop/memtable_apply/tests.rs:85](../../src/runtime/event_loop/memtable_apply/tests.rs#L85) `should_publish_one_ordered_cut_when_later_apply_finishes_before_first_unit`: Concurrent same-group effects remain hidden behind one ordered cut; no reader sees the physically completed later unit ahead of the held lower unit. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 110, 111, 112, 113, 114, 115, 116, 117, 119, 123, 127, 129, 131, 141, 145.
- [src/runtime/event_loop/memtable_apply/tests.rs:151](../../src/runtime/event_loop/memtable_apply/tests.rs#L151) `should_fence_every_family_and_recover_whole_group_when_authority_is_lost_during_apply`: Authority loss during concurrent apply preserves the old cut and permanently rejects cross-family BestEffort admission; strict reopen recovers every key of both WAL units. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 171, 178, 188, 208, 215, 216, 230.

Preconditions: Live visibility test uses memory mode and a coordinated reader/writer; replay test uses actual FsWalWriterIo files.

Uncovered boundary: Live visibility and incomplete replay are separate schedules. These do not prove all maximum-size, spilled or crash-position multi-key transactions; acknowledged spill atomicity has separate fault tests.

Catalog review: Transaction horizons, sequence filtering, conditional validation and ordered whole-unit publication are WAL/apply dependencies; no serializable or cross-family transaction guarantee is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## TX-5

Production: [src/engine/api/transaction.rs:559](../../src/engine/api/transaction.rs#L559) `Transaction::assert_value`; [src/engine/api/transaction.rs:782](../../src/engine/api/transaction.rs#L782) `validate_assertions`; [src/runtime/actors/wal/transaction.rs:457](../../src/runtime/actors/wal/transaction.rs#L457) `validate_transaction_preconditions`.

Active regression references (not executed by this refresh):

- [tests/transactions.rs:4270](../../tests/transactions.rs#L4270) `should_abort_second_commit_given_conflicting_writes_when_abort_on_write_conflict_enabled`: Second conflicting commit is WriteConflict and final value belongs to first commit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 4297, 4308.
- [tests/transactions.rs:2150](../../tests/transactions.rs#L2150) `should_enforce_assertion_conflict_even_under_last_write_wins_policy`: Assertion plus staged write conflicts with a later update despite LastWriteWins and returns WriteConflict. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2169, 2195.
- [tests/transactions.rs:2202](../../tests/transactions.rs#L2202) `should_validate_assertion_only_commit_without_allocating_a_sequence`: Successful assertion-only commit leaves sequence metric unchanged. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2227, 2231.

Preconditions: Strict write-conflict test runs all common modes; explicit LastWriteWins and assertion-only controls use actual memory Engine.

Uncovered boundary: These tests assert final values, typed conflicts and no assertion-only sequence allocation. They do not directly observe physical WAL append count for every rejected assertion; ordering before publication is source-backed.

Catalog review: Transaction horizons, sequence filtering, conditional validation and ordered whole-unit publication are WAL/apply dependencies; no serializable or cross-family transaction guarantee is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## TX-6

Production: [src/engine/api/transaction.rs:821](../../src/engine/api/transaction.rs#L821) `Transaction::rollback`; [src/engine/api/transaction.rs:826](../../src/engine/api/transaction.rs#L826) `unregister_snapshot`; [src/engine/api/transaction.rs:624](../../src/engine/api/transaction.rs#L624) `Transaction::commit`.

Active regression references (not executed by this refresh):

- [tests/transactions.rs:298](../../tests/transactions.rs#L298) `should_rollback_all_writes_given_multiple_operations_when_dropped`: Dropping uncommitted overwrite/insertion keeps original value and leaves new key absent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 326, 330.
- [tests/transactions.rs:5303](../../tests/transactions.rs#L5303) `should_unregister_snapshot_when_rollback_ends_transaction`: Explicit rollback unregisters snapshot; adjacent drop case is at 5252. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Listed markers are the actual behavior-assertion helper calls in this function. Assertion/helper call markers: 5313, 5318.
- [tests/snapshot_spill_pressure.rs:251](../../tests/snapshot_spill_pressure.rs#L251) `should_preserve_snapshot_isolation_when_spill_pressure_is_released`: Actual uncommitted spill keys and the capacity-rejected bounded write remain absent from other transactions; releasing the spill owner permits the same write without invalidating the old snapshot. Scope: One deterministic public-API filesystem-backed CloudSimulated schedule; 16 MiB engine budget, 2 MiB local capacity, 128 KiB transaction pool, controlled TTL time, sixteen history rounds, actual spill saturation and exact clean reopen. No native-provider, crash, whole-process RSS or exhaustive-concurrency proof. Assertion/helper call markers: 271, 304, 41, 44, 50, 55.

Preconditions: Actual public transaction behavior in common modes; client-side intents never reach a commit call. The composed spill-pressure regression uses the controlled TTL clock and actual simulated-cloud spill/GC files described in snapshot-spill-pressure-fixture.md.

Uncovered boundary: No claim that rolling back an already accepted/timed-out runtime commit cancels it. That is a different ownership contract (WAL-2/RES-5).

Catalog review: Transaction horizons, sequence filtering, conditional validation and ordered whole-unit publication are WAL/apply dependencies; no serializable or cross-family transaction guarantee is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## TX-7

Production: [src/engine/api/transaction.rs:955](../../src/engine/api/transaction.rs#L955) `Transaction::scan`; [src/runtime/read_snapshot/snapshot.rs:290](../../src/runtime/read_snapshot/snapshot.rs#L290) `ReadSnapshot::range_scan`; [src/engine/api/iterator.rs:106](../../src/engine/api/iterator.rs#L106) `try_collect`.

Active regression references (not executed by this refresh):

- [tests/transactions.rs:4631](../../tests/transactions.rs#L4631) `should_honor_prefix_upper_bound_given_prefix_ending_in_ff_when_scanning`: Real local binary-key query with prefix [0x10,0xff] returns exact expected key set. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 4655.
- [src/runtime/read_snapshot/tests.rs:703](../../src/runtime/read_snapshot/tests.rs#L703) `should_match_brute_force_oracle_when_scans_cross_overlapping_range_tombstones`: Exact oracle comparisons include start/end, forward/reverse and limit 3. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 800.
- [src/engine/api/iterator.rs:192](../../src/engine/api/iterator.rs#L192) `should_replay_terminal_error_without_marking_iterator_exhausted`: Constructed corruption row is returned on both calls; state=Failed, failed=true, exhausted=false. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 202, 205, 208, 209, 210.

Preconditions: Binary-prefix test is public local API; bound/direction/limit oracle is memtable-only; failed-iterator test supplies an error iterator.

Uncovered boundary: The failed-state unit does not itself corrupt a real storage object. Actual remote corrupt-block qualification belongs to broader storage tests; source-defined coverage here is not universal scan fault coverage.

Catalog review: Transaction horizons, sequence filtering, conditional validation and ordered whole-unit publication are WAL/apply dependencies; no serializable or cross-family transaction guarantee is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## TX-8

Production: [src/engine/api/transaction.rs:408](../../src/engine/api/transaction.rs#L408) `Transaction::new`; [src/runtime/actors/wal/transaction.rs:457](../../src/runtime/actors/wal/transaction.rs#L457) `validate_transaction_preconditions`; [docs/user-guides/transaction-durability-contract.md:1](../../docs/user-guides/transaction-durability-contract.md#L1) `transaction contract`.

Active regression references (not executed by this refresh):

- [tests/transactions.rs:2773](../../tests/transactions.rs#L2773) `should_allow_lost_update_given_put_read_modify_write_when_concurrent`: Two actual transactions read zero and each writes one; final counter is one under default policy. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2813.
- [tests/transactions.rs:4270](../../tests/transactions.rs#L4270) `should_abort_second_commit_given_conflicting_writes_when_abort_on_write_conflict_enabled`: Positive alternative requires strict conflict outcome. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 4297, 4308.

Preconditions: Common storage modes; intentionally bounded two-transaction lost-update schedule.

Uncovered boundary: Serializable isolation, predicate locks and atomic cross-CF transactions are unsupported promises, not missing successful tests. The lost-update control must not be interpreted as a defect or universal concurrency model.

Catalog review: Transaction horizons, sequence filtering, conditional validation and ordered whole-unit publication are WAL/apply dependencies; no serializable or cross-family transaction guarantee is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## WAL-1

Production: [src/engine/api/write_options.rs:149](../../src/engine/api/write_options.rs#L149) `effective_wal_durability_policy`; [src/runtime/actors/wal/transaction.rs:224](../../src/runtime/actors/wal/transaction.rs#L224) `append_prepared_transactions`; [src/runtime/frontiers.rs:59](../../src/runtime/frontiers.rs#L59) `advance_cloud_to`.

Active regression references (not executed by this refresh):

- [tests/durability_sync_count.rs:76](../../tests/durability_sync_count.rs#L76) `should_issue_one_physical_wal_sync_when_non_empty_sync_transaction_commits`: Actual nonempty sync transaction increases physical WAL fsync counter by exactly one. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 93.
- [src/engine/api/write_options.rs:181](../../src/engine/api/write_options.rs#L181) `should_map_every_local_write_option_to_expected_wal_policy_given_local_storage_when_committing`: Local sync/buffered/best_effort map to Strict/Batched/BestEffort respectively. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 201.
- [src/runtime/event_loop/cloud_integration/tests.rs:4943](../../src/runtime/event_loop/cloud_integration/tests.rs#L4943) `should_not_advance_cloud_durability_across_unacked_segment_gap`: Cloud frontier stays zero for later ACK alone, retains local WAL and buffered ACK; preceding ACK advances contiguous frontier and drains covered WAL bookkeeping. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 4964, 4985, 4993, 5004, 5009, 5017, 5027, 5032, 5039.

Preconditions: Physical sync count is actual local Fs I/O. Policy mapping is a unit predicate; cloud-gap test uses an actual cloud event-loop fixture with controlled ACK delivery.

Uncovered boundary: Mapping is not crash persistence proof for every mode. In-memory has no crash-persistent medium; buffered/best_effort/cloud_async must retain their weaker return contracts. Native strict cache-loss evidence is at CLOUD-5.

Catalog review: Direct WAL ownership, append/sync outcome, exact durable-floor, recovery and family-retirement dependency. Current fsync cadence remains exact. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## WAL-2

Production: [src/runtime/actors/wal.rs:812](../../src/runtime/actors/wal.rs#L812) `WalActor::admit_wal_records`; [src/runtime/actors/wal.rs:956](../../src/runtime/actors/wal.rs#L956) `WalActor::fail_unapplied_transactions`; [src/wal/recovery/streaming.rs:278](../../src/wal/recovery/streaming.rs#L278) `replay_wal_with_options`; [src/runtime/actors/wal/transaction.rs:694](../../src/runtime/actors/wal/transaction.rs#L694) `append_prepared_transaction_batches`.

Active regression references (not executed by this refresh):

- [src/engine/tests/flush_retry.rs:475](../../src/engine/tests/flush_retry.rs#L475) `should_complete_one_cloud_flush_when_abandoned_barrier_callers_retry`: Actual accepted held cloud flush outlives caller waits; retries converge on one built/uploaded SST with exact rows and successful strict reopen. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 485, 486, 487, 488, 497.
- [src/runtime/tests/compaction_deadline_tests.rs:37](../../src/runtime/tests/compaction_deadline_tests.rs#L37) `should_preserve_manual_caller_budget_when_queued_compaction_outlives_response_wait`: real RuntimeHandle/router with no event loop; original override remains captured after bounded caller timeout and late completion updates tombstone accounting. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 65, 69, 70, 71, 75, 76, 77, 78, 79, 80.
- [src/runtime/actors/wal/tests/admission.rs:501](../../src/runtime/actors/wal/tests/admission.rs#L501) `should_retain_wal_admission_when_failed_append_has_no_rollback_proof`: An ambiguous append reports OutcomeUnknown, fences the writer, and retains its WAL admission charge. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 523, 524, 528, 530.
- [src/runtime/actors/wal/tests/admission.rs:592](../../src/runtime/actors/wal/tests/admission.rs#L592) `should_keep_wal_open_when_spilled_transaction_exceeds_remaining_wal_budget`: A whole-unit budget rejection writes no WAL bytes, releases no earlier reservation, keeps the actor open, and permits retry after capacity returns. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 647, 679, 683, 684, 688, 692, 696.
- [tests/fault_injection.rs:2906](../../tests/fault_injection.rs#L2906) `should_not_replay_commit_reported_as_retryable_when_wal_sync_fails`: A public sync commit followed by injected fsync failure and restart cannot be both reported retryable and recovered. Scope: failpoints feature; public Engine commit and strict restart Assertion/helper call markers: 2924, 2928, 2948.

Preconditions: Flush test exercises accepted public Engine work with conditional filesystem cloud. The outcome and admission tests run the real WAL actor; the sync-failure case drives public Engine commit and restart with the fsync failpoint.

Uncovered boundary: Caller timeout is not cancellation, and an ambiguous write still requires idempotency or readback before retry. Provider-level request ambiguity remains bounded by each provider's conditional publication contract.

Catalog review: Direct WAL ownership, append/sync outcome, exact durable-floor, recovery and family-retirement dependency. Current fsync cadence remains exact. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## WAL-3

Production: [src/wal/recovery/streaming.rs:257](../../src/wal/recovery/streaming.rs#L257) `replay_wal_with_checkpoint`; [src/wal/recovery/streaming.rs:278](../../src/wal/recovery/streaming.rs#L278) `replay_wal_with_options`; [src/runtime/cloud_startup/streaming_wal_plan.rs:486](../../src/runtime/cloud_startup/streaming_wal_plan.rs#L486) `apply_open_cut`.

Active regression references (not executed by this refresh):

- [src/wal/recovery/tests.rs:1056](../../src/wal/recovery/tests.rs#L1056) `should_recover_only_committed_transaction_given_split_wal_records_when_commit_marker_is_missing`: Synced transaction begin/put without marker decodes but materializes no recovered memtable. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1102, 1103.
- [src/wal/planner/tests.rs:170](../../src/wal/planner/tests.rs#L170) `should_plan_every_ledger_member_after_its_anchor`: A ledger with sealed members 1 and 2 and open member 3 plans replay from its anchor in that order, sealed first and the open member last, each checked against its ledger facts. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 179, 180, 187.
- [src/wal/planner/tests.rs:533](../../src/wal/planner/tests.rs#L533) `should_cut_a_torn_open_member_without_keeping_a_copy`: An open member holding one record and then a torn frame is cut back to its 160 verified bytes without a retained copy and replays; strict recovery is not flagged as salvage. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 555, 563, 564.
- [tests/storage_root_injection.rs:811](../../tests/storage_root_injection.rs#L811) `should_open_strictly_when_power_loss_tears_unsynced_multi_page_tail`: Strict recovery accepts every CrashFs power-loss image with the synced value intact and never rejects a torn unsynced tail. Scope: CrashFs power-loss images; strict public reopen Assertion/helper call markers: 823, 829, 830.
- [src/wal/recovery/streaming/tests.rs:748](../../src/wal/recovery/streaming/tests.rs#L748) `should_checkpoint_after_abandoned_split_transaction_when_newer_epoch_records_follow`: A later epoch checkpoints complete records despite an abandoned split transaction; the abandoned transaction remains absent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 796, 797, 801, 803, 805, 806.

Preconditions: Actual local filesystem WAL and public strict reopen; the torn-tail test enumerates CrashFs power-loss images. The split-transaction test drives the production streaming replay implementation with real encoded segment bytes and bounded checkpoint storage.

Uncovered boundary: These selected assertions cover bounded crash images and replay budgets; they do not prove every device failure schedule or provider-specific timing.

Catalog review: Direct WAL ownership, append/sync outcome, exact durable-floor, recovery and family-retirement dependency. Current fsync cadence remains exact. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## WAL-4

Production: [src/wal/planner.rs:355](../../src/wal/planner.rs#L355) `plan`; [src/runtime/cloud_startup/mod.rs:92](../../src/runtime/cloud_startup/mod.rs#L92) `commit_recovery_plan_within`; [src/runtime/wal_history.rs:748](../../src/runtime/wal_history.rs#L748) `WalHistory::set_aside`; [src/wal/recovery/streaming.rs:278](../../src/wal/recovery/streaming.rs#L278) `replay_wal_with_options`.

Active regression references (not executed by this refresh):

- [tests/durability.rs:872](../../tests/durability.rs#L872) `should_fail_strict_but_salvage_valid_prefix_given_corrupted_first_wal_frame_when_reopening`: Genuine corrupted first WAL frame fails strict recovery and salvage exposes no corrupted key. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 921, 934.
- [src/runtime/cloud_startup/streaming_wal_plan/tests.rs:872](../../src/runtime/cloud_startup/streaming_wal_plan/tests.rs#L872) `should_not_replay_segments_after_invalid_segment_when_cloud_salvage_skips_one`: Real catalog fixture corruption at segment 2 with no local copy yields the salvage flag, remote replay only [1], set-aside catalog entries [2,3] and open segment kept aside, max_unreplayed_sequence=4, a rebase at segment 4 or later, and one catalog change leaving entries [1] with seq_floor=4. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 886, 887, 896, 904, 913, 914, 915, 916, 917, 926, 934.
- [src/wal/planner/tests.rs:570](../../src/wal/planner/tests.rs#L570) `should_replay_only_the_chain_prefix_before_the_first_hole`: Seeded property over published and unpublished segments with damaged objects or members: strict planning fails exactly when a hole precedes the open segment, salvage replays exactly the segments before the first hole, and salvage is flagged exactly when a hole exists. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 593, 596, 597.
- [tests/wal_replay_io_failure.rs:73](../../tests/wal_replay_io_failure.rs#L73) `should_keep_acknowledged_writes_when_salvage_replay_hits_a_transient_io_error`: Each transient WAL read fault leaves every acknowledged write recoverable on a later healthy open. Scope: public Engine plus CrashFs; every WAL read fault is retried against a separate device copy Assertion/helper call markers: 112, 116, 121.
- [src/runtime/cloud_startup/streaming_wal_plan/tests.rs:872](../../src/runtime/cloud_startup/streaming_wal_plan/tests.rs#L872) `should_not_replay_segments_after_invalid_segment_when_cloud_salvage_skips_one`: Cloud salvage stops before the first missing authoritative segment, sets aside the suffix, and persists a sequence floor over it. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 886, 887, 896, 904, 913, 914, 915, 916, 917, 926, 934.
- [src/runtime/cloud_startup/streaming_wal_plan/tests.rs:2368](../../src/runtime/cloud_startup/streaming_wal_plan/tests.rs#L2368) `should_ignore_corrupt_local_copy_of_retired_prefix_when_later_history_remains`: A corrupt local copy of cloud-retired segment 1 does not pin or block strict recovery of cataloged segment 2. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2395, 2404, 2405, 2407.
- [tests/fault_injection/wal_authority_regressions.rs:259](../../tests/fault_injection/wal_authority_regressions.rs#L259) `should_allow_strict_writes_when_salvage_substitutes_intact_local_segment`: Substitution from verified local segment bytes permits a strict write and later strict reopen while preserving the damaged remote identity. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 272, 282, 286, 290, 300, 301, 302.
- [src/runtime/cloud_startup/streaming_wal_plan/tests/crashes.rs:300](../../src/runtime/cloud_startup/streaming_wal_plan/tests/crashes.rs#L300) `should_open_strictly_without_set_aside_segments_when_salvage_stops_after_its_catalog_commit`: A salvage child aborts at midge::recovery::after_salvage_floor_before_quarantine after its catalog commit; a strict reopen replays only remote segment 1, owes no local segment, replays no open segment, keeps new writes above sequence 5 and links its new open segment to the catalog head. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 326, 335, 336, 337, 338.

Preconditions: Explicit RecoveryPolicy::Strict versus Salvage; real catalog/segment bytes and actual corrupted publication identity. The crash case aborts a real child process at the named failpoint. Transient read faults use CrashFs and the public Engine; retired-prefix and substitute cases use cloud catalog fixtures.

Uncovered boundary: Salvage is degraded recovery, not normal complete-history success. The selected schedules do not qualify every timeout, I/O error, resource-limit, or provider implementation.

Catalog review: Direct WAL ownership, append/sync outcome, exact durable-floor, recovery and family-retirement dependency. Current fsync cadence remains exact. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## WAL-5

Production: [src/lease/fs_leader_store.rs:698](../../src/lease/fs_leader_store.rs#L698) `FsLeaderStore::with_writer_epoch`; [src/runtime/wal_history.rs:60](../../src/runtime/wal_history.rs#L60) `with_writer_fence`; [src/runtime/wal_history.rs:381](../../src/runtime/wal_history.rs#L381) `WalHistory::under_writer_fence`; [src/wal/fs/writer_runner.rs:422](../../src/wal/fs/writer_runner.rs#L422) `WriterRunner::append_once`.

Active regression references (not executed by this refresh):

- [src/wal/planner/tests.rs:403](../../src/wal/planner/tests.rs#L403) `should_stop_at_the_first_segment_whose_header_breaks_the_chain`: A cache segment written by epoch 7 after the catalog's epoch-8 segment breaks the chain: strict planning refuses it naming the lower epoch; salvage replays only segment 1, rebases onto it and lifts the catalog floor over the set-aside segment. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 418, 422, 423, 430.
- [src/runtime/cloud_startup/streaming_wal_plan/tests.rs:1156](../../src/runtime/cloud_startup/streaming_wal_plan/tests.rs#L1156) `should_reject_strict_recovery_when_later_segment_has_lower_writer_epoch`: Real catalog fixture: a local sealed segment by epoch 7 after published epoch-8 history fails strict recovery with the epoch-regression reason and stays in place. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1166, 1171.
- [src/runtime/cloud_startup/streaming_wal_plan/tests.rs:1176](../../src/runtime/cloud_startup/streaming_wal_plan/tests.rs#L1176) `should_reject_strict_recovery_when_active_wal_has_lower_writer_epoch`: An open segment by epoch 7 after published epoch-8 history fails strict recovery; the file stays in place and no salvage copy is made. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1186, 1191, 1192.
- [src/wal/planner/tests.rs:345](../../src/wal/planner/tests.rs#L345) `should_rebase_strictly_when_the_catalog_set_this_history_aside`: A catalog that set segments 2 and 3 aside with floor 5 is Behind this ledger and covers it: strict planning replays segment 1, rebases onto it, keeps new writes above sequence 5 and needs no further catalog change. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 362, 363, 364, 371, 372.
- [src/wal/ledger_store/tests.rs:300](../../src/wal/ledger_store/tests.rs#L300) `should_refuse_append_when_ledger_changes_without_changing_length`: An unauthorized same-length LEDGER rewrite is rejected before append and remains byte-for-byte unchanged. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 314, 315.
- [src/runtime/wal_history/tests.rs:45](../../src/runtime/wal_history/tests.rs#L45) `should_fence_stale_history_before_successor_wal_effects`: A predecessor WalHistory cannot edit the ledger after successor acquisition; its image and membership are unchanged. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 69, 70, 74.
- [src/lease/fs_leader_store.rs:1686](../../src/lease/fs_leader_store.rs#L1686) `should_finish_an_admitted_writer_effect_before_successor_acquisition`: A successor is refused while a real epoch-fenced writer effect is admitted and acquires a higher epoch after it completes. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1729, 1730.
- [src/wal/fs/writer_io.rs:1467](../../src/wal/fs/writer_io.rs#L1467) `should_hold_filesystem_writer_fence_around_background_append`: The actual background write_at is held inside the filesystem epoch fence; takeover cannot pass it. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1536.
- [src/lease/fs_leader_store.rs:1912](../../src/lease/fs_leader_store.rs#L1912) `should_allow_successor_after_power_loss_follows_completed_release`: A CrashFs power loss after completed release permits a greater-epoch successor. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1922, 1931, 1932.
- [src/lease/fs_leader_store.rs:1979](../../src/lease/fs_leader_store.rs#L1979) `should_fail_closed_when_power_loss_interrupts_acquisition_lock`: Power loss during acquisition either preserves a blocking uncommitted lock or allows a clean first epoch; it does not publish a false owner. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2006, 2007, 2010.
- [tests/wal_campaigns.rs:172](../../tests/wal_campaigns.rs#L172) `should_allow_successor_when_power_loss_follows_completed_lease_release`: The engine campaign control passes power loss immediately after a completed filesystem lease release. Scope: nightly campaign selector; deterministic power-loss simulation Listed markers are the actual behavior-assertion helper calls in this function. Assertion/helper call markers: 193.
- [src/wal/format/segment/tests.rs:82](../../src/wal/format/segment/tests.rs#L82) `should_reject_unsupported_segment_version_with_a_valid_header_checksum`: Both zero and future segment versions fail despite a recomputed valid header CRC. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 94.
- [src/wal/format/segment/tests.rs:101](../../src/wal/format/segment/tests.rs#L101) `should_reject_unsupported_record_version_inside_a_valid_crc_frame`: A future record payload version is rejected inside an otherwise valid segment header and frame CRC. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 112.

Preconditions: Real filesystem lease state, CrashFs power-loss states, a real background WAL writer, and actual LEDGER bytes. Epoch-regression planner tests use real sealed segment bytes and catalog entries; the stale-cache takeover case is qualified separately under CLOUD-5 with Sqrzl.

Uncovered boundary: Filesystem effect fencing is qualified on the shared filesystem store. Provider requests already submitted can have ambiguous outcomes; their immutable identities and conditional catalog publication remain the authority boundary. These tests do not exhaust every scheduler interleaving.

Catalog review: Direct WAL ownership, append/sync outcome, exact durable-floor, recovery and family-retirement dependency. Current fsync cadence remains exact. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## WAL-6

Production: [src/runtime/frontiers.rs:47](../../src/runtime/frontiers.rs#L47) `advance_synced_to`; [src/runtime/frontiers.rs:53](../../src/runtime/frontiers.rs#L53) `advance_local_to`; [src/runtime/frontiers.rs:59](../../src/runtime/frontiers.rs#L59) `advance_cloud_to`; [src/runtime/frontiers.rs:83](../../src/runtime/frontiers.rs#L83) `reset_cloud_for_recovery`.

Active regression references (not executed by this refresh):

- [src/runtime/frontiers.rs:108](../../src/runtime/frontiers.rs#L108) `should_not_lower_frontiers_when_an_older_sequence_arrives`: After advances to synced/local 20 and cloud 15, lower advances leave those exact values unchanged. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 120, 121, 122.
- [src/runtime/frontiers.rs:141](../../src/runtime/frontiers.rs#L141) `should_lower_cloud_frontier_only_through_recovery_reset`: Explicit recovery reset can set cloud frontier to 7. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 150.

Preconditions: Real frontier value type with controlled sequence operations; no provider side effects in these units.

Uncovered boundary: A unit reset allowance is not itself proof that callers re-prove all cloud authority. Runtime catalog/gap tests at CLOUD-2 supply a separate integration boundary.

Catalog review: Direct WAL ownership, append/sync outcome, exact durable-floor, recovery and family-retirement dependency. Current fsync cadence remains exact. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## WAL-7

Production: [src/wal/replay.rs:25](../../src/wal/replay.rs#L25) `ReplaySelection::should_apply`; [src/metadata/manifest.rs:113](../../src/metadata/manifest.rs#L113) `Manifest::prepare_edit`; [src/runtime/event_loop/wal_retention.rs:8](../../src/runtime/event_loop/wal_retention.rs#L8) `prune_local_wal_segments_covered_by_manifest`; [src/runtime/hybrid_persistence.rs:414](../../src/runtime/hybrid_persistence.rs#L414) `prune_cloud_wal_segments_within`.

Active regression references (not executed by this refresh):

- [src/wal/replay.rs:52](../../src/wal/replay.rs#L52) `should_select_whole_unit_when_commit_exceeds_its_family_frontier`: Pure whole-unit selection uses the owning family commit boundary and preserves equality as already flushed. Scope: Pure frontier values; no I/O or public acknowledgment Assertion/helper call markers: 60.
- [src/wal/recovery/streaming/tests.rs:854](../../src/wal/recovery/streaming/tests.rs#L854) `should_replay_complete_batch_when_frontier_precedes_commit`: Real framed atomic batch replays every member when its commit exceeds the family frontier, including earlier-sequence members. Scope: Encoded WAL + actual streaming decoder; no native provider qualification Assertion/helper call markers: 880.
- [src/wal/recovery/streaming/tests.rs:888](../../src/wal/recovery/streaming/tests.rs#L888) `should_replay_complete_spooled_transaction_when_frontier_precedes_commit`: Split transaction spool replays every committed member across the family frontier. Scope: Encoded split transaction + actual spool decoder Assertion/helper call markers: 922.
- [src/metadata/manifest/tests.rs:23](../../src/metadata/manifest/tests.rs#L23) `should_leave_every_field_unchanged_when_a_nested_batch_has_an_invalid_transition`: An invalid nested edit leaves all manifest fields exactly unchanged. Scope: Pure manifest preparation and serialized state comparison Assertion/helper call markers: 42, 43.
- [src/metadata/manifest/tests.rs:183](../../src/metadata/manifest/tests.rs#L183) `should_retain_frontiers_when_compaction_or_reclamation_removes_every_sst`: Removing all SSTs preserves the live frontier; drop and reclamation retain the permanent tombstone and refuse id reuse. Scope: Manifest transitions; no physical provider deletion Assertion/helper call markers: 197, 218, 219, 223, 224.
- [src/runtime/event_loop/flush_pipeline/tests.rs:1278](../../src/runtime/event_loop/flush_pipeline/tests.rs#L1278) `should_retain_local_wal_when_sst_metadata_has_no_flush_frontier`: SST metadata alone does not authorize local WAL retirement. Scope: Actual local runtime/WAL fixture; no SST coverage proof Assertion/helper call markers: 1298, 1300.
- [src/runtime/event_loop/flush_pipeline/tests.rs:1310](../../src/runtime/event_loop/flush_pipeline/tests.rs#L1310) `should_retire_local_wal_from_durable_frontiers_without_opening_sst_payloads`: Durable family frontiers permit local retirement without fetching SST payloads. Scope: Actual local publication and ledger retirement Assertion/helper call markers: 1339, 1341, 1347.
- [src/runtime/hybrid_persistence/tests.rs:2687](../../src/runtime/hybrid_persistence/tests.rs#L2687) `should_retain_remote_wal_until_the_family_frontier_is_committed_to_shared_metadata`: Local-only advancement retains remote WAL; a committed shared generation authorizes catalog retirement without WAL/SST payload GETs. Scope: Filesystem-backed shared pointer and mock provider; bounded conditional deletion Assertion/helper call markers: 2705, 2706, 2723, 2724, 2726, 2737, 2738.
- [src/wal/recovery/streaming/tests.rs:1010](../../src/wal/recovery/streaming/tests.rs#L1010) `should_refuse_missing_family_authority_when_replaying_under_salvage_policy`: Salvage replay returns typed Corruption for a family without persisted authority instead of treating it as a truncated WAL prefix. Scope: Actual encoded WAL and streaming selector under salvage policy Assertion/helper call markers: 1033, 1037.
- [src/runtime/state/recovery/authority_tests.rs:149](../../src/runtime/state/recovery/authority_tests.rs#L149) `should_leave_wal_history_unchanged_when_salvage_lacks_family_authority`: The local salvage caller propagates missing family authority without mutating the exact WAL ledger bytes. Scope: Typed semantic failure and actual MockFs ledger bytes Assertion/helper call markers: 184, 185.
- [src/runtime/state/tests/frontier_authority.rs:116](../../src/runtime/state/tests/frontier_authority.rs#L116) `should_not_read_ssts_to_decide_wal_replay_or_retention`: Actual local recovery skips the flushed record, replays its live successor and durably retires exactly the sealed prefix while an injected filesystem refuses and counts every SST payload open. Scope: Real framed WAL, sealed facts, manifest publication and runtime recovery; no public Engine or real provider Assertion/helper call markers: 140, 169, 170, 174, 175, 176, 177.

Preconditions: Canonical required family frontier rows, complete validated replay units, durable paired flush batches, and verified sealed family summaries. Remote retirement reads a committed immutable shared generation.

Uncovered boundary: Source assertions cover bounded local/mock-provider schedules. These references do not establish exhaustive crash interleavings, real-provider transport behavior or hour-long performance acceptance.

Catalog review: Direct WAL ownership, append/sync outcome, exact durable-floor, recovery and family-retirement dependency. Current fsync cadence remains exact. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## SST-1

Production: [src/runtime/sst_read_view.rs:69](../../src/runtime/sst_read_view.rs#L69) `SstReadView::new`; [src/runtime/event_loop/flush_pipeline/publication.rs:398](../../src/runtime/event_loop/flush_pipeline/publication.rs#L398) `install_flush_publication`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:473](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L473) `reconcile_manifest_ssts`.

Active regression references (not executed by this refresh):

- [tests/fault_injection.rs:559](../../tests/fault_injection.rs#L559) `should_ignore_orphan_sst_when_flush_intent_log_save_hits_no_space`: Flush intent-save NoSpace leaves accepted data recoverable on reopen but sst_count=0, so output presence is not publication authority. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 594.

Preconditions: Actual local Engine/files and real intent-persistence failure; test reopens rather than merely examining an in-memory manifest.

Uncovered boundary: This selected test proves a flush orphan case. Separate raw cache, unreferenced remote generation and compaction residue cases are not all implied by that one assertion (see CLOUD-3/SST-5).

Catalog review: Reviewed despite the earlier non-WAL selection: flush/compaction publication, source pins, bounds and level order interact with WAL family frontiers and pipeline drains. SSTV5 byte qualification is a separate slice. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## SST-2

Production: [src/runtime/sst_read_view.rs:81](../../src/runtime/sst_read_view.rs#L81) `SstReadView::from_shared`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:531](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L531) `validate_manifest_sst_size`.

Active regression references (not executed by this refresh):

- [tests/storage_invariants.rs:5](../../tests/storage_invariants.rs#L5) `should_preserve_published_sst_bytes_given_later_flush_then_restart`: Captures first published SST bytes, flushes later data and reopens; first bytes stay identical after both steps and both values remain readable. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 37, 38, 39, 40.

Preconditions: Actual local filesystem and SST readers; no external provider lifecycle policy.

Uncovered boundary: Immutability is sampled through later flush/restart and deliberate corruption. It is not an external guarantee that cloud operators cannot replace/delete authoritative objects; those changes must surface as unavailable/corrupt authority.

Catalog review: Reviewed despite the earlier non-WAL selection: flush/compaction publication, source pins, bounds and level order interact with WAL family frontiers and pipeline drains. SSTV5 byte qualification is a separate slice. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## SST-3

Production: [src/runtime/sst_read_view.rs:126](../../src/runtime/sst_read_view.rs#L126) `build_level`; [src/runtime/sst_read_view.rs:199](../../src/runtime/sst_read_view.rs#L199) `point_candidates`; [src/runtime/sst_read_view.rs:264](../../src/runtime/sst_read_view.rs#L264) `range_candidates`.

Active regression references (not executed by this refresh):

- [tests/storage_layer.rs:1747](../../tests/storage_layer.rs#L1747) `should_reject_manifest_bounds_when_valid_sst_summary_disagrees`: Actual valid v3 fixture SST bytes/size/CRC stay unchanged while both manifests' bounds are altered; verifier returns Corruption. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1752, 1766, 1795.
- [src/runtime/sst_read_view.rs:551](../../src/runtime/sst_read_view.rs#L551) `should_keep_legacy_file_in_conservative_fallback_bucket`: Incomplete advisory bounds retain the file even for an outside key. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 563, 564.
- [src/runtime/sst_read_view.rs:568](../../src/runtime/sst_read_view.rs#L568) `should_quarantine_lower_level_when_complete_bounds_truly_overlap`: Constructed complete overlapping bounds quarantine L1 and return both candidates for outside-key probe. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 581, 582.

Preconditions: Verifier uses genuine persisted fixture; candidate/quarantine tests operate the real view with supplied metadata.

Uncovered boundary: View units do not establish complete-repair publication or every endpoint/tombstone equality schedule. Their assertions are conservative selection, not proof all metadata becomes trusted.

Catalog review: Reviewed despite the earlier non-WAL selection: flush/compaction publication, source pins, bounds and level order interact with WAL family frontiers and pipeline drains. SSTV5 byte qualification is a separate slice. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## SST-4

Production: [src/runtime/event_loop/flush_pipeline/publication.rs:117](../../src/runtime/event_loop/flush_pipeline/publication.rs#L117) `prepare_flush_publication`; [src/runtime/event_loop/flush_pipeline/publication.rs:263](../../src/runtime/event_loop/flush_pipeline/publication.rs#L263) `commit_flush_metadata`; [src/runtime/event_loop/flush_pipeline/publication.rs:398](../../src/runtime/event_loop/flush_pipeline/publication.rs#L398) `install_flush_publication`.

Active regression references (not executed by this refresh):

- [tests/fault_injection.rs:559](../../tests/fault_injection.rs#L559) `should_ignore_orphan_sst_when_flush_intent_log_save_hits_no_space`: Real failed flush intent save preserves ten accepted rows after reopen without publishing its SST. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 594.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/publication_orders.rs:323](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/publication_orders.rs#L323) `should_defer_compaction_completion_when_manual_flush_publishes_first`: Actual held compactor and real flush/publisher order inspects authoritative cloud metadata and exact rows after settling both obligations. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 352, 367, 374, 379.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/publication_orders.rs:240](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/publication_orders.rs#L240) `should_reuse_prereserved_flush_name_when_compaction_publication_wins_build_race`: Opposite actual publication order retains the assigned flush identity and exact committed metadata. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 310, 311, 312, 313, 318.

Preconditions: Fault fixture uses local Fs; order fixtures use real CloudStorageLease/conditional mock storage, actual workers and cleanup joins.

Uncovered boundary: Two publication orders are bounded schedules. They do not license concurrent authority publishers; only explicit flush compute overlaps are admitted while publication/retirement owners still gate authority.

Catalog review: Reviewed despite the earlier non-WAL selection: flush/compaction publication, source pins, bounds and level order interact with WAL family frontiers and pipeline drains. SSTV5 byte qualification is a separate slice. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## SST-5

Production: [src/runtime/event_loop/compaction.rs:680](../../src/runtime/event_loop/compaction.rs#L680) `start_publication`; [src/runtime/event_loop/compaction.rs:1357](../../src/runtime/event_loop/compaction.rs#L1357) `begin_intent_clear_publication`; [src/runtime/actors/gc.rs:160](../../src/runtime/actors/gc.rs#L160) `GcActor::delete_ssts`.

Active regression references (not executed by this refresh):

- [tests/fault_injection.rs:4501](../../tests/fault_injection.rs#L4501) `should_retain_input_ssts_given_compaction_failure_before_manifest_publish`: Actual process abort after output durability retains old manifest set, keeps inputs on disk, and strict reopen recovers exact acknowledged data. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 4512, 4516, 4528, 4532, 4543.
- [src/runtime/state/tests.rs:1602](../../src/runtime/state/tests.rs#L1602) `should_replay_every_compaction_publication_crash_point_to_complete_authority`: Recovered manifest resolves the constructed crash-phase matrix to complete authority. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1657, 1658, 1684, 1688, 1690, 1696.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/authority_timeout_tests.rs:293](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/authority_timeout_tests.rs#L293) `should_retain_both_generations_when_committed_metadata_cas_response_exceeds_manual_deadline`: actual manifest CAS commits but response outlives origin; typed Timeout, no fresh final phase, matching durable intent and exact retained input/output bytes, exact committed output authority and genuine same-path recovery. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 322, 323, 324, 325, 329, 339, 344, 352, 353.

Preconditions: Process test uses real files and genuine abort failpoint; runtime-state phase matrix is replay-oriented and must not be labeled an exhaustive native process schedule.

Uncovered boundary: Prepublication crash is a strong old-set case; full old/new authority is sampled across the phase matrix. GC protects current manifest/compacting/snapshot-pinned names but this map proves no arbitrary forged completion is safe.

Catalog review: Reviewed despite the earlier non-WAL selection: flush/compaction publication, source pins, bounds and level order interact with WAL family frontiers and pipeline drains. SSTV5 byte qualification is a separate slice. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## SST-6

Production: [src/runtime/snapshot_pins.rs:278](../../src/runtime/snapshot_pins.rs#L278) `pinned_sst_names`; [src/runtime/actors/gc.rs:160](../../src/runtime/actors/gc.rs#L160) `delete_ssts`; [src/compaction/mod.rs:42](../../src/compaction/mod.rs#L42) `execute_compaction`; [src/runtime/handle.rs:162](../../src/runtime/handle.rs#L162) `read_with_authority`.

Active regression references (not executed by this refresh):

- [tests/transactions.rs:5345](../../tests/transactions.rs#L5345) `should_preserve_snapshot_value_when_delete_is_compacted_with_snapshot_active`: Requires four actual L0 files, compacts to fewer L0 plus L1, old snapshot still reads v1 while current snapshot sees deletion. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 5399, 5419, 5423, 5428, 5441.
- [src/compaction/mod.rs:1229](../../src/compaction/mod.rs#L1229) `should_drop_obsolete_point_tombstone_when_compaction_has_bottommost_proof`: Real SST compaction with explicit bottommost eligibility removes alpha tombstone while beta value/sequence 6 survives. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1251, 1255.
- [src/runtime/snapshot_pins.rs:733](../../src/runtime/snapshot_pins.rs#L733) `should_retain_timed_out_snapshot_pin_until_unregister`: Time warning does not authorize removal of an active SST pin. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 736, 743, 744, 745, 747.
- [tests/snapshot_spill_pressure.rs:251](../../tests/snapshot_spill_pressure.rs#L251) `should_preserve_snapshot_isolation_when_spill_pressure_is_released`: Actual retired snapshot input files remain physically present while pinned and all are physically reclaimed within ten seconds after release. Scope: One deterministic public-API filesystem-backed CloudSimulated schedule; 16 MiB engine budget, 2 MiB local capacity, 128 KiB transaction pool, controlled TTL time, sixteen history rounds, actual spill saturation and exact clean reopen. No native-provider, crash, whole-process RSS or exhaustive-concurrency proof. Assertion/helper call markers: 271, 304, 41, 44, 50, 55.
- [tests/cloud_provider_engine_qualification/authority_publication/read_fencing.rs:99](../../tests/cloud_provider_engine_qualification/authority_publication/read_fencing.rs#L99) `should_fence_predecessor_reads_when_successor_reclaims_remote_snapshot_files`: Actual successor compaction deletes every original remote SST after real lease expiry. Existing predecessor point reads, scan creation and pre-created forward/reverse iterators return sticky Fenced; successor ordered rows and point values are exact before and after reopen. Scope: Native S3, independent caches and real HTTP ACK/deletion observations against pinned Sqrzl. One takeover schedule and same-process successor reopen; not exhaustive simulation or independent-process recovery. Assertion/helper call markers: 119, 135, 152, 154, 156, 162, 163, 167, 195, 197, 199.
- [src/engine/tests/cloud_read_fencing.rs:5](../../src/engine/tests/cloud_read_fencing.rs#L5) `should_invalidate_cloud_reads_when_monotonic_lease_expires_before_notification`: Cloud cached/own-write/missing reads and pre-created lazy scans are fenced after monotonic expiry without a notification. Expired local snapshots remain readable; new cloud transactions are rejected. Scope: Deterministic public Engine API over Local and CloudSimulated, stopped heartbeat and expired actual LeaseValidity; no native transport or cross-owner GC claim. Assertion/helper call markers: 32, 33, 45, 54, 56, 62, 63, 67, 73, 74, 75, 76, 77, 78, 79, 90.
- [src/runtime/handle/read_authority_tests.rs:24](../../src/runtime/handle/read_authority_tests.rs#L24) `should_discard_read_outcomes_when_lease_expires_during_the_read`: Read results, missing-file errors and end-of-range outcomes are replaced by Fenced if shared lease validity expires during the read closure. Scope: Focused synchronous read-guard callback, not an actual SST I/O timing or native network cancellation fixture. Assertion/helper call markers: 36, 37, 38.

Preconditions: Snapshot API test uses durable local/CloudSimulated modes with positive physical-compaction prerequisites; bottommost unit writes and reads real SSTs. The composed spill-pressure regression uses the controlled TTL clock and actual simulated-cloud spill/GC files described in snapshot-spill-pressure-fixture.md. Cloud snapshot retention is limited to the current lease holder; read-fencing regressions cover invalidation on authority loss.

Uncovered boundary: Bottommost eligibility is explicitly supplied in the unit; not every production overlap planner proof is independently tested here. Pin/GC schedules are bounded, not a universal concurrency proof. Remote reader retention across takeover is not provided; successor reclamation is permitted and predecessor cloud reads are invalidated.

Catalog review: Reviewed despite the earlier non-WAL selection: flush/compaction publication, source pins, bounds and level order interact with WAL family frontiers and pipeline drains. SSTV5 byte qualification is a separate slice. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## SST-7

Production: [src/runtime/event_loop/compaction.rs:32](../../src/runtime/event_loop/compaction.rs#L32) `assign_compaction_output_sequence_within`; [src/runtime/event_loop/compaction.rs:62](../../src/runtime/event_loop/compaction.rs#L62) `prepare_compaction_plan_for_launch_within`; [src/runtime/event_loop/sst_names.rs:99](../../src/runtime/event_loop/sst_names.rs#L99) `reserve_sst_name_durably_within`; [src/runtime/actors/compaction.rs:1070](../../src/runtime/actors/compaction.rs#L1070) `prepare_compaction`.

Active regression references (not executed by this refresh):

- [src/runtime/event_loop/flush_pipeline/tests.rs:1546](../../src/runtime/event_loop/flush_pipeline/tests.rs#L1546) `should_not_reuse_reserved_flush_sst_name_when_reopened_after_crash`: Reserves three names, loses in-memory cursor and reopens; new sequence is greater than every earlier reservation. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1562.
- [src/compaction/mod.rs:1523](../../src/compaction/mod.rs#L1523) `should_leave_inputs_untouched_when_compaction_is_cancelled`: Genuine input SST remains byte-identical, typed Aborted, and canonical output does not exist. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1546, 1547, 1548.
- [src/runtime/event_loop/compaction/name_budget_tests.rs:344](../../src/runtime/event_loop/compaction/name_budget_tests.rs#L344) `should_keep_name_reservation_uncovered_when_actual_metadata_read_outlives_original_budget`: actual authority GET held beyond original budget: typed Timeout, journal counter never reused, reserved_through and remote committed counter do not advance, real row/bytes/lease survive. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 386, 387, 392, 393, 397, 403, 406, 410, 427, 428, 430.

Preconditions: The actual name-reservation control acquires a conditional provider lease and proves baseline FORMAT/manifest/SST bytes before its immutable clock. Cancellation and flush-name controls use genuine Fs.

Uncovered boundary: Pre-worker identity assignment remains source-backed. The merged name-budget controls cover retained durable reservations and expired admission, not cancellation of already submitted provider mutations or every output-allocation crash schedule.

Catalog review: Reviewed despite the earlier non-WAL selection: flush/compaction publication, source pins, bounds and level order interact with WAL family frontiers and pipeline drains. SSTV5 byte qualification is a separate slice. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## SST-8

Production: [src/metadata/journal/recovery.rs:77](../../src/metadata/journal/recovery.rs#L77) `replay_identified_edits_after_with_fs_unlocked_within`; [src/metadata/persistence.rs:428](../../src/metadata/persistence.rs#L428) `save_snapshot_and_truncate_journal_with_fs`; [src/metadata/persistence.rs:444](../../src/metadata/persistence.rs#L444) `save_snapshot_unlocked`.

Active regression references (not executed by this refresh):

- [src/metadata/persistence.rs:1398](../../src/metadata/persistence.rs#L1398) `should_preserve_concurrent_journal_edit_when_checkpoint_uses_stale_manifest`: Actual concurrent journal append survives stale snapshot checkpoint; loaded file and edit frontier remain included. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1427, 1434.
- [src/metadata/persistence.rs:1491](../../src/metadata/persistence.rs#L1491) `should_replay_only_post_checkpoint_edits_given_crash_after_snapshot_rename`: Real post-rename failpoint leaves journal; next edit replays once, snapshot edit ID 1/loaded 2, exact two files and one post-checkpoint edit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1518, 1519, 1552, 1553, 1557, 1558, 1559, 1564, 1573.

Preconditions: Actual RealFs manifest/journal writes and existing checkpoint failpoint; thread schedule has a deliberate stale manifest.

Uncovered boundary: These crash/idempotence fixtures remain correctness evidence. Separately, all nine fixed-cell attempts of 37274074026 at c8f0de80 passed source-bound controlled local readback; B/C each miss the predeclared snapshot-payload target in three repeats and A has only one time miss. That conditional policy-investigation result is mapped at ACCT-6; cadence stays unchanged and neither physical bytes nor final fourteen-hour qualification follows.

Catalog review: Reviewed despite the earlier non-WAL selection: flush/compaction publication, source pins, bounds and level order interact with WAL family frontiers and pipeline drains. SSTV5 byte qualification is a separate slice. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## SST-9

Production: [src/io/durable_dir.rs:47](../../src/io/durable_dir.rs#L47) `create_path_durably`; [src/io/durable_dir.rs:151](../../src/io/durable_dir.rs#L151) `sync_dir_path`; [src/io/durable_dir.rs:195](../../src/io/durable_dir.rs#L195) `create_dir_all_durably`.

Active regression references (not executed by this refresh):

- [src/io/durable_dir.rs:291](../../src/io/durable_dir.rs#L291) `should_sync_the_parent_of_every_created_directory_below_root`: Actual nested a/b/c creation exists and observes exactly root, root/a, root/a/b parent syncs. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 301, 302.

Preconditions: Real filesystem directory creation with sync observation instrumentation; OS-specific sync semantics still apply.

Uncovered boundary: Call observation is not physical power-loss proof for every filesystem/device. Deletion currently lacks parent-directory sync and may resurrect obsolete bytes; that is a documented retention/leak boundary, not new authority.

Catalog review: Reviewed despite the earlier non-WAL selection: flush/compaction publication, source pins, bounds and level order interact with WAL family frontiers and pipeline drains. SSTV5 byte qualification is a separate slice. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## SST-10

Production: [src/compaction/strategy.rs:981](../../src/compaction/strategy.rs#L981) `select_level_ordered_l0_batch`; [src/runtime/state/manifest.rs:49](../../src/runtime/state/manifest.rs#L49) `l0_level_order_violations`.

Active regression references (not executed by this refresh):

- [src/compaction/strategy.rs:2364](../../src/compaction/strategy.rs#L2364) `should_keep_remaining_l0_versions_newer_than_compacted_ones_given_hot_newer_files_when_planning`: The picker or apply-time invariant check preserves/logs the level-order condition described by this explicit synthetic/property fixture. Scope: Active Rust picker/property or apply-time invariant regression source; execution deferred. Assertion/helper call markers: 2385.
- [src/compaction/strategy.rs:2429](../../src/compaction/strategy.rs#L2429) `should_never_leave_older_overlapping_l0_files_behind_given_random_layouts_when_planning`: The picker or apply-time invariant check preserves/logs the level-order condition described by this explicit synthetic/property fixture. Scope: Active Rust picker/property or apply-time invariant regression source; execution deferred. Assertion/helper call markers: 2454.
- [src/compaction/strategy.rs:2393](../../src/compaction/strategy.rs#L2393) `should_block_newer_files_given_older_l0_file_with_incomplete_bounds_when_planning`: The picker or apply-time invariant check preserves/logs the level-order condition described by this explicit synthetic/property fixture. Scope: Active Rust picker/property or apply-time invariant regression source; execution deferred. Assertion/helper call markers: 2422, 2423.
- [src/runtime/state/manifest.rs:743](../../src/runtime/state/manifest.rs#L743) `should_report_remaining_l0_file_given_older_overlapping_versions_when_sibling_compacted`: The picker or apply-time invariant check preserves/logs the level-order condition described by this explicit synthetic/property fixture. Scope: Active Rust picker/property or apply-time invariant regression source; execution deferred. Assertion/helper call markers: 758.
- [src/runtime/state/manifest.rs:765](../../src/runtime/state/manifest.rs#L765) `should_report_nothing_given_remaining_file_inside_output_span_when_it_overlaps_no_input`: The picker or apply-time invariant check preserves/logs the level-order condition described by this explicit synthetic/property fixture. Scope: Active Rust picker/property or apply-time invariant regression source; execution deferred. Assertion/helper call markers: 779.
- [src/runtime/state/manifest.rs:783](../../src/runtime/state/manifest.rs#L783) `should_report_remaining_file_given_incomplete_bounds_when_disjoint_sibling_compacted`: The picker or apply-time invariant check preserves/logs the level-order condition described by this explicit synthetic/property fixture. Scope: Active Rust picker/property or apply-time invariant regression source; execution deferred. Assertion/helper call markers: 795.
- [src/runtime/state/manifest.rs:802](../../src/runtime/state/manifest.rs#L802) `should_report_nothing_given_no_l0_input_when_inner_level_compaction_applied`: The picker or apply-time invariant check preserves/logs the level-order condition described by this explicit synthetic/property fixture. Scope: Active Rust picker/property or apply-time invariant regression source; execution deferred. Assertion/helper call markers: 813.

Preconditions: Picker/property fixtures use synthetic FileMeta sequence/key bounds. Incomplete bounds conservatively overlap everything; apply-time logging skips missing sequence ranges.

Uncovered boundary: The rule constrains new plans and logs violations; it does not repair manifests written by an older picker. Read early-stop qualification remains separately gated on complete level ordering.

Catalog review: Reviewed despite the earlier non-WAL selection: flush/compaction publication, source pins, bounds and level order interact with WAL family frontiers and pipeline drains. SSTV5 byte qualification is a separate slice. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## CLOUD-1

Production: [src/wal/cloud_catalog.rs:130](../../src/wal/cloud_catalog.rs#L130) `CatalogEntry::object_key`; [src/wal/cloud_catalog.rs:650](../../src/wal/cloud_catalog.rs#L650) `WalPublicationCatalog::validate`; [src/wal/cloud_catalog.rs:141](../../src/wal/cloud_catalog.rs#L141) `CatalogEntry::validate_bytes`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:348](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L348) `reject_cloud_wal_without_catalog`.

Active regression references (not executed by this refresh):

- [src/engine/tests.rs:951](../../src/engine/tests.rs#L951) `should_reject_epoch_scoped_simulated_cloud_wal_without_catalog`: Actual orphan epoch-scoped valid WAL bytes are rejected without the publication catalog. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 973.
- [src/runtime/cloud_startup/streaming_wal_plan/tests.rs:737](../../src/runtime/cloud_startup/streaming_wal_plan/tests.rs#L737) `should_validate_catalog_authority_before_exposing_replay_sources`: Strict rejects checksum-invalid catalog history; salvage flags degraded recovery, exposes only the first validated segment and no open segment, keeps the open segment aside and rebases at segment 4 or later. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 745, 751, 752, 761, 762, 770, 771, 772, 779.
- [tests/cloud_provider_engine_qualification/authority_publication.rs:56](../../tests/cloud_provider_engine_qualification/authority_publication.rs#L56) `should_preserve_acknowledged_history_when_predecessor_wal_resumes_after_takeover_and_cleanup`: Held native immutable WAL PUT succeeds, remains an uncatalogued orphan, and cannot alter the successor catalog. Two absent-cache fresh-process recoveries exclude its complete transaction. Scope: Three bounded native S3 schedules against pinned local Sqrzl: before upstream PUT, after upstream HTTP 200, and lost successful response. Genuine lease expiry; shortened predecessor shutdown drain budget only. Successor is quiesced before predecessor release to isolate catalog mutation. No exhaustive simulation or live-provider qualification claim. Assertion/helper call markers: 488, 496, 503, 519.
- [src/runtime/cloud_startup/streaming_wal_plan/tests.rs:2482](../../src/runtime/cloud_startup/streaming_wal_plan/tests.rs#L2482) `should_fail_strict_recovery_when_a_cataloged_object_does_not_link_its_entry`: A cataloged object whose header links another segment than its entry names fails strict recovery. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2512.
- [src/wal/cloud_catalog.rs:913](../../src/wal/cloud_catalog.rs#L913) `should_reject_catalog_whose_first_entry_does_not_link_the_anchor`: A decoded catalog whose first entry does not link its anchor is invalid. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 923.

Preconditions: Actual filesystem CloudSimulated objects and real catalog bytes through the recovery planner; explicit checksum and chain-link faults.

Uncovered boundary: These prove object existence is insufficient and catalog identity is checked. They do not simulate every late native upload schedule or establish provider-side immutable-object retention settings.

Catalog review: Reviewed across upload/catalog/metadata authority and inventory: remote publication and retirement must preserve the same acknowledged WAL prefix and lease scope. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## CLOUD-2

Production: [src/storage/hybrid/backend/uploads.rs:505](../../src/storage/hybrid/backend/uploads.rs#L505) `process_wal_upload`; [src/runtime/hybrid_persistence.rs:320](../../src/runtime/hybrid_persistence.rs#L320) `verify_uploaded_wal_segment`; [src/runtime/hybrid_persistence.rs:382](../../src/runtime/hybrid_persistence.rs#L382) `publish_wal_segments`; [src/runtime/frontiers.rs:59](../../src/runtime/frontiers.rs#L59) `advance_cloud_to`.

Active regression references (not executed by this refresh):

- [src/runtime/hybrid_persistence/tests.rs:2122](../../src/runtime/hybrid_persistence/tests.rs#L2122) `should_readback_remote_wal_before_upload_worker_emits_ack`: Actual upload worker emits ACK for segment 9/sequence 13 only with recorded GET download of exact epoch/segment key. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2156, 2166.
- [src/runtime/event_loop/cloud_integration/tests.rs:4902](../../src/runtime/event_loop/cloud_integration/tests.rs#L4902) `should_reject_cloud_ack_given_writer_fenced_after_upload_was_enqueued`: Real sealed local WAL plus controlled fenced ACK leaves cloud frontier 0 and WAL present, marks anomaly. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 4922, 4936, 4937, 4938.
- [src/runtime/event_loop/cloud_integration/tests.rs:4943](../../src/runtime/event_loop/cloud_integration/tests.rs#L4943) `should_not_advance_cloud_durability_across_unacked_segment_gap`: Out-of-order ACK stays buffered and WAL retained; earlier ACK permits exact contiguous cloud frontier and drains covered tracking. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 4964, 4985, 4993, 5004, 5009, 5017, 5027, 5032, 5039.
- [tests/cloud_provider_engine_qualification/authority_publication.rs:56](../../tests/cloud_provider_engine_qualification/authority_publication.rs#L56) `should_preserve_acknowledged_history_when_predecessor_wal_resumes_after_takeover_and_cleanup`: Late native upload completion after real takeover leaves the predecessor cloud frontier unchanged. A deliberate early-frontier mutation is rejected by the fixture. Scope: Three bounded native S3 schedules against pinned local Sqrzl: before upstream PUT, after upstream HTTP 200, and lost successful response. Genuine lease expiry; shortened predecessor shutdown drain budget only. Successor is quiesced before predecessor release to isolate catalog mutation. No exhaustive simulation or live-provider qualification claim. Assertion/helper call markers: 488, 496, 503, 519.

Preconditions: Upload test has valid real local WAL and a conditional mock cloud backend; event-loop tests intentionally deliver ACK events after genuine append/seal.

Uncovered boundary: Injected event delivery is not an actual native upload cancellation race. The final active authority/catalog checks are also source-backed; exact readback bytes and checksum predicates should not be weakened to mere object presence.

Catalog review: Reviewed across upload/catalog/metadata authority and inventory: remote publication and retirement must preserve the same acknowledged WAL prefix and lease scope. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## CLOUD-3

Production: [src/runtime/cloud_startup/cloud_recovery/metadata.rs:30](../../src/runtime/cloud_startup/cloud_recovery/metadata.rs#L30) `read_committed_cloud_metadata_within`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:302](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L302) `mirror_cloud_metadata_within`; [src/runtime/hybrid_persistence/metadata_snapshot.rs:492](../../src/runtime/hybrid_persistence/metadata_snapshot.rs#L492) `mirror_control_metadata_within`; [src/lease/cloud.rs:280](../../src/lease/cloud.rs#L280) `ProviderLeaderStore::publish_committed_metadata`; [src/metadata/format.rs:79](../../src/metadata/format.rs#L79) `validate_format_marker`; [src/wal/cloud_catalog.rs:325](../../src/wal/cloud_catalog.rs#L325) `WalPublicationCatalog::decode`.

Active regression references (not executed by this refresh):

- [src/engine/tests.rs:1291](../../src/engine/tests.rs#L1291) `should_ignore_stale_manifest_json_during_strict_cloud_recovery`: Committed snapshot sequence 10 wins over a newer unpointed mutable manifest; loaded sequence remains 10 and legacy file is absent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1320, 1321.
- [src/lease/cloud/tests.rs:2605](../../src/lease/cloud/tests.rs#L2605) `should_reject_metadata_pointer_cas_that_lands_after_successor_acquires`: Held predecessor CAS loses to successor and exact committed winner pointer survives. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2668, 2672.
- [src/lease/cloud/tests/startup_deadline/scoped_metadata.rs:407](../../src/lease/cloud/tests/startup_deadline/scoped_metadata.rs#L407) `should_preserve_native_mirror_operation_budget_when_startup_scope_is_unbounded`: Real native acquired lease and FORMAT/snapshot mirror succeeds through default/unbounded scope, retaining finite ordinary aggregate budget and exact remote descriptor bytes. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Listed markers are the actual behavior-assertion helper calls in this function. Assertion/helper call markers: 415.
- [src/lease/cloud/tests/startup_deadline/scoped_metadata.rs:419](../../src/lease/cloud/tests/startup_deadline/scoped_metadata.rs#L419) `should_publish_native_mirror_when_bounded_startup_scope_has_remaining_budget`: Actual native mirror succeeds while every observed bounded operation deadline equals the original captured scope. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Listed markers are the actual behavior-assertion helper calls in this function. Assertion/helper call markers: 427.
- [src/wal/cloud_catalog.rs:982](../../src/wal/cloud_catalog.rs#L982) `should_report_another_format_version_as_incompatible`: A version 1 cloud WAL catalog is rejected as incompatible before becoming current authority. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 990.
- [src/metadata/format.rs:250](../../src/metadata/format.rs#L250) `should_reject_open_given_format_version_older_than_nine`: The current FORMAT9 gate rejects older persisted formats without writing a replacement. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 264.

Preconditions: Source includes merged #713 default-None finite mirror fallback; native metadata fixture is an in-process actual S3 HTTP service with conditional responses, not an external production account. Format tests use real persisted markers and catalog bytes.

Uncovered boundary: Native fixture checks one protocol and controlled sequences. It does not authorize unpointed generations or prove distributed multi-region consistency. Unsupported formats are rejected; migration between persisted versions is not provided.

Catalog review: Reviewed across upload/catalog/metadata authority and inventory: remote publication and retirement must preserve the same acknowledged WAL prefix and lease scope. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## CLOUD-4

Production: [src/runtime/cloud_startup/cloud_recovery/mod.rs:423](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L423) `validate_manifest_ssts_in_store`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:473](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L473) `reconcile_manifest_ssts`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:531](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L531) `validate_manifest_sst_size`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:840](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L840) `cloud_recovery_sst_proofs_for_intent_replay`.

Active regression references (not executed by this refresh):

- [src/engine/tests.rs:1793](../../src/engine/tests.rs#L1793) `should_reject_manifest_sst_when_cloud_object_size_differs_from_manifest`: Genuine different-size SST has positive unequal-size prerequisite; actual inventory validation fails and no local cache SST is installed. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1804, 1836, 1840.
- [tests/storage_layer.rs:1747](../../tests/storage_layer.rs#L1747) `should_reject_manifest_bounds_when_valid_sst_summary_disagrees`: Valid persisted SST bytes with conflicting authoritative bounds return Corruption. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1752, 1766, 1795.

Preconditions: Actual real SST objects/metadata in filesystem/mock-cloud fixtures, not a forged length-only unit result.

Uncovered boundary: Selected assertions prove size and bounds validation. They do not cover all missing/corrupt/incompatible object variants or complete native provider salvage policy; full body/block integrity is lazy in some metadata-only startup paths.

Catalog review: Reviewed across upload/catalog/metadata authority and inventory: remote publication and retirement must preserve the same acknowledged WAL prefix and lease scope. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## CLOUD-5

Production: [src/runtime/cloud_startup/streaming_wal_plan.rs:70](../../src/runtime/cloud_startup/streaming_wal_plan.rs#L70) `StreamingCloudWalRecovery::build_within`; [src/runtime/cloud_startup/streaming_wal_plan.rs:200](../../src/runtime/cloud_startup/streaming_wal_plan.rs#L200) `PublishedObject::probe`; [src/wal/planner.rs:54](../../src/wal/planner.rs#L54) `lineage`; [src/wal/planner.rs:492](../../src/wal/planner.rs#L492) `Planner::admit_tail`; [src/wal/recovery/streaming.rs:278](../../src/wal/recovery/streaming.rs#L278) `replay_wal_with_options`.

Active regression references (not executed by this refresh):

- [tests/cloud_provider_engine_qualification/operational.rs:34](../../tests/cloud_provider_engine_qualification/operational.rs#L34) `should_recover_cloud_backlog_after_complete_local_disk_loss`: Actual native child campaign deletes all local files, requires interrupted checkpoint exit 73, deletes interrupted cache, recovers, loses cache again and verifies in another process. Scope: ignored native Sqrzl qualification; explicit features/environment required Assertion/helper call markers: 51, 70, 75, 81, 83, 84, 85.
- [tests/cloud_provider_engine_qualification/authority_publication.rs:586](../../tests/cloud_provider_engine_qualification/authority_publication.rs#L586) `should_verify_acknowledged_state_in_fresh_recovery_process`: Two independent native recovery processes start with absent caches and require exact ordered scan and point values for acknowledged successor overwrites, deletion and atomic pairs after actual WAL/SST reclamation. Scope: Three bounded native S3 schedules against pinned local Sqrzl: before upstream PUT, after upstream HTTP 200, and lost successful response. Genuine lease expiry; shortened predecessor shutdown drain budget only. Successor is quiesced before predecessor release to isolate catalog mutation. No exhaustive simulation or live-provider qualification claim. Assertion/helper call markers: 592, 125, 130, 136.
- [src/wal/planner/tests.rs:262](../../src/wal/planner/tests.rs#L262) `should_replay_the_tail_after_the_catalog_head_from_the_cache`: The catalog holds segment 1; the cache sealed unpublished segment 2 and opened 3. Replay reads 1 from the catalog and 2 and 3 from the cache, 2 stays owed to the catalog, and 3 replays last, unsealed. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 276, 280, 284.
- [src/wal/planner/tests.rs:434](../../src/wal/planner/tests.rs#L434) `should_fall_back_to_the_cached_copy_when_a_shared_object_is_damaged`: A damaged shared object 2 fails strict planning; salvage replays the cache's identical copy instead, flags degraded recovery, sets no catalog entry aside and owes segment 2 again. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 454, 455, 456, 457, 458.
- [tests/fault_injection/wal_authority_regressions.rs:87](../../tests/fault_injection/wal_authority_regressions.rs#L87) `should_preserve_successor_strict_commits_when_predecessor_reopens_old_cache`: A predecessor reopening its stale cache cannot override successor strict commits and flush authority. Scope: Sqrzl-backed provider test; feature sqrzl-tests required Assertion/helper call markers: 109, 127, 128, 141.
- [tests/fault_injection.rs:7082](../../tests/fault_injection.rs#L7082) `should_recover_local_cloud_async_wal_when_child_aborts_before_upload`: An acknowledged cloud_async record is recoverable from the same surviving cache after a child abort before upload. Scope: failpoints feature; actual engine child abort and same-cache reopen Listed markers are the actual behavior-assertion helper calls in this function. Assertion/helper call markers: 7094.
- [src/runtime/cloud_startup/streaming_wal_plan/tests.rs:2368](../../src/runtime/cloud_startup/streaming_wal_plan/tests.rs#L2368) `should_ignore_corrupt_local_copy_of_retired_prefix_when_later_history_remains`: Corruption in a cloud-retired local prefix does not prevent recovery of later catalog history. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2395, 2404, 2405, 2407.

Supporting helper source:

- [tests/cloud_provider_engine_qualification/operational.rs:156](../../tests/cloud_provider_engine_qualification/operational.rs#L156) `assert_complete_state`: Requires every source value, acknowledged seed and exact ordered complete keyset with no duplicates/unexpected/missing keys; campaign reports enforce disk bound and one completed replay. Scope: Behavior-assertion helper called by separate native qualification fixtures; this helper is not an independent Cargo test.

Preconditions: Ignored native suite requires cloud-all, sqrzl-tests, failpoints and an explicitly qualified Sqrzl environment; exact campaign source/process receipts bind qualification. The predecessor-cache test requires Sqrzl. The cloud_async test uses the actual engine with a child abort and surviving local cache. Planner tests use real sealed segment bytes, a real ledger and constructed probes.

Uncovered boundary: cloud_async() provides same-cache recovery for unpublished writes, not a durability promise across takeover or cache loss; shared catalog authority wins. Qualification at a historical source revision does not qualify a later source revision. Emulator protocol success does not establish IAM, quotas, real-cloud availability or optional 128 MiB release-cost evidence.

Catalog review: Reviewed across upload/catalog/metadata authority and inventory: remote publication and retirement must preserve the same acknowledged WAL prefix and lease scope. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## CLOUD-6

Production: [src/runtime/hybrid_persistence.rs:414](../../src/runtime/hybrid_persistence.rs#L414) `prune_cloud_wal_segments_within`; [src/runtime/hybrid_persistence/metadata_snapshot.rs:186](../../src/runtime/hybrid_persistence/metadata_snapshot.rs#L186) `SharedMetadataSnapshot::committed_retirement_authority_within`; [src/runtime/hybrid_persistence/catalog.rs:489](../../src/runtime/hybrid_persistence/catalog.rs#L489) `commit_catalog_with_authority`.

Active regression references (not executed by this refresh):

- [src/runtime/hybrid_persistence/tests.rs:1884](../../src/runtime/hybrid_persistence/tests.rs#L1884) `should_reject_stale_remote_wal_target_identity_when_guarded_delete_runs`: Actual cloud object proof is captured, same bytes are republished with a new ETag; guarded delete errors and object still exists. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1907.
- [src/runtime/hybrid_persistence/tests.rs:2799](../../src/runtime/hybrid_persistence/tests.rs#L2799) `should_retain_catalog_authority_when_the_captured_shared_generation_changes`: A changed committed shared generation fences retirement, retains the catalog entry and retains the WAL object. Scope: Actual filesystem-backed shared generation and mock provider Assertion/helper call markers: 2820, 2821.
- [src/runtime/hybrid_persistence/tests.rs:2743](../../src/runtime/hybrid_persistence/tests.rs#L2743) `should_retain_a_newer_flushed_family_until_the_oldest_family_is_dropped`: An unflushed oldest family blocks the entire retired prefix even when a newer family is covered; its durable drop tombstone removes the blocker. Scope: Canonical per-family summaries and shared metadata publication; mock provider Assertion/helper call markers: 2775, 2776, 2795.
- [src/storage/hybrid/backend/tests.rs:1194](../../src/storage/hybrid/backend/tests.rs#L1194) `should_retry_a_retired_object_with_its_original_identity_after_transport_failure`: The normal storage tick retries a failed conditional deletion using its retained object identity without another caller. Scope: Mock provider transport failure, bounded retry worker and actual target disappearance Assertion/helper call markers: 1223, 1224.
- [src/storage/hybrid/backend/tests.rs:1228](../../src/storage/hybrid/backend/tests.rs#L1228) `should_queue_a_retired_object_until_the_conditional_delete_worker_is_available`: A retired identity target survives temporary worker exhaustion and is eventually conditionally deleted by the storage tick. Scope: One held worker and bounded second admission; mock provider Assertion/helper call markers: 1289.

Preconditions: Persisted catalog family summaries and a validated committed shared manifest generation are required; local-only frontiers are insufficient.

Uncovered boundary: These bounded mock/FS cases do not establish real provider consistency or exhaustive takeover/delete schedules; conditional object identity remains mandatory.

Catalog review: Reviewed across upload/catalog/metadata authority and inventory: remote publication and retirement must preserve the same acknowledged WAL prefix and lease scope. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## CLOUD-7

Production: [src/storage/cloud/dispatcher.rs:175](../../src/storage/cloud/dispatcher.rs#L175) `submit_get_within`; [src/storage/cloud/dispatcher.rs:273](../../src/storage/cloud/dispatcher.rs#L273) `submit_list_within`; [src/storage/cloud/dispatcher.rs:329](../../src/storage/cloud/dispatcher.rs#L329) `submit_head_within`; [src/storage/cloud/blocking.rs:92](../../src/storage/cloud/blocking.rs#L92) `BlockingCloud::get_optional`.

Active regression references (not executed by this refresh):

- [src/storage/cloud/tests/native_metadata_deadline.rs:188](../../src/storage/cloud/tests/native_metadata_deadline.rs#L188) `should_cancel_native_metadata_get_when_operation_deadline_expires`: Real S3/Azure/GCS-XML/GCS-JSON delayed HTTP GET requests require typed Timeout and observed native socket cancellation; simple/blocking GET controls adjacent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Listed markers are the actual behavior-assertion helper calls in this function. Assertion/helper call markers: 193.
- [src/storage/cloud/tests/native_startup_deadline.rs:529](../../src/storage/cloud/tests/native_startup_deadline.rs#L529) `should_stop_native_pagination_when_aggregate_list_budget_expires`: All four native modes consume real continuation pages within one captured budget instead of granting a fresh page allowance. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 536, 540, 544, 555, 559.
- [src/storage/cloud/tests.rs:1826](../../src/storage/cloud/tests.rs#L1826) `should_preserve_precondition_failed_kind_when_cloud_error_crosses_storage_backend`: A PreconditionFailed payload beginning with old NotFound text remains typed precondition failure, not absence. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1835, 1836.

Preconditions: Native deadline fixtures use real local HTTP servers/backends and positive request-arrival/cancellation observations; type-mapping unit supplies a typed error.

Uncovered boundary: Header-wait cancellation is not proof no provider mutation committed; PUT remains ambiguous where required. Selected tests do not establish every body-read/range bound/signing retry contract or external provider implementation. Compatibility defaults on custom backends may be callback-bounded only.

Catalog review: Reviewed across upload/catalog/metadata authority and inventory: remote publication and retirement must preserve the same acknowledged WAL prefix and lease scope. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## CLOUD-8

Production: [src/engine/mod.rs:504](../../src/engine/mod.rs#L504) `Engine::cloud_durable_sequence`; [src/engine/mod.rs:533](../../src/engine/mod.rs#L533) `Engine::wait_cloud_durable`; [src/runtime/frontiers.rs:41](../../src/runtime/frontiers.rs#L41) `WalFrontiers::cloud_durable`.

Active regression references (not executed by this refresh):

- [tests/cloud_core/cloud_durable_wait.rs:70](../../tests/cloud_core/cloud_durable_wait.rs#L70) `should_cover_commit_sequence_given_cloud_async_write_when_its_upload_lands`: The covering upload makes wait_cloud_durable report covered and advances the frontier at or above the returned commit sequence. Scope: Active simulator/local Rust regression source; no native-provider execution claim. Assertion/helper call markers: 86, 88, 92.
- [tests/cloud_core/cloud_durable_wait.rs:100](../../tests/cloud_core/cloud_durable_wait.rs#L100) `should_report_not_covered_given_late_wal_uploads_when_wait_times_out`: A late WAL PUT leaves the write visible but the cloud-durable frontier below its sequence; waiting reports not covered. Scope: Active simulator/local Rust regression source; no native-provider execution claim. Assertion/helper call markers: 120, 121, 122.
- [tests/cloud_core/cloud_durable_wait.rs:130](../../tests/cloud_core/cloud_durable_wait.rs#L130) `should_lose_unacknowledged_cloud_async_write_given_cache_loss_when_successor_opens`: A successor opening from the published cloud image does not recover an uncovered async write after cache loss. Scope: Active simulator/local Rust regression source; no native-provider execution claim. Assertion/helper call markers: 142, 147, 166, 171, 173.
- [tests/cloud_core/cloud_durable_wait.rs:183](../../tests/cloud_core/cloud_durable_wait.rs#L183) `should_keep_acknowledged_cloud_async_write_given_cache_loss_when_wait_returned_covered`: A successor with a fresh cache recovers a write after its cloud-durable wait reported covered. Scope: Active simulator/local Rust regression source; no native-provider execution claim. Assertion/helper call markers: 192, 209.
- [tests/cloud_core/cloud_durable_wait.rs:219](../../tests/cloud_core/cloud_durable_wait.rs#L219) `should_reject_cloud_frontier_queries_given_local_storage_when_called`: Cloud frontier queries on local storage return InvalidArgument. Scope: Active simulator/local Rust regression source; no native-provider execution claim. Assertion/helper call markers: 235, 239.
- [tests/cloud_core/cloud_durable_wait.rs:246](../../tests/cloud_core/cloud_durable_wait.rs#L246) `should_return_increasing_assigned_sequences_given_local_writes_when_committed`: Assigned commit sequences increase; read-only commit returns a sequence at or below the committed horizon. Scope: Active simulator/local Rust regression source; no native-provider execution claim. Assertion/helper call markers: 285, 287, 288.

Preconditions: Simulator-backed Engine uses an explicit object store and lease clock; late PUT schedules model missing cloud coverage. Sqrzl and live-provider behavior remain separate.

Uncovered boundary: Reads are not pinned to the cloud-durable cut. A not-covered wait does not distinguish caller deadline from exhausted upload-confirmation budget. This source inventory is not native-provider qualification.

Catalog review: Reviewed across upload/catalog/metadata authority and inventory: remote publication and retirement must preserve the same acknowledged WAL prefix and lease scope. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## LIFE-1

Production: [src/runtime/event_loop/manifest.rs:219](../../src/runtime/event_loop/manifest.rs#L219) `drop_column_family`; [src/engine/mod.rs:766](../../src/engine/mod.rs#L766) `Engine::drop_column_family`; [src/engine/mod.rs:782](../../src/engine/mod.rs#L782) `Engine::drop_column_family_discarding_unflushed`; [src/metadata/manifest.rs:374](../../src/metadata/manifest.rs#L374) `delete_column_family_with_reclamation`.

Active regression references (not executed by this refresh):

- [tests/column_families.rs:266](../../tests/column_families.rs#L266) `should_fail_drop_column_family_given_unflushed_data_when_memtable_not_empty`: Actual committed unflushed value causes discard-licence refusal and remains readable after rejected safe drop. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 283, 290.
- [tests/column_families.rs:541](../../tests/column_families.rs#L541) `should_allocate_monotonic_column_family_ids_given_deleted_column_family_when_creating`: Actual destructive drop/recreate keeps old key absent and new key present. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 564, 565, 574, 575.

Preconditions: Common memory/local/CloudSimulated public API tests, deliberate unflushed accepted data and explicit destructive call.

Uncovered boundary: The refusal test requires licenses_unflushed_discard(), not an invented generic Busy class. It does not prove all accepted flush/publication interleavings or post-crash reclamation resume by itself.

Catalog review: Family lifecycle serializes against admitted writes and publication; retained pins and permanent dropped-family frontiers remain WAL retirement dependencies. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## LIFE-2

Production: [src/runtime/snapshot_pins.rs:278](../../src/runtime/snapshot_pins.rs#L278) `pinned_sst_names`; [src/runtime/actors/gc.rs:160](../../src/runtime/actors/gc.rs#L160) `delete_ssts`; [src/metadata/manifest.rs:423](../../src/metadata/manifest.rs#L423) `mark_column_family_reclaimed`.

Active regression references (not executed by this refresh):

- [tests/transactions.rs:5807](../../tests/transactions.rs#L5807) `should_return_stable_results_when_scanning_after_column_family_dropped_mid_transaction`: Real flushed three-row local snapshot reads a, concurrent drop completes, remaining scan returns exactly b/c; a new transaction for dropped CF is InvalidArgument. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 5842, 5843, 5844, 5845, 5846.
- [src/runtime/snapshot_pins.rs:733](../../src/runtime/snapshot_pins.rs#L733) `should_retain_timed_out_snapshot_pin_until_unregister`: Warning does not evict retained names/horizon. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 736, 743, 744, 745, 747.

Preconditions: Actual local Engine concurrent drop and retained snapshot iterator with flushed files; pin registry unit is complementary.

Uncovered boundary: Stable scan proves logical read retention during one drop schedule. It does not directly inspect every final unlink or prove reclamation completes after the last pin under failures.

Catalog review: Family lifecycle serializes against admitted writes and publication; retained pins and permanent dropped-family frontiers remain WAL retirement dependencies. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## BACKUP-1

Production: [src/engine/backup.rs:77](../../src/engine/backup.rs#L77) `Engine::backup_to`; [src/engine/backup.rs:262](../../src/engine/backup.rs#L262) `pin_durable_files`; [src/engine/backup.rs:420](../../src/engine/backup.rs#L420) `materialize_backup`.

Active regression references (not executed by this refresh):

- [tests/engine_api.rs:5020](../../tests/engine_api.rs#L5020) `should_preserve_cross_family_frontier_given_concurrent_backup_capture`: Actual ordered event/checkpoint writer has positive acknowledged work and nonzero backup frontier; restored Engine contains every event through restored frontier. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 5067, 5073, 5101, 5106.

Preconditions: Actual local Engine and real backup/restore files, two CFs with ordered per-CF transactions, pinned capture and concurrent writer.

Uncovered boundary: This does not imply atomic transactions spanning CFs. Inventory checksum completeness is source-backed by materialization/validation and additional corrupt-artifact control; it is not an exhaustive adversarial filesystem proof.

Catalog review: Reviewed despite the earlier non-WAL selection: capture drains/syncs an exact WAL cut and pins files; path disjointness, staged restore and support limits are adjacent unchanged contracts. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## BACKUP-2

Production: [src/engine/backup/paths.rs:6](../../src/engine/backup/paths.rs#L6) `resolve`; [src/engine/backup/paths.rs:91](../../src/engine/backup/paths.rs#L91) `prepare_target`; [src/engine/backup/paths.rs:114](../../src/engine/backup/paths.rs#L114) `publish`.

Active regression references (not executed by this refresh):

- [tests/backup_paths.rs:28](../../tests/backup_paths.rs#L28) `should_reject_backup_overlap_before_mutation`: Actual nested destination rejects with InvalidArgument and source inventory remains byte-for-byte unchanged. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 47, 51, 52.
- [tests/backup_paths.rs:59](../../tests/backup_paths.rs#L59) `should_reject_restore_overlap_without_changing_artifact`: Overlapping restore target rejects and artifact inventory is unchanged. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 79, 83.
- [tests/backup_publish_races.rs:9](../../tests/backup_publish_races.rs#L9) `should_preserve_target_created_immediately_before_publication`: Genuine before-publish failpoint creates foreign directory/symlink; backup and restore reject, foreign sentinel survives and owned stages are absent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 65, 66, 70, 72, 74.

Preconditions: Actual filesystem path resolution/publication, real backup artifact, failpoints for the last-moment race; symlink variants are Unix-only.

Uncovered boundary: Tests cover selected alias and late-target cases. Platform-native rename/nonreplacement semantics and remote mounts remain deployment/filesystem boundaries; no universal hostile-filesystem guarantee.

Catalog review: Reviewed despite the earlier non-WAL selection: capture drains/syncs an exact WAL cut and pins files; path disjointness, staged restore and support limits are adjacent unchanged contracts. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## BACKUP-3

Production: [src/engine/backup.rs:186](../../src/engine/backup.rs#L186) `Engine::restore_backup`; [src/engine/backup.rs:487](../../src/engine/backup.rs#L487) `validate_backup`; [src/engine/backup.rs:563](../../src/engine/backup.rs#L563) `copy_verified_objects`; [src/engine/verification.rs:265](../../src/engine/verification.rs#L265) `verify_storage_path`.

Active regression references (not executed by this refresh):

- [tests/engine_api.rs:5118](../../tests/engine_api.rs#L5118) `should_leave_restore_target_absent_given_corrupt_backup_object`: Corrupts actual backup FORMAT object, restore returns Corruption and target stays absent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 5140, 5141.
- [tests/engine_api.rs:5210](../../tests/engine_api.rs#L5210) `should_restore_split_cloud_simulation_layout_given_valid_backup`: Backup inventory excludes lease files, real CloudSimulated restore opens healthy and exact accepted value survives. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 5237, 5242, 5248, 5254, 5273, 5280.

Preconditions: Actual local corrupt artifact and actual matching CloudSimulated split layout, strict staging verification and fresh Engine open.

Uncovered boundary: Healthy fresh acquisition plus excluded lease objects does not compare lease tokens numerically. Provider-backed cloud restore is unsupported; checksums do not make untrusted artifact parsing immune to every malformed input.

Catalog review: Reviewed despite the earlier non-WAL selection: capture drains/syncs an exact WAL cut and pins files; path disjointness, staged restore and support limits are adjacent unchanged contracts. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## BACKUP-4

Production: [src/engine/backup.rs:186](../../src/engine/backup.rs#L186) `Engine::restore_backup`; [src/engine/backup/paths.rs:114](../../src/engine/backup/paths.rs#L114) `publish`; [src/engine/backup.rs:563](../../src/engine/backup.rs#L563) `copy_verified_objects`.

Active regression references (not executed by this refresh):

- [tests/backup_paths.rs:87](../../tests/backup_paths.rs#L87) `should_preserve_foreign_stage_when_retrying_restore`: Foreign crash-like stage with sentinel coexists with real successful restore; sentinel bytes remain exact. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 111, 112.
- [tests/engine_api.rs:5175](../../tests/engine_api.rs#L5175) `should_leave_restore_target_absent_given_incomplete_backup_artifact`: Removing one real inventory object makes restore fail, target absent and no owned .midge-restore- stage remains. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 5198, 5199, 5200.

Preconditions: Real backup artifact and filesystem stage names; the first test intentionally creates a stage that this attempt does not own.

Uncovered boundary: Foreign-stage retention and failed owned-stage cleanup are tested; hard process death can still leave an owned temp stage. Retry safety does not promise immediate storage reclamation.

Catalog review: Reviewed despite the earlier non-WAL selection: capture drains/syncs an exact WAL cut and pins files; path disjointness, staged restore and support limits are adjacent unchanged contracts. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## BACKUP-5

Production: [src/engine/backup.rs:77](../../src/engine/backup.rs#L77) `Engine::backup_to`; [src/engine/backup.rs:186](../../src/engine/backup.rs#L186) `Engine::restore_backup`.

Active regression references (not executed by this refresh):

- [tests/engine_api.rs:5210](../../tests/engine_api.rs#L5210) `should_restore_split_cloud_simulation_layout_given_valid_backup`: Actual supported CloudSimulated split-layout restore succeeds with exact value and healthy new owner. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 5237, 5242, 5248, 5254, 5273, 5280.
- [tests/engine_api.rs:5118](../../tests/engine_api.rs#L5118) `should_leave_restore_target_absent_given_corrupt_backup_object`: Supported local artifact is validated and corrupt input refused. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 5140, 5141.

Preconditions: Local and matching filesystem CloudSimulated layouts are the allowed source/target kinds.

Uncovered boundary: No dedicated public negative test for every unsupported memory/provider-backed/mismatched-layout combination was verified in this bounded pass. Explicit NotSupported/InvalidArgument source branches are the boundary; positive tests cannot expand support.

Catalog review: Reviewed despite the earlier non-WAL selection: capture drains/syncs an exact WAL cut and pins files; path disjointness, staged restore and support limits are adjacent unchanged contracts. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## RES-1

Production: [src/common/resource_budget.rs:112](../../src/common/resource_budget.rs#L112) `ResourceBudget::reserve`; [src/runtime/transaction_spill/mod.rs:362](../../src/runtime/transaction_spill/mod.rs#L362) `TransactionWriteSet::push`.

Active regression references (not executed by this refresh):

- [src/common/resource_budget.rs:522](../../src/common/resource_budget.rs#L522) `should_report_current_charge_until_the_last_shared_reservation_is_released`: 7-byte shared charge remains after first owner drops, then used=0 after last owner drops. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 527, 535, 536.
- [src/common/resource_budget.rs:540](../../src/common/resource_budget.rs#L540) `should_reject_reservation_when_resource_budget_would_be_exceeded`: 7 of 10 held bytes causes 4-byte request ResourceLimit, peak remains 7, release permits full 10. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 549, 550, 552.
- [tests/fault_injection.rs:12443](../../tests/fault_injection.rs#L12443) `should_return_resource_limit_when_memory_mode_exhausts_transaction_pool`: Actual memory Engine with 8 KiB transaction pool rejects growing accepted-intent set as typed ResourceLimit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 12476.

Preconditions: Reservation units operate actual RAII budget ownership; public memory test uses real transaction code. Engine pool is distinct from whole-process RSS.

Uncovered boundary: Representative charged ownership does not prove every allocator/backend/queue byte is included, nor that process RSS equals configured engine budget. Delivered ACCT-1 through ACCT-6 map owner-bound persistent attempted/returned/durable metadata accounting and post-shutdown integrity; that bounded ledger does not extend allocator or process RSS coverage.

Catalog review: Reviewed resource envelopes and effect ownership: queued raw/encoded WAL, concurrent apply, replay and background workers remain bounded and owned through terminal completion. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## RES-2

Production: [src/runtime/transaction_spill/mod.rs:362](../../src/runtime/transaction_spill/mod.rs#L362) `TransactionWriteSet::push`; [src/runtime/actors/wal/transaction.rs:111](../../src/runtime/actors/wal/transaction.rs#L111) `prepare_transaction_append`; [src/runtime/event_loop/write_batch.rs:631](../../src/runtime/event_loop/write_batch.rs#L631) `prepare_transaction_for_coalescing`.

Active regression references (not executed by this refresh):

- [tests/storage_layer.rs:320](../../tests/storage_layer.rs#L320) `should_preserve_transaction_when_oversized_range_delete_is_rejected`: Actual oversized endpoint yields ResourceLimit while previously staged middle value remains visible, commits and reads after flush. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 333, 334, 340.
- [tests/fault_injection.rs:266](../../tests/fault_injection.rs#L266) `should_reject_transaction_when_no_space_hits_before_batch_append_and_remain_usable`: Actual before-batch WAL NoSpace leaves both failed keys absent; subsequent sync commit succeeds and exact accepted/failure key states survive reopen. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Listed markers are the actual behavior-assertion helper calls in this function. Assertion/helper call markers: 294, 295, 296, 314, 315, 316.
- [tests/snapshot_spill_pressure.rs:251](../../tests/snapshot_spill_pressure.rs#L251) `should_preserve_snapshot_isolation_when_spill_pressure_is_released`: More than 1.5 MiB of actual caller-owned spill files exhaust a 2 MiB local budget; rejected mutation preserves exact acknowledged state and the same write resumes after release. Scope: One deterministic public-API filesystem-backed CloudSimulated schedule; 16 MiB engine budget, 2 MiB local capacity, 128 KiB transaction pool, controlled TTL time, sixteen history rounds, actual spill saturation and exact clean reopen. No native-provider, crash, whole-process RSS or exhaustive-concurrency proof. Assertion/helper call markers: 271, 304, 41, 44, 50, 55.

Preconditions: Real local Engine and genuine admission/failpoint positions; previously accepted intents deliberately precede rejected operation. The composed spill-pressure regression uses the controlled TTL clock and actual simulated-cloud spill/GC files described in snapshot-spill-pressure-fixture.md.

Uncovered boundary: Before-publication rejection can be unapplied; once a write is submitted or partially persisted, error is not universally proof of no mutation. Partial-tail failure uses degradation/fencing instead (RES-3).

Catalog review: Reviewed resource envelopes and effect ownership: queued raw/encoded WAL, concurrent apply, replay and background workers remain bounded and owned through terminal completion. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## RES-3

Production: [src/runtime/event_loop/write_batch.rs:117](../../src/runtime/event_loop/write_batch.rs#L117) `ensure_l0_write_admission`; [src/runtime/actors/wal/transaction.rs:694](../../src/runtime/actors/wal/transaction.rs#L694) `append_prepared_transaction_batches`; [src/storage/hybrid/backend/uploads.rs:505](../../src/storage/hybrid/backend/uploads.rs#L505) `process_wal_upload`; [benches/bench_support/checkpoint_commit.rs:45](../../benches/bench_support/checkpoint_commit.rs#L45) `commit_with_backpressure`; [benches/tier4_system_checkpoint_write_amplification.rs:415](../../benches/tier4_system_checkpoint_write_amplification.rs#L415) `run_ingestion`.

Active regression references (not executed by this refresh):

- [tests/fault_injection.rs:320](../../tests/fault_injection.rs#L320) `should_preserve_acknowledged_state_when_no_space_tears_wal_frame`: Actual positional partial WAL write rolls physical length back; Engine becomes Degraded, follow-up is Fenced, prior ACK fixture survives both reopens and later repaired writes. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 379, 383, 384.
- [src/runtime/event_loop/write_batch/tests.rs:2222](../../src/runtime/event_loop/write_batch/tests.rs#L2222) `should_stall_before_wal_when_transaction_would_exceed_hard_l0_ceiling`: Typed WriteStall keeps sequence and actual WAL append count unchanged. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2252, 2258, 2275, 2282, 2283, 2288.

Preconditions: Actual filesystem partial-append failure and loop-owned L0 admission fixtures retain prior acknowledged data. The eight removed scripted benchmark commit controls are not current evidence.

Uncovered boundary: Bounded no-space/admission schedules do not prove permanent saturation clears. Removed helper clock/retry controls are an explicit assurance gap. Historical campaign objects retain their original source, rejection accounting and invalid/valid outcome boundaries; none qualifies the current branch.

Catalog review: Reviewed resource envelopes and effect ownership: queued raw/encoded WAL, concurrent apply, replay and background workers remain bounded and owned through terminal completion. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## RES-4

Production: [src/runtime/retry_schedule.rs:38](../../src/runtime/retry_schedule.rs#L38) `RetrySchedule::defer`; [src/runtime/event_loop/cloud_maintenance.rs:81](../../src/runtime/event_loop/cloud_maintenance.rs#L81) `schedule_cloud_maintenance`; [src/storage/hybrid/backend/uploads.rs:505](../../src/storage/hybrid/backend/uploads.rs#L505) `process_wal_upload`.

Active regression references (not executed by this refresh):

- [src/runtime/retry_schedule.rs:93](../../src/runtime/retry_schedule.rs#L93) `should_wake_when_retry_deadline_arrives`: Controlled clock leaves deferred retry not ready with 1s remaining; advancing original clock gives ready and zero remaining. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 101, 102, 106, 107.
- [src/runtime/hybrid_persistence/tests.rs:2212](../../src/runtime/hybrid_persistence/tests.rs#L2212) `should_keep_retrying_failed_wal_upload_when_every_attempt_fails`: Actual queued valid local WAL with always-failing storage and an injected zero-delay retry policy reports failures 1 through 6 in order and stays queued. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2234, 2245.

Preconditions: Backoff unit uses a controllable clock; actual upload-worker test uses an intentionally failing StorageBackend and finite polling/worker progression.

Uncovered boundary: Attempt count/backoff assertions do not alone prove CPU utilization, fairness or every terminal error path under all fault schedules. Permanent provider failure can halt useful maintenance while retaining authoritative state.

Catalog review: Reviewed resource envelopes and effect ownership: queued raw/encoded WAL, concurrent apply, replay and background workers remain bounded and owned through terminal completion. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## RES-5

Production: [src/engine/startup/deadline.rs:18](../../src/engine/startup/deadline.rs#L18) `deadline::open`; [src/common/deadline_scope.rs:71](../../src/common/deadline_scope.rs#L71) `DeadlineScope::check`; [src/runtime/router.rs:112](../../src/runtime/router.rs#L112) `ResponseRouter::request_deadline`; [src/runtime/event_loop/compaction.rs:489](../../src/runtime/event_loop/compaction.rs#L489) `CompactionCoordinator::compact_all`; [src/runtime/event_loop/compaction.rs:112](../../src/runtime/event_loop/compaction.rs#L112) `EventLoop::launch_compaction`.

Active regression references (not executed by this refresh):

- [tests/cloud_provider_engine_qualification/startup_deadline.rs:367](../../tests/cloud_provider_engine_qualification/startup_deadline.rs#L367) `should_keep_startup_deadline_when_mandatory_native_catalog_read_follows_acquisition`: Actual acquired-owner native open is bounded by the original 600ms deadline, no late runtime admission; cleanup and same-path healthy reopen preserve rows. Scope: ignored native Sqrzl qualification; explicit features/environment required Assertion/helper call markers: 391, 396, 400, 409, 410, 411, 413, 417.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs:317](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs#L317) `should_keep_manual_origin_when_later_shorter_waiter_expires`: actual generation retains exactly the original OperationDeadline after shorter/lower-ID waiter joins and expires; original and longer callers succeed with exact committed output. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 363, 364, 365, 366, 367, 369, 376, 380.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs:387](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs#L387) `should_refuse_budget_extension_when_longer_waiter_joins_accepted_manual_work`: actual second authority phase times out at original budget while longer route remains live; predecessor remote authority, local output intent/input bytes and exact rows remain. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 434, 435, 436, 440, 441, 445, 450, 454.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs:254](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs#L254) `should_preserve_background_owner_when_later_manual_waiter_expires`: actual accepted background generation retains None after later short waiter expires; real phases complete, exact cloud committed output/rows survive and inputs retire. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 294, 298, 299, 300, 302, 306, 310.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs:461](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs#L461) `should_inherit_original_clock_when_actual_manual_work_reaches_second_family`: two genuine CF input sets have the same immutable accepted origin; all phases finish before it, committed metadata has both exact output sets, exact rows survive and inputs/intents retire. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 499, 500, 501, 502, 503, 506, 520, 521, 522, 526, 527.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs:534](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs#L534) `should_stop_second_family_when_original_manual_clock_expires`: First CF commits; the second accepted generation retains the original origin and times out either before phase submission or after a genuine output-durability phase. Exact prior second-family authority/bytes remain intact; only an accepted publication phase owns a persisted intent. Only the target CF2 response is held; its actual provider budget cannot exceed original remaining time sampled before submission. Scope: Real compute, conditional provider lease, seeded SST bytes and actual completion dispositions. No callback is fabricated for prepublication rejection. This is not native socket cancellation or public transaction ACK proof. Assertion/helper call markers: 574, 622, 623, 624, 625, 626, 628, 641, 646, 656, 657, 658.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs:542](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs#L542) `should_retain_input_authority_when_second_family_compute_arrives_after_manual_deadline`: A real accepted second-family compute receipt is held past the original deadline, then rejected without publication phase or intent. Caller gets typed Timeout; first-family committed output and exact second-family input authority/bytes survive. Scope: Deterministic queued compute receipt over the actual coordinator/compute actor and conditional mock-backed storage; seeded SST rows, not public Engine ACKs or a native provider. Assertion/helper call markers: 574, 622, 623, 624, 625, 626, 628, 641, 646, 656, 657, 658.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/queued_phase_tests.rs:294](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/queued_phase_tests.rs#L294) `should_keep_typed_timeout_when_actual_final_clear_mirror_exhausts_original_budget`: Unheld genuine output/manifest phases and real GC/intent-clear work precede accepted final-worker submission. Only its actual provider reply crosses the unchanged original caller deadline; the clamped provider budget is bounded by pre-submission original remaining time. Typed Timeout, exact committed output, safe GC, complete reservation release and replay settlement survive. Scope: Actual compute/coordinator/non-inline publisher and conditional mock metadata; five-second original caller versus eight-second test provider cap. No production budget change, native transport or public ACK claim. Assertion/helper call markers: 302, 307, 308, 309, 310, 312, 313, 315, 316, 247, 253, 254, 283, 284, 285, 286.

Preconditions: The ignored native open case requires explicit sqrzl-tests prerequisites and --ignored execution. Compaction fixtures own a real conditional lease, genuine SST/input bytes and actual phase receipts; all setup/read-gate acquisition precedes the immutable deadline.

Uncovered boundary: Merged startup Some and manual compaction Some use one original deadline; None and Complete preserve ordinary operation caps. A joining waiter cannot replace a background None or an accepted manual origin. Caller Timeout is not provider cancellation or permission to release an accepted worker, lease, body or reservation. Local syscalls are cooperative at unit boundaries, not preemptible.

Catalog review: Reviewed resource envelopes and effect ownership: queued raw/encoded WAL, concurrent apply, replay and background workers remain bounded and owned through terminal completion. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## RES-6

Production: [src/runtime/state/flush.rs:229](../../src/runtime/state/flush.rs#L229) `l0_slot_usage`; [src/runtime/state/flush.rs:259](../../src/runtime/state/flush.rs#L259) `l0_write_slot_unavailable`; [src/runtime/event_loop/write_batch.rs:117](../../src/runtime/event_loop/write_batch.rs#L117) `ensure_l0_write_admission`.

Active regression references (not executed by this refresh):

- [src/runtime/state/tests.rs:843](../../src/runtime/state/tests.rs#L843) `should_stall_next_write_after_active_generation_reserves_last_l0_slot`: Constructed published/queued capacity plus actual active put reaches hard ceiling and sets slot unavailable/hard stall. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 859, 860, 872, 873, 874, 875.
- [src/runtime/event_loop/write_batch/tests.rs:2222](../../src/runtime/event_loop/write_batch/tests.rs#L2222) `should_stall_before_wal_when_transaction_would_exceed_hard_l0_ceiling`: Actual first accepted generation fills final slot; second returns WriteStall without sequence or WAL append change. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2252, 2258, 2275, 2282, 2283, 2288.

Preconditions: Configured small trigger/immutable capacity, positive equality of actual slot usage to hard ceiling before rejection; metadata input names are constructed in the event-loop fixture.

Uncovered boundary: The before-WAL assertion is stronger than checking a stall metric. It does not itself prove every pressure-recovery scheduling path when background compaction is disabled or every backlog geometry.

Catalog review: Reviewed resource envelopes and effect ownership: queued raw/encoded WAL, concurrent apply, replay and background workers remain bounded and owned through terminal completion. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## RES-7

Production: [src/compaction/executor/merge.rs:408](../../src/compaction/executor/merge.rs#L408) `collect_compaction_stream_inputs`; [src/runtime/event_loop/cloud_maintenance.rs:81](../../src/runtime/event_loop/cloud_maintenance.rs#L81) `schedule_cloud_maintenance`; [src/runtime/state/flush.rs:229](../../src/runtime/state/flush.rs#L229) `l0_slot_usage`; [src/compaction/repair.rs:24](../../src/compaction/repair.rs#L24) `local_capacity`.

Active regression references (not executed by this refresh):

- [src/compaction/mod.rs:2297](../../src/compaction/mod.rs#L2297) `should_keep_compaction_work_bounded_across_ten_thousand_targets`: Constructed stream input setup for one source and 10,000 target names has exactly two merge heads and charged peak <= pool limit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 2320, 2321.
- [src/compaction/mod.rs:1523](../../src/compaction/mod.rs#L1523) `should_leave_inputs_untouched_when_compaction_is_cancelled`: Actual cancelled Fs compaction preserves input bytes and produces no canonical output. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1546, 1547, 1548.
- [src/compaction/repair.rs:231](../../src/compaction/repair.rs#L231) `should_find_native_capacity_when_real_filesystem_root_is_canonical`: Actual RealFs root and canonicalized native mount selection report positive available capacity; the real overlapping repair remains separately checked, without fabricated disk capacity or a platform skip. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 249, 258, 259.
- [src/runtime/actors/compaction/publication_error_tests.rs:255](../../src/runtime/actors/compaction/publication_error_tests.rs#L255) `should_keep_exact_rows_when_healthy_background_repair_uses_no_manual_deadline`: Actual healthy background None compaction exercises genuine overlapping SST repair, scratch admission and exact output rows; this is the native Windows capacity regression rather than a synthetic drive-string test. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 232, 236, 237, 239, 240, 246, 250.

Preconditions: 10,000-target test is a real stream-construction path over MockFs/names, not 10,000 populated SSTs or completed sustained compaction. Cancellation control has genuine input SST.

Uncovered boundary: Bounded merge heads and conservative canonical-mount scratch capacity are safety properties. They do not prove a global decreasing-progress measure or sustained service SLO under arbitrary admission rates. Real filesystem-issued byte accounting does not establish physical device write amplification or production capacity.

Catalog review: Reviewed resource envelopes and effect ownership: queued raw/encoded WAL, concurrent apply, replay and background workers remain bounded and owned through terminal completion. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## RES-8

Production: [src/engine/mod.rs:122](../../src/engine/mod.rs#L122) `Engine::drop`; [src/engine/mod.rs:621](../../src/engine/mod.rs#L621) `Engine::shutdown`; [src/runtime/event_loop/shutdown.rs:18](../../src/runtime/event_loop/shutdown.rs#L18) `EventLoop::handle_shutdown`.

Active regression references (not executed by this refresh):

- [tests/fault_injection.rs:5339](../../tests/fault_injection.rs#L5339) `should_retain_writer_lease_until_blocked_flush_worker_exits`: Genuine paused publication makes short shutdown Timeout and contender open LeaseHeld; release owned worker, successful shutdown and same-path reacquisition follow. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 5371, 5372.
- [tests/fault_injection/compaction_deadline.rs:155](../../tests/fault_injection/compaction_deadline.rs#L155) `should_retain_acknowledged_inputs_when_expired_publication_remains_owned_during_shutdown`: actual CloudStrict-ACK data plus OutputDurable accepted failpoint worker survives compaction Timeout and short shutdown; contender cannot acquire until release/join, then strict reopen has exact acknowledged dataset. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 201, 205, 206, 207, 208, 209, 213, 217, 222, 223, 227, 228.
- [tests/checkpoint_accounting.rs:36](../../tests/checkpoint_accounting.rs#L36) `should_release_actual_engine_lease_when_retained_checkpoint_metrics_outlive_shutdown`: 32 genuinely sync-acknowledged local rows survive real flush, owned shutdown and same-path reopen while original counter-only handle remains alive; original and reopened owner identities differ and original final counters remain stable. Scope: actual public local Engine strict ACK, worker publication, same-path reopen and owned shutdown Assertion/helper call markers: 75, 79, 87, 88, 89, 90, 91, 92.

Preconditions: Public held-worker controls retain real acknowledged data and release their finite holds before joins/assertions. The counter-lifetime control uses local strict ACK and an actual same-path Engine open; no constructed owner counter substitutes for lease reacquisition.

Uncovered boundary: Caller teardown wait can expire while reaper continues. These schedules prove fencing retention and acknowledged data for their accepted workers, not termination under a permanently hung arbitrary backend. No new optional reclamation should be started just to finish shutdown.

Catalog review: Reviewed resource envelopes and effect ownership: queued raw/encoded WAL, concurrent apply, replay and background workers remain bounded and owned through terminal completion. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## PROG-1

Production: [src/telemetry/recovery_progress.rs:30](../../src/telemetry/recovery_progress.rs#L30) `WorkProgress::completed`; [src/telemetry/recovery_progress.rs:58](../../src/telemetry/recovery_progress.rs#L58) `WorkProgress::finish`; [src/telemetry/recovery_progress.rs:99](../../src/telemetry/recovery_progress.rs#L99) `RecoveryReadObserver::remote_range_completed`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:473](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L473) `reconcile_manifest_ssts`.

No active regression target is recorded for this row.

Preconditions: Completed recovery work is reported only after successful bounded I/O or parsing; source markers remain available. The removed native watchdog target is not current evidence.

Uncovered boundary: No active Cargo regression currently proves delayed WAL reads, inventory HEADs or journal replay keep a native watchdog healthy, or that a held first request receives no progress credit. Historical watchdog claims are not current coverage or provider qualification.

Catalog review: Reviewed current producer/listener sources. Removed watchdog test targets are named assurance gaps, not surviving regression evidence. Current production/helper source only; no active regression currently covers the named assurance gap.

## PROG-2

Production: [benches/bench_support/stress_scenarios/recovery_progress.rs:33](../../benches/bench_support/stress_scenarios/recovery_progress.rs#L33) `RecoveryScope::enter`; [benches/bench_support/stress_scenarios/recovery_progress.rs:103](../../benches/bench_support/stress_scenarios/recovery_progress.rs#L103) `RecoveryProgressLayer::on_event`.

No active regression target is recorded for this row.

Supporting helper source:

- [benches/bench_support/stress_scenarios/recovery_progress.rs:170](../../benches/bench_support/stress_scenarios/recovery_progress.rs#L170) `assert_scope_isolation`: Real layer/phase handle exercises scope generation changes and unrelated thread emission. Scope: Unexecuted cfg(test) helper source; no active Cargo test target currently invokes this helper.

Preconditions: The surviving recovery-listener helper uses constructed tracing events and a thread-bound caller scope, but no active Cargo test target invokes it.

Uncovered boundary: No active regression currently proves listener filtering of background events, failed/empty work, stale scopes or old generations. Source/helper assertions alone do not execute that contract.

Catalog review: Reviewed current producer/listener sources. Removed watchdog test targets are named assurance gaps, not surviving regression evidence. Current production and unexecuted supporting helper source only; no active regression currently covers the named assurance gap.

## ACCT-1

Production: [src/metadata/accounting.rs:361](../../src/metadata/accounting.rs#L361) `MetricsHandle`; [src/metadata/accounting.rs:374](../../src/metadata/accounting.rs#L374) `Owner`; [src/runtime/state/accounting.rs:46](../../src/runtime/state/accounting.rs#L46) `checkpoint_metrics`.

Active regression references (not executed by this refresh):

- [tests/checkpoint_accounting.rs:36](../../tests/checkpoint_accounting.rs#L36) `should_release_actual_engine_lease_when_retained_checkpoint_metrics_outlive_shutdown`: Actual public local sync ACK of 32 values, exact points/ordered scan, public flush, both owned shutdowns and same-path reopen precede the original-owner stable snapshot and distinct owner identity assertions. Scope: actual public local Engine strict ACK, worker publication, same-path reopen and owned shutdown Assertion/helper call markers: 75, 79, 87, 88, 89, 90, 91, 92.

Preconditions: Persistent local Engine with background compaction disabled and rows below the automatic threshold; genuine API acknowledgement and reacquisition, not a synthetic counter-owner lifetime check.

Uncovered boundary: This fixture proves the local retained-handle boundary. It is not native cloud deployment qualification or a proof that every unrelated diagnostic object retains no resources.

Catalog review: Reviewed owner-scoped counters and historical benchmark evidence. Reference existence is not executed accounting proof; separate WAL/ledger/total physical fsync accounting remains tracked under #904. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## ACCT-2

Production: [src/metadata/accounting.rs:18](../../src/metadata/accounting.rs#L18) `Origin`; [src/metadata/accounting.rs:60](../../src/metadata/accounting.rs#L60) `Medium`; [src/metadata/store.rs:141](../../src/metadata/store.rs#L141) `new_with_accounting`; [src/runtime/state/accounting.rs:50](../../src/runtime/state/accounting.rs#L50) `accept_flush_publication_attempt`; [src/engine/startup/storage.rs:715](../../src/engine/startup/storage.rs#L715) `bootstrap_staging_fs`.

Active regression references (not executed by this refresh):

- [src/runtime/event_loop/flush_pipeline/accounting_tests.rs:262](../../src/runtime/event_loop/flush_pipeline/accounting_tests.rs#L262) `should_preserve_original_publication_when_actual_retry_runs_during_shutdown`: Real destination-directory publication failure retains built output/name/origin/start; shutdown-drain retries the same immutable and credits two attempts, one failure and exactly one original-origin committed SST. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 284, 294, 295, 296, 297, 303, 309, 310, 311, 325, 326, 327, 335, 337, 338, 339, 340, 344, 351.
- [src/engine/tests/checkpoint_origins.rs:49](../../src/engine/tests/checkpoint_origins.rs#L49) `should_attribute_cloud_shutdown_flush_when_acknowledged_rows_remain_active`: 32 real filesystem-CloudSimulated cloud_strict ACK rows remain active until owned shutdown; exact same-path recovery and Shutdown credit 1 with CloudFlush/OrdinaryLocalFlush credit 0. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 63, 84, 85, 86, 87, 88, 89, 96, 103.
- [src/engine/tests/checkpoint_origins.rs:111](../../src/engine/tests/checkpoint_origins.rs#L111) `should_attribute_compaction_checkpoint_when_real_local_outputs_replace_inputs`: Two actual local sync-ACK flushes followed by public compact_all yield actual compaction count, exact reopened values and distinct CompactionBeforeGc checkpoint/SST credit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 150, 154, 158, 159, 160, 161, 168, 175, 182.
- [src/engine/startup/streaming_recovery/tests/checkpoint_accounting.rs:30](../../src/engine/startup/streaming_recovery/tests/checkpoint_accounting.rs#L30) `should_attribute_recovery_publication_when_real_wal_exceeds_local_capacity`: Actual encoded/cataloged WAL larger than configured local capacity forces multiple genuine recovery SST checkpoints and exact 384-row recovery/reopen; Recovery credit is present at open and ordinary/CloudFlush/Shutdown credit remains zero. Scope: actual Engine recovery from seeded encoded/cataloged WAL; no public ACK claim Assertion/helper call markers: 42, 57, 61, 62, 66, 67, 68, 69, 75, 83, 84.

Preconditions: The recovery fixture seeds genuine encoded and cataloged WAL; it does not claim those rows were publicly acknowledged. CloudSimulated is filesystem-backed, not native S3/Azure/GCS. Independent bootstrap staging is MemoryOnly and outside the later exported runtime owner. Serialized origins are ordinary_local_flush, cloud_flush, recovery, bootstrap, ddl, compaction_before_gc, administration, shutdown and unclassified; media are persistent and memory_only.

Uncovered boundary: Explicit origin/medium boundaries do not account pre-state FORMAT creation, load/repair/hydration or independently owned bootstrap into the exported runtime counters. Those pre-state costs are unknown, not zero. Native cloud wire bytes and provider write cost are outside this accounting.

Catalog review: Reviewed owner-scoped counters and historical benchmark evidence. Reference existence is not executed accounting proof; separate WAL/ledger/total physical fsync accounting remains tracked under #904. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## ACCT-3

Production: [src/metadata/accounted_fs.rs:160](../../src/metadata/accounted_fs.rs#L160) `write_at`; [src/metadata/accounted_fs.rs:174](../../src/metadata/accounted_fs.rs#L174) `append`; [src/metadata/accounting.rs:559](../../src/metadata/accounting.rs#L559) `snapshot_durable`; [src/metadata/accounting.rs:569](../../src/metadata/accounting.rs#L569) `checkpoint_complete`; [src/metadata/store.rs:201](../../src/metadata/store.rs#L201) `save_snapshot_for`.

Active regression references (not executed by this refresh):

- [src/metadata/accounting/tests/fs_tests.rs:72](../../src/metadata/accounting/tests/fs_tests.rs#L72) `should_account_exact_frames_when_actual_single_and_batch_edits_commit`: Independent delegated RealFs oracle sees actual one-edit and atomic batch frames plus durability markers; actual journal replay and exact bytes precede issued/returned/durable producer equality. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named Assertion/helper call markers: 117, 118, 119, 129, 131, 135, 140, 141, 142, 144, 145, 146, 147, 148, 149, 150, 156.
- [src/metadata/accounting/tests/fs_tests.rs:167](../../src/metadata/accounting/tests/fs_tests.rs#L167) `should_credit_completed_checkpoint_when_actual_snapshot_and_truncation_commit`: Actual serialized snapshot, rename/barriers and zero-length journal are strictly reloaded before durable/checkpoint-count and oracle-byte comparisons. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named Assertion/helper call markers: 192, 193, 194, 205, 209, 210, 211, 213, 214, 215, 216, 217, 218, 219.
- [src/metadata/accounting/tests/fs_tests.rs:224](../../src/metadata/accounting/tests/fs_tests.rs#L224) `should_preserve_attempted_payload_when_actual_checkpoint_io_fails`: Actual open/write/rename errors preserve authoritative snapshot bytes and original typed failures; oracle-issued/returned bytes survive without snapshot-durable or checkpoint-complete credit. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named Assertion/helper call markers: 255, 260, 264, 271, 273, 275, 284, 285, 286, 287, 288, 289.
- [src/metadata/accounting/tests/fs_tests.rs:295](../../src/metadata/accounting/tests/fs_tests.rs#L295) `should_preserve_returned_frames_when_actual_journal_sync_fails`: Both actual journal frames return, then required sync fails; the real retained prefix and original Io error precede returned bytes >0 and journal durable bytes 0. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named Assertion/helper call markers: 326, 330, 331, 332, 333, 335, 336, 337, 338.
- [src/metadata/accounting/tests/integrity.rs:292](../../src/metadata/accounting/tests/integrity.rs#L292) `should_account_forced_checkpoint_failure_when_snapshot_commits_before_truncation`: Actual snapshot durability precedes real journal-truncation failure; snapshot durable count remains 1 while checkpoint complete is 0 and operation failure is recorded. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named Assertion/helper call markers: 339, 344, 355, 356, 357, 358, 362, 363, 364, 365, 366, 367, 368, 369.

Preconditions: RealFs writes and real framed/serialized metadata are independently observed. Fault delegates preserve the requested actual operation/error boundary; their controlled failure does not model every device partial-write implementation.

Uncovered boundary: Filesystem-issued payload is not physical/device/FTL bytes, filesystem journaling, compression, delayed writeback or another process. Presence after failed sync is not crash durability; provider upload bodies/retries and provider-side writes are outside the byte gate.

Catalog review: Reviewed owner-scoped counters and historical benchmark evidence. Reference existence is not executed accounting proof; separate WAL/ledger/total physical fsync accounting remains tracked under #904. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## ACCT-4

Production: [src/runtime/state/accounting.rs:62](../../src/runtime/state/accounting.rs#L62) `finish_flush_publication_attempt`; [src/runtime/event_loop/flush_pipeline/publication.rs:398](../../src/runtime/event_loop/flush_pipeline/publication.rs#L398) `install_flush_publication`; [src/engine/startup/streaming_recovery.rs:401](../../src/engine/startup/streaming_recovery.rs#L401) `publish_checkpoint_output`.

Active regression references (not executed by this refresh):

- [src/runtime/event_loop/flush_pipeline/accounting_tests.rs:135](../../src/runtime/event_loop/flush_pipeline/accounting_tests.rs#L135) `should_credit_installed_sst_once_when_actual_mirror_receipt_is_replayed`: Actual Build/Publish/Mirror, exact installed SST row/size/sequence and immutable removal precede replay of the captured genuine Mirror receipt; the entire snapshot remains identical and committed SST count/bytes are credited once. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 145, 146, 154, 176, 181, 187, 188, 189, 190, 194, 195.
- [src/runtime/event_loop/flush_pipeline/accounting_tests.rs:262](../../src/runtime/event_loop/flush_pipeline/accounting_tests.rs#L262) `should_preserve_original_publication_when_actual_retry_runs_during_shutdown`: Actual failing Publish receipt and its duplicate fold one failed attempt; genuine shutdown retry preserves identity/logical start and installs exactly once. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 284, 294, 295, 296, 297, 303, 309, 310, 311, 325, 326, 327, 335, 337, 338, 339, 340, 344, 351.
- [src/runtime/event_loop/flush_pipeline/accounting_tests.rs:357](../../src/runtime/event_loop/flush_pipeline/accounting_tests.rs#L357) `should_not_count_publication_when_actual_actor_build_exhausts_its_memory`: Actual bounded-memory Build ResourceLimit preserves the genuine row and immutable without an accepted publication clock, publication attempts or committed SST credit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 369, 385, 386, 387, 388, 389, 399, 400, 401, 402, 403.

Preconditions: Actual actor/event-loop worker receipts and real SST bytes are prerequisites; duplicates replay captured genuine receipts. Publication-attempt totals also include rejected submit attempts, not only accepted workers.

Uncovered boundary: The full-publication denominator excludes SST build and admission time before first accepted publication; public flush_cf latency is separate. Recoverable per-attempt publication failures remain diagnostic; successful final logical installation and settled-owner integrity are still required.

Catalog review: Reviewed owner-scoped counters and historical benchmark evidence. Reference existence is not executed accounting proof; separate WAL/ledger/total physical fsync accounting remains tracked under #904. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## ACCT-5

Production: [src/metadata/accounting.rs:513](../../src/metadata/accounting.rs#L513) `observe_mutation`; [src/metadata/accounting.rs:573](../../src/metadata/accounting.rs#L573) `finish`; [src/metadata/accounting.rs:307](../../src/metadata/accounting.rs#L307) `Snapshot::delta`; [src/metadata/accounted_fs.rs:28](../../src/metadata/accounted_fs.rs#L28) `observe_mutation`.

Active regression references (not executed by this refresh):

- [src/metadata/accounting/tests/late_failed_payload.rs:115](../../src/metadata/accounting/tests/late_failed_payload.rs#L115) `should_detect_late_error_when_actual_append_finishes_after_operation_seals`: An actual positive-byte delegate is held before its original injected typed Err returns; late error completion invalidates the sealed owner with no successful returned-byte credit. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named Assertion/helper call markers: 87, 91, 92, 97, 99, 103, 104, 105, 106, 107, 108.
- [src/metadata/accounting/tests/late_failed_payload.rs:125](../../src/metadata/accounting/tests/late_failed_payload.rs#L125) `should_detect_late_error_when_actual_write_at_finishes_after_operation_seals`: The same held genuine positional payload checks the escaped completion flag and preserves the failure class. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named Assertion/helper call markers: 87, 91, 92, 97, 99, 103, 104, 105, 106, 107, 108.
- [src/metadata/accounting/tests/late_failed_payload.rs:135](../../src/metadata/accounting/tests/late_failed_payload.rs#L135) `should_preserve_failure_without_late_flag_when_actual_append_finishes_before_sealing`: Releasing/joining the actual failed delegate while its operation is live records failure/issued bytes without a spurious late flag. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named Assertion/helper call markers: 87, 91, 92, 97, 99, 103, 104, 105, 106, 107, 108.
- [src/metadata/accounting/tests/integrity.rs:236](../../src/metadata/accounting/tests/integrity.rs#L236) `should_detect_late_mutation_when_a_persistent_file_truncates_without_payload`: Actual non-payload file truncation after operation sealing marks escaped accounting. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named Assertion/helper call markers: 264, 265, 266, 267, 278, 282, 283, 285, 286, 287.
- [src/metadata/accounting/tests/bounded.rs:61](../../src/metadata/accounting/tests/bounded.rs#L61) `should_subtract_histogram_buckets_when_warmup_precedes_measurement`: Constructed histogram recording control subtracts a slow warmup bin and retains exactly 100 measured observations with p95 below 5ms. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 64, 69, 72, 77, 78.
- [src/metadata/accounting/tests/bounded.rs:105](../../src/metadata/accounting/tests/bounded.rs#L105) `should_retain_late_invalidation_when_sealed_observation_counter_is_saturated`: Constructed private-state boundary deliberately seeds the late counter to u64::MAX, then invokes the real sealed-ledger mutation observation. Saturation remains sticky, delta stays invalid and no issued/returned payload is fabricated; this is arithmetic/invalidation evidence, not an actual u64::MAX workload. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 120, 121, 128, 129, 131, 132, 133, 134.

Preconditions: Actual held/delegated Fs cases prove wrapper lifecycle boundaries. Histogram values and the private u64::MAX seed are intentionally constructed; those controls prove arithmetic and sticky invalidation only, not actual checkpoint cost, device I/O, accumulated escaped filesystem mutations or hosted measurement.

Uncovered boundary: Operation-ledger completeness and the active_operations gauge do not mean every runtime or background worker has finished. The selected assertions do not prove the semantic adequacy of every failure schedule.

Catalog review: Reviewed owner-scoped counters and historical benchmark evidence. Reference existence is not executed accounting proof; separate WAL/ledger/total physical fsync accounting remains tracked under #904. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## ACCT-6

Production: [benches/bench_support/checkpoint_gate.rs:17](../../benches/bench_support/checkpoint_gate.rs#L17) `evaluate`; [benches/bench_support/checkpoint_gate.rs:128](../../benches/bench_support/checkpoint_gate.rs#L128) `settlement_invalid_reasons`; [examples/checkpoint_campaign_readback/accounting.rs:184](../../examples/checkpoint_campaign_readback/accounting.rs#L184) `persistent_integrity`; [examples/checkpoint_campaign_readback/campaign.rs:379](../../examples/checkpoint_campaign_readback/campaign.rs#L379) `evaluate`; [examples/checkpoint_campaign_readback/campaign.rs:432](../../examples/checkpoint_campaign_readback/campaign.rs#L432) `evaluate_construction_smoke`; [benches/tier4_system_checkpoint_write_amplification.rs:374](../../benches/tier4_system_checkpoint_write_amplification.rs#L374) `finish_window`; [examples/checkpoint_campaign_readback/commit_backpressure.rs:86](../../examples/checkpoint_campaign_readback/commit_backpressure.rs#L86) `qualify`; [examples/checkpoint_campaign_readback/commit_backpressure.rs:121](../../examples/checkpoint_campaign_readback/commit_backpressure.rs#L121) `check_native_no_progress`; [docs/development/checkpoint-write-amplification.md:265](../../docs/development/checkpoint-write-amplification.md#L265) `## Recorded hosted outcome`; [docs/development/performance-targets.md:198](../../docs/development/performance-targets.md#L198) `### Recorded checkpoint campaign`; [docs/development/evidence/checkpoint-715-readback.json:24](../../docs/development/evidence/checkpoint-715-readback.json#L24) `"conditional_policy_measurement_predicate_met": true,`; [benches/bench_support/checkpoint_boundary.rs:58](../../benches/bench_support/checkpoint_boundary.rs#L58) `capture_metadata_boundary`; [benches/tier4_system_checkpoint_write_amplification.rs:322](../../benches/tier4_system_checkpoint_write_amplification.rs#L322) `begin_window`; [benches/tier4_system_checkpoint_write_amplification.rs:294](../../benches/tier4_system_checkpoint_write_amplification.rs#L294) `capture_boundary`; [examples/checkpoint_campaign_readback/metadata_boundary.rs:88](../../examples/checkpoint_campaign_readback/metadata_boundary.rs#L88) `qualify`.

Active regression references (not executed by this refresh):

- [examples/checkpoint_campaign_readback/contract_tests.rs:37](../../examples/checkpoint_campaign_readback/contract_tests.rs#L37) `should_retain_all_nine_planned_rows_when_campaign_evidence_is_missing`: Constructed missing-evidence readback retains nine invalid planned rows and cannot qualify any cell. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 43, 44, 49, 53.
- [examples/checkpoint_campaign_readback/contract_tests.rs:191](../../examples/checkpoint_campaign_readback/contract_tests.rs#L191) `should_report_exact_five_ms_histogram_boundary_when_p95_rank_reaches_the_later_bin`: Constructed histogram boundary control reports the exact p95 interval; no actual checkpoint latency claim. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 199.
- [examples/checkpoint_campaign_readback/contract_tests.rs:316](../../examples/checkpoint_campaign_readback/contract_tests.rs#L316) `should_reject_final_owner_when_forced_checkpoint_fails_after_valid_measured_window`: Constructed post-window failure invalidates the actual reader final-owner gate rather than entering the ordinary cost ratio. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 330, 333.
- [examples/checkpoint_campaign_readback/contract_tests.rs:408](../../examples/checkpoint_campaign_readback/contract_tests.rs#L408) `should_check_both_final_owner_files_when_constructed_later_forced_work_fails`: Actual qualifier reads each constructed final filename mutation and rejects later forced failure in either original or reopened owner. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 412, 426.
- [examples/checkpoint_campaign_readback/contract_tests.rs:57](../../examples/checkpoint_campaign_readback/contract_tests.rs#L57) `should_refuse_download_binding_when_destination_already_contains_unbound_files`: Constructed hosted capture plus actual old local bytes refuses fresh provenance without replacing the retained file. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 67, 68, 69.
- [examples/checkpoint_campaign_readback/contract_tests.rs:73](../../examples/checkpoint_campaign_readback/contract_tests.rs#L73) `should_preserve_originating_provenance_when_readback_requests_another_run`: Constructed run identity mismatch leaves the actual originating receipt byte-identical. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 85, 86.
- [examples/checkpoint_campaign_readback/contract_tests.rs:90](../../examples/checkpoint_campaign_readback/contract_tests.rs#L90) `should_refuse_incomplete_pagination_when_hosted_artifact_capture_omits_rows`: Constructed REST total-count/capture mismatch is rejected as incomplete pagination; no actual Actions pagination is performed. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 101.
- [examples/checkpoint_campaign_readback/contract_tests.rs:105](../../examples/checkpoint_campaign_readback/contract_tests.rs#L105) `should_reject_hosted_capture_drift_without_replacing_pending_download_evidence`: Constructed REST capture changes after prepare; real seal refuses it and preserves the original pending receipt bytes. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 119, 122.
- [examples/checkpoint_campaign_readback/contract_tests.rs:132](../../examples/checkpoint_campaign_readback/contract_tests.rs#L132) `should_ignore_latest_alias_when_binding_exact_native_timestamp_and_pid`: Constructed canonical receipt and alias exercise exact timestamp/PID matching without fabricated completed workload. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 142, 143.
- [examples/checkpoint_campaign_readback/contract_tests.rs:147](../../examples/checkpoint_campaign_readback/contract_tests.rs#L147) `should_reject_two_distinct_canonical_receipts_when_both_match_one_process_identity`: Two constructed native-shape canonical receipts with one PID/source/selector are rejected as ambiguous. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 159.
- [examples/checkpoint_campaign_readback/contract_tests.rs:163](../../examples/checkpoint_campaign_readback/contract_tests.rs#L163) `should_preserve_native_typed_failure_when_diagnostic_receipt_contains_no_progress`: Constructed native failure metadata preserves no_progress and partial counters independently of successful work. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 171, 172, 173.
- [examples/checkpoint_campaign_readback/contract_tests.rs:177](../../examples/checkpoint_campaign_readback/contract_tests.rs#L177) `should_skip_malformed_nested_native_metadata_without_panicking`: Malformed constructed nested metadata and ambiguous selector flags are refused without panic. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 184, 185.
- [examples/checkpoint_campaign_readback/contract_tests.rs:258](../../examples/checkpoint_campaign_readback/contract_tests.rs#L258) `should_apply_exact_snapshot_threshold_when_fixed_policy_counters_cross_five_percent`: Constructed accounting counters on both sides of the exact 5% policy boundary exercise rational comparison; no measured checkpoint-byte claim. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 267, 268, 269.
- [examples/checkpoint_campaign_readback/contract_tests.rs:273](../../examples/checkpoint_campaign_readback/contract_tests.rs#L273) `should_reject_forced_checkpoint_failure_when_ordinary_policy_denominators_are_valid`: Constructed failed forced-origin metadata invalidates persistence integrity while the original ordinary ratio remains valid. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 276, 281.
- [examples/checkpoint_campaign_readback/contract_tests.rs:287](../../examples/checkpoint_campaign_readback/contract_tests.rs#L287) `should_reject_original_owner_delta_when_histogram_or_counters_decrease`: Constructed cumulative histogram decrease is refused instead of being interpreted as a valid same-owner measured delta. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 295.
- [examples/checkpoint_campaign_readback/contract_tests.rs:299](../../examples/checkpoint_campaign_readback/contract_tests.rs#L299) `should_retain_nine_unqualified_rows_when_transport_smoke_has_no_native_evidence`: Missing native construction evidence retains all nine unqualified rows and cannot accept a cadence or campaign predicate. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 305, 306, 307, 308, 312.
- [examples/checkpoint_campaign_readback/contract_tests.rs:340](../../examples/checkpoint_campaign_readback/contract_tests.rs#L340) `should_reject_final_owner_when_persistent_publication_histogram_omits_completed_flush`: Constructed completed-flush counter with missing full-publication histogram rejects the final owner integrity gate. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 347.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:30](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L30) `should_retain_stall_diagnostics_when_constructed_strict_successes_match_fixed_cycles`: Constructed completed-prefix records preserve measured strict-success/stall diagnostics while declaring that wait-clear is not ACK and JSON does not verify executed waiter bodies. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 36, 37, 38, 39.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:43](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L43) `should_reject_missing_or_altered_attempts_when_constructed_backpressure_endpoint_is_checked`: Constructed missing or altered attempts cannot default to zero or escape successes-plus-commit-stalls equality. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 51, 56.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:60](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L60) `should_reject_warmup_counter_reset_when_constructed_measured_endpoint_appears_complete`: Constructed erased warmup successes invalidate the otherwise plausible measured endpoint. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 65.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:69](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L69) `should_reject_waiter_clear_as_ack_when_constructed_commit_counters_are_incomplete`: Constructed waiter-clear credit cannot replace one of the fixed actual-strict-success denominators. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 75.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:79](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L79) `should_reject_excluded_wait_time_when_constructed_measured_clock_is_shorter_than_wait_delta`: Constructed positive wait elapsed greater than the supplied measured window is rejected; the coarse JSON inequality does not prove a real waiter body. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 84.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:88](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L88) `should_reject_changed_budget_or_native_watchdog_when_constructed_policy_is_checked`: Constructed >30s retry policy and altered native print-config watchdog are refused; the declared 60s native watchdog text is separately parsed. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 93, 94, 98.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:105](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L105) `should_reject_waiter_calls_when_constructed_cumulative_endpoint_has_no_commit_rejections`: #730 constructed cumulative zero-commit-rejection endpoint cannot fabricate waiter calls through waiter-route rejection counters. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 115.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:119](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L119) `should_reject_waiter_calls_when_constructed_measured_delta_has_no_new_commit_rejections`: #730 constructed individually coherent completed prefixes cannot add measured waiter calls/time without a new measured commit rejection; no waiter spans the completed warmup boundary. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 129.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:106](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L106) `should_retain_busy_warmup_diagnostics_when_constructed_metadata_endpoints_are_idle`: Constructed parser evidence permits 7s warmup capture beside 6s measured ingestion, actual-shaped scheduler pause 3ms above requested 2ms and MemoryOnly activity; it does not qualify a campaign or execute a waiter. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 113.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:120](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L120) `should_reject_missing_boundary_evidence_when_constructed_observations_are_checked`: Constructed missing endpoint observation is not an implicit idle/complete record. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 131.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:135](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L135) `should_reject_changed_boundary_policy_when_constructed_clock_scope_or_budget_differs`: Constructed altered>30s cap or excluded end-clock declaration is rejected under the fixed original 900s cell policy. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 140, 145.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:149](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L149) `should_reject_boundary_sample_arithmetic_when_constructed_busy_polls_are_omitted`: Constructed sample omission cannot satisfy successful samples=active+1 and pauses=active. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 156.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:160](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L160) `should_reject_excluded_end_capture_when_constructed_elapsed_exceeds_measured_interval`: Constructed end capture larger than actual-shaped measured elapsed is refused; warmup elapsed remains separately excluded. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 171.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:175](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L175) `should_reject_fake_idle_boundary_when_constructed_selected_snapshot_has_persistent_work`: Constructed declared idle observation contradicting the chosen raw Persistent-active snapshot is rejected; later settled shutdown cannot replace the selected endpoint. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 184.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:188](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L188) `should_reject_boundary_copy_drift_when_constructed_final_status_differs_from_ingestion`: Constructed status/ingestion endpoint drift cannot substitute a later observation for the selected capture. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 195.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:199](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L199) `should_reject_expired_warmup_capture_when_constructed_elapsed_equals_original_allowance`: Constructed complete warmup capture exactly at the 30s allowance is rejected, matching producer late-idle admission; this parser arithmetic control is not a native expired-query run. Scope: constructed recording/readback contract; no workload execution claim Assertion/helper call markers: 210.

Preconditions: Current reader controls construct JSON/counter evidence and sometimes temporary-file reads; they do not execute hosted campaigns, waiter bodies, device I/O or full-hour qualification. Removed boundary clock controls are not current evidence. Actual owner-lifetime coverage uses public strict ACK, flush, exact reads, shutdown and reopen.

Uncovered boundary: No active test directly pins capture_metadata_boundary deadline admission, active-to-idle sampling, late idle rejection, pause accounting or terminal query errors. Initial campaign 37268459342 at c8983928 remains invalid after the three A repeats terminated on genuine L0 WriteStall; six other rows cannot qualify it. Historical smoke 37270512400 validates A/r1 transport/construction only at 32911805: 256 commits, 230 measured, 100 compactions, zero stalls/waits and retained too_few_samples diagnostics. Historical nine-run 37270845568 at 32911805 remains invalid: A/r1 and A/r2 verified all data but selected active CompactionBeforeGc metadata endpoints, despite later settled owners; seven other valid rows cannot repair it. Measured c8f0de80 smoke 37273556125 is transport/construction-valid; all nine fresh attempts of 37274074026 are valid, totaling 1,376,256 real strict ACK rows, exact point/ordered scans before and after reopen and two owned shutdowns per attempt. B/C each miss the snapshot-payload target in three repeats (13.023–13.534% / 80.261–80.262%); A has one time miss and does not qualify. A/r1 end and B/r1/B/r3 warmup captured real active candidates then selected idle, without weakening the gate. This satisfies only the conditional policy-investigation predicate. Cadence remains unchanged, pre-state costs are unknown/excluded, and no physical-device, native confidence or final fourteen-hour qualification follows. Adjacent samples are not global barriers and JSON cannot prove executed body/clock. Final fourteen hours use the final merged #711 source.

Catalog review: Reviewed owner-scoped counters and historical benchmark evidence. Reference existence is not executed accounting proof; separate WAL/ledger/total physical fsync accounting remains tracked under #904. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## FORMAT-1

Production: [src/metadata/format.rs:79](../../src/metadata/format.rs#L79) `validate_format_marker`; [src/wal/frame.rs:528](../../src/wal/frame.rs#L528) `next_frame`; [src/codec.rs:346](../../src/codec.rs#L346) `decompress_block_with_trailer`.

Active regression references (not executed by this refresh):

- [src/metadata/format.rs:272](../../src/metadata/format.rs#L272) `should_reject_open_given_future_format_version_when_starting`: Actual FORMAT=current+1 yields CompatibilityError. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 286.
- [tests/storage_layer.rs:1112](../../tests/storage_layer.rs#L1112) `should_reject_nonshipping_codec_codes_without_fallback`: Codec 4/5/255 bytes with correct trailer CRC yield Corruption/unknown-code outcome. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1128, 1129.
- [tests/storage_layer.rs:1136](../../tests/storage_layer.rs#L1136) `should_reject_corrupt_compressed_payload_for_every_shipping_codec`: Real LZ4/Zstd3/Zstd9 payload corruption with recomputed CRC still yields Corruption rather than raw fallback. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1165.
- [src/wal/format/segment/tests.rs:82](../../src/wal/format/segment/tests.rs#L82) `should_reject_unsupported_segment_version_with_a_valid_header_checksum`: Both zero and future segment versions fail despite a recomputed valid header CRC. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 94.
- [src/wal/format/segment/tests.rs:101](../../src/wal/format/segment/tests.rs#L101) `should_reject_unsupported_record_version_inside_a_valid_crc_frame`: A future record payload version is rejected inside an otherwise valid segment header and frame CRC. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 112.

Preconditions: Actual persisted marker and real compressed block encoders/decoders; deliberately valid CRC isolates semantic codec/payload failures.

Uncovered boundary: No blanket historical decoder: database FORMAT 3/4 and SST V4 policy applies; FORMAT 1/2 and SST <=3 are rejected. Fuzz/golden evidence is bounded, not all malformed-byte proof.

Catalog review: Reviewed compatibility boundaries for WAL/ledger/catalog and current metadata/SST readers. Unsupported formats fail before authoritative mutation; no migration or new execution qualification is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## FORMAT-2

Production: [src/engine/api/transaction.rs:442](../../src/engine/api/transaction.rs#L442) `Transaction::put`; [src/engine/api/transaction.rs:469](../../src/engine/api/transaction.rs#L469) `Transaction::insert`; [src/engine/api/transaction.rs:519](../../src/engine/api/transaction.rs#L519) `Transaction::delete_range`; [src/compaction/mod.rs:42](../../src/compaction/mod.rs#L42) `execute_compaction`.

Active regression references (not executed by this refresh):

- [tests/storage_layer.rs:282](../../tests/storage_layer.rs#L282) `should_reject_oversized_value_before_transaction_stages_it`: Actual oversized put/insert returns ResourceLimit, rejected key remains absent, valid staged value still commits/flushes/reads. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 300, 303, 311.
- [tests/storage_layer.rs:320](../../tests/storage_layer.rs#L320) `should_preserve_transaction_when_oversized_range_delete_is_rejected`: Oversized endpoint rejection preserves previously accepted middle value and successful later commit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 333, 334, 340.
- [src/compaction/mod.rs:471](../../src/compaction/mod.rs#L471) `should_compact_legacy_oversized_uncompressed_entry_without_losing_readability`: Genuine supported oversized raw V4 input compacts under sufficient pool through raw/LZ4/Zstd3 settings; output exact value, sequence 7 and expiration MAX survive and source remains. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 495, 508, 510, 514.

Preconditions: Real local public transactions and actual FsSST legacy raw encoding fixture; compaction explicitly supplies 1 GiB working budget.

Uncovered boundary: Supported historical raw data is distinct from oversized compressed blocks, which retain decoder limits. The test does not guarantee compaction admission under an insufficient pool or broaden format support.

Catalog review: Reviewed compatibility boundaries for WAL/ledger/catalog and current metadata/SST readers. Unsupported formats fail before authoritative mutation; no migration or new execution qualification is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## FORMAT-3

Production: [src/engine/verification.rs:65](../../src/engine/verification.rs#L65) `StorageVerifier::verify_path`; [src/engine/verification.rs:265](../../src/engine/verification.rs#L265) `verify_storage_path`; [src/metadata/format.rs:79](../../src/metadata/format.rs#L79) `validate_format_marker`.

Active regression references (not executed by this refresh):

- [src/metadata/format.rs:228](../../src/metadata/format.rs#L228) `should_reject_version_four_marker_without_writing_when_opening`: An unsupported prior marker is rejected with CompatibilityError and its exact bytes remain unchanged. Scope: Actual local FORMAT file; no migration or repair Assertion/helper call markers: 242, 243.
- [tests/verification_cli.rs:74](../../tests/verification_cli.rs#L74) `should_emit_v1_json_given_populated_release_fixture_when_midge_verify_runs`: The release fixture produces the exact healthy v1 verification JSON schema and success exit status. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 85, 86, 87.

Preconditions: Path-only real CLI fixture and explicit read-only marker validation; JSON schema has local scope independent of exit contract.

Uncovered boundary: The CLI golden does not itself compare full before/after filesystem inventory. No local path-only verifier can establish remote lease/catalog authority or provider deployment qualification; it does not repair storage.

Catalog review: Reviewed compatibility boundaries for WAL/ledger/catalog and current metadata/SST readers. Unsupported formats fail before authoritative mutation; no migration or new execution qualification is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## FORMAT-4

Production: [src/engine/api/options.rs:962](../../src/engine/api/options.rs#L962) `OpenOptionsBuilder::build`; [src/engine/api/options.rs:940](../../src/engine/api/options.rs#L940) `validate_open_timeout`; [src/engine/api/write_options.rs:149](../../src/engine/api/write_options.rs#L149) `effective_wal_durability_policy`; [src/common/error.rs:11](../../src/common/error.rs#L11) `MidgeError`.

Active regression references (not executed by this refresh):

- [src/engine/api/options/tests.rs:933](../../src/engine/api/options/tests.rs#L933) `should_reject_clock_skew_tolerance_larger_than_lease_ttl`: Invalid lease tolerance returns InvalidArgument before opening. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 943.
- [src/engine/api/write_options.rs:206](../../src/engine/api/write_options.rs#L206) `should_reject_sync_buffered_options_given_cloud_storage_when_committing`: Local-only sync/buffered policy mapping rejects cloud storage. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 214.
- [tests/engine_api/startup_deadline.rs:44](../../tests/engine_api/startup_deadline.rs#L44) `should_preserve_compatibility_error_when_timed_open_finds_unsupported_format`: Actual timed public open preserves unsupported-format CompatibilityError instead of deadline/error wrapping. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 58, 59.

Preconditions: Configuration builder and policy predicates plus real unsupported FORMAT public open; ordinary None and bounded Some remain distinct.

Uncovered boundary: Policy unit checks Err, not every exact public class. Other rows supply focused Fenced/Timeout/ResourceLimit/Corruption classes. This is not a complete provider-configuration matrix or every builder invalid combination.

Catalog review: Reviewed compatibility boundaries for WAL/ledger/catalog and current metadata/SST readers. Unsupported formats fail before authoritative mutation; no migration or new execution qualification is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.

## FORMAT-5

Production: [docs/development/support-matrix.md:1](../../docs/development/support-matrix.md#L1) `supported behavior matrix`; [docs/development/format-compatibility.md:1](../../docs/development/format-compatibility.md#L1) `0.x compatibility boundaries`; [src/metadata/format.rs:79](../../src/metadata/format.rs#L79) `validate_format_marker`.

Active regression references (not executed by this refresh):

- [tests/storage_layer.rs:1007](../../tests/storage_layer.rs#L1007) `should_preserve_baseline_compressed_block_fixture`: Shipping LZ4/Zstd3/Zstd9 encoded block fixture code/digest remain exact. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 1040, 1041.
- [src/metadata/format.rs:272](../../src/metadata/format.rs#L272) `should_reject_open_given_future_format_version_when_starting`: Unsupported future marker remains CompatibilityError. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions Assertion/helper call markers: 286.
- [tests/verification_cli.rs:74](../../tests/verification_cli.rs#L74) `should_emit_v1_json_given_populated_release_fixture_when_midge_verify_runs`: The release fixture produces the exact healthy v1 verification JSON schema and success exit status. Scope: Active Rust regression source; execution and mutation red/green runs deferred for the implementation batch. Assertion/helper call markers: 85, 86, 87.

Preconditions: Pinned shipping fixtures/contracts provide narrow versioned evidence. Release evidence must name exact source revision and actual supported environment.

Uncovered boundary: No executable test establishes unlimited release/API/on-disk compatibility. #724 refreshed identified present-tense current-version claims and historical notice without relabeling old examples. The distinct #715 controlled local campaign 37274074026 at c8f0de80 is recorded in the guide/readback at ACCT-6; it does not extend format/API compatibility or supply the separate final fourteen-hour qualification.

Catalog review: Reviewed compatibility boundaries for WAL/ledger/catalog and current metadata/SST readers. Unsupported formats fail before authoritative mutation; no migration or new execution qualification is inferred. Active test symbols and assertion/helper markers resolve in the current tree; behavioral execution and mutation proof remain pending.
