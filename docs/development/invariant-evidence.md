# Exact Invariant Evidence

Every catalog ID maps to production symbols, exact assertions, fixture
assumptions and untested boundaries. This is source/reference evidence;
execution results remain separately scoped in qualification records and PRs.
References bind recovery seek-index source commit
c1e5709e376d29090bb331d37f27fa941d763b96 before this documentation commit;
the changed replay-coverage locations and source hash are refreshed while
existing invariant claims and historical qualification objects are retained;
the historical measured c8 source and original report remain explicit. Historical 32911805 A/r1 smoke 37270512400
is transport-valid only; its later nine-run 37270845568 remains invalid
after selected active metadata endpoints. Eight shared commit controls,
nine boundary controls and 33 reader contracts retain explicit scripted
and constructed scopes. At measured c8f0de80, smoke 37273556125 and all nine
attempts of 37274074026 passed retained original-archive readback: B/C have
three payload misses each; A has one time miss and does not qualify. This
meets only the conditional policy-investigation predicate; cadence remains
unchanged, physical/device bytes are not measured and native confidence is
not upgraded. Documentation references bind the notification-routing source above; historical compatibility and #733 controls retain their original scopes. The
recorded c8f0de80 campaign and byte-identical original report remain historical
measurement evidence; this change supplies no new native-hour qualification. Full-hour acceptance uses the final merged
catalog source; run identities and artifact readbacks are tracked on
[GitHub issue #711](https://github.com/cntryl/midge/issues/711).
Constructed counters,
seeded encoded WAL and genuine public ACK are labeled separately.

Common-mode integration fixtures use memory, local and filesystem-backed
CloudSimulated. Native ignored Sqrzl qualification needs its explicit
features/environment and --ignored execution. None of these fixtures
establish deployment IAM/quota/network policy or physical device bytes.

The [JSON inventory](invariant-evidence.json) and [source hashes](invariant-evidence-source-sha256.json)
are checked by `ruby tools/validate-invariant-catalog.rb`; reference checks
do not establish semantic sufficiency or execute the linked tests.

## AUTH-1

Composed native regression: [should_preserve_acknowledged_history_when_predecessor_wal_resumes_after_takeover_and_cleanup](../../tests/cloud_provider_engine_qualification/authority_publication.rs) — Native S3 predecessor loses real lease validity; a higher epoch successor publishes and reclaims WAL/SST state before held immutable WAL completion. Strict predecessor write is fenced and no subsequent control mutation is forwarded. Scope: Three bounded native S3 schedules against pinned local Sqrzl: before upstream PUT, after upstream HTTP 200, and lost successful response. Genuine lease expiry; shortened predecessor shutdown drain budget only. Successor is quiesced before predecessor release to isolate catalog mutation. No exhaustive simulation or live-provider qualification claim. See [fixture protocol](authority-publication-fixture.md).

Production: [src/runtime/event_loop/fencing.rs:14](../../src/runtime/event_loop/fencing.rs#L14) `RuntimeFence::check_health`; [src/runtime/event_loop/fencing.rs:39](../../src/runtime/event_loop/fencing.rs#L39) `RuntimeFence::validate_within`; [src/lease/heartbeat.rs:141](../../src/lease/heartbeat.rs#L141) `run_watchdog_worker`.

Exact evidence and assertion markers:

- [src/runtime/event_loop/fencing.rs:193](../../src/runtime/event_loop/fencing.rs#L193) `should_reject_expired_validity_when_cached_health_is_true`: Expires actual LeaseValidity while cached health is true; authority check returns Fenced. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 215.
- [src/lease/cloud/tests.rs:2446](../../src/lease/cloud/tests.rs#L2446) `should_reject_metadata_pointer_cas_that_lands_after_successor_acquires`: Holds predecessor conditional pointer CAS, releases predecessor and acquires successor; stale publication fails and successor committed pointer remains exact. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 2509, 2513.

Preconditions: A real validity object and conditional provider mock are required. The delayed-CAS schedule has positive request/ownership prerequisites.

Uncovered boundary: These are bounded expiry/takeover schedules. A previously submitted provider mutation remains outcome-ambiguous; the tests do not claim transport cancellation proves no commit.

## AUTH-2

Production: [src/runtime/event_loop/fencing.rs:39](../../src/runtime/event_loop/fencing.rs#L39) `RuntimeFence::validate_within`; [src/runtime/event_loop/write_batch.rs:71](../../src/runtime/event_loop/write_batch.rs#L71) `ensure_l0_write_admission`.

Exact evidence and assertion markers:

- [src/runtime/event_loop/fencing.rs:148](../../src/runtime/event_loop/fencing.rs#L148) `should_not_poison_lease_health_when_leader_store_read_fails_transiently`: Transient provider validation returns Busy, preserves healthy=true, and subsequent cached health check succeeds. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 156, 157, 158.
- [src/runtime/event_loop/fencing.rs:162](../../src/runtime/event_loop/fencing.rs#L162) `should_report_timeout_without_poisoning_when_lease_validation_times_out`: Requires typed Timeout and healthy=true. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 170, 171.
- [src/runtime/event_loop/fencing.rs:175](../../src/runtime/event_loop/fencing.rs#L175) `should_poison_lease_health_when_ownership_is_lost`: Requires typed Fenced and healthy=false. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 183, 184.

Preconditions: Tests use controlled LeaderStore outcomes rather than native service availability; each outcome class is intentional.

Uncovered boundary: These assertions establish mutation-admission/error distinctions, not a full public matrix proving every read and diagnostic remains available after every fencing cause.

## AUTH-3

Production: [src/runtime/event_loop/compaction.rs:571](../../src/runtime/event_loop/compaction.rs#L571) `CompactionCoordinator::start_publication`; [src/runtime/event_loop/flush_pipeline.rs:614](../../src/runtime/event_loop/flush_pipeline.rs#L614) `commit_flush_metadata`; [src/runtime/actors/compaction/publication.rs:39](../../src/runtime/actors/compaction/publication.rs#L39) `CompactionPublishTask`.

Exact evidence and assertion markers:

- [tests/fault_injection.rs:596](../../tests/fault_injection.rs#L596) `should_ignore_orphan_sst_when_flush_intent_log_save_hits_no_space`: Actual intent-save NoSpace failure followed by reopen retains all ten accepted values and runtime sst_count=0. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 618, 629, 631.
- [tests/fault_injection.rs:4244](../../tests/fault_injection.rs#L4244) `should_retain_input_ssts_given_compaction_failure_before_manifest_publish`: Actual process-abort schedule resolves to the old authoritative manifest/input set and exact acknowledged rows. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4255, 4259, 4271, 4275, 4286.
- [tests/solid_governance.rs:555](../../tests/solid_governance.rs#L555) `should_route_manifest_file_list_mutation_through_runtime_owner`: Source-governance control rejects mutable manifest dereferencing and selected direct file-list bypass spellings in production owner modules; this is a source-structure check, not an executed publication schedule. Scope: source-string governance check; no executed runtime/data proof. Assertion/call markers at lines 582, 586.

Preconditions: Genuine filesystem outputs and configured intent/crash failpoints; raw output bytes exist without a successful publication boundary. The governance control inspects source strings only, independently of the real crash/no-space fixtures.

Uncovered boundary: Representative publication-boundary tests support the architecture. They are not a mechanical proof that no actor can mutate any authoritative runtime field; event-loop ownership is also a source-structure property.

## AUTH-4

Production: [src/runtime/event_loop/manifest.rs:308](../../src/runtime/event_loop/manifest.rs#L308) `record_ddl_authority_ambiguity`; [src/runtime/ddl.rs:629](../../src/runtime/ddl.rs#L629) `reconcile_prepared_within`; [src/metadata/manifest.rs:244](../../src/metadata/manifest.rs#L244) `Manifest::next_cf_id`.

Exact evidence and assertion markers:

- [src/runtime/ddl.rs:1359](../../src/runtime/ddl.rs#L1359) `should_reject_paused_create_cas_after_successor_fences_ddl_registry`: Shared actual conditional-registry schedule rejects stale create CAS after successor epoch and preserves successor visibility; drop variant is adjacent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1360.
- [tests/fault_injection.rs:9101](../../tests/fault_injection.rs#L9101) `should_recover_ambiguous_ddl_once_when_reopening_after_crash_before_remote_cas_submission`: Actual prepared record has remote_cas_ambiguous=true while remote registry lacks the op; reopen exposes CF, removes prepare, and registry contains exactly one matching op. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 9123, 9127, 9136, 9141, 9149, 9150, 9161, 9162.
- [tests/column_families.rs:538](../../tests/column_families.rs#L538) `should_allocate_monotonic_column_family_ids_given_deleted_column_family_when_creating`: Destructive drop/recreate has old key absent and newly written key present. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 566, 567.

Preconditions: DDL crash/conditional-provider tests require failpoints; common integration modes are memory, local, and filesystem CloudSimulated.

Uncovered boundary: The drop/recreate test asserts data isolation, not a direct numeric inequality of returned CF IDs. ID nonreuse also depends on next_cf_id considering retained deleted entries. Native IAM/quota semantics are outside these fixtures.

## TX-1

Composed pressure regression: [should_preserve_snapshot_isolation_when_spill_pressure_is_released](../../tests/snapshot_spill_pressure.rs) — Frozen old scans and point reads survive TTL expiry, sixteen acknowledged mixed-history compactions and actual spill saturation. See [fixture scope and mutation validation](snapshot-spill-pressure-fixture.md).

Production: [src/engine/mod.rs:337](../../src/engine/mod.rs#L337) `Engine::begin_tx`; [src/engine/mod.rs:412](../../src/engine/mod.rs#L412) `acquire_transaction_snapshot`; [src/runtime/snapshot_pins.rs:57](../../src/runtime/snapshot_pins.rs#L57) `SnapshotPinRegistry::register`; [src/runtime/snapshot_pins.rs:173](../../src/runtime/snapshot_pins.rs#L173) `begin_acquisition`.

Exact evidence and assertion markers:

- [tests/transactions.rs:3562](../../tests/transactions.rs#L3562) `should_return_old_value_given_snapshot_before_write_when_reading`: Current reader sees v2 after later commit while the preexisting transaction still sees v1. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 3592, 3596.
- [src/runtime/snapshot_pins.rs:353](../../src/runtime/snapshot_pins.rs#L353) `should_track_pinned_sst_names_when_snapshot_pin_registered`: Registry has one active pin, horizon 42, and both named SSTs pinned. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 361, 362, 363, 365, 366.
- [src/runtime/snapshot_pins.rs:610](../../src/runtime/snapshot_pins.rs#L610) `should_retain_timed_out_snapshot_pin_until_unregister`: Warning does not remove active pin, horizon, or pinned SST. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 613, 620, 621, 622, 624.

Preconditions: Public snapshot test runs common memory/local/CloudSimulated modes. Pin tests operate the real registry with supplied names; actual pinned compaction is separately mapped at SST-6.

Uncovered boundary: Representative snapshots and the pin registry do not enumerate all snapshot-acquisition/GC interleavings. The API's single CF ID is a structural limit rather than cross-CF atomicity evidence.

## TX-2

Production: [src/runtime/read_snapshot.rs:807](../../src/runtime/read_snapshot.rs#L807) `ReadSnapshot::get_bytes`; [src/runtime/read_snapshot.rs:1021](../../src/runtime/read_snapshot.rs#L1021) `ReadSnapshot::range_scan`; [src/engine/api/transaction.rs:818](../../src/engine/api/transaction.rs#L818) `Transaction::get`; [src/engine/api/transaction.rs:848](../../src/engine/api/transaction.rs#L848) `Transaction::scan`.

Exact evidence and assertion markers:

- [tests/transactions.rs:409](../../tests/transactions.rs#L409) `should_preserve_write_set_semantics_given_put_delete_put_sequence_when_reading_own_writes`: Put/delete/put intent sequence returns the final value three. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 424.
- [src/runtime/read_snapshot.rs:1831](../../src/runtime/read_snapshot.rs#L1831) `should_match_brute_force_oracle_when_scans_cross_overlapping_range_tombstones`: Forty constructed active/immutable histories compare exact eligible rows to a separate brute-force oracle at horizons 3, 5 and MAX, both directions, bounds, and optional limit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1928.

Preconditions: Oracle builds genuine memtables with deterministic generated versions/tombstones, but no SST I/O. Public own-intent test uses all common modes.

Uncovered boundary: The oracle is bounded and covers active/immutable selection; it does not independently prove every SST version tie or heavily spilled intent lookup path.

## TX-3

Composed pressure regression: [should_preserve_snapshot_isolation_when_spill_pressure_is_released](../../tests/snapshot_spill_pressure.rs) — A controlled clock expires only the first eight keys for a new reader before overwrites; the original snapshot retains all thirty-two original values through compaction. See [fixture scope and mutation validation](snapshot-spill-pressure-fixture.md).

Production: [src/runtime/read_snapshot.rs:807](../../src/runtime/read_snapshot.rs#L807) `ReadSnapshot::get_bytes`; [src/runtime/read_snapshot.rs:1021](../../src/runtime/read_snapshot.rs#L1021) `ReadSnapshot::range_scan`; [src/compaction/mod.rs:33](../../src/compaction/mod.rs#L33) `execute_compaction`; [src/engine/api/transaction.rs:510](../../src/engine/api/transaction.rs#L510) `Transaction::delete_range`.

Exact evidence and assertion markers:

- [tests/transactions.rs:4888](../../tests/transactions.rs#L4888) `should_persist_range_tombstone_when_flush_survives_restart`: Real local flush/shutdown/reopen preserves a and suppresses b/c after deletion [b,d). Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4930, 4931, 4932.
- [tests/transactions.rs:4855](../../tests/transactions.rs#L4855) `should_use_transaction_snapshot_time_for_ttl_visibility`: After real TTL expiry the earlier snapshot still sees value and a new snapshot sees None. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4876, 4884.
- [src/compaction/mod.rs:964](../../src/compaction/mod.rs#L964) `should_preserve_expired_ttl_metadata_given_compaction_then_mask_at_read_time`: Actual SST compaction retains raw value/sequence/expiration, while timed read returns Tombstone(9). Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 993, 1000.
- [tests/engine_api.rs:1717](../../tests/engine_api.rs#L1717) `should_return_correct_boundary_rows_when_reverse_scanning_with_explicit_start_and_end`: Reverse [b,e) returns exactly d,c,b. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1747.

Preconditions: The restart and TTL tests use real local Engine/files; TTL test uses elapsed wall time. Reverse API test runs common modes; compaction test writes/reads real SSTs.

Uncovered boundary: This is a representative combination of restart, TTL, compaction and direction evidence, not an exhaustive cross-product of every memtable/SST/tombstone/TTL state. Binary-prefix evidence is also mapped at TX-7.

## TX-4

Production: [src/runtime/actors/wal/transaction.rs:345](../../src/runtime/actors/wal/transaction.rs#L345) `allocate_transaction_sequences`; [src/runtime/actors/wal/transaction.rs:375](../../src/runtime/actors/wal/transaction.rs#L375) `build_transaction_wal_batch`; [src/runtime/event_loop/write_batch.rs:566](../../src/runtime/event_loop/write_batch.rs#L566) `finish_drained_write_after_publish`; [src/wal/recovery/streaming.rs:283](../../src/wal/recovery/streaming.rs#L283) `replay_wal_with_options`.

Exact evidence and assertion markers:

- [tests/durability.rs:2309](../../tests/durability.rs#L2309) `should_maintain_atomicity_given_concurrent_reads_when_transaction_commits`: Actual concurrent two-key memory transaction exposes only initial or updated pair, then both updated values. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 2379, 2394, 2395.
- [src/wal/recovery/tests.rs:863](../../src/wal/recovery/tests.rs#L863) `should_recover_only_committed_transaction_given_split_wal_records_when_commit_marker_is_missing`: Real WAL with synced TxnBegin and Put but no commit marker reports two decoded records and creates no recovered CF memtable. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 899, 900.

Preconditions: Live visibility test uses memory mode and a coordinated reader/writer; replay test uses actual FsWalWriterIo files.

Uncovered boundary: Live visibility and incomplete replay are separate schedules. These do not prove all maximum-size, spilled or crash-position multi-key transactions; acknowledged spill atomicity has separate fault tests.

## TX-5

Production: [src/engine/api/transaction.rs:550](../../src/engine/api/transaction.rs#L550) `Transaction::assert_value`; [src/engine/api/transaction.rs:700](../../src/engine/api/transaction.rs#L700) `validate_assertions`; [src/runtime/actors/wal/transaction.rs:238](../../src/runtime/actors/wal/transaction.rs#L238) `validate_transaction_preconditions`.

Exact evidence and assertion markers:

- [tests/transactions.rs:4207](../../tests/transactions.rs#L4207) `should_abort_second_commit_given_conflicting_writes_when_abort_on_write_conflict_enabled`: Second conflicting commit is WriteConflict and final value belongs to first commit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4234, 4245.
- [tests/transactions.rs:2103](../../tests/transactions.rs#L2103) `should_enforce_assertion_conflict_even_under_last_write_wins_policy`: Assertion plus staged write conflicts with a later update despite LastWriteWins and returns WriteConflict. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 2122, 2148.
- [tests/transactions.rs:2155](../../tests/transactions.rs#L2155) `should_validate_assertion_only_commit_without_allocating_a_sequence`: Successful assertion-only commit leaves sequence metric unchanged. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 2180, 2184.

Preconditions: Strict write-conflict test runs all common modes; explicit LastWriteWins and assertion-only controls use actual memory Engine.

Uncovered boundary: These tests assert final values, typed conflicts and no assertion-only sequence allocation. They do not directly observe physical WAL append count for every rejected assertion; ordering before publication is source-backed.

## TX-6

Composed pressure regression: [should_preserve_snapshot_isolation_when_spill_pressure_is_released](../../tests/snapshot_spill_pressure.rs) — Actual uncommitted spill keys and the capacity-rejected bounded write remain absent from other transactions; releasing the spill owner permits the same write without invalidating the old snapshot. See [fixture scope and mutation validation](snapshot-spill-pressure-fixture.md).

Production: [src/engine/api/transaction.rs:739](../../src/engine/api/transaction.rs#L739) `Transaction::rollback`; [src/engine/api/transaction.rs:744](../../src/engine/api/transaction.rs#L744) `unregister_snapshot`; [src/engine/api/transaction.rs:602](../../src/engine/api/transaction.rs#L602) `Transaction::commit`.

Exact evidence and assertion markers:

- [tests/transactions.rs:272](../../tests/transactions.rs#L272) `should_rollback_all_writes_given_multiple_operations_when_dropped`: Dropping uncommitted overwrite/insertion keeps original value and leaves new key absent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 300, 304.
- [tests/transactions.rs:5231](../../tests/transactions.rs#L5231) `should_unregister_snapshot_when_rollback_ends_transaction`: Explicit rollback unregisters snapshot; adjacent drop case is at 5252. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 5246.

Preconditions: Actual public transaction behavior in common modes; client-side intents never reach a commit call.

Uncovered boundary: No claim that rolling back an already accepted/timed-out runtime commit cancels it. That is a different ownership contract (WAL-2/RES-5).

## TX-7

Production: [src/engine/api/transaction.rs:848](../../src/engine/api/transaction.rs#L848) `Transaction::scan`; [src/runtime/read_snapshot.rs:1021](../../src/runtime/read_snapshot.rs#L1021) `ReadSnapshot::range_scan`; [src/engine/api/iterator.rs:83](../../src/engine/api/iterator.rs#L83) `try_collect`.

Exact evidence and assertion markers:

- [tests/transactions.rs:4568](../../tests/transactions.rs#L4568) `should_honor_prefix_upper_bound_given_prefix_ending_in_ff_when_scanning`: Real local binary-key query with prefix [0x10,0xff] returns exact expected key set. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4592.
- [src/runtime/read_snapshot.rs:1831](../../src/runtime/read_snapshot.rs#L1831) `should_match_brute_force_oracle_when_scans_cross_overlapping_range_tombstones`: Exact oracle comparisons include start/end, forward/reverse and limit 3. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1928.
- [src/engine/api/iterator.rs:158](../../src/engine/api/iterator.rs#L158) `should_replay_terminal_error_without_marking_iterator_exhausted`: Constructed corruption row is returned on both calls; state=Failed, failed=true, exhausted=false. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 168, 171, 174, 175, 176.

Preconditions: Binary-prefix test is public local API; bound/direction/limit oracle is memtable-only; failed-iterator test supplies an error iterator.

Uncovered boundary: The failed-state unit does not itself corrupt a real storage object. Actual remote corrupt-block qualification belongs to broader storage tests; source-defined coverage here is not universal scan fault coverage.

## TX-8

Production: [src/engine/api/transaction.rs:399](../../src/engine/api/transaction.rs#L399) `Transaction::new`; [src/runtime/actors/wal/transaction.rs:238](../../src/runtime/actors/wal/transaction.rs#L238) `validate_transaction_preconditions`; [docs/user-guides/transaction-durability-contract.md:1](../../docs/user-guides/transaction-durability-contract.md#L1) `transaction contract`.

Exact evidence and assertion markers:

- [tests/transactions.rs:2710](../../tests/transactions.rs#L2710) `should_allow_lost_update_given_put_read_modify_write_when_concurrent`: Two actual transactions read zero and each writes one; final counter is one under default policy. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 2750.
- [tests/transactions.rs:4207](../../tests/transactions.rs#L4207) `should_abort_second_commit_given_conflicting_writes_when_abort_on_write_conflict_enabled`: Positive alternative requires strict conflict outcome. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4234, 4245.

Preconditions: Common storage modes; intentionally bounded two-transaction lost-update schedule.

Uncovered boundary: Serializable isolation, predicate locks and atomic cross-CF transactions are unsupported promises, not missing successful tests. The lost-update control must not be interpreted as a defect or universal concurrency model.

## WAL-1

Production: [src/engine/api/write_options.rs:149](../../src/engine/api/write_options.rs#L149) `effective_wal_durability_policy`; [src/runtime/actors/wal/transaction.rs:161](../../src/runtime/actors/wal/transaction.rs#L161) `append_prepared_transactions`; [src/runtime/frontiers.rs:58](../../src/runtime/frontiers.rs#L58) `advance_cloud_to`.

Exact evidence and assertion markers:

- [tests/durability_sync_count.rs:84](../../tests/durability_sync_count.rs#L84) `should_issue_one_physical_wal_sync_when_non_empty_sync_transaction_commits`: Actual nonempty sync transaction increases physical WAL fsync counter by exactly one. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 101.
- [src/engine/api/write_options.rs:181](../../src/engine/api/write_options.rs#L181) `should_map_every_local_write_option_to_expected_wal_policy_given_local_storage_when_committing`: Local sync/buffered/best_effort map to Strict/Batched/BestEffort respectively. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 201.
- [src/runtime/event_loop/cloud_integration/tests.rs:5767](../../src/runtime/event_loop/cloud_integration/tests.rs#L5767) `should_not_advance_cloud_durability_across_unacked_segment_gap`: Cloud frontier stays zero for later ACK alone, retains local WAL and buffered ACK; preceding ACK advances contiguous frontier and drains covered WAL bookkeeping. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 5788, 5808, 5815, 5826, 5831, 5838, 5848, 5853, 5859.

Preconditions: Physical sync count is actual local Fs I/O. Policy mapping is a unit predicate; cloud-gap test uses an actual cloud event-loop fixture with controlled ACK delivery.

Uncovered boundary: Mapping is not crash persistence proof for every mode. In-memory has no crash-persistent medium; buffered/best_effort/cloud_async must retain their weaker return contracts. Native strict cache-loss evidence is at CLOUD-5.

## WAL-2

Production: [src/runtime/handle.rs:241](../../src/runtime/handle.rs#L241) `send_and_wait_with_timeout`; [src/runtime/router.rs:261](../../src/runtime/router.rs#L261) `ResponseRouter::abandon`; [src/runtime/event_loop/flush_pipeline.rs:138](../../src/runtime/event_loop/flush_pipeline.rs#L138) `schedule_next_flush_worker_with_shutdown`.

Exact evidence and assertion markers:

- [src/engine/tests/flush_retry.rs:425](../../src/engine/tests/flush_retry.rs#L425) `should_complete_one_cloud_flush_when_abandoned_barrier_callers_retry`: Actual accepted held cloud flush outlives caller waits; retries converge on one built/uploaded SST with exact rows and successful strict reopen. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 435, 436, 437, 438, 439, 445, 446, 447, 451.
- [src/runtime/tests/compaction_deadline_tests.rs:37](../../src/runtime/tests/compaction_deadline_tests.rs#L37) `should_preserve_manual_caller_budget_when_queued_compaction_outlives_response_wait`: real RuntimeHandle/router with no event loop; original override remains captured after bounded caller timeout and late completion updates tombstone accounting. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 65, 69, 70, 71, 75, 76, 77, 78, 79, 80.

Preconditions: Flush test exercises accepted public Engine work with conditional filesystem cloud. The delivered router control isolates actual RuntimeHandle/route ownership and explicitly has no event loop or durability pipeline.

Uncovered boundary: Flush/router timeout ownership is representative of accepted work, not a dedicated generic non-idempotent commit replay test. Caller retry must not infer that an ambiguous write was unapplied. The shared checkpoint helper reconstructs only a typed pre-WAL WriteStall and preserves unknown Timeout or other terminal errors; its scripted policy controls do not fabricate a real pressure schedule.

## WAL-3

Production: [src/wal/recovery.rs:436](../../src/wal/recovery.rs#L436) `collect_replay_paths`; [src/wal/recovery/streaming.rs:283](../../src/wal/recovery/streaming.rs#L283) `replay_wal_with_options`; [src/runtime/cloud_startup/streaming_wal_plan.rs:593](../../src/runtime/cloud_startup/streaming_wal_plan.rs#L593) `active_local_source`.

Exact evidence and assertion markers:

- [tests/durability.rs:672](../../tests/durability.rs#L672) `should_keep_valid_prefix_given_truncated_wal_tail_when_reopening_in_strict_mode`: Actual synced prefix plus truncated/torn active record reopens strictly with prefix present and torn key absent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 717, 721.
- [src/wal/recovery/tests.rs:863](../../src/wal/recovery/tests.rs#L863) `should_recover_only_committed_transaction_given_split_wal_records_when_commit_marker_is_missing`: Synced transaction begin/put without marker decodes but materializes no recovered memtable. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 899, 900.

Preconditions: Actual local filesystem WAL and public strict reopen; split record test directly drives real recovery.

Uncovered boundary: These selected assertions prove retained prefix and transaction atomicity, not directly the truncation+fsync-before-next-append sequence. That ordering must also remain source-backed; individual frame/crash variants are bounded schedules.

## WAL-4

Production: [src/runtime/cloud_startup/streaming_wal_plan.rs:351](../../src/runtime/cloud_startup/streaming_wal_plan.rs#L351) `recover_or_salvage`; [src/runtime/cloud_startup/streaming_wal_plan.rs:795](../../src/runtime/cloud_startup/streaming_wal_plan.rs#L795) `stop_at_first_hole`; [src/wal/recovery.rs:410](../../src/wal/recovery.rs#L410) `replay_wal_with_manifest_filter_within`.

Exact evidence and assertion markers:

- [tests/durability.rs:803](../../tests/durability.rs#L803) `should_fail_strict_but_salvage_valid_prefix_given_corrupted_first_wal_frame_when_reopening`: Genuine corrupted first WAL frame fails strict recovery and salvage exposes no corrupted key. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 849, 862.
- [src/runtime/cloud_startup/streaming_wal_plan/tests.rs:452](../../src/runtime/cloud_startup/streaming_wal_plan/tests.rs#L452) `should_not_replay_segments_after_invalid_segment_when_cloud_salvage_skips_one`: Real catalog fixture corruption at segment 2 yields salvage flag, remote replay only [1], unreplayed [2,3], active WAL quarantined, floor=4 and next segment=4. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 466, 467, 476, 484, 493, 494, 495, 496, 497.

Preconditions: Explicit RecoveryPolicy::Strict versus Salvage; real catalog/segment bytes and actual corrupted publication identity.

Uncovered boundary: Salvage is degraded recovery, not normal complete-history success. These schedules do not license skipping a hole or guarantee every damaged format is salvageable.

## WAL-5

Production: [src/runtime/cloud_startup/streaming_wal_plan.rs:694](../../src/runtime/cloud_startup/streaming_wal_plan.rs#L694) `enforce_epoch_order`; [src/runtime/cloud_startup/streaming_wal_plan.rs:795](../../src/runtime/cloud_startup/streaming_wal_plan.rs#L795) `stop_at_first_hole`; [src/wal/recovery.rs:553](../../src/wal/recovery.rs#L553) `max_writer_epoch`.

Exact evidence and assertion markers:

- [src/runtime/cloud_startup/streaming_wal_plan/tests.rs:1027](../../src/runtime/cloud_startup/streaming_wal_plan/tests.rs#L1027) `should_stop_replay_at_epoch_regressed_sealed_segment`: Published epochs 8,7,9 give replay segments [1], set-aside [2,3] and max_unreplayed_sequence=3. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1038, 1047, 1056.
- [src/runtime/cloud_startup/streaming_wal_plan/tests.rs:1120](../../src/runtime/cloud_startup/streaming_wal_plan/tests.rs#L1120) `should_preserve_wal_files_when_strict_recovery_rejects_epoch_regression`: Strict rejection preserves input files rather than quarantining/deleting them. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1135, 1137.

Preconditions: Actual planner/catalog fixture with deliberate epoch regression; salvage and strict paths are distinct.

Uncovered boundary: The selected planner assertions establish ordering and floor inputs, not every public post-recovery sequence allocator path or epoch overflow boundary.

## WAL-6

Production: [src/runtime/frontiers.rs:47](../../src/runtime/frontiers.rs#L47) `advance_synced_to`; [src/runtime/frontiers.rs:53](../../src/runtime/frontiers.rs#L53) `advance_local_to`; [src/runtime/frontiers.rs:58](../../src/runtime/frontiers.rs#L58) `advance_cloud_to`; [src/runtime/frontiers.rs:64](../../src/runtime/frontiers.rs#L64) `reset_cloud_for_recovery`.

Exact evidence and assertion markers:

- [src/runtime/frontiers.rs:89](../../src/runtime/frontiers.rs#L89) `should_not_lower_frontiers_when_an_older_sequence_arrives`: After advances to synced/local 20 and cloud 15, lower advances leave those exact values unchanged. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 101, 102, 103.
- [src/runtime/frontiers.rs:107](../../src/runtime/frontiers.rs#L107) `should_lower_cloud_frontier_only_through_recovery_reset`: Explicit recovery reset can set cloud frontier to 7. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 116.

Preconditions: Real frontier value type with controlled sequence operations; no provider side effects in these units.

Uncovered boundary: A unit reset allowance is not itself proof that callers re-prove all cloud authority. Runtime catalog/gap tests at CLOUD-2 supply a separate integration boundary.

## WAL-7

Production: [src/runtime/cloud_startup/replay_coverage.rs:154](../../src/runtime/cloud_startup/replay_coverage.rs#L154) `ReplayCoverage::contains`; [src/runtime/hybrid_persistence/streaming_prune.rs:40](../../src/runtime/hybrid_persistence/streaming_prune.rs#L40) `validate`; [src/runtime/hybrid_persistence.rs:755](../../src/runtime/hybrid_persistence.rs#L755) `prune_cloud_wal_segments_within`; [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:79](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L79) `CandidateIndex::visit`; [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:90](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L90) `CandidateIndex::visit_nodes`.

Exact evidence and assertion markers:

- [src/runtime/cloud_startup/replay_coverage.rs:1079](../../src/runtime/cloud_startup/replay_coverage.rs#L1079) `should_replay_put_when_equal_sequence_sst_expiration_differs`: Actual SST fixture value/sequence equality with different TTL still returns covered=false. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1086.
- [src/runtime/cloud_startup/replay_coverage.rs:1160](../../src/runtime/cloud_startup/replay_coverage.rs#L1160) `should_retain_wal_when_pinned_sst_identity_changes_before_block_reload`: Actual SST bytes change after initial proof; after reader/cache reload, contains=false. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1164, 1177, 1181.
- [src/runtime/hybrid_persistence/tests.rs:225](../../src/runtime/hybrid_persistence/tests.rs#L225) `should_require_full_range_coverage_for_wal_tombstones`: Constructed authoritative [a,m] coverage covers [c,k) but rejects [c,z). Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 254, 255.
- [src/runtime/cloud_startup/replay_coverage.rs:754](../../src/runtime/cloud_startup/replay_coverage.rs#L754) `should_replay_when_unreadable_interval_overlaps_verified_exact_sst`: The first real checksummed SST is verified while the second overlapping authoritative object was removed before the query. The exact visitor still returns replay, counts both candidates and releases all budget; this is a bounded filesystem-backed coverage control, not public ACK or native-hour recovery. Scope: actual seeded SST bytes/removed authoritative object; no public ACK or native provider/hour qualification. Assertion/call markers at lines 771, 772, 773, 775.
- [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:190](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L190) `should_preserve_all_linear_matches_when_intervals_overlap_across_families`: Constructed unsorted multi-CF/nested/same-start/disjoint interval metadata is compared against the old linear candidate predicate across inclusive endpoints, empty keys and u64::MAX. This checks complete selection sets, not independent SST-body proof. Scope: constructed metadata selection oracle against existing predicate; no SST/Engine/provider execution. Assertion/call markers at lines 228.
- [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:235](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L235) `should_keep_unproved_candidates_when_metadata_cannot_establish_coverage`: Constructed CRC-less and incomplete-bound candidates remain selectable when the old predicate selects them; only impossible missing/inverted bounds are excluded. Later exact proof remains mandatory. Scope: constructed candidate metadata only; no immutable body proof. Assertion/call markers at lines 255.
- [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:259](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L259) `should_stop_visiting_when_an_overlapping_candidate_rejects_proof`: Constructed matching intervals and a false visitor result stop traversal conservatively after the actual third callback; no positive exact-coverage decision is fabricated. Scope: actual selector callback traversal over constructed metadata; scripted proof result. Assertion/call markers at lines 282, 283.
- [src/runtime/cloud_startup/replay_coverage/tests/streaming_scale.rs:349](../../src/runtime/cloud_startup/replay_coverage/tests/streaming_scale.rs#L349) `should_bound_manifest_lookup_work_when_streaming_real_transaction_batches_at_scale`: The actual strict streaming planner validates one published sealed WAL and replays 258 framed 32-operation transaction batches. Exact assertions prove all 8192 SST-covered rows stay absent from replay while the 64 interior uncovered keys retain their exact values/sequences, the encoded sequence/epoch frontier is unchanged, and original SST/WAL/catalog bytes remain identical. Scope: one actual filesystem-seeded fixture: 64 checksummed SSTs hold 8192 rows and 64 interior holes are replayed from 258 framed transaction batches; no public ACK, native socket/hour, wall-time speed or physical-device amplification proof. Assertion/call markers at lines 354, 355, 389, 390, 392, 394, 395, 396, 400, 409, 410, 414, 415, 418, 422, 313, 317, 318, 319, 320, 321, 322, 296, 298, 302, 307, 210, 212, 216.

Preconditions: TTL and changed-identity controls use real SST bytes; range-span unit constructs FileMeta and is narrower than end-to-end semantic replay.

Uncovered boundary: Agreement test at replay_coverage.rs:879 compares two implementations of one shared rule and is not an independent oracle. These examples do not prove every tombstone/TTL overlap; uncertain coverage must retain WAL.

## SST-1

Production: [src/runtime/sst_read_view.rs:69](../../src/runtime/sst_read_view.rs#L69) `SstReadView::new`; [src/runtime/event_loop/flush_pipeline.rs:740](../../src/runtime/event_loop/flush_pipeline.rs#L740) `install_flush_publication`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:705](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L705) `reconcile_manifest_ssts`.

Exact evidence and assertion markers:

- [tests/fault_injection.rs:596](../../tests/fault_injection.rs#L596) `should_ignore_orphan_sst_when_flush_intent_log_save_hits_no_space`: Flush intent-save NoSpace leaves accepted data recoverable on reopen but sst_count=0, so output presence is not publication authority. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 618, 629, 631.

Preconditions: Actual local Engine/files and real intent-persistence failure; test reopens rather than merely examining an in-memory manifest.

Uncovered boundary: This selected test proves a flush orphan case. Separate raw cache, unreferenced remote generation and compaction residue cases are not all implied by that one assertion (see CLOUD-3/SST-5).

## SST-2

Production: [src/runtime/sst_read_view.rs:81](../../src/runtime/sst_read_view.rs#L81) `SstReadView::from_shared`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:747](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L747) `validate_manifest_sst_size`; [src/runtime/cloud_startup/replay_coverage.rs:154](../../src/runtime/cloud_startup/replay_coverage.rs#L154) `ReplayCoverage::contains`.

Exact evidence and assertion markers:

- [tests/storage_invariants.rs:5](../../tests/storage_invariants.rs#L5) `should_preserve_published_sst_bytes_given_later_flush_then_restart`: Captures first published SST bytes, flushes later data and reopens; first bytes stay identical after both steps and both values remain readable. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 37, 38, 39, 40.
- [src/runtime/cloud_startup/replay_coverage.rs:1160](../../src/runtime/cloud_startup/replay_coverage.rs#L1160) `should_retain_wal_when_pinned_sst_identity_changes_before_block_reload`: Changed real SST identity invalidates coverage after reload. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1164, 1177, 1181.

Preconditions: Actual local filesystem and SST readers; no external provider lifecycle policy.

Uncovered boundary: Immutability is sampled through later flush/restart and deliberate corruption. It is not an external guarantee that cloud operators cannot replace/delete authoritative objects; those changes must surface as unavailable/corrupt authority.

## SST-3

Production: [src/runtime/sst_read_view.rs:126](../../src/runtime/sst_read_view.rs#L126) `build_level`; [src/runtime/sst_read_view.rs:199](../../src/runtime/sst_read_view.rs#L199) `point_candidates`; [src/runtime/sst_read_view.rs:264](../../src/runtime/sst_read_view.rs#L264) `range_candidates`.

Exact evidence and assertion markers:

- [tests/storage_layer.rs:1589](../../tests/storage_layer.rs#L1589) `should_reject_manifest_bounds_when_valid_sst_summary_disagrees`: Actual valid v3 fixture SST bytes/size/CRC stay unchanged while both manifests' bounds are altered; verifier returns Corruption. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1594, 1608, 1648.
- [src/runtime/sst_read_view.rs:528](../../src/runtime/sst_read_view.rs#L528) `should_keep_legacy_file_in_conservative_fallback_bucket`: Incomplete advisory bounds retain the file even for an outside key. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 540, 541.
- [src/runtime/sst_read_view.rs:545](../../src/runtime/sst_read_view.rs#L545) `should_quarantine_lower_level_when_complete_bounds_truly_overlap`: Constructed complete overlapping bounds quarantine L1 and return both candidates for outside-key probe. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 558, 559.

Preconditions: Verifier uses genuine persisted fixture; candidate/quarantine tests operate the real view with supplied metadata.

Uncovered boundary: View units do not establish complete-repair publication or every endpoint/tombstone equality schedule. Their assertions are conservative selection, not proof all metadata becomes trusted.

## SST-4

Production: [src/runtime/event_loop/flush_pipeline.rs:480](../../src/runtime/event_loop/flush_pipeline.rs#L480) `prepare_flush_publication`; [src/runtime/event_loop/flush_pipeline.rs:614](../../src/runtime/event_loop/flush_pipeline.rs#L614) `commit_flush_metadata`; [src/runtime/event_loop/flush_pipeline.rs:740](../../src/runtime/event_loop/flush_pipeline.rs#L740) `install_flush_publication`.

Exact evidence and assertion markers:

- [tests/fault_injection.rs:596](../../tests/fault_injection.rs#L596) `should_ignore_orphan_sst_when_flush_intent_log_save_hits_no_space`: Real failed flush intent save preserves ten accepted rows after reopen without publishing its SST. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 618, 629, 631.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/publication_orders.rs:307](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/publication_orders.rs#L307) `should_defer_compaction_completion_when_manual_flush_publishes_first`: Actual held compactor and real flush/publisher order inspects authoritative cloud metadata and exact rows after settling both obligations. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 334, 349, 356, 360, 361, 362.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/publication_orders.rs:226](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/publication_orders.rs#L226) `should_reuse_prereserved_flush_name_when_compaction_publication_wins_build_race`: Opposite actual publication order retains the assigned flush identity and exact committed metadata. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 294, 295, 296, 297, 301, 302, 303.

Preconditions: Fault fixture uses local Fs; order fixtures use real CloudStorageLease/conditional mock storage, actual workers and cleanup joins.

Uncovered boundary: Two publication orders are bounded schedules. They do not license concurrent authority publishers; only explicit flush compute overlaps are admitted while publication/retirement owners still gate authority.

## SST-5

Production: [src/runtime/event_loop/compaction.rs:571](../../src/runtime/event_loop/compaction.rs#L571) `start_publication`; [src/runtime/event_loop/compaction.rs:1081](../../src/runtime/event_loop/compaction.rs#L1081) `begin_intent_clear_publication`; [src/runtime/actors/gc.rs:148](../../src/runtime/actors/gc.rs#L148) `GcActor::delete_ssts`.

Exact evidence and assertion markers:

- [tests/fault_injection.rs:4244](../../tests/fault_injection.rs#L4244) `should_retain_input_ssts_given_compaction_failure_before_manifest_publish`: Actual process abort after output durability retains old manifest set, keeps inputs on disk, and strict reopen recovers exact acknowledged data. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4255, 4259, 4271, 4275, 4286.
- [src/runtime/state/tests.rs:1509](../../src/runtime/state/tests.rs#L1509) `should_replay_every_compaction_publication_crash_point_to_complete_authority`: Recovered manifest resolves the constructed crash-phase matrix to complete authority. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1564, 1565, 1591, 1595, 1597, 1603.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/authority_timeout_tests.rs:270](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/authority_timeout_tests.rs#L270) `should_retain_both_generations_when_committed_metadata_cas_response_exceeds_manual_deadline`: actual manifest CAS commits but response outlives origin; typed Timeout, no fresh final phase, matching durable intent and exact retained input/output bytes, exact committed output authority and genuine same-path recovery. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 299, 300, 301, 302, 306, 316, 320, 321, 322, 327, 329, 330, 331.

Preconditions: Process test uses real files and genuine abort failpoint; runtime-state phase matrix is replay-oriented and must not be labeled an exhaustive native process schedule.

Uncovered boundary: Prepublication crash is a strong old-set case; full old/new authority is sampled across the phase matrix. GC protects current manifest/compacting/snapshot-pinned names but this map proves no arbitrary forged completion is safe.

## SST-6

Composed pressure regression: [should_preserve_snapshot_isolation_when_spill_pressure_is_released](../../tests/snapshot_spill_pressure.rs) — Actual retired snapshot input files remain physically present while pinned and all are physically reclaimed within ten seconds after release. See [fixture scope and mutation validation](snapshot-spill-pressure-fixture.md).

Production: [src/runtime/snapshot_pins.rs:190](../../src/runtime/snapshot_pins.rs#L190) `pinned_sst_names`; [src/runtime/actors/gc.rs:148](../../src/runtime/actors/gc.rs#L148) `delete_ssts`; [src/compaction/mod.rs:33](../../src/compaction/mod.rs#L33) `execute_compaction`.

Exact evidence and assertion markers:

- [tests/transactions.rs:5273](../../tests/transactions.rs#L5273) `should_preserve_snapshot_value_when_delete_is_compacted_with_snapshot_active`: Requires four actual L0 files, compacts to fewer L0 plus L1, old snapshot still reads v1 while current snapshot sees deletion. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 5327, 5347, 5351, 5356, 5369.
- [src/compaction/mod.rs:1147](../../src/compaction/mod.rs#L1147) `should_drop_obsolete_point_tombstone_when_compaction_has_bottommost_proof`: Real SST compaction with explicit bottommost eligibility removes alpha tombstone while beta value/sequence 6 survives. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1169, 1173.
- [src/runtime/snapshot_pins.rs:610](../../src/runtime/snapshot_pins.rs#L610) `should_retain_timed_out_snapshot_pin_until_unregister`: Time warning does not authorize removal of an active SST pin. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 613, 620, 621, 622, 624.

Preconditions: Snapshot API test uses durable local/CloudSimulated modes with positive physical-compaction prerequisites; bottommost unit writes and reads real SSTs.

Uncovered boundary: Bottommost eligibility is explicitly supplied in the unit; not every production overlap planner proof is independently tested here. Pin/GC schedules are bounded, not a universal concurrency proof.

## SST-7

Production: [src/runtime/event_loop/compaction.rs:26](../../src/runtime/event_loop/compaction.rs#L26) `assign_compaction_output_sequence_within`; [src/runtime/event_loop/compaction.rs:56](../../src/runtime/event_loop/compaction.rs#L56) `prepare_compaction_plan_for_launch_within`; [src/runtime/event_loop/sst_names.rs:28](../../src/runtime/event_loop/sst_names.rs#L28) `reserve_sst_name_durably_within`; [src/runtime/actors/compaction.rs:916](../../src/runtime/actors/compaction.rs#L916) `prepare_compaction`.

Exact evidence and assertion markers:

- [src/runtime/event_loop/flush_pipeline.rs:2777](../../src/runtime/event_loop/flush_pipeline.rs#L2777) `should_not_reuse_reserved_flush_sst_name_when_reopened_after_crash`: Reserves three names, loses in-memory cursor and reopens; new sequence is greater than every earlier reservation. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 2793.
- [src/compaction/mod.rs:1441](../../src/compaction/mod.rs#L1441) `should_leave_inputs_untouched_when_compaction_is_cancelled`: Genuine input SST remains byte-identical, typed Aborted, and canonical output does not exist. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1464, 1465, 1466.
- [src/runtime/event_loop/compaction/name_budget_tests.rs:317](../../src/runtime/event_loop/compaction/name_budget_tests.rs#L317) `should_keep_name_reservation_uncovered_when_actual_metadata_read_outlives_original_budget`: actual authority GET held beyond original budget: typed Timeout, journal counter never reused, reserved_through and remote committed counter do not advance, real row/bytes/lease survive. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 359, 360, 364, 365, 366, 370, 376, 379, 383, 389, 400, 401, 403, 404.

Preconditions: The actual name-reservation control acquires a conditional provider lease and proves baseline FORMAT/manifest/SST bytes before its immutable clock. Cancellation and flush-name controls use genuine Fs.

Uncovered boundary: Pre-worker identity assignment remains source-backed. The merged name-budget controls cover retained durable reservations and expired admission, not cancellation of already submitted provider mutations or every output-allocation crash schedule.

## SST-8

Production: [src/metadata/journal.rs:315](../../src/metadata/journal.rs#L315) `replay_identified_edits_after_with_fs_unlocked_within`; [src/metadata/persistence.rs:434](../../src/metadata/persistence.rs#L434) `save_snapshot_and_truncate_journal_with_fs`; [src/metadata/persistence.rs:450](../../src/metadata/persistence.rs#L450) `save_snapshot_unlocked`.

Exact evidence and assertion markers:

- [src/metadata/persistence.rs:1152](../../src/metadata/persistence.rs#L1152) `should_preserve_concurrent_journal_edit_when_checkpoint_uses_stale_manifest`: Actual concurrent journal append survives stale snapshot checkpoint; loaded file and edit frontier remain included. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1181, 1188.
- [src/metadata/persistence.rs:1241](../../src/metadata/persistence.rs#L1241) `should_replay_only_post_checkpoint_edits_given_crash_after_snapshot_rename`: Real post-rename failpoint leaves journal; next edit replays once, snapshot edit ID 1/loaded 2, exact two files and one post-checkpoint edit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1266, 1267, 1300, 1301, 1305, 1306, 1307, 1312, 1321.

Preconditions: Actual RealFs manifest/journal writes and existing checkpoint failpoint; thread schedule has a deliberate stale manifest.

Uncovered boundary: These crash/idempotence fixtures remain correctness evidence. Separately, all nine fixed-cell attempts of 37274074026 at c8f0de80 passed source-bound controlled local readback; B/C each miss the predeclared snapshot-payload target in three repeats and A has only one time miss. That conditional policy-investigation result is mapped at ACCT-6; cadence stays unchanged and neither physical bytes nor final fourteen-hour qualification follows.

## SST-9

Production: [src/io/durable_dir.rs:47](../../src/io/durable_dir.rs#L47) `create_path_durably`; [src/io/durable_dir.rs:151](../../src/io/durable_dir.rs#L151) `sync_dir_path`; [src/io/durable_dir.rs:195](../../src/io/durable_dir.rs#L195) `create_dir_all_durably`.

Exact evidence and assertion markers:

- [src/io/durable_dir.rs:291](../../src/io/durable_dir.rs#L291) `should_sync_the_parent_of_every_created_directory_below_root`: Actual nested a/b/c creation exists and observes exactly root, root/a, root/a/b parent syncs. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 301, 302.

Preconditions: Real filesystem directory creation with sync observation instrumentation; OS-specific sync semantics still apply.

Uncovered boundary: Call observation is not physical power-loss proof for every filesystem/device. Deletion currently lacks parent-directory sync and may resurrect obsolete bytes; that is a documented retention/leak boundary, not new authority.

## CLOUD-1

Composed native regression: [should_preserve_acknowledged_history_when_predecessor_wal_resumes_after_takeover_and_cleanup](../../tests/cloud_provider_engine_qualification/authority_publication.rs) — Held native immutable WAL PUT succeeds, remains an uncatalogued orphan, and cannot alter the successor catalog. Two absent-cache fresh-process recoveries exclude its complete transaction. Scope: Three bounded native S3 schedules against pinned local Sqrzl: before upstream PUT, after upstream HTTP 200, and lost successful response. Genuine lease expiry; shortened predecessor shutdown drain budget only. Successor is quiesced before predecessor release to isolate catalog mutation. No exhaustive simulation or live-provider qualification claim. See [fixture protocol](authority-publication-fixture.md).

Production: [src/wal/cloud_segment.rs:39](../../src/wal/cloud_segment.rs#L39) `object_key`; [src/wal/cloud_catalog.rs:190](../../src/wal/cloud_catalog.rs#L190) `WalPublicationCatalog::validate`; [src/runtime/cloud_startup/streaming_wal_plan.rs:333](../../src/runtime/cloud_startup/streaming_wal_plan.rs#L333) `validate_publication_identity`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:371](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L371) `reject_cloud_wal_without_catalog_within`.

Exact evidence and assertion markers:

- [src/engine/tests.rs:959](../../src/engine/tests.rs#L959) `should_reject_epoch_scoped_simulated_cloud_wal_without_catalog`: Actual orphan epoch-scoped valid WAL bytes are rejected without the publication catalog. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 977.
- [src/runtime/cloud_startup/streaming_wal_plan/tests.rs:321](../../src/runtime/cloud_startup/streaming_wal_plan/tests.rs#L321) `should_validate_catalog_authority_before_exposing_replay_sources`: Strict rejects regressed/checksum-invalid catalog history; salvage flags degraded and exposes only first validated segment, no active WAL. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 337, 343, 344, 353, 354, 362, 363, 368.

Preconditions: Actual filesystem CloudSimulated object and real catalog planner; explicit epoch/checksum faults.

Uncovered boundary: These prove object existence is insufficient and catalog identity is checked. They do not simulate every late native upload schedule or establish provider-side immutable-object retention settings.

## CLOUD-2

Composed native regression: [should_preserve_acknowledged_history_when_predecessor_wal_resumes_after_takeover_and_cleanup](../../tests/cloud_provider_engine_qualification/authority_publication.rs) — Late native upload completion after real takeover leaves the predecessor cloud frontier unchanged. A deliberate early-frontier mutation is rejected by the fixture. Scope: Three bounded native S3 schedules against pinned local Sqrzl: before upstream PUT, after upstream HTTP 200, and lost successful response. Genuine lease expiry; shortened predecessor shutdown drain budget only. Successor is quiesced before predecessor release to isolate catalog mutation. No exhaustive simulation or live-provider qualification claim. See [fixture protocol](authority-publication-fixture.md).

Production: [src/storage/hybrid/backend/uploads.rs:372](../../src/storage/hybrid/backend/uploads.rs#L372) `process_wal_upload`; [src/runtime/hybrid_persistence.rs:671](../../src/runtime/hybrid_persistence.rs#L671) `publish_remote_wal_segment`; [src/runtime/frontiers.rs:58](../../src/runtime/frontiers.rs#L58) `advance_cloud_to`.

Exact evidence and assertion markers:

- [src/runtime/hybrid_persistence/tests.rs:2949](../../src/runtime/hybrid_persistence/tests.rs#L2949) `should_readback_remote_wal_before_upload_worker_emits_ack`: Actual upload worker emits ACK for segment 9/sequence 13 only with recorded GET download of exact epoch/segment key. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 2979, 2989.
- [src/runtime/event_loop/cloud_integration/tests.rs:5726](../../src/runtime/event_loop/cloud_integration/tests.rs#L5726) `should_reject_cloud_ack_given_writer_fenced_after_upload_was_enqueued`: Real sealed local WAL plus controlled fenced ACK leaves cloud frontier 0 and WAL present, marks anomaly. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 5746, 5760, 5761, 5762.
- [src/runtime/event_loop/cloud_integration/tests.rs:5767](../../src/runtime/event_loop/cloud_integration/tests.rs#L5767) `should_not_advance_cloud_durability_across_unacked_segment_gap`: Out-of-order ACK stays buffered and WAL retained; earlier ACK permits exact contiguous cloud frontier and drains covered tracking. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 5788, 5808, 5815, 5826, 5831, 5838, 5848, 5853, 5859.

Preconditions: Upload test has valid real local WAL and a conditional mock cloud backend; event-loop tests intentionally deliver ACK events after genuine append/seal.

Uncovered boundary: Injected event delivery is not an actual native upload cancellation race. The final active authority/catalog checks are also source-backed; exact readback bytes and checksum predicates should not be weakened to mere object presence.

## CLOUD-3

Production: [src/runtime/cloud_startup/cloud_recovery/metadata.rs:32](../../src/runtime/cloud_startup/cloud_recovery/metadata.rs#L32) `read_committed_cloud_metadata_within`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:314](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L314) `mirror_cloud_metadata_within`; [src/runtime/hybrid_persistence/metadata_snapshot.rs:366](../../src/runtime/hybrid_persistence/metadata_snapshot.rs#L366) `mirror_control_metadata_within`; [src/lease/cloud.rs:421](../../src/lease/cloud.rs#L421) `ProviderLeaderStore::publish_committed_metadata`.

Exact evidence and assertion markers:

- [src/engine/tests.rs:1269](../../src/engine/tests.rs#L1269) `should_ignore_stale_manifest_json_during_strict_cloud_recovery`: Committed snapshot sequence 10 wins over a newer unpointed mutable manifest; loaded sequence remains 10 and legacy file is absent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1303, 1304.
- [src/lease/cloud/tests.rs:2446](../../src/lease/cloud/tests.rs#L2446) `should_reject_metadata_pointer_cas_that_lands_after_successor_acquires`: Held predecessor CAS loses to successor and exact committed winner pointer survives. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 2509, 2513.
- [src/lease/cloud/tests/startup_deadline/scoped_metadata.rs:407](../../src/lease/cloud/tests/startup_deadline/scoped_metadata.rs#L407) `should_preserve_native_mirror_operation_budget_when_startup_scope_is_unbounded`: Real native acquired lease and FORMAT/snapshot mirror succeeds through default/unbounded scope, retaining finite ordinary aggregate budget and exact remote descriptor bytes. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 415.
- [src/lease/cloud/tests/startup_deadline/scoped_metadata.rs:419](../../src/lease/cloud/tests/startup_deadline/scoped_metadata.rs#L419) `should_publish_native_mirror_when_bounded_startup_scope_has_remaining_budget`: Actual native mirror succeeds while every observed bounded operation deadline equals the original captured scope. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 427.

Preconditions: Source includes merged #713 default-None finite mirror fallback; native metadata fixture is an in-process actual S3 HTTP service with conditional responses, not an external production account.

Uncovered boundary: Native fixture checks one protocol and controlled sequences. It does not authorize unpointed generations or prove distributed multi-region consistency; ordinary None/Complete compatibility must remain separate from active aggregate scope.

## CLOUD-4

Production: [src/runtime/cloud_startup/cloud_recovery/mod.rs:673](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L673) `ensure_local_sst_cache_from_cloud_storage`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:705](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L705) `reconcile_manifest_ssts`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:747](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L747) `validate_manifest_sst_size`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:1086](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L1086) `cloud_recovery_sst_proofs_for_intent_replay`.

Exact evidence and assertion markers:

- [src/engine/tests.rs:1777](../../src/engine/tests.rs#L1777) `should_reject_manifest_sst_when_cloud_object_size_differs_from_manifest`: Genuine different-size SST has positive unequal-size prerequisite; actual inventory validation fails and no local cache SST is installed. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1791, 1823, 1827.
- [tests/storage_layer.rs:1589](../../tests/storage_layer.rs#L1589) `should_reject_manifest_bounds_when_valid_sst_summary_disagrees`: Valid persisted SST bytes with conflicting authoritative bounds return Corruption. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1594, 1608, 1648.

Preconditions: Actual real SST objects/metadata in filesystem/mock-cloud fixtures, not a forged length-only unit result.

Uncovered boundary: Selected assertions prove size and bounds validation. They do not cover all missing/corrupt/incompatible object variants or complete native provider salvage policy; full body/block integrity is lazy in some metadata-only startup paths.

## CLOUD-5

Composed native regression: [should_verify_acknowledged_state_in_fresh_recovery_process](../../tests/cloud_provider_engine_qualification/authority_publication.rs) — Two independent native recovery processes start with absent caches and require exact ordered scan and point values for acknowledged successor overwrites, deletion and atomic pairs after actual WAL/SST reclamation. Scope: Three bounded native S3 schedules against pinned local Sqrzl: before upstream PUT, after upstream HTTP 200, and lost successful response. Genuine lease expiry; shortened predecessor shutdown drain budget only. Successor is quiesced before predecessor release to isolate catalog mutation. No exhaustive simulation or live-provider qualification claim. See [fixture protocol](authority-publication-fixture.md).

Production: [src/runtime/cloud_startup/streaming_wal_plan.rs:118](../../src/runtime/cloud_startup/streaming_wal_plan.rs#L118) `build_with_local_fs_within`; [src/runtime/cloud_startup/streaming_wal_plan.rs:373](../../src/runtime/cloud_startup/streaming_wal_plan.rs#L373) `remote_source`; [src/runtime/cloud_startup/streaming_wal_plan.rs:593](../../src/runtime/cloud_startup/streaming_wal_plan.rs#L593) `active_local_source`; [src/wal/recovery/streaming.rs:283](../../src/wal/recovery/streaming.rs#L283) `replay_wal_with_options`.

Exact evidence and assertion markers:

- [tests/cloud_provider_engine_qualification/operational.rs:35](../../tests/cloud_provider_engine_qualification/operational.rs#L35) `should_recover_cloud_backlog_after_complete_local_disk_loss`: Actual native child campaign deletes all local files, requires interrupted checkpoint exit 73, deletes interrupted cache, recovers, loses cache again and verifies in another process. Scope: ignored native Sqrzl qualification; explicit features/environment required. Assertion/call markers at lines 52, 71, 76, 82, 83.
- [tests/cloud_provider_engine_qualification/operational.rs:160](../../tests/cloud_provider_engine_qualification/operational.rs#L160) `assert_complete_state`: Requires every source value, acknowledged seed and exact ordered complete keyset with no duplicates/unexpected/missing keys; campaign reports enforce disk bound and one completed replay. Scope: ignored native Sqrzl qualification; explicit features/environment required. Assertion/call markers at lines 166, 177, 192, 198, 200, 204.

Preconditions: Ignored native suite requires cloud-all, sqrzl-tests, failpoints and an explicitly qualified Sqrzl environment; exact campaign source/process receipts bind qualification.

Uncovered boundary: No native campaign was executed during this preparation. Qualification at a historical source revision does not qualify a later source revision. Emulator protocol success does not establish IAM, quotas, real-cloud availability or optional 128 MiB release-cost evidence.

## CLOUD-6

Production: [src/runtime/hybrid_persistence/streaming_prune.rs:40](../../src/runtime/hybrid_persistence/streaming_prune.rs#L40) `validate`; [src/runtime/hybrid_persistence.rs:755](../../src/runtime/hybrid_persistence.rs#L755) `prune_cloud_wal_segments_within`; [src/runtime/hybrid_persistence.rs:876](../../src/runtime/hybrid_persistence.rs#L876) `authoritative_wal_entry_within`; [src/runtime/hybrid_persistence/catalog.rs:205](../../src/runtime/hybrid_persistence/catalog.rs#L205) `commit_catalog_with_authority`.

Exact evidence and assertion markers:

- [src/runtime/hybrid_persistence/tests.rs:2446](../../src/runtime/hybrid_persistence/tests.rs#L2446) `should_reject_stale_remote_wal_target_identity_when_guarded_delete_runs`: Actual cloud object proof is captured, same bytes are republished with a new ETag; guarded delete errors and object still exists. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 2465, 2469.
- [src/runtime/cloud_startup/replay_coverage.rs:1079](../../src/runtime/cloud_startup/replay_coverage.rs#L1079) `should_replay_put_when_equal_sequence_sst_expiration_differs`: Equal value/sequence with different TTL is not sufficient coverage. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1086.

Preconditions: Conditional mock/cloud proof captures real object replacement identity; TTL test uses actual SST bytes.

Uncovered boundary: These selected controls establish identity and logical-coverage refusal, not a proof of every authority transition immediately before native deletion. Unknown outcomes must retain bytes/catalog intent instead of weakening preconditions.

## CLOUD-7

Production: [src/storage/cloud/dispatcher.rs:150](../../src/storage/cloud/dispatcher.rs#L150) `submit_get_within`; [src/storage/cloud/dispatcher.rs:233](../../src/storage/cloud/dispatcher.rs#L233) `submit_list_within`; [src/storage/cloud/dispatcher.rs:261](../../src/storage/cloud/dispatcher.rs#L261) `submit_head_within`; [src/storage/cloud/blocking.rs:92](../../src/storage/cloud/blocking.rs#L92) `BlockingCloud::get_optional`.

Exact evidence and assertion markers:

- [src/storage/cloud/tests/native_metadata_deadline.rs:188](../../src/storage/cloud/tests/native_metadata_deadline.rs#L188) `should_cancel_native_metadata_get_when_operation_deadline_expires`: Real S3/Azure/GCS-XML/GCS-JSON delayed HTTP GET requests require typed Timeout and observed native socket cancellation; simple/blocking GET controls adjacent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 193.
- [src/storage/cloud/tests/native_startup_deadline.rs:295](../../src/storage/cloud/tests/native_startup_deadline.rs#L295) `should_stop_native_pagination_when_aggregate_list_budget_expires`: All four native modes consume real continuation pages within one captured budget instead of granting a fresh page allowance. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 302, 306, 310, 321, 325.
- [src/storage/cloud/tests.rs:1770](../../src/storage/cloud/tests.rs#L1770) `should_preserve_precondition_failed_kind_when_cloud_error_crosses_storage_backend`: A PreconditionFailed payload beginning with old NotFound text remains typed precondition failure, not absence. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1779, 1780.

Preconditions: Native deadline fixtures use real local HTTP servers/backends and positive request-arrival/cancellation observations; type-mapping unit supplies a typed error.

Uncovered boundary: Header-wait cancellation is not proof no provider mutation committed; PUT remains ambiguous where required. Selected tests do not establish every body-read/range bound/signing retry contract or external provider implementation. Compatibility defaults on custom backends may be callback-bounded only.

## LIFE-1

Production: [src/runtime/event_loop/manifest.rs:200](../../src/runtime/event_loop/manifest.rs#L200) `drop_column_family`; [src/engine/mod.rs:672](../../src/engine/mod.rs#L672) `Engine::drop_column_family`; [src/engine/mod.rs:688](../../src/engine/mod.rs#L688) `Engine::drop_column_family_discarding_unflushed`; [src/metadata/manifest.rs:311](../../src/metadata/manifest.rs#L311) `delete_column_family_with_reclamation`.

Exact evidence and assertion markers:

- [tests/column_families.rs:263](../../tests/column_families.rs#L263) `should_fail_drop_column_family_given_unflushed_data_when_memtable_not_empty`: Actual committed unflushed value causes discard-licence refusal and remains readable after rejected safe drop. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 280, 287.
- [tests/column_families.rs:538](../../tests/column_families.rs#L538) `should_allocate_monotonic_column_family_ids_given_deleted_column_family_when_creating`: Actual destructive drop/recreate keeps old key absent and new key present. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 566, 567.

Preconditions: Common memory/local/CloudSimulated public API tests, deliberate unflushed accepted data and explicit destructive call.

Uncovered boundary: The refusal test requires licenses_unflushed_discard(), not an invented generic Busy class. It does not prove all accepted flush/publication interleavings or post-crash reclamation resume by itself.

## LIFE-2

Production: [src/runtime/snapshot_pins.rs:190](../../src/runtime/snapshot_pins.rs#L190) `pinned_sst_names`; [src/runtime/actors/gc.rs:148](../../src/runtime/actors/gc.rs#L148) `delete_ssts`; [src/metadata/manifest.rs:366](../../src/metadata/manifest.rs#L366) `mark_column_family_reclaimed`.

Exact evidence and assertion markers:

- [tests/transactions.rs:5735](../../tests/transactions.rs#L5735) `should_return_stable_results_when_scanning_after_column_family_dropped_mid_transaction`: Real flushed three-row local snapshot reads a, concurrent drop completes, remaining scan returns exactly b/c; a new transaction for dropped CF is InvalidArgument. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 5770, 5771, 5772, 5773, 5774.
- [src/runtime/snapshot_pins.rs:610](../../src/runtime/snapshot_pins.rs#L610) `should_retain_timed_out_snapshot_pin_until_unregister`: Warning does not evict retained names/horizon. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 613, 620, 621, 622, 624.

Preconditions: Actual local Engine concurrent drop and retained snapshot iterator with flushed files; pin registry unit is complementary.

Uncovered boundary: Stable scan proves logical read retention during one drop schedule. It does not directly inspect every final unlink or prove reclamation completes after the last pin under failures.

## BACKUP-1

Production: [src/engine/backup.rs:77](../../src/engine/backup.rs#L77) `Engine::backup_to`; [src/engine/backup.rs:248](../../src/engine/backup.rs#L248) `pin_durable_files`; [src/engine/backup.rs:348](../../src/engine/backup.rs#L348) `materialize_backup`.

Exact evidence and assertion markers:

- [tests/engine_api.rs:4729](../../tests/engine_api.rs#L4729) `should_preserve_cross_family_frontier_given_concurrent_backup_capture`: Actual ordered event/checkpoint writer has positive acknowledged work and nonzero backup frontier; restored Engine contains every event through restored frontier. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4776, 4782, 4811.

Preconditions: Actual local Engine and real backup/restore files, two CFs with ordered per-CF transactions, pinned capture and concurrent writer.

Uncovered boundary: This does not imply atomic transactions spanning CFs. Inventory checksum completeness is source-backed by materialization/validation and additional corrupt-artifact control; it is not an exhaustive adversarial filesystem proof.

## BACKUP-2

Production: [src/engine/backup/paths.rs:6](../../src/engine/backup/paths.rs#L6) `resolve`; [src/engine/backup/paths.rs:91](../../src/engine/backup/paths.rs#L91) `prepare_target`; [src/engine/backup/paths.rs:114](../../src/engine/backup/paths.rs#L114) `publish`.

Exact evidence and assertion markers:

- [tests/backup_paths.rs:28](../../tests/backup_paths.rs#L28) `should_reject_backup_overlap_before_mutation`: Actual nested destination rejects with InvalidArgument and source inventory remains byte-for-byte unchanged. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 47, 51, 52.
- [tests/backup_paths.rs:59](../../tests/backup_paths.rs#L59) `should_reject_restore_overlap_without_changing_artifact`: Overlapping restore target rejects and artifact inventory is unchanged. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 79, 83.
- [tests/backup_publish_races.rs:9](../../tests/backup_publish_races.rs#L9) `should_preserve_target_created_immediately_before_publication`: Genuine before-publish failpoint creates foreign directory/symlink; backup and restore reject, foreign sentinel survives and owned stages are absent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 65, 66, 70, 72, 74.

Preconditions: Actual filesystem path resolution/publication, real backup artifact, failpoints for the last-moment race; symlink variants are Unix-only.

Uncovered boundary: Tests cover selected alias and late-target cases. Platform-native rename/nonreplacement semantics and remote mounts remain deployment/filesystem boundaries; no universal hostile-filesystem guarantee.

## BACKUP-3

Production: [src/engine/backup.rs:181](../../src/engine/backup.rs#L181) `Engine::restore_backup`; [src/engine/backup.rs:415](../../src/engine/backup.rs#L415) `validate_backup`; [src/engine/backup.rs:491](../../src/engine/backup.rs#L491) `copy_verified_objects`; [src/engine/verification.rs:242](../../src/engine/verification.rs#L242) `verify_storage_path`.

Exact evidence and assertion markers:

- [tests/engine_api.rs:4822](../../tests/engine_api.rs#L4822) `should_leave_restore_target_absent_given_corrupt_backup_object`: Corrupts actual backup FORMAT object, restore returns Corruption and target stays absent. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4844, 4845.
- [tests/engine_api.rs:4914](../../tests/engine_api.rs#L4914) `should_restore_split_cloud_simulation_layout_given_valid_backup`: Backup inventory excludes lease files, real CloudSimulated restore opens healthy and exact accepted value survives. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4941, 4945, 4962, 4969.

Preconditions: Actual local corrupt artifact and actual matching CloudSimulated split layout, strict staging verification and fresh Engine open.

Uncovered boundary: Healthy fresh acquisition plus excluded lease objects does not compare lease tokens numerically. Provider-backed cloud restore is unsupported; checksums do not make untrusted artifact parsing immune to every malformed input.

## BACKUP-4

Production: [src/engine/backup.rs:181](../../src/engine/backup.rs#L181) `Engine::restore_backup`; [src/engine/backup/paths.rs:114](../../src/engine/backup/paths.rs#L114) `publish`; [src/engine/backup.rs:491](../../src/engine/backup.rs#L491) `copy_verified_objects`.

Exact evidence and assertion markers:

- [tests/backup_paths.rs:87](../../tests/backup_paths.rs#L87) `should_preserve_foreign_stage_when_retrying_restore`: Foreign crash-like stage with sentinel coexists with real successful restore; sentinel bytes remain exact. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 111, 112.
- [tests/engine_api.rs:4879](../../tests/engine_api.rs#L4879) `should_leave_restore_target_absent_given_incomplete_backup_artifact`: Removing one real inventory object makes restore fail, target absent and no owned .midge-restore- stage remains. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4902, 4903, 4904.

Preconditions: Real backup artifact and filesystem stage names; the first test intentionally creates a stage that this attempt does not own.

Uncovered boundary: Foreign-stage retention and failed owned-stage cleanup are tested; hard process death can still leave an owned temp stage. Retry safety does not promise immediate storage reclamation.

## BACKUP-5

Production: [src/engine/backup.rs:77](../../src/engine/backup.rs#L77) `Engine::backup_to`; [src/engine/backup.rs:181](../../src/engine/backup.rs#L181) `Engine::restore_backup`.

Exact evidence and assertion markers:

- [tests/engine_api.rs:4914](../../tests/engine_api.rs#L4914) `should_restore_split_cloud_simulation_layout_given_valid_backup`: Actual supported CloudSimulated split-layout restore succeeds with exact value and healthy new owner. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4941, 4945, 4962, 4969.
- [tests/engine_api.rs:4822](../../tests/engine_api.rs#L4822) `should_leave_restore_target_absent_given_corrupt_backup_object`: Supported local artifact is validated and corrupt input refused. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 4844, 4845.

Preconditions: Local and matching filesystem CloudSimulated layouts are the allowed source/target kinds.

Uncovered boundary: No dedicated public negative test for every unsupported memory/provider-backed/mismatched-layout combination was verified in this bounded pass. Explicit NotSupported/InvalidArgument source branches are the boundary; positive tests cannot expand support.

## RES-1

Production: [src/common/resource_budget.rs:117](../../src/common/resource_budget.rs#L117) `ResourceBudget::reserve`; [src/runtime/transaction_spill/mod.rs:311](../../src/runtime/transaction_spill/mod.rs#L311) `TransactionWriteSet::push`; [src/runtime/cloud_startup/replay_coverage.rs:120](../../src/runtime/cloud_startup/replay_coverage.rs#L120) `ReplayCoverage::new`; [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:24](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L24) `CandidateIndex::new`; [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:20](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L20) `CandidateIndex::allocation_bytes`.

Exact evidence and assertion markers:

- [src/common/resource_budget.rs:494](../../src/common/resource_budget.rs#L494) `should_report_current_charge_until_the_last_shared_reservation_is_released`: 7-byte shared charge remains after first owner drops, then used=0 after last owner drops. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 499, 507, 508.
- [src/common/resource_budget.rs:512](../../src/common/resource_budget.rs#L512) `should_reject_reservation_when_resource_budget_would_be_exceeded`: 7 of 10 held bytes causes 4-byte request ResourceLimit, peak remains 7, release permits full 10. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 521, 522, 524.
- [tests/fault_injection.rs:12062](../../tests/fault_injection.rs#L12062) `should_return_resource_limit_when_memory_mode_exhausts_transaction_pool`: Actual memory Engine with 8 KiB transaction pool rejects growing accepted-intent set as typed ResourceLimit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 12095.
- [src/runtime/cloud_startup/replay_coverage.rs:779](../../src/runtime/cloud_startup/replay_coverage.rs#L779) `should_preserve_exact_proof_when_candidate_index_cannot_fit_read_budget`: One actual checksummed SST fits the configured read budget while 8192 disjoint metadata descriptors make the index exceed it. The legacy full scan retains exact proof, leaves no index and releases all reservations. Scope: actual seeded SST/legacy fallback; other descriptors are metadata-only, not 8193 actual SSTs. Assertion/call markers at lines 797, 798, 799, 800, 801, 803.
- [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:386](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L386) `should_preserve_exact_charge_when_index_is_dropped_or_admission_fails`: Constructed descriptors exercise actual RAII denied/exact admission/drop and checked allocation overflow. Index bytes remain charged to the same pool and are fully released. Scope: constructed index metadata plus actual bounded reservation lifecycle; no process-RSS or backend allocation proof. Assertion/call markers at lines 403, 404, 405, 406, 407, 408, 409.
- [src/runtime/cloud_startup/replay_coverage/tests/streaming_scale.rs:349](../../src/runtime/cloud_startup/replay_coverage/tests/streaming_scale.rs#L349) `should_bound_manifest_lookup_work_when_streaming_real_transaction_batches_at_scale`: The same actual planner/replay fixture admits index and exact SST proof under the existing 2MiB coverage budget. Positive observed peak remains within the configured limit, all remote read observations complete successfully, and releasing readers/index/proofs returns the pool to zero; this is bounded reservation accounting rather than process-RSS proof. Scope: one actual filesystem-seeded fixture: 64 checksummed SSTs hold 8192 rows and 64 interior holes are replayed from 258 framed transaction batches; no public ACK, native socket/hour, wall-time speed or physical-device amplification proof. Assertion/call markers at lines 354, 355, 389, 390, 392, 394, 395, 396, 400, 409, 410, 414, 415, 418, 422, 313, 317, 318, 319, 320, 321, 322, 296, 298, 302, 307, 210, 212, 216.

Preconditions: Reservation units operate actual RAII budget ownership; public memory test uses real transaction code. Engine pool is distinct from whole-process RSS.

Uncovered boundary: Representative charged ownership does not prove every allocator/backend/queue byte is included, nor that process RSS equals configured engine budget. Delivered ACCT-1 through ACCT-6 map owner-bound persistent attempted/returned/durable metadata accounting and post-shutdown integrity; that bounded ledger does not extend allocator or process RSS coverage.

## RES-2

Composed pressure regression: [should_preserve_snapshot_isolation_when_spill_pressure_is_released](../../tests/snapshot_spill_pressure.rs) — More than 1.5 MiB of actual caller-owned spill files exhaust a 2 MiB local budget; rejected mutation preserves exact acknowledged state and the same write resumes after release. See [fixture scope and mutation validation](snapshot-spill-pressure-fixture.md).

Production: [src/runtime/transaction_spill/mod.rs:311](../../src/runtime/transaction_spill/mod.rs#L311) `TransactionWriteSet::push`; [src/runtime/actors/wal/transaction.rs:86](../../src/runtime/actors/wal/transaction.rs#L86) `prepare_transaction_append`; [src/runtime/event_loop/write_batch.rs:467](../../src/runtime/event_loop/write_batch.rs#L467) `prepare_transaction_for_coalescing`.

Exact evidence and assertion markers:

- [tests/storage_layer.rs:317](../../tests/storage_layer.rs#L317) `should_preserve_transaction_when_oversized_range_delete_is_rejected`: Actual oversized endpoint yields ResourceLimit while previously staged middle value remains visible, commits and reads after flush. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 330, 331, 337.
- [tests/fault_injection.rs:303](../../tests/fault_injection.rs#L303) `should_reject_transaction_when_no_space_hits_before_batch_append_and_remain_usable`: Actual before-batch WAL NoSpace leaves both failed keys absent; subsequent sync commit succeeds and exact accepted/failure key states survive reopen. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 331, 332, 333, 351, 352, 353.

Preconditions: Real local Engine and genuine admission/failpoint positions; previously accepted intents deliberately precede rejected operation.

Uncovered boundary: Before-publication rejection can be unapplied; once a write is submitted or partially persisted, error is not universally proof of no mutation. Partial-tail failure uses degradation/fencing instead (RES-3).

## RES-3

Production: [src/runtime/event_loop/write_batch.rs:71](../../src/runtime/event_loop/write_batch.rs#L71) `ensure_l0_write_admission`; [src/runtime/actors/wal/transaction.rs:458](../../src/runtime/actors/wal/transaction.rs#L458) `append_prepared_transaction_batches`; [src/storage/hybrid/backend/uploads.rs:372](../../src/storage/hybrid/backend/uploads.rs#L372) `process_wal_upload`; [benches/bench_support/checkpoint_commit.rs:45](../../benches/bench_support/checkpoint_commit.rs#L45) `commit_with_backpressure`; [benches/tier4_system_checkpoint_write_amplification.rs:415](../../benches/tier4_system_checkpoint_write_amplification.rs#L415) `run_ingestion`.

Exact evidence and assertion markers:

- [tests/fault_injection.rs:357](../../tests/fault_injection.rs#L357) `should_preserve_acknowledged_state_when_no_space_tears_wal_frame`: Actual positional partial WAL write rolls physical length back; Engine becomes Degraded, follow-up is Fenced, prior ACK fixture survives both reopens and later repaired writes. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 415, 416, 420, 421, 422, 423, 424, 429, 430, 431, 442, 443, 449.
- [src/runtime/event_loop/write_batch/tests.rs:1683](../../src/runtime/event_loop/write_batch/tests.rs#L1683) `should_stall_before_wal_when_transaction_would_exceed_hard_l0_ceiling`: Typed WriteStall keeps sequence and actual WAL append count unchanged. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1713, 1719, 1736, 1743, 1744.
- [tests/checkpoint_commit.rs:47](../../tests/checkpoint_commit.rs#L47) `should_retry_scripted_stall_when_next_attempt_receives_actual_strict_ack`: First rejection is explicitly scripted, then the shared benchmark helper executes a real public stall-clear wait and local strict transaction ACK of 32 rows. Exact ordered values after owned shutdown and same-path reopen precede one-ACK/two-attempt/one-stall assertions; this does not reproduce hosted L0 pressure. Scope: scripted admission rejection followed by actual public local strict ACK, public waiter, owned shutdown and same-path reopen; no genuine saturation schedule claim. Assertion/call markers at lines 88, 89, 90, 91, 92, 93, 94.
- [tests/checkpoint_commit.rs:101](../../tests/checkpoint_commit.rs#L101) `should_invoke_neither_commit_nor_wait_when_original_cell_budget_is_expired`: Constructed original expired clock admits neither commit nor wait and records no successful commit. Scope: constructed clock/error/clear-result helper policy; no real pressure or workload execution claim. Assertion/call markers at lines 125, 126, 127, 128, 129.
- [tests/checkpoint_commit.rs:133](../../tests/checkpoint_commit.rs#L133) `should_end_permanent_stall_when_original_logical_budget_expires`: Constructed permanent rejection and false waiter results consume exactly the captured three-second allowance in bounded slices; zero commit success is retained. Scope: constructed clock/error/clear-result helper policy; no real pressure or workload execution claim. Assertion/call markers at lines 159, 160, 161, 162, 163, 164, 165, 166.
- [tests/checkpoint_commit.rs:170](../../tests/checkpoint_commit.rs#L170) `should_preserve_original_allowance_when_clear_signals_lead_to_more_stalls`: Constructed clear signals permit later rejected attempts without refreshing the original allowance or becoming ACK credit. Scope: constructed clock/error/clear-result helper policy; no real pressure or workload execution claim. Assertion/call markers at lines 190, 191, 192, 193, 194, 195, 196.
- [tests/checkpoint_commit.rs:200](../../tests/checkpoint_commit.rs#L200) `should_cap_stall_allowance_when_requested_budget_exceeds_thirty_seconds`: Constructed clock caps a requested longer stall allowance at 30 seconds, with zero successful commits; this is an admission clock control, not syscall preemption. Scope: constructed clock/error/clear-result helper policy; no real pressure or workload execution claim. Assertion/call markers at lines 220, 221, 222, 223.
- [tests/checkpoint_commit.rs:227](../../tests/checkpoint_commit.rs#L227) `should_clamp_wait_to_original_cell_budget_when_it_is_shorter_than_stall_allowance`: Constructed 250ms original cell remainder bounds the single wait and cannot be replaced by a fresh logical stall budget. Scope: constructed clock/error/clear-result helper policy; no real pressure or workload execution claim. Assertion/call markers at lines 250, 251, 252, 253.
- [tests/checkpoint_commit.rs:257](../../tests/checkpoint_commit.rs#L257) `should_preserve_terminal_commit_errors_when_reconstruction_would_be_ambiguous`: Constructed Timeout, Busy, NoSpace, Fenced, Corruption and ResourceLimit retain exact discriminant/message, one attempt and no waiter or successful-commit credit; unknown accepted writes are never reconstructed. Scope: constructed clock/error/clear-result helper policy; no real pressure or workload execution claim. Assertion/call markers at lines 288, 289, 290, 291, 292.
- [tests/checkpoint_commit.rs:297](../../tests/checkpoint_commit.rs#L297) `should_preserve_terminal_wait_error_when_rejected_commit_has_not_succeeded`: Constructed rejected commit followed by Fenced waiter failure preserves the original terminal type/detail and records no successful commit. Scope: constructed clock/error/clear-result helper policy; no real pressure or workload execution claim. Assertion/call markers at lines 312, 315, 316, 317.

Preconditions: Real local filesystem partial-write fault and separate actual event-loop WAL admission fixture; prior acknowledged values are explicit positive baseline. The shared benchmark helper controls are separate: one scripted WriteStall is followed by genuine public local strict ACK/reopen; the other seven construct clocks, rejections or terminal errors. The real pre-WAL L0 admission fixture remains the independent rejection-safety evidence. All retry/wait/reconstruction costs remain inside the existing accounting/time window; progress advances only after actual commit/flush success.

Uncovered boundary: These are bounded no-space/stall schedules, not a guarantee of progress under permanent capacity exhaustion. Distinguish expected ResourceLimit/WriteStall from corruption or lost acknowledged data; worker-panic coverage is not universal here. Rejection-only reconstruction is bounded by one original cell deadline and an allowance capped at 30s, with waiter slices <=1s. Wait-clear never means ACK, and non-WriteStall errors remain terminal. These controls do not prove every actual saturation schedule clears or that a blocked accepted syscall is preemptible. Historical A/r1 smoke 37270512400 at 32911805 recorded zero stalls/waits. Historical campaign 37270845568 retained real six/nine A/r1 and A/r2 rejections and clears but failed its metadata endpoint gate; it remains invalid. At measured c8f0de80, same-source smoke 37273556125 and all nine attempts of 37274074026 passed retained readback. A/r3 records three genuine L0 WriteStall rejections, three clears and 256 successful commits with exact reopen; JSON does not independently prove waiter bodies or clocks. This observed schedule does not prove arbitrary saturation clears. Full-hour acceptance uses the final merged catalog source, with run identities and artifact readbacks tracked on GitHub issue #711.

## RES-4

Production: [src/runtime/retry_schedule.rs:38](../../src/runtime/retry_schedule.rs#L38) `RetrySchedule::defer`; [src/runtime/event_loop/cloud_maintenance.rs:78](../../src/runtime/event_loop/cloud_maintenance.rs#L78) `schedule_cloud_maintenance`; [src/storage/hybrid/backend/uploads.rs:372](../../src/storage/hybrid/backend/uploads.rs#L372) `process_wal_upload`.

Exact evidence and assertion markers:

- [src/runtime/retry_schedule.rs:93](../../src/runtime/retry_schedule.rs#L93) `should_wake_when_retry_deadline_arrives`: Controlled clock leaves deferred retry not ready with 1s remaining; advancing original clock gives ready and zero remaining. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 101, 102, 106, 107.
- [src/runtime/hybrid_persistence/tests.rs:2999](../../src/runtime/hybrid_persistence/tests.rs#L2999) `should_stop_retrying_failed_wal_upload_after_retry_budget_exhausted`: Actual queued valid local WAL with always-failing storage makes exactly three write attempts, two retryable failures then one terminal failure, and pending count drains. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 3042, 3047, 3056.

Preconditions: Backoff unit uses a controllable clock; actual upload-worker test uses an intentionally failing StorageBackend and finite polling/worker progression.

Uncovered boundary: Attempt count/backoff assertions do not alone prove CPU utilization, fairness or every terminal error path under all fault schedules. Permanent provider failure can halt useful maintenance while retaining authoritative state.

## RES-5

Production: [src/engine/startup/deadline.rs:18](../../src/engine/startup/deadline.rs#L18) `deadline::open`; [src/common/deadline_scope.rs:58](../../src/common/deadline_scope.rs#L58) `DeadlineScope::check`; [src/runtime/router.rs:112](../../src/runtime/router.rs#L112) `ResponseRouter::request_deadline`; [src/runtime/event_loop/compaction.rs:390](../../src/runtime/event_loop/compaction.rs#L390) `CompactionCoordinator::compact_all`; [src/runtime/event_loop/compaction.rs:106](../../src/runtime/event_loop/compaction.rs#L106) `EventLoop::launch_compaction`; [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:79](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L79) `CandidateIndex::visit`.

Exact evidence and assertion markers:

- [tests/cloud_provider_engine_qualification/startup_deadline.rs:364](../../tests/cloud_provider_engine_qualification/startup_deadline.rs#L364) `should_keep_startup_deadline_when_mandatory_native_catalog_read_follows_acquisition`: Actual acquired-owner native open is bounded by the original 600ms deadline, no late runtime admission; cleanup and same-path healthy reopen preserve rows. Scope: ignored native Sqrzl qualification; explicit features/environment required. Assertion/call markers at lines 388, 393, 397, 406, 407, 408, 410, 414, 418.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs:314](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs#L314) `should_keep_manual_origin_when_later_shorter_waiter_expires`: actual generation retains exactly the original OperationDeadline after shorter/lower-ID waiter joins and expires; original and longer callers succeed with exact committed output. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 360, 361, 362, 363, 364, 365, 366, 369, 370, 371, 372, 373, 377.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs:384](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs#L384) `should_refuse_budget_extension_when_longer_waiter_joins_accepted_manual_work`: actual second authority phase times out at original budget while longer route remains live; predecessor remote authority, local output intent/input bytes and exact rows remain. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 427, 428, 429, 433, 434, 438, 439, 440, 441, 442, 443, 447.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs:251](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs#L251) `should_preserve_background_owner_when_later_manual_waiter_expires`: actual accepted background generation retains None after later short waiter expires; real phases complete, exact cloud committed output/rows survive and inputs retire. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 291, 295, 296, 297, 298, 299, 300, 301, 302, 303, 307.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs:454](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs#L454) `should_inherit_original_clock_when_actual_manual_work_reaches_second_family`: two genuine CF input sets have the same immutable accepted origin; all phases finish before it, committed metadata has both exact output sets, exact rows survive and inputs/intents retire. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 492, 493, 494, 495, 496, 497, 498, 499, 503, 504, 505, 513, 514, 515, 519, 520.
- [src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs:527](../../src/runtime/event_loop/cloud_integration/tests/maintenance_tests/manual_flush_tests/compaction_budget_tests/deadline_owner_tests.rs#L527) `should_stop_second_family_when_original_manual_clock_expires`: first CF commits; second actual generation keeps original origin and times out without fresh later phase, retaining second predecessor cloud/input authority and exact per-CF rows. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 571, 572, 573, 574, 575, 576, 577, 580, 581, 585, 586, 587, 588, 589, 590, 591, 598, 599, 600.
- [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:293](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L293) `should_propagate_typed_timeout_when_candidate_visitor_fails`: Actual traversal preserves a scripted typed visitor Timeout instead of returning a covered or conservative-negative answer. Scope: constructed metadata/scripted visitor error; no native deadline execution. Assertion/call markers at lines 313.
- [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:317](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L317) `should_reject_cancelled_lookup_when_index_is_already_built`: A real cancelled DeadlineScope rejects the warmed metadata index before any visitor callback. Scope: actual scope cancellation/index traversal with constructed metadata; no provider cancellation claim. Assertion/call markers at lines 339, 340.
- [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:344](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L344) `should_escape_timeout_when_scope_is_cancelled_during_candidate_traversal`: The first actual candidate callback cancels the real scope; traversal escapes typed Timeout before completing every candidate. Scope: actual scope/callback ordering over constructed metadata; no SST/Engine/native-hour execution. Assertion/call markers at lines 366, 367.
- [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:371](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L371) `should_release_index_charge_when_expired_construction_returns_timeout`: An immediately expired real scope returns typed Timeout from index construction and unwinds its actual reservation to zero. Scope: actual expired scope/RAII over constructed metadata; no syscall preemption. Assertion/call markers at lines 381, 382.

Preconditions: The ignored native open case requires explicit sqrzl-tests prerequisites and --ignored execution. Compaction fixtures own a real conditional lease, genuine SST/input bytes and actual phase receipts; all setup/read-gate acquisition precedes the immutable deadline.

Uncovered boundary: Merged startup Some and manual compaction Some use one original deadline; None and Complete preserve ordinary operation caps. A joining waiter cannot replace a background None or an accepted manual origin. Caller Timeout is not provider cancellation or permission to release an accepted worker, lease, body or reservation. Local syscalls are cooperative at unit boundaries, not preemptible.

## RES-6

Production: [src/runtime/state/flush.rs:218](../../src/runtime/state/flush.rs#L218) `l0_slot_usage`; [src/runtime/state/flush.rs:248](../../src/runtime/state/flush.rs#L248) `l0_write_slot_unavailable`; [src/runtime/event_loop/write_batch.rs:71](../../src/runtime/event_loop/write_batch.rs#L71) `ensure_l0_write_admission`.

Exact evidence and assertion markers:

- [src/runtime/state/tests.rs:733](../../src/runtime/state/tests.rs#L733) `should_stall_next_write_after_active_generation_reserves_last_l0_slot`: Constructed published/queued capacity plus actual active put reaches hard ceiling and sets slot unavailable/hard stall. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 749, 750, 762, 763, 764, 765.
- [src/runtime/event_loop/write_batch/tests.rs:1683](../../src/runtime/event_loop/write_batch/tests.rs#L1683) `should_stall_before_wal_when_transaction_would_exceed_hard_l0_ceiling`: Actual first accepted generation fills final slot; second returns WriteStall without sequence or WAL append change. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1713, 1719, 1736, 1743, 1744.

Preconditions: Configured small trigger/immutable capacity, positive equality of actual slot usage to hard ceiling before rejection; metadata input names are constructed in the event-loop fixture.

Uncovered boundary: The before-WAL assertion is stronger than checking a stall metric. It does not itself prove every pressure-recovery scheduling path when background compaction is disabled or every backlog geometry.

## RES-7

Production: [src/compaction/executor.rs:451](../../src/compaction/executor.rs#L451) `collect_compaction_stream_inputs`; [src/runtime/event_loop/cloud_maintenance.rs:78](../../src/runtime/event_loop/cloud_maintenance.rs#L78) `schedule_cloud_maintenance`; [src/runtime/state/flush.rs:218](../../src/runtime/state/flush.rs#L218) `l0_slot_usage`; [src/compaction/repair.rs:24](../../src/compaction/repair.rs#L24) `local_capacity`; [src/runtime/cloud_startup/replay_coverage/candidate_index.rs:79](../../src/runtime/cloud_startup/replay_coverage/candidate_index.rs#L79) `CandidateIndex::visit`.

Exact evidence and assertion markers:

- [src/compaction/mod.rs:2221](../../src/compaction/mod.rs#L2221) `should_keep_compaction_work_bounded_across_ten_thousand_targets`: Constructed stream input setup for one source and 10,000 target names has exactly two merge heads and charged peak <= pool limit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 2244, 2245.
- [src/compaction/mod.rs:1441](../../src/compaction/mod.rs#L1441) `should_leave_inputs_untouched_when_compaction_is_cancelled`: Actual cancelled Fs compaction preserves input bytes and produces no canonical output. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1464, 1465, 1466.
- [src/compaction/repair.rs:231](../../src/compaction/repair.rs#L231) `should_find_native_capacity_when_real_filesystem_root_is_canonical`: Actual RealFs root and canonicalized native mount selection report positive available capacity; the real overlapping repair remains separately checked, without fabricated disk capacity or a platform skip. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 249, 258, 259.
- [src/runtime/actors/compaction/publication_error_tests.rs:255](../../src/runtime/actors/compaction/publication_error_tests.rs#L255) `should_keep_exact_rows_when_healthy_background_repair_uses_no_manual_deadline`: Actual healthy background None compaction exercises genuine overlapping SST repair, scratch admission and exact output rows; this is the native Windows capacity regression rather than a synthetic drive-string test. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 232, 236, 237, 239, 240, 246, 250.
- [tests/solid_governance.rs:659](../../tests/solid_governance.rs#L659) `should_bound_legacy_sst_backfill_work_when_persisting`: Source-governance control requires the legacy-bound worker spelling and the current batched Administration-origin append_batch_for path. It does not execute a backfill, prove service time or replace the real SST repair controls. Scope: source-string governance check; no executed runtime/data proof. Assertion/call markers at lines 669, 670.
- [src/runtime/cloud_startup/replay_coverage.rs:807](../../src/runtime/cloud_startup/replay_coverage.rs#L807) `should_bound_manifest_work_when_replay_probes_sparse_sequence_intervals`: 100 genuine coverage probes read one real checksummed candidate SST amid 8192 disjoint metadata descriptors; query-node/candidate/open counters bound selection without a machine elapsed assertion. Construction is once per index and is outside the query-node metric. Scope: actual seeded checksummed SST plus constructed disjoint descriptors; per-query selection-work bound, not elapsed/native-hour qualification. Assertion/call markers at lines 827, 832, 833, 834, 839.
- [src/runtime/cloud_startup/replay_coverage/tests/streaming_scale.rs:349](../../src/runtime/cloud_startup/replay_coverage/tests/streaming_scale.rs#L349) `should_bound_manifest_lookup_work_when_streaming_real_transaction_batches_at_scale`: The actual strict streaming planner/replay pipeline issues 16512 coverage probes over 64 actual SSTs. All exact row/frontier/object/budget assertions precede a logarithmic query-node gate of 264192, with one reader open per SST and every SST byte checksum-verified. This counts query traversal, excluding one-time index construction; all-overlap manifests may still require linear matching work. Scope: one actual filesystem-seeded fixture: 64 checksummed SSTs hold 8192 rows and 64 interior holes are replayed from 258 framed transaction batches; no public ACK, native socket/hour, wall-time speed or physical-device amplification proof. Assertion/call markers at lines 354, 355, 389, 390, 392, 394, 395, 396, 400, 409, 410, 414, 415, 418, 422, 313, 317, 318, 319, 320, 321, 322, 296, 298, 302, 307, 210, 212, 216.

Preconditions: 10,000-target test is a real stream-construction path over MockFs/names, not 10,000 populated SSTs or completed sustained compaction. Cancellation control has genuine input SST.

Uncovered boundary: Bounded merge heads and conservative canonical-mount scratch capacity are safety properties. They do not prove a global decreasing-progress measure or sustained service SLO under arbitrary admission rates. Real filesystem-issued byte accounting does not establish physical device write amplification or production capacity.

## RES-8

Production: [src/engine/mod.rs:115](../../src/engine/mod.rs#L115) `Engine::drop`; [src/engine/mod.rs:527](../../src/engine/mod.rs#L527) `Engine::shutdown`; [src/runtime/event_loop/shutdown.rs:18](../../src/runtime/event_loop/shutdown.rs#L18) `EventLoop::handle_shutdown`.

Exact evidence and assertion markers:

- [tests/fault_injection.rs:5082](../../tests/fault_injection.rs#L5082) `should_retain_writer_lease_until_blocked_flush_worker_exits`: Genuine paused publication makes short shutdown Timeout and contender open LeaseHeld; release owned worker, successful shutdown and same-path reacquisition follow. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 5114, 5115.
- [tests/fault_injection/compaction_deadline.rs:155](../../tests/fault_injection/compaction_deadline.rs#L155) `should_retain_acknowledged_inputs_when_expired_publication_remains_owned_during_shutdown`: actual CloudStrict-ACK data plus OutputDurable accepted failpoint worker survives compaction Timeout and short shutdown; contender cannot acquire until release/join, then strict reopen has exact acknowledged dataset. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 201, 205, 206, 207, 208, 209, 213, 217, 222, 223, 227, 228, 229, 230.
- [tests/checkpoint_accounting.rs:36](../../tests/checkpoint_accounting.rs#L36) `should_release_actual_engine_lease_when_retained_checkpoint_metrics_outlive_shutdown`: 32 genuinely sync-acknowledged local rows survive real flush, owned shutdown and same-path reopen while original counter-only handle remains alive; original and reopened owner identities differ and original final counters remain stable. Scope: actual public local Engine strict ACK, worker publication, same-path reopen and owned shutdown. Assertion/call markers at lines 75, 79, 87, 88, 89, 90, 91, 92.

Preconditions: Public held-worker controls retain real acknowledged data and release their finite holds before joins/assertions. The counter-lifetime control uses local strict ACK and an actual same-path Engine open; no constructed owner counter substitutes for lease reacquisition.

Uncovered boundary: Caller teardown wait can expire while reaper continues. These schedules prove fencing retention and acknowledged data for their accepted workers, not termination under a permanently hung arbitrary backend. No new optional reclamation should be started just to finish shutdown.

## PROG-1

Production: [src/telemetry/recovery_progress.rs:30](../../src/telemetry/recovery_progress.rs#L30) `WorkProgress::completed`; [src/telemetry/recovery_progress.rs:58](../../src/telemetry/recovery_progress.rs#L58) `WorkProgress::finish`; [src/telemetry/recovery_progress.rs:99](../../src/telemetry/recovery_progress.rs#L99) `RecoveryReadObserver::remote_range_completed`; [src/runtime/cloud_startup/cloud_recovery/mod.rs:705](../../src/runtime/cloud_startup/cloud_recovery/mod.rs#L705) `reconcile_manifest_ssts`.

Exact evidence and assertion markers:

- [tests/stress_workload_watchdog.rs:321](../../tests/stress_workload_watchdog.rs#L321) `should_remain_healthy_when_actual_cloud_recovery_completes_read_only_work`: Actual planner/replay exceeds 1s idle window with >=16 successful reads, >=1 MiB bytes, maximum 64 KiB range, exact records/values/epoch 7, no staged local WAL and zero sampled local DB bytes. Scope: actual main-driven native watchdog case or explicitly constructed consumer helper; see preconditions. Assertion/call markers at lines 333, 334, 335.
- [tests/stress_workload_watchdog.rs:433](../../tests/stress_workload_watchdog.rs#L433) `should_remain_healthy_when_cached_recovery_finishes_verified_coverage_work`: Actual cached replay exceeds idle window, verifies exact rows and coverage checks >= records while replay remote reads=0. Scope: actual main-driven native watchdog case or explicitly constructed consumer helper; see preconditions. Assertion/call markers at lines 444, 447.
- [tests/stress_workload_watchdog.rs:367](../../tests/stress_workload_watchdog.rs#L367) `should_report_no_progress_when_actual_inventory_holds_first_head`: Actual held inventory request has held_requests=1, no successful HEAD/size/range work, no_progress_timeout and zero completed progress units. Scope: actual main-driven native watchdog case or explicitly constructed consumer helper; see preconditions. Assertion/call markers at lines 384, 385, 386, 388, 390, 391, 392, 393, 394, 395.
- [tests/stress_workload_watchdog.rs:450](../../tests/stress_workload_watchdog.rs#L450) `should_remain_healthy_when_actual_journal_restores_its_durable_frontier`: Actual manifest journal replay has exact 16 edits/frontier and successful delayed local reads, flat sampled database bytes and healthy workload. Scope: actual main-driven native watchdog case or explicitly constructed consumer helper; see preconditions. Assertion/call markers at lines 462, 463, 464, 465, 469, 470, 472, 473.

Preconditions: Custom native watchdog executable (main-driven functions, not ordinary #[test] filtering), internal-testing actual fixture and stress-soak; child logs can have recovery formatting off. Independent observation files outlive failed child.

Uncovered boundary: The 500ms partial emission happens only after successful positive bounded work; it is not a timer pulse. Completed work need not mean whole startup success. These are actual bounded fixtures, not full one-hour provider soak qualification.

## PROG-2

Production: [benches/bench_support/stress_scenarios/recovery_progress.rs:33](../../benches/bench_support/stress_scenarios/recovery_progress.rs#L33) `RecoveryScope::enter`; [benches/bench_support/stress_scenarios/recovery_progress.rs:103](../../benches/bench_support/stress_scenarios/recovery_progress.rs#L103) `RecoveryProgressLayer::on_event`.

Exact evidence and assertion markers:

- [tests/stress_workload_watchdog.rs:476](../../tests/stress_workload_watchdog.rs#L476) `should_count_recovery_work_only_within_its_active_caller_scope`: Actual subscriber child accepts exactly five intended positive units while unrelated/background, wrong schema, failure/empty, old generation and completed scopes remain uncredited. Scope: actual main-driven native watchdog case or explicitly constructed consumer helper; see preconditions. Assertion/call markers at lines 485, 490.
- [benches/bench_support/stress_scenarios/recovery_progress.rs:170](../../benches/bench_support/stress_scenarios/recovery_progress.rs#L170) `assert_scope_isolation`: Real layer/phase handle exercises scope generation changes and unrelated thread emission. Scope: actual main-driven native watchdog case or explicitly constructed consumer helper; see preconditions. Assertion/call markers at lines 177, 182, 188, 196, 213.

Preconditions: Subscriber assertion helper intentionally emits constructed schema events to test consumer filtering; actual producer workloads are separately proven at PROG-1. TLS guard is non-Send.

Uncovered boundary: This is a consumer isolation test, not proof that every engine worker thread transfers no context. Recovery listener's caller scope is separate from timed Engine tracing dispatcher inheritance; startup phase start/completion accounting is also separate (#721).

## FORMAT-1

Production: [src/metadata/format.rs:68](../../src/metadata/format.rs#L68) `validate_format_marker`; [src/wal/frame.rs:524](../../src/wal/frame.rs#L524) `next_frame`; [src/codec.rs:346](../../src/codec.rs#L346) `decompress_block_with_trailer`.

Exact evidence and assertion markers:

- [src/metadata/format.rs:279](../../src/metadata/format.rs#L279) `should_reject_open_given_future_format_version_when_starting`: Actual FORMAT=current+1 yields CompatibilityError. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 292.
- [tests/storage_layer.rs:1052](../../tests/storage_layer.rs#L1052) `should_reject_nonshipping_codec_codes_without_fallback`: Codec 4/5/255 bytes with correct trailer CRC yield Corruption/unknown-code outcome. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1068, 1069.
- [tests/storage_layer.rs:1076](../../tests/storage_layer.rs#L1076) `should_reject_corrupt_compressed_payload_for_every_shipping_codec`: Real LZ4/Zstd3/Zstd9 payload corruption with recomputed CRC still yields Corruption rather than raw fallback. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 1105.

Preconditions: Actual persisted marker and real compressed block encoders/decoders; deliberately valid CRC isolates semantic codec/payload failures.

Uncovered boundary: No blanket historical decoder: database FORMAT 3/4 and SST V4 policy applies; FORMAT 1/2 and SST <=3 are rejected. Fuzz/golden evidence is bounded, not all malformed-byte proof.

## FORMAT-2

Production: [src/engine/api/transaction.rs:433](../../src/engine/api/transaction.rs#L433) `Transaction::put`; [src/engine/api/transaction.rs:460](../../src/engine/api/transaction.rs#L460) `Transaction::insert`; [src/engine/api/transaction.rs:510](../../src/engine/api/transaction.rs#L510) `Transaction::delete_range`; [src/compaction/mod.rs:33](../../src/compaction/mod.rs#L33) `execute_compaction`.

Exact evidence and assertion markers:

- [tests/storage_layer.rs:279](../../tests/storage_layer.rs#L279) `should_reject_oversized_value_before_transaction_stages_it`: Actual oversized put/insert returns ResourceLimit, rejected key remains absent, valid staged value still commits/flushes/reads. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 297, 300, 308.
- [tests/storage_layer.rs:317](../../tests/storage_layer.rs#L317) `should_preserve_transaction_when_oversized_range_delete_is_rejected`: Oversized endpoint rejection preserves previously accepted middle value and successful later commit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 330, 331, 337.
- [src/compaction/mod.rs:397](../../src/compaction/mod.rs#L397) `should_compact_legacy_oversized_uncompressed_entry_without_losing_readability`: Genuine supported oversized raw V4 input compacts under sufficient pool through raw/LZ4/Zstd3 settings; output exact value, sequence 7 and expiration MAX survive and source remains. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 421, 434, 436, 440.

Preconditions: Real local public transactions and actual FsSST legacy raw encoding fixture; compaction explicitly supplies 1 GiB working budget.

Uncovered boundary: Supported historical raw data is distinct from oversized compressed blocks, which retain decoder limits. The test does not guarantee compaction admission under an insufficient pool or broaden format support.

## FORMAT-3

Production: [src/engine/verification.rs:58](../../src/engine/verification.rs#L58) `StorageVerifier::verify_path`; [src/engine/verification.rs:242](../../src/engine/verification.rs#L242) `verify_storage_path`; [src/metadata/format.rs:68](../../src/metadata/format.rs#L68) `validate_format_marker`.

Exact evidence and assertion markers:

- [tests/verification_cli.rs:39](../../tests/verification_cli.rs#L39) `should_emit_v1_json_given_healthy_database_when_midge_verify_runs`: Actual CLI healthy fixture exits 0, matches exact v1 golden JSON and empty stderr. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 50, 51, 52.
- [src/metadata/format.rs:241](../../src/metadata/format.rs#L241) `should_accept_version_three_marker_without_writing_when_validating`: Actual FORMAT 3 validation accepts without rewriting marker. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 254, 255.

Preconditions: Path-only real CLI fixture and explicit read-only marker validation; JSON schema has local scope independent of exit contract.

Uncovered boundary: The CLI golden does not itself compare full before/after filesystem inventory. No local path-only verifier can establish remote lease/catalog authority or provider deployment qualification; it does not repair storage.

## FORMAT-4

Production: [src/engine/api/options.rs:711](../../src/engine/api/options.rs#L711) `OpenOptionsBuilder::build`; [src/engine/api/options.rs:689](../../src/engine/api/options.rs#L689) `validate_open_timeout`; [src/engine/api/write_options.rs:149](../../src/engine/api/write_options.rs#L149) `effective_wal_durability_policy`; [src/common/error.rs:11](../../src/common/error.rs#L11) `MidgeError`.

Exact evidence and assertion markers:

- [src/engine/api/options/tests.rs:914](../../src/engine/api/options/tests.rs#L914) `should_reject_clock_skew_tolerance_larger_than_lease_ttl`: Invalid lease tolerance returns InvalidArgument before opening. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 924.
- [src/engine/api/write_options.rs:206](../../src/engine/api/write_options.rs#L206) `should_reject_sync_buffered_options_given_cloud_storage_when_committing`: Local-only sync/buffered policy mapping rejects cloud storage. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 214.
- [tests/engine_api/startup_deadline.rs:44](../../tests/engine_api/startup_deadline.rs#L44) `should_preserve_compatibility_error_when_timed_open_finds_unsupported_format`: Actual timed public open preserves unsupported-format CompatibilityError instead of deadline/error wrapping. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 58, 59.

Preconditions: Configuration builder and policy predicates plus real unsupported FORMAT public open; ordinary None and bounded Some remain distinct.

Uncovered boundary: Policy unit checks Err, not every exact public class. Other rows supply focused Fenced/Timeout/ResourceLimit/Corruption classes. This is not a complete provider-configuration matrix or every builder invalid combination.

## FORMAT-5

Production: [docs/development/support-matrix.md:1](../../docs/development/support-matrix.md#L1) `supported behavior matrix`; [docs/development/format-compatibility.md:1](../../docs/development/format-compatibility.md#L1) `0.x compatibility boundaries`; [src/metadata/format.rs:68](../../src/metadata/format.rs#L68) `validate_format_marker`.

Exact evidence and assertion markers:

- [tests/storage_layer.rs:947](../../tests/storage_layer.rs#L947) `should_preserve_baseline_compressed_block_fixture`: Shipping LZ4/Zstd3/Zstd9 encoded block fixture code/digest remain exact. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 980, 981.
- [tests/verification_cli.rs:39](../../tests/verification_cli.rs#L39) `should_emit_v1_json_given_healthy_database_when_midge_verify_runs`: Exact local CLI schema fixture is stable. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 50, 51, 52.
- [src/metadata/format.rs:279](../../src/metadata/format.rs#L279) `should_reject_open_given_future_format_version_when_starting`: Unsupported future marker remains CompatibilityError. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 292.

Preconditions: Pinned shipping fixtures/contracts provide narrow versioned evidence. Release evidence must name exact source revision and actual supported environment.

Uncovered boundary: No executable test establishes unlimited release/API/on-disk compatibility. #724 refreshed identified present-tense current-version claims and historical notice without relabeling old examples. The distinct #715 controlled local campaign 37274074026 at c8f0de80 is recorded in the guide/readback at ACCT-6; it does not extend format/API compatibility or supply the separate final fourteen-hour qualification.

## ACCT-1

Production: [src/metadata/accounting.rs:372](../../src/metadata/accounting.rs#L372) `MetricsHandle`; [src/metadata/accounting.rs:385](../../src/metadata/accounting.rs#L385) `Owner`; [src/runtime/state/accounting.rs:46](../../src/runtime/state/accounting.rs#L46) `checkpoint_metrics`.

Exact evidence and assertion markers:

- [tests/checkpoint_accounting.rs:36](../../tests/checkpoint_accounting.rs#L36) `should_release_actual_engine_lease_when_retained_checkpoint_metrics_outlive_shutdown`: Actual public local sync ACK of 32 values, exact points/ordered scan, public flush, both owned shutdowns and same-path reopen precede the original-owner stable snapshot and distinct owner identity assertions. Scope: actual public local Engine strict ACK, worker publication, same-path reopen and owned shutdown. Assertion/call markers at lines 75, 79, 87, 88, 89, 90, 91, 92.

Preconditions: Persistent local Engine with background compaction disabled and rows below the automatic threshold; genuine API acknowledgement and reacquisition, not a synthetic counter-owner lifetime check.

Uncovered boundary: This fixture proves the local retained-handle boundary. It is not native cloud deployment qualification or a proof that every unrelated diagnostic object retains no resources.

## ACCT-2

Production: [src/metadata/accounting.rs:20](../../src/metadata/accounting.rs#L20) `Origin`; [src/metadata/accounting.rs:62](../../src/metadata/accounting.rs#L62) `Medium`; [src/metadata/store.rs:81](../../src/metadata/store.rs#L81) `new_with_accounting`; [src/runtime/state/accounting.rs:50](../../src/runtime/state/accounting.rs#L50) `accept_flush_publication_attempt`; [src/engine/startup/storage.rs:538](../../src/engine/startup/storage.rs#L538) `bootstrap_staging_fs`; [src/runtime/event_loop/read_path.rs:87](../../src/runtime/event_loop/read_path.rs#L87) `publish_backfilled_bounds`.

Exact evidence and assertion markers:

- [src/runtime/event_loop/flush_pipeline/accounting_tests.rs:254](../../src/runtime/event_loop/flush_pipeline/accounting_tests.rs#L254) `should_preserve_original_publication_when_actual_retry_runs_during_shutdown`: Real destination-directory publication failure retains built output/name/origin/start; shutdown-drain retries the same immutable and credits two attempts, one failure and exactly one original-origin committed SST. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 276, 286, 287, 288, 289, 290, 296, 297, 298, 312, 313, 314, 322, 324, 325, 326, 327, 331, 338.
- [src/engine/tests/checkpoint_origins.rs:49](../../src/engine/tests/checkpoint_origins.rs#L49) `should_attribute_cloud_shutdown_flush_when_acknowledged_rows_remain_active`: 32 real filesystem-CloudSimulated cloud_strict ACK rows remain active until owned shutdown; exact same-path recovery and Shutdown credit 1 with CloudFlush/OrdinaryLocalFlush credit 0. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 63, 84, 85, 86, 87, 88, 89, 96, 103.
- [src/engine/tests/checkpoint_origins.rs:111](../../src/engine/tests/checkpoint_origins.rs#L111) `should_attribute_compaction_checkpoint_when_real_local_outputs_replace_inputs`: Two actual local sync-ACK flushes followed by public compact_all yield actual compaction count, exact reopened values and distinct CompactionBeforeGc checkpoint/SST credit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 150, 154, 158, 159, 160, 161, 168, 175, 182.
- [src/engine/startup/streaming_recovery/tests/checkpoint_accounting.rs:30](../../src/engine/startup/streaming_recovery/tests/checkpoint_accounting.rs#L30) `should_attribute_recovery_publication_when_real_wal_exceeds_local_capacity`: Actual encoded/cataloged WAL larger than configured local capacity forces multiple genuine recovery SST checkpoints and exact 384-row recovery/reopen; Recovery credit is present at open and ordinary/CloudFlush/Shutdown credit remains zero. Scope: actual Engine recovery from seeded encoded/cataloged WAL; no public ACK claim. Assertion/call markers at lines 42, 57, 61, 62, 66, 67, 68, 69, 75, 83, 84.

Preconditions: The recovery fixture seeds genuine encoded and cataloged WAL; it does not claim those rows were publicly acknowledged. CloudSimulated is filesystem-backed, not native S3/Azure/GCS. Independent bootstrap staging is MemoryOnly and outside the later exported runtime owner. Serialized origins are ordinary_local_flush, cloud_flush, recovery, bootstrap, ddl, compaction_before_gc, administration, shutdown and unclassified; media are persistent and memory_only.

Uncovered boundary: Explicit origin/medium boundaries do not account pre-state FORMAT creation, load/repair/hydration or independently owned bootstrap into the exported runtime counters. Those pre-state costs are unknown, not zero. Native cloud wire bytes and provider write cost are outside this accounting.

## ACCT-3

Production: [src/metadata/accounted_fs.rs:134](../../src/metadata/accounted_fs.rs#L134) `write_at`; [src/metadata/accounted_fs.rs:148](../../src/metadata/accounted_fs.rs#L148) `append`; [src/metadata/accounting.rs:570](../../src/metadata/accounting.rs#L570) `snapshot_durable`; [src/metadata/accounting.rs:580](../../src/metadata/accounting.rs#L580) `checkpoint_complete`; [src/metadata/store.rs:141](../../src/metadata/store.rs#L141) `save_snapshot_for`.

Exact evidence and assertion markers:

- [src/metadata/accounting/tests/fs_tests.rs:67](../../src/metadata/accounting/tests/fs_tests.rs#L67) `should_account_exact_frames_when_actual_single_and_batch_edits_commit`: Independent delegated RealFs oracle sees actual one-edit and atomic batch frames plus durability markers; actual journal replay and exact bytes precede issued/returned/durable producer equality. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named. Assertion/call markers at lines 103, 104, 105, 109, 111, 113, 118, 119, 120, 122, 123, 124, 125, 126, 127, 128, 134.
- [src/metadata/accounting/tests/fs_tests.rs:145](../../src/metadata/accounting/tests/fs_tests.rs#L145) `should_credit_completed_checkpoint_when_actual_snapshot_and_truncation_commit`: Actual serialized snapshot, rename/barriers and zero-length journal are strictly reloaded before durable/checkpoint-count and oracle-byte comparisons. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named. Assertion/call markers at lines 170, 171, 172, 176, 180, 181, 182, 184, 185, 186, 187, 188, 189, 190.
- [src/metadata/accounting/tests/fs_tests.rs:195](../../src/metadata/accounting/tests/fs_tests.rs#L195) `should_preserve_attempted_payload_when_actual_checkpoint_io_fails`: Actual open/write/rename errors preserve authoritative snapshot bytes and original typed failures; oracle-issued/returned bytes survive without snapshot-durable or checkpoint-complete credit. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named. Assertion/call markers at lines 226, 242, 244, 246, 255, 256, 257, 258, 259, 260.
- [src/metadata/accounting/tests/fs_tests.rs:266](../../src/metadata/accounting/tests/fs_tests.rs#L266) `should_preserve_returned_frames_when_actual_journal_sync_fails`: Both actual journal frames return, then required sync fails; the real retained prefix and original Io error precede returned bytes >0 and journal durable bytes 0. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named. Assertion/call markers at lines 294, 298, 299, 300, 301, 303, 304, 305, 306.
- [src/metadata/accounting/tests/integrity.rs:291](../../src/metadata/accounting/tests/integrity.rs#L291) `should_account_forced_checkpoint_failure_when_snapshot_commits_before_truncation`: Actual snapshot durability precedes real journal-truncation failure; snapshot durable count remains 1 while checkpoint complete is 0 and operation failure is recorded. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named. Assertion/call markers at lines 336, 341, 352, 353, 354, 355, 359, 360, 361, 362, 363, 364, 365, 366.

Preconditions: RealFs writes and real framed/serialized metadata are independently observed. Fault delegates preserve the requested actual operation/error boundary; their controlled failure does not model every device partial-write implementation.

Uncovered boundary: Filesystem-issued payload is not physical/device/FTL bytes, filesystem journaling, compression, delayed writeback or another process. Presence after failed sync is not crash durability; provider upload bodies/retries and provider-side writes are outside the byte gate.

## ACCT-4

Production: [src/runtime/state/accounting.rs:62](../../src/runtime/state/accounting.rs#L62) `finish_flush_publication_attempt`; [src/runtime/event_loop/flush_pipeline.rs:740](../../src/runtime/event_loop/flush_pipeline.rs#L740) `install_flush_publication`; [src/engine/startup/streaming_recovery.rs:290](../../src/engine/startup/streaming_recovery.rs#L290) `publish_checkpoint_output`.

Exact evidence and assertion markers:

- [src/runtime/event_loop/flush_pipeline/accounting_tests.rs:127](../../src/runtime/event_loop/flush_pipeline/accounting_tests.rs#L127) `should_credit_installed_sst_once_when_actual_mirror_receipt_is_replayed`: Actual Build/Publish/Mirror, exact installed SST row/size/sequence and immutable removal precede replay of the captured genuine Mirror receipt; the entire snapshot remains identical and committed SST count/bytes are credited once. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 137, 138, 146, 168, 173, 179, 180, 181, 182, 186, 187.
- [src/runtime/event_loop/flush_pipeline/accounting_tests.rs:254](../../src/runtime/event_loop/flush_pipeline/accounting_tests.rs#L254) `should_preserve_original_publication_when_actual_retry_runs_during_shutdown`: Actual failing Publish receipt and its duplicate fold one failed attempt; genuine shutdown retry preserves identity/logical start and installs exactly once. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 276, 286, 287, 288, 289, 290, 296, 297, 298, 312, 313, 314, 322, 324, 325, 326, 327, 331, 338.
- [src/runtime/event_loop/flush_pipeline/accounting_tests.rs:344](../../src/runtime/event_loop/flush_pipeline/accounting_tests.rs#L344) `should_not_count_publication_when_actual_actor_build_exhausts_its_memory`: Actual bounded-memory Build ResourceLimit preserves the genuine row and immutable without an accepted publication clock, publication attempts or committed SST credit. Scope: source/assertion evidence; exact fixture modes and assumptions stated in preconditions. Assertion/call markers at lines 356, 372, 373, 374, 375, 376, 386, 387, 388, 389, 390.

Preconditions: Actual actor/event-loop worker receipts and real SST bytes are prerequisites; duplicates replay captured genuine receipts. Publication-attempt totals also include rejected submit attempts, not only accepted workers.

Uncovered boundary: The full-publication denominator excludes SST build and admission time before first accepted publication; public flush_cf latency is separate. Recoverable per-attempt publication failures remain diagnostic; successful final logical installation and settled-owner integrity are still required.

## ACCT-5

Production: [src/metadata/accounting.rs:524](../../src/metadata/accounting.rs#L524) `observe_mutation`; [src/metadata/accounting.rs:584](../../src/metadata/accounting.rs#L584) `finish`; [src/metadata/accounting.rs:318](../../src/metadata/accounting.rs#L318) `Snapshot::delta`; [src/metadata/accounted_fs.rs:28](../../src/metadata/accounted_fs.rs#L28) `observe_mutation`.

Exact evidence and assertion markers:

- [src/metadata/accounting/tests/late_failed_payload.rs:115](../../src/metadata/accounting/tests/late_failed_payload.rs#L115) `should_detect_late_error_when_actual_append_finishes_after_operation_seals`: An actual positive-byte delegate is held before its original injected typed Err returns; late error completion invalidates the sealed owner with no successful returned-byte credit. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named. Assertion/call markers at lines 121, 87, 91, 92, 97, 99, 103, 104, 105, 106, 107, 108.
- [src/metadata/accounting/tests/late_failed_payload.rs:125](../../src/metadata/accounting/tests/late_failed_payload.rs#L125) `should_detect_late_error_when_actual_write_at_finishes_after_operation_seals`: The same held genuine positional payload checks the escaped completion flag and preserves the failure class. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named. Assertion/call markers at lines 131, 87, 91, 92, 97, 99, 103, 104, 105, 106, 107, 108.
- [src/metadata/accounting/tests/late_failed_payload.rs:135](../../src/metadata/accounting/tests/late_failed_payload.rs#L135) `should_preserve_failure_without_late_flag_when_actual_append_finishes_before_sealing`: Releasing/joining the actual failed delegate while its operation is live records failure/issued bytes without a spurious late flag. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named. Assertion/call markers at lines 141, 87, 91, 92, 97, 99, 103, 104, 105, 106, 107, 108.
- [src/metadata/accounting/tests/integrity.rs:235](../../src/metadata/accounting/tests/integrity.rs#L235) `should_detect_late_mutation_when_a_persistent_file_truncates_without_payload`: Actual non-payload file truncation after operation sealing marks escaped accounting. Scope: actual delegated filesystem/accounting lifecycle; controlled errors where named. Assertion/call markers at lines 263, 264, 265, 266, 277, 281, 282, 284, 285, 286.
- [src/metadata/accounting/tests/bounded.rs:61](../../src/metadata/accounting/tests/bounded.rs#L61) `should_subtract_histogram_buckets_when_warmup_precedes_measurement`: Constructed histogram recording control subtracts a slow warmup bin and retains exactly 100 measured observations with p95 below 5ms. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 64, 69, 72, 77, 78.
- [src/metadata/accounting/tests/bounded.rs:105](../../src/metadata/accounting/tests/bounded.rs#L105) `should_retain_late_invalidation_when_sealed_observation_counter_is_saturated`: Constructed private-state boundary deliberately seeds the late counter to u64::MAX, then invokes the real sealed-ledger mutation observation. Saturation remains sticky, delta stays invalid and no issued/returned payload is fabricated; this is arithmetic/invalidation evidence, not an actual u64::MAX workload. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 120, 121, 128, 129, 131, 132, 133, 134.

Preconditions: Actual held/delegated Fs cases prove wrapper lifecycle boundaries. Histogram values and the private u64::MAX seed are intentionally constructed; those controls prove arithmetic and sticky invalidation only, not actual checkpoint cost, device I/O, accumulated escaped filesystem mutations or hosted measurement.

Uncovered boundary: The lightweight reference validator checks symbols/assertion markers, not the semantic adequacy of every failure schedule. Operation-ledger completeness and active_operations gauge do not mean all runtime/background work has finished.

## ACCT-6

Production: [benches/bench_support/checkpoint_gate.rs:17](../../benches/bench_support/checkpoint_gate.rs#L17) `evaluate`; [benches/bench_support/checkpoint_gate.rs:128](../../benches/bench_support/checkpoint_gate.rs#L128) `settlement_invalid_reasons`; [examples/checkpoint_campaign_readback/accounting.rs:184](../../examples/checkpoint_campaign_readback/accounting.rs#L184) `persistent_integrity`; [examples/checkpoint_campaign_readback/campaign.rs:379](../../examples/checkpoint_campaign_readback/campaign.rs#L379) `evaluate`; [examples/checkpoint_campaign_readback/campaign.rs:432](../../examples/checkpoint_campaign_readback/campaign.rs#L432) `evaluate_construction_smoke`; [benches/tier4_system_checkpoint_write_amplification.rs:374](../../benches/tier4_system_checkpoint_write_amplification.rs#L374) `finish_window`; [examples/checkpoint_campaign_readback/commit_backpressure.rs:86](../../examples/checkpoint_campaign_readback/commit_backpressure.rs#L86) `qualify`; [examples/checkpoint_campaign_readback/commit_backpressure.rs:121](../../examples/checkpoint_campaign_readback/commit_backpressure.rs#L121) `check_native_no_progress`; [docs/development/checkpoint-write-amplification.md:258](../../docs/development/checkpoint-write-amplification.md#L258) `## Recorded hosted outcome`; [docs/development/performance-targets.md:141](../../docs/development/performance-targets.md#L141) `### Recorded checkpoint campaign`; [docs/development/evidence/checkpoint-715-readback.json:24](../../docs/development/evidence/checkpoint-715-readback.json#L24) `"conditional_policy_measurement_predicate_met": true,`; [benches/bench_support/checkpoint_boundary.rs:58](../../benches/bench_support/checkpoint_boundary.rs#L58) `capture_metadata_boundary`; [benches/tier4_system_checkpoint_write_amplification.rs:322](../../benches/tier4_system_checkpoint_write_amplification.rs#L322) `begin_window`; [benches/tier4_system_checkpoint_write_amplification.rs:294](../../benches/tier4_system_checkpoint_write_amplification.rs#L294) `capture_boundary`; [examples/checkpoint_campaign_readback/metadata_boundary.rs:88](../../examples/checkpoint_campaign_readback/metadata_boundary.rs#L88) `qualify`.

Exact evidence and assertion markers:

- [examples/checkpoint_campaign_readback/contract_tests.rs:37](../../examples/checkpoint_campaign_readback/contract_tests.rs#L37) `should_retain_all_nine_planned_rows_when_campaign_evidence_is_missing`: Constructed missing-evidence readback retains nine invalid planned rows and cannot qualify any cell. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 43, 44, 49, 53.
- [examples/checkpoint_campaign_readback/contract_tests.rs:191](../../examples/checkpoint_campaign_readback/contract_tests.rs#L191) `should_report_exact_five_ms_histogram_boundary_when_p95_rank_reaches_the_later_bin`: Constructed histogram boundary control reports the exact p95 interval; no actual checkpoint latency claim. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 199.
- [examples/checkpoint_campaign_readback/contract_tests.rs:316](../../examples/checkpoint_campaign_readback/contract_tests.rs#L316) `should_reject_final_owner_when_forced_checkpoint_fails_after_valid_measured_window`: Constructed post-window failure invalidates the actual reader final-owner gate rather than entering the ordinary cost ratio. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 330, 333.
- [examples/checkpoint_campaign_readback/contract_tests.rs:408](../../examples/checkpoint_campaign_readback/contract_tests.rs#L408) `should_check_both_final_owner_files_when_constructed_later_forced_work_fails`: Actual qualifier reads each constructed final filename mutation and rejects later forced failure in either original or reopened owner. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 412, 426.
- [examples/checkpoint_campaign_readback/contract_tests.rs:57](../../examples/checkpoint_campaign_readback/contract_tests.rs#L57) `should_refuse_download_binding_when_destination_already_contains_unbound_files`: Constructed hosted capture plus actual old local bytes refuses fresh provenance without replacing the retained file. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 67, 68, 69.
- [examples/checkpoint_campaign_readback/contract_tests.rs:73](../../examples/checkpoint_campaign_readback/contract_tests.rs#L73) `should_preserve_originating_provenance_when_readback_requests_another_run`: Constructed run identity mismatch leaves the actual originating receipt byte-identical. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 85, 86.
- [examples/checkpoint_campaign_readback/contract_tests.rs:90](../../examples/checkpoint_campaign_readback/contract_tests.rs#L90) `should_refuse_incomplete_pagination_when_hosted_artifact_capture_omits_rows`: Constructed REST total-count/capture mismatch is rejected as incomplete pagination; no actual Actions pagination is performed. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 101.
- [examples/checkpoint_campaign_readback/contract_tests.rs:105](../../examples/checkpoint_campaign_readback/contract_tests.rs#L105) `should_reject_hosted_capture_drift_without_replacing_pending_download_evidence`: Constructed REST capture changes after prepare; real seal refuses it and preserves the original pending receipt bytes. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 119, 122.
- [examples/checkpoint_campaign_readback/contract_tests.rs:132](../../examples/checkpoint_campaign_readback/contract_tests.rs#L132) `should_ignore_latest_alias_when_binding_exact_native_timestamp_and_pid`: Constructed canonical receipt and alias exercise exact timestamp/PID matching without fabricated completed workload. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 142, 143.
- [examples/checkpoint_campaign_readback/contract_tests.rs:147](../../examples/checkpoint_campaign_readback/contract_tests.rs#L147) `should_reject_two_distinct_canonical_receipts_when_both_match_one_process_identity`: Two constructed native-shape canonical receipts with one PID/source/selector are rejected as ambiguous. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 159.
- [examples/checkpoint_campaign_readback/contract_tests.rs:163](../../examples/checkpoint_campaign_readback/contract_tests.rs#L163) `should_preserve_native_typed_failure_when_diagnostic_receipt_contains_no_progress`: Constructed native failure metadata preserves no_progress and partial counters independently of successful work. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 171, 172, 173.
- [examples/checkpoint_campaign_readback/contract_tests.rs:177](../../examples/checkpoint_campaign_readback/contract_tests.rs#L177) `should_skip_malformed_nested_native_metadata_without_panicking`: Malformed constructed nested metadata and ambiguous selector flags are refused without panic. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 184, 185.
- [examples/checkpoint_campaign_readback/contract_tests.rs:258](../../examples/checkpoint_campaign_readback/contract_tests.rs#L258) `should_apply_exact_snapshot_threshold_when_fixed_policy_counters_cross_five_percent`: Constructed accounting counters on both sides of the exact 5% policy boundary exercise rational comparison; no measured checkpoint-byte claim. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 267, 268, 269.
- [examples/checkpoint_campaign_readback/contract_tests.rs:273](../../examples/checkpoint_campaign_readback/contract_tests.rs#L273) `should_reject_forced_checkpoint_failure_when_ordinary_policy_denominators_are_valid`: Constructed failed forced-origin metadata invalidates persistence integrity while the original ordinary ratio remains valid. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 276, 281.
- [examples/checkpoint_campaign_readback/contract_tests.rs:287](../../examples/checkpoint_campaign_readback/contract_tests.rs#L287) `should_reject_original_owner_delta_when_histogram_or_counters_decrease`: Constructed cumulative histogram decrease is refused instead of being interpreted as a valid same-owner measured delta. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 295.
- [examples/checkpoint_campaign_readback/contract_tests.rs:299](../../examples/checkpoint_campaign_readback/contract_tests.rs#L299) `should_retain_nine_unqualified_rows_when_transport_smoke_has_no_native_evidence`: Missing native construction evidence retains all nine unqualified rows and cannot accept a cadence or campaign predicate. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 305, 306, 307, 308, 312.
- [examples/checkpoint_campaign_readback/contract_tests.rs:340](../../examples/checkpoint_campaign_readback/contract_tests.rs#L340) `should_reject_final_owner_when_persistent_publication_histogram_omits_completed_flush`: Constructed completed-flush counter with missing full-publication histogram rejects the final owner integrity gate. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 347.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:30](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L30) `should_retain_stall_diagnostics_when_constructed_strict_successes_match_fixed_cycles`: Constructed completed-prefix records preserve measured strict-success/stall diagnostics while declaring that wait-clear is not ACK and JSON does not verify executed waiter bodies. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 36, 37, 38, 39.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:43](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L43) `should_reject_missing_or_altered_attempts_when_constructed_backpressure_endpoint_is_checked`: Constructed missing or altered attempts cannot default to zero or escape successes-plus-commit-stalls equality. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 51, 56.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:60](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L60) `should_reject_warmup_counter_reset_when_constructed_measured_endpoint_appears_complete`: Constructed erased warmup successes invalidate the otherwise plausible measured endpoint. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 65.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:69](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L69) `should_reject_waiter_clear_as_ack_when_constructed_commit_counters_are_incomplete`: Constructed waiter-clear credit cannot replace one of the fixed actual-strict-success denominators. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 75.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:79](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L79) `should_reject_excluded_wait_time_when_constructed_measured_clock_is_shorter_than_wait_delta`: Constructed positive wait elapsed greater than the supplied measured window is rejected; the coarse JSON inequality does not prove a real waiter body. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 84.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:88](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L88) `should_reject_changed_budget_or_native_watchdog_when_constructed_policy_is_checked`: Constructed >30s retry policy and altered native print-config watchdog are refused; the declared 60s native watchdog text is separately parsed. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 93, 94, 98.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:105](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L105) `should_reject_waiter_calls_when_constructed_cumulative_endpoint_has_no_commit_rejections`: #730 constructed cumulative zero-commit-rejection endpoint cannot fabricate waiter calls through waiter-route rejection counters. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 115.
- [examples/checkpoint_campaign_readback/commit_backpressure/tests.rs:119](../../examples/checkpoint_campaign_readback/commit_backpressure/tests.rs#L119) `should_reject_waiter_calls_when_constructed_measured_delta_has_no_new_commit_rejections`: #730 constructed individually coherent completed prefixes cannot add measured waiter calls/time without a new measured commit rejection; no waiter spans the completed warmup boundary. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 129.
- [tests/checkpoint_boundary.rs:55](../../tests/checkpoint_boundary.rs#L55) `should_capture_real_engine_after_scripted_active_metadata_boundary`: First active gauge is explicitly scripted, then the same benchmark helper performs an actual public Engine runtime query followed by its actual same-owner idle snapshot. Independently real local strict 32 ACK, flush, owned shutdown, same-path reopen and exact scan/values are required; this does not hold a real metadata operation or qualify native settlement. Scope: scripted first active gauge followed by actual public local Engine query/snapshot, strict ACK, flush, owned shutdown and same-path reopen; no actual concurrent metadata-settlement claim. Assertion/call markers at lines 119, 120, 121, 122, 123, 124, 125, 126, 127.
- [tests/checkpoint_boundary.rs:131](../../tests/checkpoint_boundary.rs#L131) `should_admit_no_sample_when_original_cell_deadline_is_expired`: Constructed already-expired original cell clock admits neither query nor pause and cannot complete a boundary. Scope: constructed clock/activity/error capture policy and source-clock formulas; no native or wall-clock settlement proof. Assertion/call markers at lines 150, 151, 152, 153.
- [tests/checkpoint_boundary.rs:157](../../tests/checkpoint_boundary.rs#L157) `should_end_permanent_active_metadata_when_original_boundary_allowance_expires`: Constructed perpetual metadata activity consumes the captured three-ms allowance in one-ms requested pauses without resetting the clock or fabricating idle. Scope: constructed clock/activity/error capture policy and source-clock formulas; no native or wall-clock settlement proof. Assertion/call markers at lines 172, 173, 177, 178, 179, 180, 181.
- [tests/checkpoint_boundary.rs:185](../../tests/checkpoint_boundary.rs#L185) `should_cap_sampling_allowance_when_requested_boundary_budget_exceeds_thirty_seconds`: Constructed costly active queries retain decreasing remaining allowances and stop exactly at the captured 30s cap; no syscall-preemption or native wall-clock claim. Scope: constructed clock/activity/error capture policy and source-clock formulas; no native or wall-clock settlement proof. Assertion/call markers at lines 205, 206, 207, 208, 209, 210.
- [tests/checkpoint_boundary.rs:214](../../tests/checkpoint_boundary.rs#L214) `should_clamp_boundary_sampling_when_original_cell_remainder_is_shorter`: Constructed 250us cell remainder clamps both query allowance and requested pause rather than being replaced by a fresh boundary deadline. Scope: constructed clock/activity/error capture policy and source-clock formulas; no native or wall-clock settlement proof. Assertion/call markers at lines 237, 238, 239, 240.
- [tests/checkpoint_boundary.rs:244](../../tests/checkpoint_boundary.rs#L244) `should_reject_late_idle_sample_when_query_crosses_the_original_deadline`: Constructed successful zero-active sample arriving after the original deadline is refused before complete=true, without a following pause. Scope: constructed clock/activity/error capture policy and source-clock formulas; no native or wall-clock settlement proof. Assertion/call markers at lines 263, 264, 265, 266, 267.
- [tests/checkpoint_boundary.rs:271](../../tests/checkpoint_boundary.rs#L271) `should_preserve_terminal_query_error_when_boundary_cannot_be_observed`: Constructed proven Fenced query outcome remains the original terminal type/detail, with no retry or idle completion. Scope: constructed clock/activity/error capture policy and source-clock formulas; no native or wall-clock settlement proof. Assertion/call markers at lines 290, 293, 294, 295.
- [tests/checkpoint_boundary.rs:299](../../tests/checkpoint_boundary.rs#L299) `should_capture_persistent_boundary_when_only_memory_metadata_is_active`: Constructed MemoryOnly activity is allowed while the Persistent gauge is zero; metadata capture does not assert global runtime idleness. Scope: constructed clock/activity/error capture policy and source-clock formulas; no native or wall-clock settlement proof. Assertion/call markers at lines 314, 315, 316, 317, 318.
- [tests/checkpoint_boundary.rs:347](../../tests/checkpoint_boundary.rs#L347) `should_return_final_query_clock_when_warmup_candidates_precede_measured_ingestion`: Constructed clock uses the shared helper and actual caller clock formulas: prior warmup 6ms excluded, accepted final query 2ms included, whole end 8ms included, measured 10ms. This is arithmetic/source-placement evidence, not executed wall-clock metadata settlement. Scope: constructed clock/activity/error capture policy and source-clock formulas; no native or wall-clock settlement proof. Assertion/call markers at lines 362, 363, 364, 368, 369, 370, 371, 372, 373.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:106](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L106) `should_retain_busy_warmup_diagnostics_when_constructed_metadata_endpoints_are_idle`: Constructed parser evidence permits 7s warmup capture beside 6s measured ingestion, actual-shaped scheduler pause 3ms above requested 2ms and MemoryOnly activity; it does not qualify a campaign or execute a waiter. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 113.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:120](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L120) `should_reject_missing_boundary_evidence_when_constructed_observations_are_checked`: Constructed missing endpoint observation is not an implicit idle/complete record. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 131.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:135](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L135) `should_reject_changed_boundary_policy_when_constructed_clock_scope_or_budget_differs`: Constructed altered>30s cap or excluded end-clock declaration is rejected under the fixed original 900s cell policy. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 140, 145.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:149](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L149) `should_reject_boundary_sample_arithmetic_when_constructed_busy_polls_are_omitted`: Constructed sample omission cannot satisfy successful samples=active+1 and pauses=active. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 156.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:160](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L160) `should_reject_excluded_end_capture_when_constructed_elapsed_exceeds_measured_interval`: Constructed end capture larger than actual-shaped measured elapsed is refused; warmup elapsed remains separately excluded. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 171.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:175](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L175) `should_reject_fake_idle_boundary_when_constructed_selected_snapshot_has_persistent_work`: Constructed declared idle observation contradicting the chosen raw Persistent-active snapshot is rejected; later settled shutdown cannot replace the selected endpoint. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 184.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:188](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L188) `should_reject_boundary_copy_drift_when_constructed_final_status_differs_from_ingestion`: Constructed status/ingestion endpoint drift cannot substitute a later observation for the selected capture. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 195.
- [examples/checkpoint_campaign_readback/campaign/boundary_tests.rs:199](../../examples/checkpoint_campaign_readback/campaign/boundary_tests.rs#L199) `should_reject_expired_warmup_capture_when_constructed_elapsed_equals_original_allowance`: Constructed complete warmup capture exactly at the 30s allowance is rejected, matching producer late-idle admission; this parser arithmetic control is not a native expired-query run. Scope: constructed recording/readback contract; no workload execution claim. Assertion/call markers at lines 210.

Preconditions: All 33 linked reader controls intentionally construct JSON evidence/counters and sometimes real temporary-file reads; they do not execute hosted A/B/C, waiter bodies, device I/O or full-hour qualification. The two #730 zero-rejection cases and #731 exact-expiry parser case are constructed reachability contracts. Shared commit controls at RES-3 and nine capture controls here separate scripted rejection/gauge plus actual local ACK/reopen from pure constructed clocks/errors. Before/end capture uses actual timed runtime query then same-owner snapshot; it requires only zero Persistent metadata activity, not global compute/GC/publication idleness. The actual workload separately requires strict ACK, flush, compaction, exact verification, reopen and both owned shutdowns.

Uncovered boundary: Initial campaign 37268459342 at c8983928 remains invalid after the three A repeats terminated on genuine L0 WriteStall; six other rows cannot qualify it. Historical smoke 37270512400 validates A/r1 transport/construction only at 32911805: 256 commits, 230 measured, 100 compactions, zero stalls/waits and retained too_few_samples diagnostics. Historical nine-run 37270845568 at 32911805 remains invalid: A/r1 and A/r2 verified all data but selected active CompactionBeforeGc metadata endpoints, despite later settled owners; seven other valid rows cannot repair it. Measured c8f0de80 smoke 37273556125 is transport/construction-valid; all nine fresh attempts of 37274074026 are valid, totaling 1,376,256 real strict ACK rows, exact point/ordered scans before and after reopen and two owned shutdowns per attempt. B/C each miss the snapshot-payload target in three repeats (13.023–13.534% / 80.261–80.262%); A has one time miss and does not qualify. A/r1 end and B/r1/B/r3 warmup captured real active candidates then selected idle, without weakening the gate. This satisfies only the conditional policy-investigation predicate. Cadence remains unchanged, pre-state costs are unknown/excluded, and no physical-device, native confidence or final fourteen-hour qualification follows. Adjacent samples are not global barriers and JSON cannot prove executed body/clock. Final fourteen hours use the final merged #711 source.
