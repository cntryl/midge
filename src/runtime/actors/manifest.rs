//! Manifest Actor - handles metadata persistence
//!
//! Responsible for:
//! - Adding SST files to manifest
//! - Updating manifest after compaction
//! - Persisting manifest to disk
//! - Managing version edits

use super::super::state::RuntimeState;
use super::super::FileMeta;
use crate::common::MidgeResult;
#[cfg(test)]
use crate::types::EntryType;

/// Actor handling manifest operations
pub struct ManifestActor {
    /// Number of pending edits
    pending_edits: usize,
}

impl ManifestActor {
    pub fn new() -> Self {
        Self { pending_edits: 0 }
    }

    /// Add a new SST file to the manifest
    #[cfg(test)]
    pub fn add_sst(&mut self, state: &mut RuntimeState, file_meta: FileMeta) -> MidgeResult<()> {
        // Validate SST file exists and is readable (defensive: avoid manifest pointing at corrupt file)
        if !state.is_memory_mode() {
            let sst_path = state.sst_dir.join(&file_meta.name);
            if !sst_path.exists() {
                return Err(crate::common::MidgeError::Internal(format!(
                    "SST file '{}' not found in sst dir",
                    file_meta.name
                )));
            }

            // Try opening the SST to validate footer/format correctness
            if let Err(e) = crate::sst::fs::SstFileIo::open_with_real_fs(&sst_path) {
                return Err(crate::common::MidgeError::Corruption(format!(
                    "SST file '{}' failed validation: {}",
                    file_meta.name, e
                )));
            }
        }

        // Append to manifest journal (durable edit log) - skip in memory mode
        let mut journaled_id = None;
        if !state.is_memory_mode() {
            let edit = crate::metadata::ManifestEdit::AddSst((&file_meta).into());
            crate::failpoints::fail_point!(
                "midge::manifest::inject_no_space_on_add_sst_edit",
                |_| Err(crate::common::MidgeError::NoSpace(
                    "failpoint: no space on manifest add_sst append".to_string()
                ))
            );
            journaled_id = Some(state.manifest_store.append(&edit)?);
        }

        // Now that intent is durable, apply mutation to in-memory manifest
        // Convert to manifest FileMeta
        let manifest_meta = (&file_meta).into();

        state.manifest.add_file(manifest_meta);
        if let Some(edit_id) = journaled_id {
            state.manifest.note_applied_journal_edit(edit_id);
        }
        self.pending_edits += 1;

        tracing::info!(
            sst_name = %file_meta.name,
            level = file_meta.level,
            cf_id = file_meta.cf_id,
            "Manifest: added SST with durability guarantee"
        );

        Ok(())
    }

    /// Update manifest after compaction completes
    pub fn compaction_complete(
        &mut self,
        state: &mut RuntimeState,
        removed: &[String],
        added: &[FileMeta],
    ) -> MidgeResult<()> {
        // Append compaction edits to manifest journal as a single batch (reduces fixed overhead)
        let mut edits = Vec::with_capacity(removed.len() + added.len());
        for n in removed {
            edits.push(crate::metadata::ManifestEdit::RemoveSst { name: n.clone() });
        }
        for f in added {
            edits.push(crate::metadata::ManifestEdit::AddSst(f.into()));
        }
        let mut journaled_id = None;
        if !edits.is_empty() {
            crate::failpoints::fail_point!(
                "midge::manifest::inject_no_space_on_compaction_batch_edit",
                |_| Err(crate::common::MidgeError::NoSpace(
                    "failpoint: no space on manifest compaction batch append".to_string()
                ))
            );
            journaled_id = Some(state.manifest_store.append_batch(&edits)?);
        }

        // Now that intent is durable, apply mutations to in-memory manifest
        // Remove old files
        state.manifest.files.retain(|f| !removed.contains(&f.name));

        // Add new files
        for file_meta in added {
            let manifest_meta = file_meta.into();
            state.manifest.add_file(manifest_meta);
        }
        if let Some(edit_id) = journaled_id {
            state.manifest.note_applied_journal_edit(edit_id);
        }

        self.pending_edits += 1;

        tracing::info!(
            removed_count = removed.len(),
            added_count = added.len(),
            "Manifest: compaction complete with durability guarantee"
        );

        Ok(())
    }

    /// Persist manifest to disk
    pub fn persist(state: &mut RuntimeState) -> MidgeResult<()> {
        // Skip persistence in memory mode
        if state.is_memory_mode() {
            tracing::debug!("Manifest: skipping persistence in memory mode");
            return Ok(());
        }
        crate::failpoints::fail_point!("midge::manifest::persist", |_| Err(
            crate::common::MidgeError::Internal("injected manifest persist failure".into())
        ));
        // A persist is itself a chance to lift a failed-reload fence (#500).
        state.retry_metadata_reload()?;

        tracing::info!(
            file_count = state.manifest.files.len(),
            cf_count = state.manifest.column_families.len(),
            "Manifest: persisting"
        );

        // Publish a crash-safe snapshot before truncating the journal. Recovery
        // can therefore replay only edits newer than the checkpoint horizon.
        state
            .manifest_store
            .save_snapshot(&state.manifest)?
            .adopt_into(&mut state.manifest);

        tracing::debug!("Manifest persisted");

        Ok(())
    }

    /// Create a column family through the same validation/edit path as the
    /// runtime coordinator. This direct helper is retained only for focused
    /// event-loop tests that do not run the DDL message protocol.
    #[cfg(test)]
    pub fn create_column_family(
        &mut self,
        state: &mut RuntimeState,
        name: &str,
    ) -> MidgeResult<u32> {
        crate::runtime::ddl::validate_column_family_name(name)?;
        if let Some(existing) = state.manifest.get_column_family_by_name(name) {
            return Ok(existing.id);
        }
        let edit = crate::runtime::ddl::create_edit(state, name)?;
        let crate::metadata::ManifestEdit::CreateColumnFamily { id, .. } = &edit else {
            unreachable!("create_edit returned a non-create edit");
        };
        let cf_id = *id;
        crate::runtime::ddl::apply_local_edit(state, &edit)?;
        self.pending_edits = self.pending_edits.saturating_add(1);
        Ok(cf_id)
    }

    /// Safely drop an empty or flushed column family for focused event-loop
    /// tests. Production callers use the serialized DDL message path.
    #[cfg(test)]
    pub fn drop_column_family(
        &mut self,
        state: &mut RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
    ) -> MidgeResult<()> {
        let edit = crate::runtime::ddl::drop_edit(state, cf_id, false)?;
        crate::runtime::ddl::apply_local_edit(state, &edit)?;
        self.pending_edits = self.pending_edits.saturating_add(1);
        Ok(())
    }
}

impl Default for ManifestActor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::traits::SstFactory;

    #[test]
    fn should_initialize_manifest_actor_with_zero_pending_edits() {
        // Arrange
        // (no setup needed)

        // Act
        let actor = ManifestActor::new();

        // Assert
        assert_eq!(actor.pending_edits, 0);
    }

    fn memory_file_meta(name: &str) -> crate::runtime::FileMeta {
        crate::runtime::FileMeta {
            name: name.to_string(),
            level: 0,
            size_bytes: 0,
            content_crc32c: None,
            cf_id: 0,
            smallest_key: None,
            largest_key: None,
            smallest_seq: None,
            largest_seq: None,
            key_bounds_complete: false,
        }
    }

    #[test]
    fn should_increment_pending_edits_on_add_sst() {
        // Arrange - memory mode skips on-disk SST validation, so add_sst runs
        // its real bookkeeping without needing a file on disk
        let mut state = crate::runtime::state::RuntimeState::new("/tmp/test_midge_mf".into(), true);
        let mut actor = ManifestActor::new();
        assert_eq!(actor.pending_edits, 0);

        // Act - the real handler, not a direct field write
        actor
            .add_sst(&mut state, memory_file_meta("a.sst"))
            .expect("add_sst should succeed in memory mode");

        // Assert
        assert_eq!(actor.pending_edits, 1);
        assert_eq!(state.manifest.files.len(), 1);
    }

    #[test]
    fn should_accumulate_pending_edits_across_add_sst_calls() {
        // Arrange
        let mut state = crate::runtime::state::RuntimeState::new("/tmp/test_midge_mf".into(), true);
        let mut actor = ManifestActor::new();

        // Act - three real add_sst calls
        actor
            .add_sst(&mut state, memory_file_meta("a.sst"))
            .expect("add a.sst");
        actor
            .add_sst(&mut state, memory_file_meta("b.sst"))
            .expect("add b.sst");
        actor
            .add_sst(&mut state, memory_file_meta("c.sst"))
            .expect("add c.sst");

        // Assert
        assert_eq!(actor.pending_edits, 3);
        assert_eq!(state.manifest.files.len(), 3);
    }

    fn sst_meta(sequence: u64) -> crate::runtime::FileMeta {
        crate::runtime::FileMeta {
            name: crate::cloud_layout::file_name(0, 0, sequence),
            ..memory_file_meta("")
        }
    }

    fn read_snapshot(state: &crate::runtime::state::RuntimeState) -> crate::metadata::Manifest {
        let bytes = std::fs::read(state.db_path.join("manifest.snapshot.json"))
            .expect("snapshot should exist after persist");
        serde_json::from_slice(&bytes).expect("snapshot should parse")
    }

    #[derive(Clone, Default)]
    struct LogSink(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl std::io::Write for LogSink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn should_not_treat_runtime_manifest_as_stale_when_persisting_twice_in_one_session() {
        // Arrange
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let mut state = crate::runtime::state::RuntimeState::new(tmp.path().to_path_buf(), false);
        let mut actor = ManifestActor::new();
        let sink = LogSink::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::WARN)
            .with_writer(sink.clone())
            .finish();

        // Act
        let mut persisted_checkpoints = Vec::new();
        tracing::subscriber::with_default(subscriber, || {
            for sequence in [1, 2] {
                actor
                    .compaction_complete(&mut state, &[], &[sst_meta(sequence)])
                    .expect("compaction edit");
                ManifestActor::persist(&mut state).expect("persist");
                persisted_checkpoints.push(read_snapshot(&state).edit_checkpoint_id);
            }
        });

        // Assert
        assert_eq!(
            persisted_checkpoints,
            vec![1, 2],
            "each snapshot must record the edit the runtime just journaled"
        );
        assert_eq!(
            state.manifest.edit_checkpoint_id, 2,
            "the runtime manifest must track its own journal appends"
        );
        let logs = String::from_utf8(
            sink.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
        .unwrap();
        assert!(
            !logs.contains("stale caller"),
            "routine persists must not take the stale-caller branch: {logs}"
        );
    }

    #[test]
    fn should_keep_foreign_journal_edit_when_runtime_persists_after_racing_append() {
        // Arrange: a flush publish journals an edit from another thread that
        // the runtime manifest has not applied yet, then the runtime journals
        // and applies its own edit (a backfill) and persists.
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let mut state = crate::runtime::state::RuntimeState::new(tmp.path().to_path_buf(), false);
        let mut actor = ManifestActor::new();
        actor
            .compaction_complete(&mut state, &[], &[sst_meta(1)])
            .expect("first local edit");
        let flushed = sst_meta(2);
        crate::metadata::append_edit(
            &state.db_path,
            &crate::metadata::ManifestEdit::AddSst(crate::metadata::FileMeta {
                name: flushed.name.clone(),
                ..Default::default()
            }),
        )
        .expect("racing flush append");

        // Act
        actor
            .compaction_complete(&mut state, &[], &[sst_meta(3)])
            .expect("backfill-style local edit");
        ManifestActor::persist(&mut state).expect("persist");

        // Assert: the snapshot may not skip the edit this manifest never saw
        let snapshot = read_snapshot(&state);
        let names: Vec<_> = snapshot.files.iter().map(|f| f.name.clone()).collect();
        assert!(
            names.contains(&flushed.name),
            "racing flush edit was lost by the snapshot: {names:?}"
        );
        assert_eq!(names.len(), 3, "unexpected snapshot files: {names:?}");
        let reloaded = crate::metadata::ManifestPersistence::load(&state.db_path).expect("reload");
        assert_eq!(reloaded.files.len(), 3);
    }

    #[test]
    fn should_fail_to_add_sst_given_corrupted_file_when_validating() {
        // Arrange: create a temp dir and a corrupt SST file (partial content)
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let sst_name = crate::cloud_layout::file_name(0, 0, 1);
        let sst_path = tmp.path().join(&sst_name);

        // Write corrupted content (not a valid SST)
        std::fs::write(&sst_path, b"incomplete-sst-bytes").expect("write corrupted sst");

        // Build a FileMeta that references the corrupted file
        let file_meta = crate::runtime::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: 0,
            content_crc32c: None,
            cf_id: 0,
            smallest_key: None,
            largest_key: None,
            smallest_seq: None,
            largest_seq: None,
            key_bounds_complete: false,
        };

        let mut state = crate::runtime::state::RuntimeState::new(tmp.path().to_path_buf(), false);
        let mut actor = ManifestActor::new();

        // Act: attempt to add the SST to manifest
        let result = actor.add_sst(&mut state, file_meta);

        // Assert: adding a manifest entry for a corrupt/unreadable SST MUST fail
        // (current behavior is to accept; this test should fail until we implement validation)
        assert!(
            result.is_err(),
            "expected manifest.add_sst to validate SST file and fail for corrupted file"
        );
    }

    #[test]
    fn should_fail_to_add_sst_given_missing_final_file_when_only_tmp_exists() {
        // Arrange: create a temp dir and a leftover .tmp file (simulate crash before rename)
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let sst_name = crate::cloud_layout::file_name(0, 0, 2);
        let tmp_name = format!("{sst_name}.tmp");
        let tmp_path = tmp.path().join(&tmp_name);

        // Write a temp file but do not rename
        std::fs::write(&tmp_path, b"partial-sst-data").expect("write tmp sst");

        let file_meta = crate::runtime::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: 0,
            content_crc32c: None,
            cf_id: 0,
            smallest_key: None,
            largest_key: None,
            smallest_seq: None,
            largest_seq: None,
            key_bounds_complete: false,
        };

        let mut state = crate::runtime::state::RuntimeState::new(tmp.path().to_path_buf(), false);
        let mut actor = ManifestActor::new();

        // Act: attempt to add the SST to manifest
        let result = actor.add_sst(&mut state, file_meta);

        // Assert: adding a manifest entry for a missing final SST (only tmp present) MUST fail
        assert!(
            result.is_err(),
            "expected manifest.add_sst to fail when only tmp file exists"
        );
    }

    #[test]
    fn should_add_sst_successfully_given_valid_file_when_manifest_updated() -> MidgeResult<()> {
        // Arrange: create a valid on-disk SST via the Fs SstFactory
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let sst_name = crate::cloud_layout::file_name(0, 0, 3);
        let mut state = crate::runtime::state::RuntimeState::new(tmp.path().to_path_buf(), false);
        assert!(state.sst_dir.exists(), "sst dir must exist");
        let sst_path = state.sst_dir.join(&sst_name);

        let factory = crate::sst::FsSstFactoryIo::new(
            std::sync::Arc::new(crate::io::RealFs::new(&state.sst_dir)?),
            4096,
        );
        let mut writer = factory.create()?;
        writer.add_with_meta(b"a", Some(b"value"), 10, EntryType::Put, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &sst_path)?;
        assert!(
            sst_path.exists(),
            "sst path must exist after creating footer"
        );
        eprintln!("sst file bytes: {}", std::fs::metadata(&sst_path)?.len());

        let file_meta = crate::runtime::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: std::fs::metadata(&sst_path)?.len(),
            content_crc32c: None,
            cf_id: 0,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"a".to_vec()),
            smallest_seq: Some(10),
            largest_seq: Some(10),
            key_bounds_complete: true,
        };

        assert_eq!(
            state.sst_dir.join(&sst_name),
            sst_path,
            "state.sst_dir should match the temp dir used for file creation"
        );
        let mut actor = ManifestActor::new();

        // Act: attempt to add the SST to manifest
        let result = actor.add_sst(&mut state, file_meta);

        // Assert: valid SST should be accepted
        assert!(result.is_ok(), "add_sst failed: {:?}", result.err());
        Ok(())
    }

    /// #493: every production journal writer must advance the in-memory
    /// checkpoint horizon, or each later persist takes the stale-caller branch.
    #[test]
    fn should_track_snapshot_checkpoint_when_ddl_journals_before_compaction_persist() {
        // Arrange
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let mut state = crate::runtime::state::RuntimeState::new(tmp.path().to_path_buf(), false);
        let mut actor = ManifestActor::new();
        crate::runtime::ddl::apply_local_edit(
            &mut state,
            &crate::metadata::ManifestEdit::CreateColumnFamily {
                id: 1,
                name: "other".to_string(),
                created_at: 1,
            },
        )
        .expect("DDL edit");
        let sink = LogSink::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::WARN)
            .with_writer(sink.clone())
            .finish();

        // Act
        tracing::subscriber::with_default(subscriber, || {
            for sequence in [1, 2] {
                actor
                    .compaction_complete(&mut state, &[], &[sst_meta(sequence)])
                    .expect("compaction edit");
                ManifestActor::persist(&mut state).expect("persist");
            }
        });

        // Assert
        assert_eq!(
            state.manifest.edit_checkpoint_id,
            read_snapshot(&state).edit_checkpoint_id
        );
        let logs = String::from_utf8(
            sink.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
        .unwrap();
        assert!(!logs.contains("stale caller"), "{logs}");
    }

    /// The owned flush commit must keep an orphan journal edit that was
    /// appended before a different metadata operation failed.
    #[test]
    fn should_keep_orphan_journal_edit_when_flush_owner_commits() {
        // Arrange
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let mut state = crate::runtime::state::RuntimeState::new(tmp.path().to_path_buf(), false);
        let mut actor = ManifestActor::new();
        actor
            .compaction_complete(&mut state, &[], &[sst_meta(1)])
            .expect("compaction edit");
        ManifestActor::persist(&mut state).expect("persist");
        state
            .manifest_store
            .append(&crate::metadata::ManifestEdit::CreateColumnFamily {
                id: 7,
                name: "orphan".to_string(),
                created_at: 1,
            })
            .expect("orphan append whose writer then failed");
        let flushed = sst_meta(2);

        // Act
        state
            .record_flush_publication_intent(0, 2, &flushed)
            .expect("record output intent");
        state
            .commit_flush_publication(0, 2, &flushed, 3, false)
            .expect("commit flush");
        ManifestActor::persist(&mut state).expect("persist after flush install");

        // Assert
        let reloaded = crate::metadata::ManifestPersistence::load(&state.db_path).expect("reload");
        assert!(
            reloaded.column_families.iter().any(|cf| cf.id == 7),
            "orphan journal edit was truncated away"
        );
        assert!(reloaded.files.iter().any(|file| file.name == flushed.name));
    }
}
