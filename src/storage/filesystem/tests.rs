use super::*;

#[cfg(test)]
mod operation_tests {
    use super::*;
    use crate::storage::test_support::StorageBackendTestExt;
    use std::sync::{mpsc, Arc, Barrier};
    use tempfile::TempDir;

    // =========== Write Tests ===========

    #[test]
    fn should_write_file_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        let data = b"test data".to_vec();

        // Act
        fs.write_for_test("test.txt", data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { key, result } => {
                assert_eq!(key, "test.txt");
                assert!(result.is_ok());
                // Verify file was actually written
                let content = std::fs::read(temp_dir.path().join("test.txt")).unwrap();
                assert_eq!(content, data);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_write_empty_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.write_for_test("empty.txt", vec![], tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(temp_dir.path().join("empty.txt")).unwrap();
                assert_eq!(content.len(), 0);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_write_large_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        let large_data = vec![42u8; 1_000_000];

        // Act
        fs.write_for_test("large.bin", large_data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(temp_dir.path().join("large.bin")).unwrap();
                assert_eq!(content, large_data);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_create_parent_directories() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.write_for_test("subdir/nested/file.txt", b"data".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(temp_dir.path().join("subdir/nested/file.txt").exists());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_overwrite_existing_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let path = temp_dir.path().join("file.txt");
        std::fs::write(&path, b"old").unwrap();

        // Act
        let (tx, rx) = mpsc::channel();
        fs.write_for_test("file.txt", b"new".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(&path).unwrap();
                assert_eq!(content, b"new");
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_write_binary_data() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        let binary_data = vec![0u8, 1u8, 255u8, 254u8];

        // Act
        fs.write_for_test("binary.bin", binary_data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(temp_dir.path().join("binary.bin")).unwrap();
                assert_eq!(content, binary_data);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_create_file_when_if_none_match_star_and_missing() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let path = temp_dir.path().join("conditional-create.txt");
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write_request(
            StorageRequest::new(
                "conditional-create.txt",
                crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
                std::time::Duration::from_secs(5),
            )
            .with_precondition(StoragePrecondition::IfAbsent),
            b"new".to_vec(),
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert_eq!(std::fs::read(&path).unwrap(), b"new");
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_reject_create_when_if_none_match_star_and_existing() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let path = temp_dir.path().join("conditional-existing.txt");
        std::fs::write(&path, b"old").unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write_request(
            StorageRequest::new(
                "conditional-existing.txt",
                crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
                std::time::Duration::from_secs(5),
            )
            .with_precondition(StoragePrecondition::IfAbsent),
            b"new".to_vec(),
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_err());
                assert_eq!(std::fs::read(&path).unwrap(), b"old");
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_allow_exactly_one_concurrent_conditional_create() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let backend = Arc::new(FileSystem::new(temp_dir.path()).unwrap());
        let contenders = 8;
        let barrier = Arc::new(Barrier::new(contenders));

        // Act
        let joins: Vec<_> = (0..contenders)
            .map(|index| {
                let backend = Arc::clone(&backend);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let (tx, rx) = mpsc::channel();
                    barrier.wait();
                    backend.submit_write_request(
                        StorageRequest::new(
                            "racing-create.txt",
                            crate::common::OperationDeadline::from_budget(
                                std::time::Duration::from_secs(5),
                            ),
                            std::time::Duration::from_secs(5),
                        )
                        .with_precondition(StoragePrecondition::IfAbsent),
                        format!("writer-{index}").into_bytes(),
                        tx,
                    );
                    match rx.recv().expect("conditional create response") {
                        StorageEvent::WriteComplete { result, .. } => result.is_ok(),
                        other => panic!("unexpected response: {other:?}"),
                    }
                })
            })
            .collect();
        let success_count = joins
            .into_iter()
            .map(|join| usize::from(join.join().expect("create contender panicked")))
            .sum::<usize>();

        // Assert
        assert_eq!(success_count, 1);
        assert!(temp_dir.path().join("racing-create.txt").exists());
    }

    #[test]
    fn should_allow_one_winner_when_compare_swap_updates_race() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let key = "racing-cas.txt";
        let initial = b"initial".to_vec();
        let initial_size = initial.len() as u64;
        std::fs::write(temp_dir.path().join(key), &initial).unwrap();
        let etag = StorageObjectMetadata::content_crc(initial.len() as u64, &initial).etag;
        let backend = Arc::new(FileSystem::new(temp_dir.path()).unwrap());
        let contenders = 8;
        let barrier = Arc::new(Barrier::new(contenders));

        // Act
        let joins: Vec<_> = (0..contenders)
            .map(|index| {
                let backend = Arc::clone(&backend);
                let barrier = Arc::clone(&barrier);
                let etag = etag.clone();
                std::thread::spawn(move || {
                    let (tx, rx) = mpsc::channel();
                    barrier.wait();
                    backend.submit_write_request(
                        StorageRequest::new(
                            key,
                            crate::common::OperationDeadline::from_budget(
                                std::time::Duration::from_secs(5),
                            ),
                            std::time::Duration::from_secs(5),
                        )
                        .with_precondition(StoragePrecondition::IfMatch(
                            StorageObjectMetadata {
                                size: initial_size,
                                etag,
                                generation: None,
                            },
                        )),
                        format!("writer-{index}").into_bytes(),
                        tx,
                    );
                    match rx.recv().expect("conditional update response") {
                        StorageEvent::WriteComplete { result, .. } => result.is_ok(),
                        other => panic!("unexpected response: {other:?}"),
                    }
                })
            })
            .collect();
        let success_count = joins
            .into_iter()
            .map(|join| usize::from(join.join().expect("CAS contender panicked")))
            .sum::<usize>();

        // Assert
        assert_eq!(success_count, 1);
        assert_ne!(std::fs::read(temp_dir.path().join(key)).unwrap(), initial);
    }

    // =========== Read Tests ===========

    #[test]
    fn should_read_existing_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let data = b"hello world";
        std::fs::write(temp_dir.path().join("test.txt"), data).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_metadata_read_request(
            StorageRequest::new(
                "test.txt",
                crate::common::OperationDeadline::unbounded(),
                std::time::Duration::from_secs(5),
            ),
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            Ok((content, _metadata)) => assert_eq!(content, data),
            Err(e) => panic!("Read failed: {e}"),
        }
    }

    #[test]
    fn should_fail_reading_nonexistent_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_metadata_read_request(
            StorageRequest::new(
                "nonexistent.txt",
                crate::common::OperationDeadline::unbounded(),
                std::time::Duration::from_secs(5),
            ),
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        assert!(matches!(event, Err(error) if error.is_not_found()));
    }

    #[test]
    fn should_read_large_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let data = vec![42u8; 1_000_000];
        std::fs::write(temp_dir.path().join("large.bin"), &data).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_metadata_read_request(
            StorageRequest::new(
                "large.bin",
                crate::common::OperationDeadline::unbounded(),
                std::time::Duration::from_secs(5),
            ),
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            Ok((content, _metadata)) => assert_eq!(content, data),
            Err(e) => panic!("Read failed: {e}"),
        }
    }

    #[test]
    fn should_read_empty_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::write(temp_dir.path().join("empty.txt"), b"").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_metadata_read_request(
            StorageRequest::new(
                "empty.txt",
                crate::common::OperationDeadline::unbounded(),
                std::time::Duration::from_secs(5),
            ),
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            Ok((content, _metadata)) => assert_eq!(content.len(), 0),
            Err(e) => panic!("Read failed: {e}"),
        }
    }

    #[test]
    fn should_read_binary_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let data = vec![0u8, 1u8, 255u8, 254u8];
        std::fs::write(temp_dir.path().join("binary.bin"), &data).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_metadata_read_request(
            StorageRequest::new(
                "binary.bin",
                crate::common::OperationDeadline::unbounded(),
                std::time::Duration::from_secs(5),
            ),
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            Ok((content, _metadata)) => assert_eq!(content, data),
            Err(e) => panic!("Read failed: {e}"),
        }
    }

    #[test]
    fn should_sync_store_root_when_publishing_into_directory_not_yet_made_durable() {
        // Arrange: another thread created `sst/` and has not yet synced the
        // store root; finding the directory present proves nothing (#519).
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let root = std::fs::canonicalize(temp_dir.path()).unwrap();
        std::fs::create_dir(root.join("sst")).unwrap();
        crate::io::durable_dir::take_synced_dirs();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.write_for_test("sst/000001.sst", b"table".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert
        assert!(matches!(
            event,
            StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }
        ));
        assert!(crate::io::durable_dir::take_synced_dirs().contains(&root));
    }

    #[test]
    fn should_complete_reserved_calls_inline_when_backend_is_filesystem() {
        // Arrange: the filesystem finishes every call before returning, so a
        // reservation needs no completion thread to outlive it (#518).
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let budget = crate::common::resource_budget::ResourceBudget::new(1024 * 1024);
        let before = crate::storage::retained_callback::retain_calls_on_this_thread();
        let (write_tx, write_rx) = mpsc::channel();

        // Act
        fs.submit_write_request(
            crate::storage::StorageRequest::new(
                "object.bin",
                crate::common::OperationDeadline::unbounded(),
                std::time::Duration::from_secs(5),
            )
            .with_reservation(std::sync::Arc::new(
                budget.reserve(7, "reserved write").unwrap(),
            )),
            b"payload".to_vec(),
            write_tx,
        );
        let write = write_rx.recv().unwrap();
        let (head_tx, head_rx) = mpsc::channel();
        fs.submit_range_head_request(
            StorageRequest::new(
                "object.bin",
                crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
                std::time::Duration::from_secs(5),
            ),
            head_tx,
        );
        let StorageEvent::HeadComplete {
            result: StorageOutcome::Ok(metadata),
            ..
        } = head_rx.recv().unwrap()
        else {
            panic!("range HEAD failed");
        };
        let (read_tx, read_rx) = mpsc::channel();
        fs.submit_range_read_request(
            StorageRequest::new(
                "object.bin",
                crate::common::OperationDeadline::unbounded(),
                std::time::Duration::from_secs(5),
            )
            .with_precondition(StoragePrecondition::IfMatch(metadata))
            .with_reservation(std::sync::Arc::new(
                budget.reserve(7, "reserved read").unwrap(),
            )),
            0..7,
            read_tx,
        );
        let read = read_rx.recv().unwrap();

        // Assert
        assert!(matches!(
            write,
            StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }
        ));
        assert_eq!(read.unwrap(), b"payload");
        assert_eq!(
            crate::storage::retained_callback::retain_calls_on_this_thread(),
            before
        );
    }

    // =========== Delete Tests ===========

    #[test]
    fn should_delete_existing_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("file.txt");
        std::fs::write(&path, b"data").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.delete_for_test("file.txt", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(!path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_report_success_when_deleting_nonexistent_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.delete_for_test("nonexistent.txt", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok(), "an absent object is already deleted (#514)");
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_delete_nested_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("subdir")).unwrap();
        let path = temp_dir.path().join("subdir/file.txt");
        std::fs::write(&path, b"data").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.delete_for_test("subdir/file.txt", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(!path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_delete_file_when_if_match_header_matches_content() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("conditional.txt");
        let data = b"data".to_vec();
        std::fs::write(&path, &data).unwrap();
        let etag = StorageObjectMetadata::content_crc(data.len() as u64, &data).etag;
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete_request(
            StorageRequest::new(
                "conditional.txt",
                crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
                std::time::Duration::from_secs(5),
            )
            .with_precondition(StoragePrecondition::IfMatch(StorageObjectMetadata {
                size: data.len() as u64,
                etag,
                generation: None,
            })),
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(!path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_reject_conditional_delete_when_if_match_header_mismatches() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("conditional-stale.txt");
        std::fs::write(&path, b"data").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete_request(
            StorageRequest::new(
                "conditional-stale.txt",
                crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
                std::time::Duration::from_secs(5),
            )
            .with_precondition(StoragePrecondition::IfMatch(StorageObjectMetadata {
                size: 4,
                etag: "crc32c:00000000".into(),
                generation: None,
            })),
            tx,
        );
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_err());
                assert!(path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    // =========== List Tests ===========

    #[test]
    fn should_sanitize_path_traversal() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act - Try to use path traversal
        fs.write_for_test("../escape_dir/evil.txt", b"x".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert - Should succeed but write inside base_path
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(temp_dir.path().join("escape_dir/evil.txt").exists());
                assert!(!temp_dir.path().join("../escape_dir/evil.txt").exists());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_handle_absolute_paths() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act - Try absolute path
        fs.write_for_test("/etc/passwd", b"x".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert - Should sanitize
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(temp_dir.path().join("etc/passwd").exists());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn should_reject_write_through_symlinked_parent() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let outside_dir = TempDir::new().unwrap();
        std::os::unix::fs::symlink(outside_dir.path(), temp_dir.path().join("link")).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.write_for_test("link/escaped.txt", b"must stay inside".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => assert!(result.is_err()),
            _ => panic!("Expected WriteComplete"),
        }
        assert!(!outside_dir.path().join("escaped.txt").exists());
    }

    #[test]
    fn should_construct_with_custom_base_path() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.write_for_test("marker.txt", b"custom base path".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert: the write must land under the base path passed to `new`, not some default.
        match event {
            StorageEvent::WriteComplete { result, .. } => assert!(result.is_ok()),
            _ => panic!("Expected WriteComplete"),
        }
        assert_eq!(
            fs::read(temp_dir.path().join("marker.txt")).unwrap(),
            b"custom base path"
        );
    }

    #[test]
    fn should_create_base_directory_if_missing() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let new_dir = temp_dir.path().join("new_base");
        assert!(!new_dir.exists());

        // Act
        let fs = FileSystem::new(&new_dir);

        // Assert
        assert!(fs.is_ok());
        assert!(new_dir.exists());
    }
}

#[cfg(all(test, feature = "failpoints"))]
mod atomic_publish_tests {
    use super::*;

    fn write(
        fs: &FileSystem,
        key: &str,
        data: &[u8],
        precondition: StoragePrecondition,
    ) -> StorageOutcome<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        fs.submit_write_request(
            StorageRequest::new(
                key,
                crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
                std::time::Duration::from_secs(5),
            )
            .with_precondition(precondition),
            data.to_vec(),
            tx,
        );
        match rx.recv().expect("write completion") {
            StorageEvent::WriteComplete { result, .. } => result,
            other => panic!("unexpected event {other:?}"),
        }
    }

    fn read(fs: &FileSystem, key: &str) -> StorageOutcome<Vec<u8>> {
        let (tx, rx) = std::sync::mpsc::channel();
        fs.submit_metadata_read_request(
            StorageRequest::new(
                key,
                crate::common::OperationDeadline::unbounded(),
                std::time::Duration::from_secs(5),
            ),
            tx,
        );
        match rx.recv().expect("read completion") {
            Ok((bytes, _metadata)) => StorageOutcome::Ok(bytes),
            Err(error) => StorageOutcome::Err(error),
        }
    }

    #[test]
    fn should_not_expose_partial_object_when_conditional_create_is_interrupted() {
        // Arrange
        let _guard = crate::failpoints::test_failpoint_guard();
        let dir = tempfile::tempdir().expect("temp dir");
        let fs = FileSystem::new(dir.path()).expect("filesystem backend");
        let create = || StoragePrecondition::IfAbsent;
        fail::cfg("midge::storage::fs_after_temp_object_write", "return").expect("failpoint");

        // Act
        let interrupted = write(&fs, "wal/1.wal", b"segment", create());
        fail::remove("midge::storage::fs_after_temp_object_write");

        // Assert
        assert!(matches!(interrupted, StorageOutcome::Err(_)));
        assert!(
            matches!(read(&fs, "wal/1.wal"), StorageOutcome::Err(error) if error.is_not_found())
        );
        assert!(matches!(
            write(&fs, "wal/1.wal", b"segment", create()),
            StorageOutcome::Ok(())
        ));
        assert!(matches!(read(&fs, "wal/1.wal"), StorageOutcome::Ok(bytes) if bytes == b"segment"));
    }

    #[test]
    fn should_keep_previous_object_when_overwrite_is_interrupted() {
        // Arrange
        let _guard = crate::failpoints::test_failpoint_guard();
        let dir = tempfile::tempdir().expect("temp dir");
        let fs = FileSystem::new(dir.path()).expect("filesystem backend");
        assert!(matches!(
            write(
                &fs,
                "meta/manifest.json",
                b"previous",
                StoragePrecondition::None
            ),
            StorageOutcome::Ok(())
        ));
        fail::cfg("midge::storage::fs_after_temp_object_write", "return").expect("failpoint");

        // Act
        let interrupted = write(
            &fs,
            "meta/manifest.json",
            b"replacement-bytes",
            StoragePrecondition::None,
        );
        fail::remove("midge::storage::fs_after_temp_object_write");

        // Assert
        assert!(matches!(interrupted, StorageOutcome::Err(_)));
        assert!(
            matches!(read(&fs, "meta/manifest.json"), StorageOutcome::Ok(bytes) if bytes == b"previous")
        );
    }
}

#[cfg(all(test, feature = "failpoints"))]
mod mutation_lock_order_tests {
    use super::super::range::{mutation_lock, mutation_stripe};
    use super::*;

    #[test]
    fn should_take_stripe_while_failpoint_test_holds_gate_when_stripe_sharer_waits() {
        // Arrange: a failpoint test holds the gate's write side. Another
        // thread mutating a different key on the same stripe evaluates a
        // failpoint inside the stripe. It must wait for the gate before the
        // stripe, or it holds the stripe the test itself needs.
        let first = std::path::PathBuf::from("stripe-order-a");
        let stripe = mutation_stripe(&first);
        let second = (0..10_000)
            .map(|index| std::path::PathBuf::from(format!("stripe-order-{index}")))
            .find(|path| path != &first && mutation_stripe(path) == stripe)
            .expect("a second path on the same stripe");
        let test_guard = crate::failpoints::test_failpoint_guard();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let sharer = std::thread::spawn(move || {
            started_tx.send(()).expect("report start");
            let _lock = mutation_lock(&second);
            crate::failpoints::fail_point!("midge::storage::test_stripe_order");
        });
        started_rx.recv().expect("sharer started");
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Act
        let acquired = MUTATION_LOCKS[stripe].try_lock_for(std::time::Duration::from_secs(2));

        // Assert
        assert!(
            acquired.is_some(),
            "a stripe sharer must not hold the stripe while waiting on the failpoint gate"
        );
        drop(acquired);
        drop(test_guard);
        sharer.join().expect("join sharer");
    }
}

#[cfg(test)]
mod lock_stripe_tests {
    use super::*;
    use crate::storage::test_support::StorageBackendTestExt;

    #[test]
    fn should_bound_lock_files_when_deleting_many_distinct_missing_keys() {
        // Arrange: in real cloud mode this backend evicts one local copy per
        // SST ever produced, so per-key lock files grew without bound.
        let dir = tempfile::tempdir().expect("temp dir");
        let fs = FileSystem::new(dir.path()).expect("filesystem backend");

        // Act
        for index in 0..1_000 {
            let (tx, rx) = std::sync::mpsc::channel();
            fs.delete_for_test(&format!("sst/{index}.sst"), tx);
            let _ = rx.recv().expect("delete completion");
        }

        // Assert
        let lock_files = std::fs::read_dir(dir.path().join(".midge-locks"))
            .expect("lock dir")
            .count();
        assert!(
            lock_files <= CONDITIONAL_LOCK_STRIPES,
            "{lock_files} lock files for 1,000 keys"
        );
    }
}
