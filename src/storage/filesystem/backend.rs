impl StorageBackend for FileSystem {
    fn submit_range_read_request(
        &self,
        request: StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        let timeout = request.remaining_timeout();
        if timeout.is_zero() {
            let _ = callback.send(Err(crate::storage::storage_timeout_error(
                "range read timed out",
            )));
            return;
        }
        let StoragePrecondition::IfMatch(expected) = request.precondition else {
            let _ = callback.send(Err(crate::storage::StorageError::protocol(
                "range read requires an object identity",
            )));
            return;
        };
        let _reservation = request.reservation;
        self.read_range_within(
            &request.key,
            range.start,
            range.end,
            &expected,
            timeout,
            &callback,
        );
    }

    fn submit_metadata_read_request(
        &self,
        request: StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        crate::storage::dispatch_metadata_read_request(
            request,
            callback,
            |key, timeout, callback| {
                self.metadata_read_within(key, timeout, &callback);
            },
        );
    }

    fn submit_head_request(&self, request: StorageRequest, callback: StorageCallback) {
        crate::storage::dispatch_head_request(request, callback, |key, _, callback| {
            self.head_within(key, &callback);
        });
    }

    fn submit_range_head_request(&self, request: StorageRequest, callback: StorageCallback) {
        crate::storage::dispatch_head_request(request, callback, |key, timeout, callback| {
            self.range_head_within(key, timeout, &callback);
        });
    }

    fn submit_delete_request(&self, request: StorageRequest, callback: StorageCallback) {
        let timeout = request.remaining_timeout();
        let key = request.key;
        let result = (|| {
            if timeout.is_zero() {
                return Err(crate::storage::storage_timeout_error("delete timed out"));
            }
            let path = self.full_path(&key)?;
            let _lock = mutation_lock(&path);
            let _process_lock = self.acquire_process_lock(&path)?;
            match request.precondition {
                StoragePrecondition::None => {}
                StoragePrecondition::IfAbsent => {
                    return Err(crate::storage::StorageError::precondition_failed(
                        "delete cannot enforce absence precondition",
                    ));
                }
                StoragePrecondition::IfMatch(expected) => {
                    if expected.generation.is_some() {
                        if matches!(fs::symlink_metadata(&path), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
                        {
                            return Ok(());
                        }
                        return Err(crate::storage::StorageError::precondition_failed(
                            "filesystem cannot enforce generation precondition",
                        ));
                    }
                    let etag = expected.etag.trim().trim_matches('"');
                    if etag.is_empty() {
                        return Err(crate::storage::StorageError::precondition_failed(
                            "object identity is missing",
                        ));
                    }
                    let current = if etag.starts_with("fs:") {
                        range_path_metadata(&path).map(|metadata| metadata.etag)
                    } else {
                        fs::read(&path)
                            .map_err(crate::storage::StorageError::from)
                            .map(|bytes| {
                                StorageObjectMetadata::content_crc(bytes.len() as u64, &bytes).etag
                            })
                    };
                    match current {
                        Ok(current) if current == etag => {}
                        Ok(_) => {
                            return Err(crate::storage::StorageError::precondition_failed(
                                "etag mismatch",
                            ));
                        }
                        Err(error) if error.is_not_found() => return Ok(()),
                        Err(error) => return Err(error),
                    }
                }
            }
            match fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(crate::storage::StorageError::io(format!(
                    "delete {}: {error}",
                    path.display()
                ))),
            }
        })();
        let _reservation = request.reservation;
        let _ = callback.send(StorageEvent::DeleteComplete {
            key,
            result: match result {
                Ok(()) => StorageOutcome::Ok(()),
                Err(error) => StorageOutcome::Err(error),
            },
        });
    }

    fn submit_write_request(
        &self,
        request: StorageRequest,
        data: Vec<u8>,
        callback: StorageCallback,
    ) {
        let timeout = request.remaining_timeout();
        let key = request.key;
        let result = (|| {
            if timeout.is_zero() {
                return Err(crate::storage::storage_timeout_error("write timed out"));
            }
            let path = self.full_path(&key)?;
            let _lock = mutation_lock(&path);
            let _process_lock = self.acquire_process_lock(&path)?;
            let outcome = match request.precondition {
                StoragePrecondition::None => {
                    publish_object_atomically(&self.base_path, &path, &data, Publish::Replace)
                }
                StoragePrecondition::IfAbsent => {
                    publish_object_atomically(&self.base_path, &path, &data, Publish::CreateNew)
                }
                StoragePrecondition::IfMatch(expected) => {
                    if expected.generation.is_some() {
                        return Err(crate::storage::StorageError::precondition_failed(
                            "filesystem cannot enforce generation precondition",
                        ));
                    }
                    let etag = expected.etag.trim().trim_matches('"');
                    if etag.is_empty() {
                        return Err(crate::storage::StorageError::precondition_failed(
                            "object identity is missing",
                        ));
                    }
                    let current = if etag.starts_with("fs:") {
                        range_path_metadata(&path).map(|metadata| metadata.etag)
                    } else {
                        fs::read(&path)
                            .map_err(crate::storage::StorageError::from)
                            .map(|bytes| {
                                StorageObjectMetadata::content_crc(bytes.len() as u64, &bytes).etag
                            })
                    }?;
                    if current != etag {
                        return Err(crate::storage::StorageError::precondition_failed(
                            "etag mismatch",
                        ));
                    }
                    publish_object_atomically(&self.base_path, &path, &data, Publish::Replace)
                }
            };
            Ok(outcome)
        })();
        // Local I/O and its callback complete inline; holding the reservation
        // through the send is sufficient.
        let _reservation = request.reservation;
        let result = match result {
            Ok(outcome) => outcome,
            Err(error) => StorageOutcome::Err(error),
        };
        let _ = callback.send(StorageEvent::WriteComplete { key, result });
    }
}
