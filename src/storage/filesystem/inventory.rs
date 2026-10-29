use super::{fs, FileSystem, MidgeError, MidgeResult, Path};

impl FileSystem {
    /// Open the store at `base_path` only if its directory already exists,
    /// so read-only discovery never creates it.
    ///
    /// # Errors
    ///
    /// Fails when the directory exists but cannot be inspected or resolved.
    pub(crate) fn open_existing(base_path: &Path) -> MidgeResult<Option<Self>> {
        match fs::metadata(base_path) {
            Ok(_) => Self::new(base_path).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn inventory_error(context: &str, path: &Path, error: impl std::fmt::Display) -> MidgeError {
        MidgeError::RecoveryFailed(format!(
            "failed to {context} object store path '{}': {error}",
            path.display()
        ))
    }
}

/// A missing object is definitive only when its parent is a directory:
/// Windows reports a path under a regular file as not found too (#584).
pub(super) fn object_is_definitively_missing(path: &Path, error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound
        && path
            .parent()
            .and_then(|parent| fs::metadata(parent).ok())
            .is_some_and(|metadata| metadata.is_dir())
}

impl crate::storage::inventory::ObjectInventory for FileSystem {
    fn keys_page(
        &self,
        prefix: &str,
        page: crate::storage::cloud::CloudListPageRequest,
        scope: Option<&crate::common::DeadlineScope>,
    ) -> MidgeResult<Option<crate::storage::cloud::CloudListPage>> {
        page.validate(prefix)
            .map_err(|error| MidgeError::InvalidArgument(error.to_string()))?;
        let directory = prefix.rfind('/').map_or("", |end| &prefix[..end]);
        let directory = self
            .full_path(directory)
            .map_err(|error| Self::inventory_error("resolve page", Path::new(directory), error))?;
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if object_is_definitively_missing(&directory, &error) => {
                return Ok(Some(crate::storage::cloud::CloudListPage {
                    keys: Vec::new(),
                    continuation: None,
                }));
            }
            Err(error) => return Err(Self::inventory_error("list page", &directory, error)),
        };
        let mut selected = std::collections::BTreeSet::new();
        let mut stack = vec![entries];
        while let Some(entries) = stack.last_mut() {
            if let Some(scope) = scope {
                scope.check("filesystem inventory page")?;
            }
            let Some(entry) = entries.next() else {
                stack.pop();
                continue;
            };
            let entry =
                entry.map_err(|error| Self::inventory_error("list page", &directory, error))?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                // A directory iterator per level avoids retaining all child
                // paths. Unknown deep layouts are conservatively unsupported.
                if stack.len() >= 8 {
                    return Ok(None);
                }
                stack.push(fs::read_dir(entry.path())?);
                continue;
            }
            if !kind.is_file() {
                continue;
            }
            let path = entry.path();
            let relative = path
                .strip_prefix(&self.base_path)
                .map_err(|error| Self::inventory_error("resolve page", &path, error))?;
            let key = relative
                .components()
                .map(|part| part.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            if key.starts_with(prefix)
                && page.continuation.as_ref().is_none_or(|after| &key > after)
            {
                selected.insert(key);
                if selected.len() > page.max_keys + 1 {
                    selected.pop_last();
                }
            }
        }
        let more = selected.len() > page.max_keys;
        let keys = selected.into_iter().take(page.max_keys).collect::<Vec<_>>();
        let continuation = more.then(|| keys.last().cloned()).flatten();
        page.accept(prefix, keys, continuation)
            .map(Some)
            .map_err(|error| MidgeError::Corruption(error.to_string()))
    }

    fn object_size(
        &self,
        key: &str,
        _scope: Option<&crate::common::DeadlineScope>,
    ) -> MidgeResult<Option<u64>> {
        let path = self
            .full_path(key)
            .map_err(|error| Self::inventory_error("resolve", Path::new(key), error))?;
        match fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => Ok(Some(metadata.len())),
            Ok(_) => Err(Self::inventory_error("read", &path, "not a regular file")),
            Err(error) if object_is_definitively_missing(&path, &error) => Ok(None),
            Err(error) => Err(Self::inventory_error("inspect", &path, error)),
        }
    }

    fn keys_under(
        &self,
        prefix: &str,
        _scope: Option<&crate::common::DeadlineScope>,
    ) -> MidgeResult<Vec<String>> {
        let directory = prefix.rfind('/').map_or("", |end| &prefix[..end]);
        let start = self
            .full_path(directory)
            .map_err(|error| Self::inventory_error("resolve", Path::new(directory), error))?;
        let mut keys = Vec::new();
        let mut pending = vec![start.clone()];
        while let Some(directory) = pending.pop() {
            let entries = match fs::read_dir(&directory) {
                Ok(entries) => entries,
                // Only an absent prefix directory lists as definitively empty.
                Err(error)
                    if directory == start && object_is_definitively_missing(&start, &error) =>
                {
                    continue;
                }
                Err(error) => return Err(Self::inventory_error("list", &directory, error)),
            };
            for entry in entries {
                let entry =
                    entry.map_err(|error| Self::inventory_error("list", &directory, error))?;
                let path = entry.path();
                let file_type = entry
                    .file_type()
                    .map_err(|error| Self::inventory_error("inspect", &path, error))?;
                if file_type.is_dir() {
                    pending.push(path);
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                let relative = path
                    .strip_prefix(&self.base_path)
                    .map_err(|error| Self::inventory_error("resolve", &path, error))?;
                let key = relative
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                if key.starts_with(prefix) {
                    keys.push(key);
                }
            }
        }
        keys.sort();
        Ok(keys)
    }
}
