use crate::common::{MidgeError, MidgeResult};
use std::path::{Component, Path, PathBuf};

/// Validated, engine-private filename for an authoritative SST object.
///
/// Persisted metadata stores names rather than paths. Parsing once at the
/// persistence boundary prevents absolute paths, parent traversal, Windows
/// drive/ADS syntax, and nested paths from ever reaching filesystem joins.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PersistedSstName(String);

impl PersistedSstName {
    pub(crate) fn parse(name: &str) -> MidgeResult<Self> {
        let path = Path::new(name);
        let mut components = path.components();
        let is_single_normal_component =
            matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none();
        let has_unsafe_portable_syntax = name.contains(['/', '\\', ':', '\0']);
        let has_sst_extension = path.extension().is_some_and(|extension| extension == "sst");

        if name.is_empty()
            || path.is_absolute()
            || !is_single_normal_component
            || has_unsafe_portable_syntax
            || !has_sst_extension
        {
            return Err(MidgeError::Corruption(format!(
                "invalid persisted SST name '{name}': expected one relative .sst filename"
            )));
        }

        Ok(Self(name.to_owned()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn join_under(&self, root: &Path) -> PathBuf {
        root.join(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::PersistedSstName;

    #[test]
    fn should_accept_single_relative_sst_name_when_parsing_persisted_metadata() {
        // Arrange
        let name = "000001_00_00000000000000000042.sst";

        // Act
        let parsed = PersistedSstName::parse(name).expect("parse canonical SST name");

        // Assert
        assert_eq!(parsed.as_str(), name);
    }

    #[test]
    fn should_reject_unsafe_path_forms_when_parsing_persisted_sst_name() {
        // Arrange
        let unsafe_names = [
            "",
            ".",
            "..",
            "../escape.sst",
            "nested/escape.sst",
            "nested\\escape.sst",
            "/absolute.sst",
            "C:\\absolute.sst",
            "alternate:stream.sst",
            "not-an-sst.tmp",
        ];

        // Act
        // Assert
        for name in unsafe_names {
            assert!(
                PersistedSstName::parse(name).is_err(),
                "unsafe persisted name must be rejected: {name}"
            );
        }
    }
}
