use super::*;
use crate::runtime::cloud_startup::replay_coverage::probe::RecoveryProbeVariant;

const VARIANTS: [RecoveryProbeVariant; 4] = [
    RecoveryProbeVariant::Baseline,
    RecoveryProbeVariant::KeyIndex,
    RecoveryProbeVariant::SingleReader,
    RecoveryProbeVariant::TimersOff,
];

#[test]
fn should_preserve_exact_proof_when_bounded_probe_variant_changes() {
    // Arrange
    let scenarios = [
        (
            vec![(Some(b"value".as_slice()), 7, None)],
            put(7, None),
            true,
        ),
        (
            vec![(Some(b"newer".as_slice()), 9, None)],
            put(7, None),
            true,
        ),
        (vec![(None, 9, None)], put(7, None), true),
        (
            vec![(Some(b"value".as_slice()), 7, Some(1))],
            put(7, Some(1)),
            true,
        ),
        (
            vec![(Some(b"value".as_slice()), 7, Some(9))],
            put(7, None),
            false,
        ),
        (vec![(None, 7, None)], put(7, None), false),
        (
            vec![
                (Some(b"value".as_slice()), 7, None),
                (Some(b"other".as_slice()), 7, None),
            ],
            put(7, None),
            false,
        ),
        (
            vec![(Some(b"value".as_slice()), 5, None)],
            put(7, None),
            false,
        ),
    ];
    for variant in VARIANTS {
        for (entries, record, expected) in &scenarios {
            let (_directory, coverage) = fixture(entries);
            let coverage = coverage.with_probe_variant(variant);
            // Act
            let covered = coverage.contains(record);
            coverage.release_for_checkpoint();
            // Assert
            assert_eq!(covered, *expected, "{variant:?}");
            assert_eq!(coverage.read_budget.used(), 0);
            assert!(coverage.read_budget.peak() <= coverage.read_budget.limit());
        }
        let (_directory, coverage) = fixture(&[(None, 7, None)]);
        let coverage = coverage.with_probe_variant(variant);
        let mut delete = put(7, None);
        delete.op = WalOpKind::Delete;
        delete.value = None;
        assert!(!coverage.contains(&delete));
    }
}

#[test]
fn should_retain_wal_when_probe_variant_cannot_verify_identity_or_fit_budget() {
    // Arrange
    for variant in VARIANTS {
        let (directory, coverage) = fixture(&[(Some(b"value"), 7, None)]);
        let mut coverage = coverage.with_probe_variant(variant);
        let record = put(7, None);
        assert!(coverage.contains(&record));
        coverage.release_for_checkpoint();
        let path = directory
            .path()
            .join("cloud/sst")
            .join(&coverage.manifest.files[0].name);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 1;
        std::fs::write(path, bytes).unwrap();
        // Act
        let corrupt = coverage.contains(&record);
        coverage.release_for_checkpoint();
        coverage.read_budget = crate::common::resource_budget::ResourceBudget::new(32);
        let exhausted = coverage.contains(&record);
        coverage.release_for_checkpoint();
        // Assert
        assert!(!corrupt);
        assert!(!exhausted);
        assert_eq!(coverage.read_budget.used(), 0);
    }
}

#[test]
fn should_preserve_linear_fallback_when_experimental_index_exceeds_budget() {
    // Arrange
    let (_directory, mut coverage) = fixture(&[(Some(b"value"), 7, None)]);
    coverage = coverage.with_probe_variant(RecoveryProbeVariant::KeyIndex);
    let candidate = coverage.manifest.files[0].clone();
    for index in 0..8192_u64 {
        coverage.manifest.files.push(crate::metadata::FileMeta {
            name: format!("irrelevant-{index}.sst"),
            smallest_seq: Some(10 + index * 10),
            largest_seq: Some(19 + index * 10),
            ..candidate.clone()
        });
    }
    // Act
    let covered = coverage.contains(&put(7, None));
    coverage.release_for_checkpoint();
    // Assert
    assert!(covered);
    assert!(coverage.key_index.borrow().is_none());
    assert_eq!(coverage.manifest_scanned.get(), 8193);
    assert_eq!(coverage.read_budget.used(), 0);
}

#[test]
fn should_preserve_typed_cancellation_when_probe_variant_has_cached_exact_proof() {
    // Arrange
    for variant in VARIANTS {
        let (_directory, coverage) = fixture(&[(Some(b"value"), 7, None)]);
        let coverage = coverage.with_probe_variant(variant);
        let record = put(7, None);
        assert!(coverage.contains(&record));
        let scope =
            crate::common::DeadlineScope::new(crate::common::OperationDeadline::unbounded());
        scope.cancel();
        // Act
        let result = coverage.contains_within(&record, &scope);
        coverage.release_for_checkpoint();
        // Assert
        assert!(matches!(result, Err(crate::common::MidgeError::Timeout(_))));
        assert_eq!(coverage.read_budget.used(), 0);
    }
}

#[test]
fn should_retain_wal_when_probe_variant_has_unreadable_overlapping_authority() {
    // Arrange
    for variant in VARIANTS {
        let (directory, coverage) =
            fixture(&[(Some(b"value"), 7, None), (Some(b"value"), 7, None)]);
        let coverage = coverage.with_probe_variant(variant);
        std::fs::remove_file(
            directory
                .path()
                .join("cloud/sst")
                .join(&coverage.manifest.files[1].name),
        )
        .unwrap();
        // Act
        let covered = coverage.contains(&put(7, None));
        coverage.release_for_checkpoint();
        // Assert
        assert!(!covered);
        assert_eq!(coverage.read_budget.used(), 0);
    }
}
