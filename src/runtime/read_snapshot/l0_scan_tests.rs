use super::*;
use crate::io::RealFs;
use crate::sst::traits::SstFactory;
use crate::sst::FsSstFactoryIo;
use std::path::Path;

type Point<'a> = (&'a [u8], &'a [u8], u64);
type Tombstone<'a> = (&'a [u8], &'a [u8], u64);

fn write_sst(
    path: &Path,
    index: u64,
    points: &[Point<'_>],
    tombstones: &[Tombstone<'_>],
    bounds: (&[u8], &[u8]),
) -> MidgeResult<FileMeta> {
    let name = format!("scan-{index}.sst");
    let factory = FsSstFactoryIo::new(Arc::new(RealFs::new(path)?), 4096);
    let mut writer = factory.create()?;
    for (key, value, sequence) in points {
        writer.add_with_meta(key, Some(value), *sequence, 0, None)?;
    }
    for (start, end, sequence) in tombstones {
        writer.add_range_tombstone(start, end, *sequence)?;
    }
    let bytes = writer.finish_bytes()?;
    std::fs::write(path.join(&name), &bytes)?;
    Ok(FileMeta {
        name,
        sst_seq: index,
        size_bytes: bytes.len() as u64,
        smallest_key: Some(bounds.0.to_vec()),
        largest_key: Some(bounds.1.to_vec()),
        key_bounds_complete: true,
        ..FileMeta::default()
    })
}

fn snapshot(
    path: &Path,
    files: Vec<FileMeta>,
) -> MidgeResult<(Arc<ReadSnapshot>, Arc<ReadResources>)> {
    let fs: Arc<dyn Fs> = Arc::new(RealFs::new(path)?);
    let resources = Arc::new(ReadResources::new_with_diagnostics(
        Arc::clone(&fs),
        std::path::PathBuf::new(),
        64 * 1024,
        crate::sst::cache::CachePolicyType::Lru,
        Arc::new(crate::diagnostics::RuntimeDiagnostics::default()),
    ));
    let snapshot = ReadSnapshot::new_with_resources(
        Arc::new(SkipListMemtable::new()),
        Vec::new(),
        files,
        fs,
        std::path::PathBuf::new(),
        false,
        0,
        Some(Arc::clone(&resources)),
    );
    Ok((Arc::new(snapshot), resources))
}

#[test]
fn should_scan_disjoint_l0_inventory_when_metadata_pool_cannot_hold_all_readers() -> MidgeResult<()>
{
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut files = Vec::new();
    let mut expected = Vec::new();
    for index in 0..128 {
        let key = format!("key-{index:03}").into_bytes();
        files.push(write_sst(
            directory.path(),
            index,
            &[(&key, &key, 1)],
            &[],
            (&key, &key),
        )?);
        expected.push((bytes::Bytes::from(key.clone()), bytes::Bytes::from(key)));
    }
    assert!(files.len() * std::mem::size_of::<SstFileIo>() > 16 * 1024);
    let (snapshot, resources) = snapshot(directory.path(), files)?;
    // Act
    for reverse in [false, true] {
        let mut scan = snapshot.state_scan(None, None, reverse, u64::MAX);
        scan.initialize()?;
        let readers = scan
            .sources
            .iter()
            .filter(|source| !matches!(source.iterator, SnapshotStateIterator::Memory(_)))
            .count();
        let rows = scan.collect::<MidgeResult<Vec<_>>>()?;
        let mut ordered_expected = expected.clone();
        if reverse {
            ordered_expected.reverse();
        }
        // Assert
        assert_eq!(rows, ordered_expected);
        assert_eq!(readers, 1);
        assert!(resources.cached_reader_count() < 128);
    }
    Ok(())
}

fn overlapping_inventory(path: &Path) -> MidgeResult<Vec<FileMeta>> {
    let mut legacy = write_sst(
        path,
        4,
        &[(b"a", b"legacy", 5)],
        &[(b"d", b"e", 5)],
        (b"z", b"z"),
    )?;
    legacy.key_bounds_complete = false;
    Ok(vec![
        write_sst(
            path,
            0,
            &[(b"a", b"old", 1), (b"b", b"old", 1), (b"c", b"old", 1)],
            &[],
            (b"a", b"c"),
        )?,
        write_sst(
            path,
            1,
            &[(b"c", b"next", 2), (b"d", b"next", 2), (b"e", b"next", 2)],
            &[],
            (b"c", b"e"),
        )?,
        write_sst(
            path,
            2,
            &[(b"b", b"overlap", 3), (b"d", b"overlap", 3)],
            &[(b"c", b"d", 4)],
            (b"b", b"d"),
        )?,
        legacy,
        write_sst(path, 5, &[(b"f", b"inverted", 6)], &[], (b"z", b"a"))?,
    ])
}

#[test]
fn should_preserve_scan_visibility_when_l0_overlap_and_legacy_bounds_mix() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let (snapshot, _) = snapshot(directory.path(), overlapping_inventory(directory.path())?)?;
    // Act
    for reverse in [false, true] {
        for (sequence, expected) in [
            (
                2,
                vec![
                    (b"a", b"old".as_slice()),
                    (b"b", b"old"),
                    (b"c", b"next"),
                    (b"d", b"next"),
                    (b"e", b"next"),
                ],
            ),
            (
                u64::MAX,
                vec![
                    (b"a", b"legacy".as_slice()),
                    (b"b", b"overlap"),
                    (b"e", b"next"),
                    (b"f", b"inverted"),
                ],
            ),
        ] {
            let rows = snapshot
                .state_scan(None, None, reverse, sequence)
                .collect::<MidgeResult<Vec<_>>>()?;
            let mut expected = expected
                .into_iter()
                .map(|(key, value)| {
                    (
                        bytes::Bytes::copy_from_slice(key),
                        bytes::Bytes::copy_from_slice(value),
                    )
                })
                .collect::<Vec<_>>();
            if reverse {
                expected.reverse();
            }
            // Assert
            assert_eq!(rows, expected);
        }
    }
    Ok(())
}

#[test]
fn should_keep_newest_l0_source_precedence_when_values_share_a_sequence() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let files = vec![
        write_sst(
            directory.path(),
            3,
            &[(b"a", b"unrelated", 7)],
            &[],
            (b"a", b"a"),
        )?,
        write_sst(
            directory.path(),
            2,
            &[(b"m", b"newest", 7), (b"z", b"newest-end", 7)],
            &[],
            (b"m", b"z"),
        )?,
        write_sst(
            directory.path(),
            1,
            &[(b"m", b"oldest", 7)],
            &[],
            (b"b", b"n"),
        )?,
        write_sst(directory.path(), 0, &[(b"z", b"end", 7)], &[], (b"z", b"z"))?,
    ];
    let (snapshot, _) = snapshot(directory.path(), files)?;
    // Act
    for reverse in [false, true] {
        let rows = snapshot
            .state_scan(None, None, reverse, u64::MAX)
            .collect::<MidgeResult<Vec<_>>>()?;
        // Assert
        assert_eq!(
            rows.iter()
                .find(|(key, _)| key.as_ref() == b"m")
                .map(|(_, value)| value.as_ref()),
            Some(b"newest".as_slice())
        );
        assert_eq!(
            rows.iter()
                .find(|(key, _)| key.as_ref() == b"z")
                .map(|(_, value)| value.as_ref()),
            Some(b"newest-end".as_slice())
        );
    }
    Ok(())
}

#[test]
fn should_apply_tombstone_only_l0_files_when_disjoint_runs_are_chained() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let files = vec![
        write_sst(directory.path(), 3, &[], &[(b"k", b"m", 5)], (b"k", b"m"))?,
        write_sst(directory.path(), 2, &[(b"z", b"new", 3)], &[], (b"z", b"z"))?,
        write_sst(
            directory.path(),
            1,
            &[(b"l", b"old", 1), (b"z", b"old", 1)],
            &[],
            (b"l", b"z"),
        )?,
    ];
    let (snapshot, _) = snapshot(directory.path(), files)?;
    // Act
    for reverse in [false, true] {
        let latest = snapshot
            .state_scan(None, None, reverse, u64::MAX)
            .collect::<MidgeResult<Vec<_>>>()?;
        let historical = snapshot
            .state_scan(None, None, reverse, 2)
            .collect::<MidgeResult<Vec<_>>>()?;
        // Assert
        assert_eq!(
            latest,
            vec![(
                bytes::Bytes::from_static(b"z"),
                bytes::Bytes::from_static(b"new")
            )]
        );
        assert_eq!(historical.len(), 2);
        assert!(historical.iter().all(|(_, value)| value.as_ref() == b"old"));
    }
    Ok(())
}

#[test]
fn should_keep_newest_endpoint_value_when_l0_bounds_touch() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let files = vec![
        write_sst(
            directory.path(),
            2,
            &[(b"a", b"left", 7), (b"m", b"newest", 7)],
            &[],
            (b"a", b"m"),
        )?,
        write_sst(
            directory.path(),
            1,
            &[(b"m", b"oldest", 7), (b"z", b"right", 7)],
            &[],
            (b"m", b"z"),
        )?,
    ];
    let (snapshot, _) = snapshot(directory.path(), files)?;
    // Act
    for reverse in [false, true] {
        let rows = snapshot
            .state_scan(None, None, reverse, u64::MAX)
            .collect::<MidgeResult<Vec<_>>>()?;
        // Assert
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1].0.as_ref(), b"m");
        assert_eq!(rows[1].1.as_ref(), b"newest");
    }
    Ok(())
}

#[test]
fn should_open_only_intersecting_l0_readers_when_scan_bounds_are_narrow() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut files = Vec::new();
    for index in 0..128 {
        let key = format!("key-{index:03}").into_bytes();
        files.push(write_sst(
            directory.path(),
            index,
            &[(&key, &key, 1)],
            &[],
            (&key, &key),
        )?);
    }
    // Act
    for legacy in [false, true] {
        for reverse in [false, true] {
            let mut inventory = files.clone();
            if legacy {
                inventory[0].key_bounds_complete = false;
                inventory[0].smallest_key = Some(b"wrong".to_vec());
                inventory[0].largest_key = Some(b"wrong".to_vec());
            }
            let (snapshot, _) = snapshot(directory.path(), inventory)?;
            let rows = snapshot
                .state_scan(
                    Some(b"key-064".to_vec()),
                    Some(b"key-065".to_vec()),
                    reverse,
                    u64::MAX,
                )
                .collect::<MidgeResult<Vec<_>>>()?;
            let diagnostics = snapshot.diagnostics.snapshot();
            // Assert
            assert_eq!(
                rows,
                vec![(
                    bytes::Bytes::from_static(b"key-064"),
                    bytes::Bytes::from_static(b"key-064")
                )]
            );
            assert_eq!(diagnostics.sst_reader_cache_misses, 1 + u64::from(legacy));
            assert_eq!(
                diagnostics.candidate_sst_files_checked,
                1 + u64::from(legacy)
            );
        }
    }
    Ok(())
}
