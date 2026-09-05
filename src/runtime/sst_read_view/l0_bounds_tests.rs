use super::*;
use crate::common::MidgeResult;
use crate::diagnostics::RuntimeDiagnostics;
use crate::io::{Fs, RealFs};
use crate::runtime::read_resources::ReadResources;
use crate::runtime::read_snapshot::ReadSnapshot;
use crate::sst::traits::SstFactory;
use crate::sst::{FsSstFactoryIo, SkipListMemtable};
use std::path::Path;

const CACHE_BYTES: usize = 64 * 1024;

fn write_point_sst(path: &Path, index: u64) -> MidgeResult<FileMeta> {
    let key = format!("key-{index:03}").into_bytes();
    let name = format!("disjoint-{index}.sst");
    let factory = FsSstFactoryIo::new(Arc::new(RealFs::new(path)?), 4096);
    let mut writer = factory.create()?;
    writer.add_with_meta(&key, Some(b"value"), 7, 0, None)?;
    let bytes = writer.finish_bytes()?;
    std::fs::write(path.join(&name), &bytes)?;
    Ok(FileMeta {
        name,
        size_bytes: bytes.len() as u64,
        content_crc32c: Some(crc32c::crc32c(&bytes)),
        sst_seq: index,
        smallest_key: Some(key.clone()),
        largest_key: Some(key),
        key_bounds_complete: true,
        ..FileMeta::default()
    })
}

fn snapshot(
    directory: &Path,
    files: Vec<FileMeta>,
) -> MidgeResult<(ReadSnapshot, Arc<RuntimeDiagnostics>)> {
    let fs: Arc<dyn Fs> = Arc::new(RealFs::new(directory)?);
    let diagnostics = Arc::new(RuntimeDiagnostics::default());
    let resources = Arc::new(ReadResources::new_with_diagnostics(
        fs.clone(),
        std::path::PathBuf::new(),
        CACHE_BYTES,
        crate::sst::cache::CachePolicyType::Lru,
        diagnostics.clone(),
    ));
    Ok((
        ReadSnapshot::new_with_resources(
            Arc::new(SkipListMemtable::new()),
            Vec::new(),
            files,
            fs,
            std::path::PathBuf::new(),
            false,
            0,
            Some(resources),
        ),
        diagnostics,
    ))
}

#[test]
fn should_open_only_relevant_l0_reader_when_full_inventory_exceeds_metadata_cache(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let files = (0..128)
        .map(|index| write_point_sst(directory.path(), index))
        .collect::<MidgeResult<Vec<_>>>()?;
    assert!(files.len() * std::mem::size_of::<crate::sst::fs::SstFileIo>() > CACHE_BYTES / 4);
    let (snapshot, diagnostics) = snapshot(directory.path(), files)?;
    // Act
    let value = snapshot.get(b"key-064", u64::MAX)?;
    // Assert
    assert_eq!(value.as_deref(), Some(b"value".as_slice()));
    assert_eq!(diagnostics.snapshot().sst_reader_cache_misses, 1);
    assert_eq!(diagnostics.snapshot().candidate_sst_files_checked, 1);
    Ok(())
}

#[test]
fn should_open_legacy_l0_reader_even_when_advisory_bounds_exclude_the_point() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut files = (0..128)
        .map(|index| write_point_sst(directory.path(), index))
        .collect::<MidgeResult<Vec<_>>>()?;
    files[0].key_bounds_complete = false;
    files[0].smallest_key = Some(b"wrong".to_vec());
    files[0].largest_key = Some(b"wrong".to_vec());
    let (snapshot, diagnostics) = snapshot(directory.path(), files)?;
    // Act
    let complete = snapshot.get(b"key-064", u64::MAX)?;
    let first_read = diagnostics.snapshot();
    let legacy = snapshot.get(b"key-000", u64::MAX)?;
    // Assert
    assert_eq!(complete.as_deref(), Some(b"value".as_slice()));
    assert_eq!(first_read.sst_reader_cache_misses, 2);
    assert_eq!(first_read.candidate_sst_files_checked, 2);
    assert_eq!(legacy.as_deref(), Some(b"value".as_slice()));
    assert_eq!(diagnostics.snapshot().candidate_sst_files_checked, 3);
    Ok(())
}

#[test]
fn should_keep_uncertain_l0_bounds_in_newest_first_fallback_order() {
    // Arrange
    let mut files = Vec::new();
    for (index, (complete, minimum, maximum)) in [
        (false, Some(b"x".to_vec()), Some(b"z".to_vec())),
        (true, None, Some(b"z".to_vec())),
        (true, Some(b"z".to_vec()), Some(b"a".to_vec())),
    ]
    .into_iter()
    .enumerate()
    {
        files.push(FileMeta {
            name: format!("uncertain-{index}.sst"),
            sst_seq: index as u64,
            smallest_key: minimum,
            largest_key: maximum,
            key_bounds_complete: complete,
            ..FileMeta::default()
        });
    }
    let view = SstReadView::new(0, files);
    // Act
    let candidates = view.point_candidates(b"m");
    // Assert
    assert_eq!(
        candidates
            .iter()
            .map(|file| file.name.as_str())
            .collect::<Vec<_>>(),
        ["uncertain-2.sst", "uncertain-1.sst", "uncertain-0.sst"]
    );
}

#[test]
fn should_preserve_range_tombstones_when_l0_file_endpoints_are_inclusive() -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let factory = FsSstFactoryIo::new(Arc::new(RealFs::new(directory.path())?), 4096);
    let mut files = Vec::new();
    for tombstone in [false, true] {
        let name = format!("endpoint-{tombstone}.sst");
        let mut writer = factory.create()?;
        if tombstone {
            writer.add_range_tombstone(b"a", b"z", 2)?;
        } else {
            for key in [b"a", b"m", b"z"] {
                writer.add_with_meta(key, Some(b"visible"), 1, 0, None)?;
            }
        }
        let bytes = writer.finish_bytes()?;
        std::fs::write(directory.path().join(&name), &bytes)?;
        files.push(FileMeta {
            name,
            size_bytes: bytes.len() as u64,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"z".to_vec()),
            key_bounds_complete: true,
            ..FileMeta::default()
        });
    }
    let (snapshot, diagnostics) = snapshot(directory.path(), files)?;
    // Act
    let outside = snapshot.get(b"zz", u64::MAX)?;
    let outside_work = diagnostics.snapshot();
    let start = snapshot.get(b"a", u64::MAX)?;
    let middle = snapshot.get(b"m", u64::MAX)?;
    let end = snapshot.get(b"z", u64::MAX)?;
    // Assert
    assert!(outside.is_none());
    assert_eq!(outside_work.sst_reader_cache_misses, 0);
    assert!(start.is_none());
    assert!(middle.is_none());
    assert_eq!(end.as_deref(), Some(b"visible".as_slice()));
    assert_eq!(diagnostics.snapshot().sst_reader_cache_misses, 2);
    Ok(())
}
