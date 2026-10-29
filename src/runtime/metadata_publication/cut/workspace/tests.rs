use super::*;
use crate::common::resource_budget::ResourceBudget;
use crate::io::{Fs, FsPath};
use crate::metadata::Manifest;
use crate::runtime::{CapturedMetadataCut, IntentLogEntry};

struct AdmissionFixture {
    _directory: tempfile::TempDir,
    fs: Arc<dyn Fs>,
    lock: MetadataPublicationLock,
    memory: ResourceBudget,
}

#[derive(serde::Serialize)]
struct FirstEdit<'a> {
    sequence: u64,
    previous_checksum: u32,
    keep: usize,
    append: &'a [IntentLogEntry],
}

fn fixture(snapshot: &[u8], intents: &[IntentLogEntry]) -> AdmissionFixture {
    let directory = tempfile::tempdir().unwrap();
    let fs = crate::io::real::test_root(directory.path());
    crate::metadata::ensure_or_create_format_marker(&fs, directory.path()).unwrap();
    std::fs::write(directory.path().join(files::MANIFEST_SNAPSHOT), snapshot).unwrap();
    if !intents.is_empty() {
        let bytes = crate::metadata::codec::encode(
            crate::metadata::codec::INTENT,
            &FirstEdit {
                sequence: 1,
                previous_checksum: 0,
                keep: 0,
                append: intents,
            },
        )
        .unwrap();
        // Include decoder-accepted shapes with invalid SST names: admission
        // must precede the later semantic validator as well as allocation.
        std::fs::write(directory.path().join(files::INTENT_LOG), bytes).unwrap();
    }
    let encoded = files::CLOUD_MIRRORED
        .iter()
        .map(|name| {
            fs.metadata(&FsPath::new(*name))
                .map_or(0, |metadata| usize::try_from(metadata.len).unwrap())
        })
        .sum::<usize>();
    let memory = ResourceBudget::new(encoded * 16 + 4096);
    let lock = MetadataPublicationLock::default();
    lock.configure_capture_memory(memory.clone());
    AdmissionFixture {
        _directory: directory,
        fs,
        lock,
        memory,
    }
}

fn capture(fixture: &AdmissionFixture) -> MidgeResult<Arc<CapturedMetadataCut>> {
    fixture.lock.capture_control_cut(
        &fixture.fs,
        1,
        &OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
    )
}

fn assert_refused_without_losing_source(
    fixture: &AdmissionFixture,
    name: &str,
    before: &[u8],
    result: MidgeResult<Arc<CapturedMetadataCut>>,
) {
    let Err(error) = result else {
        panic!("dense typed decode must require independent admission")
    };
    assert!(
        matches!(error, MidgeError::ResourceLimit(message) if message.contains("typed workspace"))
    );
    assert_eq!(fixture.lock.pending_captures(), 0);
    assert_eq!(fixture.memory.used(), 0);
    assert_eq!(
        crate::io::read_all(fixture.fs.as_ref(), &FsPath::new(name)).unwrap(),
        before
    );
}

#[test]
fn should_refuse_dense_file_slots_before_allocating_an_unadmitted_manifest() {
    // Arrange: Serde accepts compact positional FileMeta structures before
    // semantic SST-name validation. Their real typed slots exceed 16x wire.
    let mut raw = serde_json::to_value(Manifest::default()).unwrap();
    raw["files"] = serde_json::json!(vec![serde_json::json!(["", 0, 0]); 8192]);
    let encoded = crate::metadata::codec::encode(crate::metadata::codec::MANIFEST, &raw).unwrap();
    let decoded: Manifest =
        crate::metadata::codec::decode(crate::metadata::codec::MANIFEST, &encoded).unwrap();
    let actual_slots = decoded.files.capacity() * std::mem::size_of::<crate::metadata::FileMeta>();
    assert!(decoded.validate_persisted().is_err());
    drop(decoded);
    let fixture = fixture(&encoded, &[]);
    assert!(actual_slots > fixture.memory.limit());

    // Act
    let result = capture(&fixture);

    // Assert: refuse before typed decode and retain the original source.
    assert_refused_without_losing_source(&fixture, files::MANIFEST_SNAPSHOT, &encoded, result);
}

#[test]
fn should_refuse_dense_name_descriptors_before_allocating_unadmitted_intents() {
    // Arrange: empty names still own String descriptors in the typed Vec.
    let snapshot =
        crate::metadata::codec::encode(crate::metadata::codec::MANIFEST, &Manifest::default())
            .unwrap();
    let fixture = fixture(
        &snapshot,
        &[IntentLogEntry::CompactionPlanned {
            input_files: vec![String::new(); 8192],
            output_level: 1,
        }],
    );
    assert!(8192 * std::mem::size_of::<String>() > fixture.memory.limit());
    let original =
        crate::io::read_all(fixture.fs.as_ref(), &FsPath::new(files::INTENT_LOG)).unwrap();

    // Act
    let result = capture(&fixture);

    // Assert
    assert_refused_without_losing_source(&fixture, files::INTENT_LOG, &original, result);
}

#[test]
fn should_admit_dense_scalar_intents_without_charging_publication_witnesses() {
    // Arrange: scalar records expand to the real enum size but create no
    // publication boundary, compact witness, or normalization buffer.
    let snapshot =
        crate::metadata::codec::encode(crate::metadata::codec::MANIFEST, &Manifest::default())
            .unwrap();
    let intents = (1..=3000)
        .map(|seqno| IntentLogEntry::SeqnoAllocated { seqno, cf_id: 0 })
        .collect::<Vec<_>>();
    let fixture = fixture(&snapshot, &intents);
    let memory = fixture.memory.clone();

    // Act
    let cut = capture(&fixture).unwrap();

    // Assert: the original minimum configured pool still makes progress.
    assert!(cut.terminal_witnesses().is_none());
    assert_eq!(fixture.lock.pending_captures(), 1);
    assert!(memory.used() <= memory.limit());
    drop(cut);
    drop(fixture);
    assert_eq!(memory.used(), 0);
}
