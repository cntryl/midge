//! Finalized-SST multipart contracts. Transport and measurement live in Storage.

use cntryl_midge_core::common::resource_budget::ResourceBudget;
use cntryl_midge_core::common::MidgeResult;
use cntryl_midge_core::io::FsError;
use cntryl_midge_storage::providers::multipart_contract_tests::{
    upload_fixture, Protocol, UploadFault, UploadPath,
};
use cntryl_midge_table::sst::SstFactory;

fn finalized_sst_source(directory: &std::path::Path) -> MidgeResult<(Vec<u8>, u64, u32)> {
    let fs = std::sync::Arc::new(
        cntryl_midge_core::io::RealFs::new(directory).map_err(FsError::into_midge)?,
    );
    let factory = cntryl_midge_table::sst::FsSstFactoryIo::new(fs, 4096);
    let mut writer = factory.create_for_compaction(ResourceBudget::new(64 * 1024 * 1024))?;
    let mut value = vec![0_u8; 4096];
    let mut random = 0x9e37_79b9_u32;
    for sequence in 0..=3583_u64 {
        for byte in &mut value {
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            *byte = u8::try_from(random & u32::from(u8::MAX))
                .expect("masked pseudo-random byte fits in u8");
        }
        writer.add_sorted_with_meta(
            &sequence.to_be_bytes(),
            Some(&value),
            sequence + 1,
            cntryl_midge_core::types::EntryType::Put,
            None,
        )?;
    }
    let finalized = writer
        .finish_to_path_with_summary(std::path::Path::new("source"))?
        .expect("filesystem writer returns finalized SST identity");
    assert!(finalized.size_bytes >= (cntryl_midge_storage::FILE_UPLOAD_PART_BYTES * 2) as u64);
    Ok((
        std::fs::read(directory.join("source"))?,
        finalized.size_bytes,
        finalized.content_crc32c,
    ))
}

#[test]
fn should_stream_finalized_sst_once_when_native_providers_publish_files() -> MidgeResult<()> {
    // Arrange: a production SST supplies the immutable file identity.
    for protocol in [
        Protocol::S3,
        Protocol::Azure,
        Protocol::GcsJson,
        Protocol::GcsXml,
    ] {
        // Act: publish through the native multipart path.
        // Assert: the fixture checks exact bytes and one source pass.
        upload_fixture(
            protocol,
            UploadFault::None,
            UploadPath::Provider,
            finalized_sst_source,
        )?;
    }
    Ok(())
}

#[test]
fn should_reject_changed_finalized_files_before_commit_when_streaming_upload_reads_them(
) -> MidgeResult<()> {
    // Arrange: mutate the finalized source for every native provider.
    for protocol in [
        Protocol::S3,
        Protocol::Azure,
        Protocol::GcsJson,
        Protocol::GcsXml,
    ] {
        // Act: try to publish the mutated file through hybrid storage.
        // Assert: the fixture requires rejection before the object is committed.
        upload_fixture(
            protocol,
            UploadFault::MutatedSource,
            UploadPath::Hybrid,
            finalized_sst_source,
        )?;
    }
    Ok(())
}
