use super::*;

#[test]
fn should_bound_warm_reconstruction_when_exact_coverage_probes_one_real_block() {
    let mut bounded = true;
    for count in [128_u64, 256] {
        // Arrange: one real uncompressed, checksummed block behind the remote adapter.
        let dir = tempfile::tempdir().unwrap();
        let fs = Arc::new(crate::io::RealFs::new(dir.path()).unwrap());
        let factory = crate::sst::FsSstFactoryIo::new(fs.clone(), 64 * 1024)
            .with_compression_policy(crate::codec::CompressionPolicy::None);
        let mut writer = factory.create().unwrap();
        for index in 0..count {
            writer
                .add_with_meta(
                    format!("key-{index:08}").as_bytes(),
                    Some(b"value"),
                    7,
                    crate::types::EntryType::Put,
                    Some(123),
                )
                .unwrap();
        }
        let bytes = writer.finish_bytes().unwrap();
        std::fs::write(dir.path().join("test.sst"), &bytes).unwrap();
        let proof_reader = crate::sst::fs::SstFileIo::open("test.sst", fs.clone()).unwrap();
        assert_eq!(proof_reader.verify_all_blocks().unwrap().data_blocks, 1);
        let name = crate::cloud_layout::file_name(0, 0, 1);
        std::fs::create_dir_all(dir.path().join("cloud/sst")).unwrap();
        std::fs::write(dir.path().join("cloud/sst").join(&name), &bytes).unwrap();
        let manifest = crate::metadata::Manifest {
            files: vec![crate::metadata::FileMeta {
                name,
                cf_id: 0,
                size_bytes: bytes.len() as u64,
                content_crc32c: Some(crc32c::crc32c(&bytes)),
                smallest_key: Some(b"key-00000000".to_vec()),
                largest_key: Some(format!("key-{:08}", count - 1).into_bytes()),
                smallest_seq: Some(7),
                largest_seq: Some(7),
                ..Default::default()
            }],
            ..Default::default()
        };
        let cloud = Arc::new(
            crate::storage::filesystem::FileSystem::new(dir.path().join("cloud")).unwrap(),
        );
        let remote = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
            fs,
            cloud,
            std::time::Duration::from_secs(5),
        ));
        let mut coverage = ReplayCoverage::new(manifest, remote, 512 * 1024);
        let reads = Arc::new(RangeCounter::default());
        coverage.fs = coverage.fs.with_read_observer(reads.clone()).unwrap();
        let record = |index| {
            let mut record = WalRecord::new(
                WalOpKind::Put,
                Bytes::from(format!("key-{index:08}")),
                Some(Bytes::from_static(b"value")),
                7,
                1,
            );
            record.expiration = Some(123);
            record
        };

        // Act: warm real immutable proof, then probe every key without remote reads.
        assert!(coverage.contains(&record(0)));
        assert!(coverage.verified_bytes.get() > 0);
        let warm_reads = reads.0.load(std::sync::atomic::Ordering::Relaxed);
        for index in 0..count {
            assert!(coverage.contains(&record(index)));
        }
        let (steps, allocations) = coverage.readers.borrow()[0].reader.recovery_decode_stats();
        let peak = coverage.read_budget.peak();
        let blocks = coverage.block_stats();
        coverage.release_reader();

        // Assert: a bounded entries+probes count replaces triangular prefix work.
        eprintln!("count={count} completed_decode_steps={steps} reconstruction_allocations={allocations} peak={peak}");
        assert_eq!(
            reads.0.load(std::sync::atomic::Ordering::Relaxed),
            warm_reads
        );
        assert_eq!(blocks.1, 1);
        assert!(peak > 0 && peak <= 512 * 1024);
        assert_eq!(coverage.read_budget.used(), 0);
        bounded &= steps <= 4 * count + 4 && allocations <= 2;
    }
    assert!(
        bounded,
        "warm reconstruction exceeds entries+probes work/allocation bounds"
    );
}
