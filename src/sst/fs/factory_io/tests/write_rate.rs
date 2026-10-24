use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[test]
fn should_share_one_rate_schedule_across_flush_and_compaction_output() -> MidgeResult<()> {
    // Arrange: both production writer constructors use the same schedule.
    let directory = tempfile::tempdir()?;
    let fs = crate::io::real::test_root(directory.path());
    let admitted = Arc::new(AtomicU64::new(0));
    let rate = 4 * 1024 * 1024;
    let limiter = Arc::new(crate::common::write_rate::BackgroundWriteLimiter::new(
        rate,
        Arc::new(AtomicBool::new(false)),
        admitted.clone(),
    ));
    let factory = Arc::new(
        FsSstFactoryIo::new(fs.clone(), 4096)
            .with_compression_policy(CompressionPolicy::None)
            .with_write_limiter(Some(limiter)),
    );
    let budget = crate::common::resource_budget::ResourceBudget::new(8 * 1024 * 1024);
    let writers = [
        factory.create_for_flush(budget.clone())?,
        factory.create_for_compaction(budget)?,
    ];
    let started = Instant::now();

    // Act: concurrent writers must not each receive an independent allowance.
    let identities = std::thread::scope(|scope| {
        let handles = writers
            .into_iter()
            .enumerate()
            .map(|(ordinal, mut writer)| {
                scope.spawn(move || {
                    for index in 0_u32..64 {
                        writer.add_sorted_with_meta(
                            &index.to_be_bytes(),
                            Some(&[b'x'; 1024]),
                            u64::from(index) + 1,
                            EntryType::Put,
                            None,
                        )?;
                    }
                    writer.finish_to_path_with_summary(Path::new(&format!("rate-{ordinal}.sst")))
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<MidgeResult<Vec<_>>>()
    })?;

    // Assert: charge scratch and final writes, preserving exact encoded identity.
    let charged = admitted.load(Ordering::Relaxed);
    let elapsed = started.elapsed();
    let minimum = Duration::from_nanos(charged.saturating_mul(1_000_000_000) / rate);
    assert!(elapsed >= minimum.saturating_sub(Duration::from_millis(2)));
    for (ordinal, finalized) in identities.into_iter().enumerate() {
        let finalized = finalized.unwrap();
        assert!(charged >= finalized.size_bytes.saturating_mul(2));
        let bytes = crate::io::read_all(fs.as_ref(), &FsPath::new(format!("rate-{ordinal}.sst")))
            .map_err(crate::io::FsError::into_midge)?;
        assert_eq!(bytes.len() as u64, finalized.size_bytes);
        assert_eq!(crc32c::crc32c(&bytes), finalized.content_crc32c);
        let reader = factory.open(Path::new(&format!("rate-{ordinal}.sst")))?;
        assert!(
            matches!(reader.get_state(&63_u32.to_be_bytes())?, KeyState::Value(value, _, _, _) if value.as_ref() == [b'x'; 1024])
        );
    }
    Ok(())
}
