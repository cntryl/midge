//! Actual bounded WAL replay checkpoint accounting; seeded rows are not public ACK claims.

use super::*;
use crate::metadata::accounting::{Medium, Origin};
use std::time::Duration;

fn verify_recovered(engine: &Engine, records: u64) {
    let cf = engine.get_column_family("default").unwrap();
    let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    for sequence in 1..=records {
        assert_eq!(
            tx.get(&sequence.to_be_bytes()).unwrap().as_deref(),
            Some(value(sequence).as_slice())
        );
    }
    let rows = tx
        .scan(&crate::Query::new())
        .unwrap()
        .collect::<crate::common::MidgeResult<Vec<_>>>()
        .unwrap();
    assert_eq!(rows.len(), usize::try_from(records).unwrap());
    for (index, (key, observed)) in rows.iter().enumerate() {
        let sequence = u64::try_from(index).unwrap() + 1;
        assert_eq!(key.as_ref(), sequence.to_be_bytes());
        assert_eq!(observed.as_ref(), value(sequence).as_slice());
    }
}

#[test]
fn should_attribute_recovery_publication_when_real_wal_exceeds_local_capacity() {
    crate::failpoints::with_read_gate(|| {
        // Arrange: the existing fixture encodes real records and authoritative catalog proofs.
        let directory = tempfile::tempdir().unwrap();
        let budget = 256 * 1024_u64;
        let options = options(directory.path(), budget);
        let mut initial = Engine::open(options.clone()).unwrap();
        let first_owner = initial.checkpoint_metrics().snapshot().owner_id;
        initial.shutdown(Duration::from_secs(30)).unwrap();
        drop(initial);
        let records = budget * 3 / 2048;
        let source = publish_wal(directory.path(), records);
        assert!(u64::try_from(source.len()).unwrap() > budget);

        // Act: actual recovery streams the backlog through bounded genuine SST checkpoints.
        let mut recovered = Engine::open(options.clone()).unwrap();
        let retained = recovered.checkpoint_metrics();
        let at_open = retained.snapshot();
        verify_recovered(&recovered, records);
        recovered.shutdown(Duration::from_secs(30)).unwrap();
        drop(recovered);
        let original_final = retained.snapshot();
        let mut reopened = Engine::open(options).unwrap();
        verify_recovered(&reopened, records);
        reopened.shutdown(Duration::from_secs(30)).unwrap();

        // Assert: actual recovery publications precede runtime; ordinary ratios stay empty.
        assert_ne!(at_open.owner_id, first_owner);
        let recovery = &at_open
            .bucket(Origin::Recovery, Medium::Persistent)
            .counters;
        assert!(recovery.flush_committed_count > 1);
        assert_eq!(
            recovery.publication_attempts,
            recovery.flush_committed_count
        );
        assert_eq!(recovery.publication_failures, 0);
        assert!(recovery.flush_committed_sst_bytes > 0);
        assert!(recovery.flush_full_publication_elapsed_ns > 0);
        assert!(recovery.checkpoint_complete_count > 0);
        for origin in [
            Origin::OrdinaryLocalFlush,
            Origin::CloudFlush,
            Origin::Shutdown,
        ] {
            assert_eq!(
                at_open
                    .bucket(origin, Medium::Persistent)
                    .counters
                    .flush_committed_count,
                0
            );
        }
        assert_eq!(at_open.incomplete_observations, 0);
        assert_eq!(
            serde_json::to_value(retained.snapshot()).unwrap(),
            serde_json::to_value(original_final).unwrap()
        );
    });
}
