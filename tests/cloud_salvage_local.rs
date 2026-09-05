//! Explicit salvage may read a verified local SST when cloud authority is lost.

use cntryl_midge::{
    Engine, EngineHealth, OpenOptions, RecoveryPolicy, TransactionMode, WriteOptions,
};
use std::time::Duration;

#[test]
fn should_read_verified_local_sst_during_salvage_when_remote_object_is_missing_or_invalid() {
    for remote_failure in ["missing", "truncated", "same-size corruption"] {
        // Arrange
        let dir = tempfile::tempdir().expect("database directory");
        let options = OpenOptions::cloud_simulated(dir.path(), "bucket", "salvage-local")
            .background_compaction(false)
            .build()
            .expect("options");
        let mut engine = Engine::open(options).expect("open");
        let cf = engine.create_column_family("data").expect("column family");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("transaction");
        tx.put(b"key".to_vec(), b"verified local value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::cloud_strict())
            .expect("cloud commit");
        engine.flush_cf(&cf).expect("flush");
        engine.shutdown(Duration::from_secs(30)).expect("shutdown");
        drop(engine);
        let remote = std::fs::read_dir(dir.path().join("cloud_store/sst"))
            .expect("remote files")
            .map(|entry| entry.expect("remote entry").path())
            .find(|path| path.extension().is_some_and(|extension| extension == "sst"))
            .expect("remote SST");
        let local = dir.path().join("sst").join(remote.file_name().unwrap());
        std::fs::copy(&remote, &local).expect("preserve valid local copy");
        match remote_failure {
            "missing" => std::fs::remove_file(&remote).expect("remove remote"),
            "truncated" => {
                std::fs::write(&remote, b"invalid remote object").expect("invalidate remote");
            }
            _ => {
                let mut bytes = std::fs::read(&remote).expect("read remote SST");
                bytes[0] ^= 1;
                std::fs::write(&remote, bytes).expect("corrupt remote without changing size");
            }
        }
        let salvage_options = OpenOptions::cloud_simulated(dir.path(), "bucket", "salvage-local")
            .background_compaction(false)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build()
            .expect("salvage options");
        // Act
        let mut reopened = Engine::open(salvage_options).expect("salvage reopen");
        let cf = reopened.get_column_family("data").expect("recovered CF");
        let tx = reopened
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read transaction");
        let actual = tx.get(b"key").expect("verified salvage read");
        // Assert
        assert_eq!(actual.as_deref(), Some(b"verified local value".as_slice()));
        assert!(
            local.exists(),
            "salvage must preserve the only verified SST copy"
        );
        assert_eq!(
            reopened.get_runtime_metrics().expect("metrics").health,
            EngineHealth::SalvageMode
        );
        drop(tx);
        reopened
            .verify_storage(Duration::from_secs(30))
            .expect("verify salvage local copy");
        reopened
            .shutdown(Duration::from_secs(30))
            .expect("salvage shutdown");
    }
}
