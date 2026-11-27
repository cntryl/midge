use std::time::Duration;

use cntryl_midge::{Bytes, Engine, OpenOptions, Query, TransactionMode, WriteOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::open(OpenOptions::in_memory().build()?)?;
    let default_cf = engine
        .get_column_family("default")
        .expect("new engines contain the default column family");

    let mut write_tx = engine.begin_tx(default_cf.id(), TransactionMode::ReadWrite)?;
    write_tx.put(b"user:1".to_vec(), b"alice".to_vec(), None)?;
    write_tx.put(b"user:2".to_vec(), b"bob".to_vec(), None)?;
    write_tx.commit(WriteOptions::sync())?;

    let read_tx = engine.begin_tx(default_cf.id(), TransactionMode::ReadOnly)?;
    assert_eq!(read_tx.get(b"user:1")?, Some(Bytes::from_static(b"alice")));

    let query = Query::new().prefix(Bytes::from_static(b"user:"));
    let rows = read_tx.scan(&query)?.try_collect()?;
    assert_eq!(rows.len(), 2);
    drop(read_tx);

    engine.shutdown(Duration::from_secs(5))?;
    Ok(())
}
