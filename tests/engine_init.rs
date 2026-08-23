mod common;
use cntryl_midge::TransactionMode;
use common::*;

#[test]
fn should_create_engine_in_all_modes() {
    // Arrange
    // (Mode and options provided by for_each_storage_mode)

    // Act
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let result = cntryl_midge::MidgeEngine::open(opts.to_open_options());

        // Assert: construction succeeded...
        let engine = match result {
            Ok(engine) => engine,
            Err(e) => panic!("Failed to create engine in mode {mode}: {e}"),
        };

        // ...and the opened engine is actually usable, not merely
        // constructed: it holds a healthy primary lease, exposes the
        // default column family, and can round-trip a write.
        assert!(
            engine.is_primary_lease_healthy(),
            "primary lease should be healthy immediately after open in mode: {mode}"
        );
        let cf = engine
            .get_column_family("default")
            .unwrap_or_else(|| panic!("default column family missing in mode: {mode}"));

        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap_or_else(|e| panic!("begin_tx failed in mode {mode}: {e}"));
        tx.put(b"init_probe".to_vec(), b"ok".to_vec(), None)
            .unwrap_or_else(|e| panic!("put failed in mode {mode}: {e}"));
        tx.commit(buffered_write_options(mode))
            .unwrap_or_else(|e| panic!("commit failed in mode {mode}: {e}"));

        let read_tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap_or_else(|e| panic!("begin_tx (read) failed in mode {mode}: {e}"));
        assert_eq!(
            read_tx.get(b"init_probe").ok().flatten(),
            Some(bytes::Bytes::from_static(b"ok")),
            "engine opened in mode {mode} could not read back a write it just committed"
        );

        println!("Engine created successfully in mode: {mode}");
    });
}
