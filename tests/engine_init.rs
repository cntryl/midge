mod common;
use common::*;

#[test]
fn should_create_engine_in_all_modes() {
    // Arrange
    // (Mode and options provided by for_each_storage_mode)

    // Act
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let result = cntryl_midge::MidgeEngine::open(opts.to_open_options());

        // Assert
        match result {
            Ok(_) => println!("Engine created successfully in mode: {mode}"),
            Err(e) => panic!("Failed to create engine in mode {mode}: {e}"),
        }
    });
}
