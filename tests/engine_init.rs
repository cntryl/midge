use cntryl_midge::testkit::*;

#[test]
fn should_create_engine_in_all_modes() {
    // Arrange
    // (Mode and options provided by for_each_storage_mode)

    // Act
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let result = cntryl_midge::MidgeEngine::open_with_options(opts);

        // Assert
        match result {
            Ok(_) => println!("Engine created successfully in mode: {}", mode),
            Err(e) => panic!("Failed to create engine in mode {}: {}", mode, e),
        }
    });
}

#[test]
fn should_create_engine_in_memory() {
    // Arrange
    let opts = opts_for_mode("memory");

    // Act
    let result = cntryl_midge::MidgeEngine::open_with_options(opts);

    // Assert
    match result {
        Ok(_) => println!("Engine created successfully"),
        Err(e) => panic!("Failed to create engine: {}", e),
    }
}

#[test]
fn should_create_engine_in_local_mode() {
    // Arrange
    let opts = opts_for_mode("local");

    // Act
    let result = cntryl_midge::MidgeEngine::open_with_options(opts);

    // Assert
    match result {
        Ok(_) => println!("Engine created successfully in local mode"),
        Err(e) => panic!("Failed to create engine in local mode: {}", e),
    }
}

#[test]
fn should_create_engine_in_cloud_mode() {
    // Arrange
    let opts = opts_for_mode("cloud");

    // Act
    let result = cntryl_midge::MidgeEngine::open_with_options(opts);

    // Assert
    match result {
        Ok(_) => println!("Engine created successfully in cloud mode"),
        Err(e) => panic!("Failed to create engine in cloud mode: {}", e),
    }
}
