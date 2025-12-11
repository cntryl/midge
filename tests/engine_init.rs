use cntryl_midge::testkit::*;

#[test]
fn should_create_engine_in_all_modes() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let result = cntryl_midge::MidgeEngine::open_with_options(opts);
        match result {
            Ok(_) => println!("Engine created successfully in mode: {}", mode),
            Err(e) => panic!("Failed to create engine in mode {}: {}", mode, e),
        }
    });
}

#[test]
fn should_create_engine_in_memory() {
    let opts = opts_for_mode("memory");
    let result = cntryl_midge::MidgeEngine::open_with_options(opts);
    match result {
        Ok(_) => println!("Engine created successfully"),
        Err(e) => panic!("Failed to create engine: {}", e),
    }
}

#[test]
fn should_create_engine_in_local_mode() {
    let opts = opts_for_mode("local");
    let result = cntryl_midge::MidgeEngine::open_with_options(opts);
    match result {
        Ok(_) => println!("Engine created successfully in local mode"),
        Err(e) => panic!("Failed to create engine in local mode: {}", e),
    }
}

#[test]
fn should_create_engine_in_cloud_mode() {
    let opts = opts_for_mode("cloud");
    let result = cntryl_midge::MidgeEngine::open_with_options(opts);
    match result {
        Ok(_) => println!("Engine created successfully in cloud mode"),
        Err(e) => panic!("Failed to create engine in cloud mode: {}", e),
    }
}
