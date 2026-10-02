use cntryl_midge::{Engine, OpenOptions, RecoveryPolicy};
use std::path::Path;
use std::time::Duration;

pub fn seed_db(path: &Path) {
    let _ = std::fs::create_dir_all(path);
    let result = Engine::open(
        OpenOptions::local(path)
            .build()
            .expect("build strict recovery options"),
    );
    finish_open(result);
}

pub fn write_relative(path: &Path, relative: &str, data: &[u8]) {
    let file_path = path.join(relative);
    if let Some(parent) = file_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(file_path, data);
}

pub fn exercise_open_and_verify(path: &Path) {
    let _ = cntryl_midge::StorageVerifier::verify_path(path);
    let result = Engine::open(
        OpenOptions::local(path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build()
            .expect("build strict recovery options"),
    );
    finish_open(result);
    let result = Engine::open(
        OpenOptions::local(path)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build()
            .expect("build salvage recovery options"),
    );
    finish_open(result);
}

fn finish_open(result: cntryl_midge::MidgeResult<Engine>) {
    if let Ok(mut engine) = result {
        // Drop delegates teardown to a detached reaper. Complete it before
        // mutating this fixture or opening the next recovery attempt.
        engine
            .shutdown(Duration::from_secs(5))
            .expect("successful fuzz open must finish shutdown");
    }
}
