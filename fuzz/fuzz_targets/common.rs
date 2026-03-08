use cntryl_midge::{Engine, OpenOptions, RecoveryPolicy};
use std::path::Path;

pub fn seed_db(path: &Path) {
    let _ = std::fs::create_dir_all(path);
    let _ = Engine::open(OpenOptions::local(path).build());
}

pub fn write_relative(path: &Path, relative: &str, data: &[u8]) {
    let file_path = path.join(relative);
    if let Some(parent) = file_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(file_path, data);
}

pub fn exercise_open_and_verify(path: &Path) {
    let _ = Engine::verify_path(path);
    let _ = Engine::open(
        OpenOptions::local(path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build(),
    );
    let _ = Engine::open(
        OpenOptions::local(path)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build(),
    );
}
