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

/// Replace the body of the open WAL segment a seeded database appends to,
/// segment 1, with `data`, keeping its header: frames, not the header, are
/// what WAL recovery fuzzes (#843).
pub fn write_open_segment_body(path: &Path, data: &[u8]) {
    let relative = "wal/00000000000000000001.wal";
    let mut bytes = std::fs::read(path.join(relative)).unwrap_or_default();
    bytes.truncate(cntryl_midge::__internal::wal::segment::HEADER_LEN);
    bytes.extend_from_slice(data);
    write_relative(path, relative, &bytes);
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
