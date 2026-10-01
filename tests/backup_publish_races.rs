//! Deterministic target creation at the final publication boundary.
#![cfg(feature = "failpoints")]
use cntryl_midge::{Engine, MidgeError, OpenOptions};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn should_preserve_target_created_immediately_before_publication() {
    // Arrange
    let _scenario = fail::FailScenario::setup();
    let directory = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(
        OpenOptions::local(directory.path().join("source"))
            .build()
            .unwrap(),
    )
    .unwrap();
    let artifact = directory.path().join("artifact");
    engine
        .backup_to(&artifact, Duration::from_secs(10))
        .unwrap();
    for restore in [false, true] {
        #[cfg(unix)]
        let modes = [false, true];
        #[cfg(not(unix))]
        let modes = [false];
        for symlink in modes {
            let target = directory.path().join(format!("target-{restore}-{symlink}"));
            let owned_target = target.clone();
            let foreign = directory
                .path()
                .join(format!("foreign-{restore}-{symlink}"));
            std::fs::create_dir(&foreign).unwrap();
            std::fs::write(foreign.join("retain"), b"foreign").unwrap();
            #[cfg(unix)]
            let owned_foreign = foreign.clone();
            let fired = Arc::new(AtomicBool::new(false));
            let callback_fired = fired.clone();
            let trigger = if restore {
                "midge::backup::before_restore_publish"
            } else {
                "midge::backup::before_backup_publish"
            };
            fail::cfg_callback(trigger, move || {
                if symlink {
                    #[cfg(unix)]
                    std::os::unix::fs::symlink(&owned_foreign, &owned_target).unwrap();
                } else {
                    std::fs::create_dir(&owned_target).unwrap();
                }
                callback_fired.store(true, Ordering::SeqCst);
            })
            .unwrap();

            // Act
            let result = if restore {
                Engine::restore_backup(&artifact, OpenOptions::local(&target).build().unwrap())
            } else {
                engine.backup_to(&target, Duration::from_secs(10))
            };
            fail::remove(trigger);

            // Assert
            assert!(fired.load(Ordering::SeqCst));
            assert!(
                matches!(result, Err(MidgeError::InvalidArgument(_))),
                "{result:?}"
            );
            assert_eq!(std::fs::read(foreign.join("retain")).unwrap(), b"foreign");
            if !symlink {
                assert!(target.read_dir().unwrap().next().is_none());
            }
            assert!(directory.path().read_dir().unwrap().all(|entry| {
                let name = entry.unwrap().file_name();
                !name.to_string_lossy().starts_with(".midge-")
            }));
        }
    }
    engine.shutdown(Duration::from_secs(10)).unwrap();
}
