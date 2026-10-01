//! Logical discovery driver. Crash prefixes, races, retries, TTL and conflict
//! policy probes require separate drivers; this target claims none of them.

#[path = "discovery/driver.rs"]
mod driver;
#[path = "discovery/histories.rs"]
mod histories;
#[path = "discovery/model.rs"]
mod model;

#[test]
fn should_match_transaction_oracle_when_replaying_histories() {
    // Arrange
    if let Some(path) = std::env::var_os("MIDGE_DISCOVERY_REPLAY") {
        // The saved concrete counterexample must pass after a repair.
        driver::replay(std::path::Path::new(&path));
        return;
    }
    let profile = std::env::var("MIDGE_DISCOVERY_PROFILE").unwrap_or_else(|_| "smoke".into());
    let (count, max_operations, fixtures) = match profile.as_str() {
        "smoke" => (
            1,
            64,
            vec![driver::Fixture {
                backend: driver::Backend::Local,
                read_path: driver::ReadPath::Resident,
            }],
        ),
        "pr" => (
            32,
            64,
            vec![
                driver::Fixture {
                    backend: driver::Backend::Local,
                    read_path: driver::ReadPath::Resident,
                },
                driver::Fixture {
                    backend: driver::Backend::Local,
                    read_path: driver::ReadPath::Spilled,
                },
            ],
        ),
        "discovery" | "release" => (
            256,
            256,
            vec![
                driver::Fixture {
                    backend: driver::Backend::Local,
                    read_path: driver::ReadPath::Resident,
                },
                driver::Fixture {
                    backend: driver::Backend::Local,
                    read_path: driver::ReadPath::Spilled,
                },
                driver::Fixture {
                    backend: driver::Backend::CloudSimulated,
                    read_path: driver::ReadPath::Resident,
                },
                driver::Fixture {
                    backend: driver::Backend::CloudSimulated,
                    read_path: driver::ReadPath::Spilled,
                },
            ],
        ),
        _ => panic!("unknown discovery profile: {profile}"),
    };
    let histories = histories::generate(count, max_operations);

    // Act
    // Assert: every public observation and final committed state must agree.
    driver::campaign(&profile, &histories, &fixtures);
}
