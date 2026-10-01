//! Logical discovery driver. Crash prefixes, races, retries, TTL and conflict
//! policy probes require separate drivers; this target claims none of them.

mod common;

#[path = "discovery/driver.rs"]
mod driver;
#[path = "discovery/histories.rs"]
mod histories;
#[path = "discovery/model.rs"]
mod model;

#[cfg(feature = "failpoints")]
#[path = "discovery/physical.rs"]
mod physical;

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
        #[cfg(feature = "sqrzl-tests")]
        "sqrzl" => (
            16,
            64,
            [
                driver::Backend::SqrzlS3,
                driver::Backend::SqrzlAzure,
                driver::Backend::SqrzlGcsXml,
                driver::Backend::SqrzlGcsJson,
            ]
            .into_iter()
            .flat_map(|backend| {
                [driver::ReadPath::Resident, driver::ReadPath::Spilled]
                    .map(|read_path| driver::Fixture { backend, read_path })
            })
            .collect(),
        ),
        _ => panic!("unknown discovery profile: {profile}"),
    };
    let histories = histories::generate(count, max_operations);

    // Act
    // Assert: every public observation and final committed state must agree.
    driver::campaign(&profile, &histories, &fixtures);
}
