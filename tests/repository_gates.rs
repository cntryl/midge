use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_workflow(path: impl AsRef<Path>) -> String {
    let path = repository_root().join(path);
    fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read workflow {}: {error}", path.display()))
}

fn named_targets(manifest: &str, table: &str) -> BTreeSet<String> {
    let mut targets = BTreeSet::new();
    let mut in_target = false;
    let target_header = format!("[[{table}]]");

    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with("[[") {
            in_target = line == target_header;
            continue;
        }
        if in_target {
            if let Some(name) = line.strip_prefix("name = \"") {
                if let Some(name) = name.strip_suffix('"') {
                    targets.insert(name.to_owned());
                }
            }
        }
    }

    targets
}

fn manifest_features(manifest: &str) -> BTreeSet<String> {
    let mut features = BTreeSet::new();
    let mut in_features = false;

    for line in manifest.lines() {
        let line = line.trim();
        if line == "[features]" {
            in_features = true;
            continue;
        }
        if in_features && line.starts_with('[') {
            break;
        }
        if in_features {
            if let Some((name, _)) = line.split_once('=') {
                features.insert(name.trim().to_owned());
            }
        }
    }

    features
}

fn command_argument(source: &str, command: &str, argument: &str) -> Vec<String> {
    source
        .lines()
        .filter(|line| line.contains(command))
        .filter_map(|line| {
            let words: Vec<_> = line.split_whitespace().collect();
            words
                .iter()
                .position(|word| *word == argument)
                .and_then(|index| words.get(index + 1))
                .map(|value| {
                    value
                        .trim_matches(|character| matches!(character, '\'' | '"' | '`'))
                        .to_owned()
                })
        })
        .collect()
}

fn target_matches(pattern: &str, target: &str) -> bool {
    pattern
        .strip_suffix('*')
        .map_or(pattern == target, |prefix| target.starts_with(prefix))
}

#[test]
fn should_keep_production_module_size_guard_wired_into_repository_tools() {
    // Arrange
    let config = repository_root().join(".cntryl/repository.toml");
    let qualification = read_workflow(".github/workflows/qualification.yml");

    // Act
    // Assert
    assert!(config.is_file(), "repository policy must be checked in");
    let config_source = fs::read_to_string(&config)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", config.display()));
    assert!(config_source.contains("warn_lines = 1200"));
    assert!(config_source.contains("max_lines = 1600"));
    assert!(config_source.contains("src/wal/encoding.rs"));
    assert!(
        qualification.contains("cntryl-tools check-module-sizes --config .cntryl/repository.toml")
    );
}

#[test]
fn should_require_rust_formatting_when_ci_runs() {
    // Arrange
    let ci = read_workflow(".github/workflows/ci.yml");

    // Act
    let ci_formats = ci.contains("cargo fmt --check");

    // Assert
    assert!(ci_formats, "CI must reject unformatted Rust sources");
}

#[test]
fn should_keep_push_ci_focused_on_the_core_suite() {
    // Arrange
    let ci = read_workflow(".github/workflows/ci.yml");

    // Act
    let has_platform_matrix = ci.contains("windows-latest") || ci.contains("macos-latest");
    let has_cloud_or_docker_gate = ci.contains("docker compose up") || ci.contains("docker build");

    // Assert
    assert!(
        !has_platform_matrix,
        "platform coverage belongs in platform.yml"
    );
    assert!(
        !has_cloud_or_docker_gate,
        "cloud and Docker qualification must not slow the core CI workflow"
    );
}

#[test]
fn should_route_extended_gates_through_triggered_workflows() {
    // Arrange
    let cloud = read_workflow(".github/workflows/cloud.yml");
    let compatibility = read_workflow(".github/workflows/compatibility.yml");
    let platform = read_workflow(".github/workflows/platform.yml");
    let qualification = read_workflow(".github/workflows/qualification.yml");
    let docker = read_workflow(".github/workflows/docker.yml");

    // Act
    let follow_up_workflows = [&compatibility, &platform];

    // Assert
    for workflow in follow_up_workflows {
        assert!(workflow.contains("workflow_run:"));
        assert!(workflow.contains("workflows: [\"CI\"]"));
        assert!(workflow.contains("workflow_dispatch:"));
        assert!(workflow.contains("schedule:"));
    }
    assert!(cloud.contains("workflow_dispatch:"));
    assert!(cloud.contains("schedule:"));
    assert!(!cloud.contains("workflow_run:"));
    assert!(qualification.contains("pull_request:"));
    assert!(docker.contains("pull_request:"));
}

#[test]
fn should_require_rust_formatting_when_publish_runs() {
    // Arrange
    let publish = read_workflow(".github/workflows/publish.yml");

    // Act
    let publish_formats = publish.contains("cargo fmt --check");

    // Assert
    assert!(
        publish_formats,
        "publish must reject unformatted Rust sources"
    );
}

#[test]
fn should_trigger_ci_when_benchmark_changes() {
    // Arrange
    let qualification = read_workflow(".github/workflows/qualification.yml");

    // Act
    let benchmark_trigger_count = qualification.matches("\"benches/**\"").count();

    // Assert
    assert_eq!(
        benchmark_trigger_count, 2,
        "push and PR must include benches"
    );
}

#[test]
fn should_trigger_ci_when_documentation_changes() {
    // Arrange
    let qualification = read_workflow(".github/workflows/qualification.yml");

    // Act
    let documentation_trigger_count = qualification.matches("\"docs/**\"").count();

    // Assert
    assert_eq!(
        documentation_trigger_count, 2,
        "push and PR must include docs"
    );
}

#[test]
fn should_use_sqrzl_emulator_for_cloud_gates() {
    // Arrange
    let compose = read_workflow("compose.yml");
    let cloud = read_workflow(".github/workflows/cloud.yml");
    let publish = read_workflow(".github/workflows/publish.yml");
    let manifest = read_workflow("Cargo.toml");

    // Act
    let uses_sqrzl_image = compose.contains("ghcr.io/sqrzl/sqrzl-emulator@sha256:");

    // Assert
    assert!(uses_sqrzl_image);
    assert!(compose.contains("  sqrzl:"));
    assert!(cloud.contains("docker compose up -d sqrzl"));
    assert!(cloud.contains("--features sqrzl-tests"));
    assert_eq!(cloud.matches("-- --ignored --test-threads=1").count(), 2);
    assert!(cloud.contains("http://127.0.0.1:9001/healthz"));
    assert!(publish.contains("docker compose up -d sqrzl"));
    assert_eq!(publish.matches("-- --ignored --test-threads=1").count(), 2);
    assert!(manifest.contains("sqrzl-tests = []"));
}

#[test]
fn should_verify_checked_in_current_fixture_in_publish_workflow() {
    // Arrange
    let publish = read_workflow(".github/workflows/publish.yml");
    let fixture = repository_root().join("tests/fixtures/compatibility/v3_populated_v4_sst_db");

    // Act
    let uses_current_fixture =
        publish.contains("tests/fixtures/compatibility/v3_populated_v4_sst_db");

    // Assert
    assert!(fixture.is_dir(), "release compatibility fixture must exist");
    assert!(
        uses_current_fixture,
        "publish must verify the checked-in current-format fixture"
    );
    assert!(!publish.contains("tests/fixtures/compatibility/v1_empty_db"));
}

#[test]
fn should_run_full_pedantic_clippy_gate_before_publish() {
    // Arrange
    let publish = read_workflow(".github/workflows/publish.yml");

    // Act
    let uses_release_clippy_gate = publish.contains(
        "cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic",
    );

    // Assert
    assert!(uses_release_clippy_gate);
}

#[test]
fn should_publish_only_version_matching_tags_with_least_privilege() {
    // Arrange
    let publish = read_workflow(".github/workflows/publish.yml");

    // Act
    // Assert
    assert!(publish.contains("tags:\n      - \"v*\""));
    assert!(!publish.contains("workflow_dispatch"));
    assert!(publish.contains("test \"${GITHUB_REF_NAME}\" = \"v${crate_version}\""));
    assert!(publish.contains("environment: crates-io"));
    assert!(publish.contains("id-token: write"));
    assert!(!publish.contains("contents: write"));
}

#[test]
fn should_use_versioned_external_actions() {
    // Arrange
    let workflows = [
        read_workflow(".github/workflows/bench.yml"),
        read_workflow(".github/workflows/ci.yml"),
        read_workflow(".github/workflows/cleanup.yml"),
        read_workflow(".github/workflows/cloud.yml"),
        read_workflow(".github/workflows/compatibility.yml"),
        read_workflow(".github/workflows/fuzz.yml"),
        read_workflow(".github/workflows/docker.yml"),
        read_workflow(".github/workflows/platform.yml"),
        read_workflow(".github/workflows/qualification.yml"),
        read_workflow(".github/workflows/publish.yml"),
    ];

    // Act
    let action_uses = workflows
        .iter()
        .flat_map(|workflow| workflow.lines())
        .filter_map(|line| line.trim().strip_prefix("uses: "));

    // Assert
    for action in action_uses {
        let revision = action
            .rsplit_once('@')
            .map(|(_, revision)| revision.split_whitespace().next().unwrap_or_default())
            .expect("external action must include a revision");
        assert!(
            revision.starts_with('v') && revision.len() > 1,
            "external action must use a version tag: {action}"
        );
    }
}

#[test]
fn should_pin_release_tools_to_immutable_revisions() {
    // Arrange
    let publish = read_workflow(".github/workflows/publish.yml");

    // Act
    // Assert
    assert!(publish.contains("cargo install --git https://github.com/cntryl/tools --rev "));
}

#[test]
fn should_test_supported_build_matrix_before_release() {
    // Arrange
    let ci = read_workflow(".github/workflows/ci.yml");
    let platform = read_workflow(".github/workflows/platform.yml");
    let compatibility = read_workflow(".github/workflows/compatibility.yml");

    // Act
    // Assert
    assert!(ci.contains("runs-on: ubuntu-latest"));
    assert!(platform.contains("os: [windows-latest, macos-latest]"));
    assert!(ci.contains("cargo test --workspace --all-features"));
    assert!(compatibility.contains("rustup toolchain install 1.97"));
    assert!(compatibility.contains("rustup run 1.97 cargo check --workspace --all-targets"));
    assert!(compatibility
        .contains("rustup run 1.97 cargo clippy --workspace --all-targets --no-default-features"));
}

#[test]
fn should_trigger_ci_for_repository_contract_changes() {
    // Arrange
    let qualification = read_workflow(".github/workflows/qualification.yml");
    let docker = read_workflow(".github/workflows/docker.yml");

    // Act
    // Assert
    for path in ["examples/**", ".cntryl/**", "fuzz/**"] {
        assert_eq!(
            qualification.matches(&format!("\"{path}\"")).count(),
            2,
            "push and pull requests must include {path}"
        );
    }
    assert_eq!(docker.matches("\"Dockerfile*\"").count(), 2);
}

#[test]
fn should_reference_only_manifest_features_when_docker_images_build() {
    // Arrange
    let manifest = read_workflow("Cargo.toml");
    let known_features = manifest_features(&manifest);
    let dockerfiles = [
        read_workflow("Dockerfile.tests"),
        read_workflow("Dockerfile.benches"),
    ];

    // Act
    let referenced_features: Vec<_> = dockerfiles
        .iter()
        .flat_map(|dockerfile| command_argument(dockerfile, "cargo ", "--features"))
        .flat_map(|features| features.split(',').map(str::to_owned).collect::<Vec<_>>())
        .collect();

    // Assert
    for feature in referenced_features {
        assert!(
            known_features.contains(&feature),
            "Dockerfile references unknown Cargo feature {feature}"
        );
    }
}

#[test]
fn should_apply_strict_feature_matrix_when_test_image_builds() {
    // Arrange
    let dockerfile = read_workflow("Dockerfile.tests");

    // Act
    // Assert
    assert!(dockerfile.contains("cargo fmt --check"));
    assert!(dockerfile.contains(
        "cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic"
    ));
    assert!(dockerfile.contains("cargo test --workspace --all-features"));
    assert!(dockerfile.contains(
        "cargo clippy --workspace --all-targets --no-default-features -- -D warnings -D clippy::pedantic"
    ));
    for provider in ["cloud-aws", "cloud-azure", "cloud-gcp", "cloud-oci"] {
        assert!(dockerfile.contains(&format!(
            "cargo check --workspace --all-targets --no-default-features --features {provider}"
        )));
    }
}

#[test]
fn should_defer_timed_execution_when_benchmark_image_builds() {
    // Arrange
    let dockerfile = read_workflow("Dockerfile.benches");

    // Act
    // Assert
    assert!(dockerfile.contains("RUN cargo bench --workspace --all-features --no-run"));
    assert!(dockerfile.contains("CMD [\"cargo\", \"bench\", \"--workspace\", \"--all-features\"]"));
}

#[test]
fn should_match_documented_benchmark_targets_when_manifest_is_authoritative() {
    // Arrange
    let manifest = read_workflow("Cargo.toml");
    let registered = named_targets(&manifest, "bench");
    let documents = [
        read_workflow("docs/development/benchmarks.md"),
        read_workflow("docs/development/performance-targets.md"),
        read_workflow("docs/operations/performance-tuning.md"),
    ];

    // Act
    let advertised: Vec<_> = documents
        .iter()
        .flat_map(|document| command_argument(document, "cargo bench", "--bench"))
        .collect();

    // Assert
    assert!(!advertised.is_empty());
    for target in advertised {
        assert!(
            registered.contains(&target),
            "documentation advertises unregistered benchmark target {target}"
        );
    }
}

#[test]
fn should_reject_criterion_contract_when_stress_benchmarks_documented() {
    // Arrange
    let documents = [
        read_workflow("docs/development/benchmarks.md"),
        read_workflow("docs/development/performance-targets.md"),
        read_workflow("docs/operations/performance-tuning.md"),
    ];

    // Act
    // Assert
    for document in documents {
        assert!(!document.contains("Use Criterion"));
        assert!(!document.contains("--save-baseline"));
        assert!(!document.contains("--baseline"));
    }
}

#[test]
fn should_cover_registered_benchmarks_when_benchmark_workflow_runs() {
    // Arrange
    let manifest = read_workflow("Cargo.toml");
    let registered = named_targets(&manifest, "bench");
    let workflow = read_workflow(".github/workflows/bench.yml");

    // Act
    let patterns = command_argument(&workflow, "cargo bench", "--bench");

    // Assert
    for target in registered {
        assert!(
            patterns
                .iter()
                .any(|pattern| target_matches(pattern, &target)),
            "benchmark workflow does not execute registered target {target}"
        );
    }
}

#[test]
fn should_describe_manual_trigger_when_benchmark_automation_documented() {
    // Arrange
    let document = read_workflow("docs/development/benchmarks.md");
    let workflow = read_workflow(".github/workflows/bench.yml");

    // Act
    let normalized_document = document.split_whitespace().collect::<Vec<_>>().join(" ");

    // Assert
    assert!(workflow.contains("workflow_dispatch:"));
    assert!(!workflow.contains("pull_request:"));
    assert!(document.contains("manually with `workflow_dispatch`"));
    assert!(normalized_document.contains("not a pull-request performance gate"));
}

#[test]
fn should_schedule_bounded_fuzz_smokes_when_fuzz_workflow_runs() {
    // Arrange
    let workflow = read_workflow(".github/workflows/fuzz.yml");

    // Act
    // Assert
    assert!(workflow.contains("schedule:"));
    assert!(workflow.contains("timeout-minutes:"));
    assert!(workflow.contains("cargo install cargo-fuzz --version 0.13.2 --locked"));
    assert!(workflow.contains("FUZZ_TOOLCHAIN: nightly-2026-07-01"));
    assert!(workflow.contains("cargo fuzz build"));
    let smoke_commands: Vec<_> = workflow
        .lines()
        .filter(|line| line.contains("cargo fuzz run"))
        .collect();
    assert_eq!(smoke_commands.len(), 4);
    for command in smoke_commands {
        assert!(command.contains("-max_total_time=30"));
        assert!(command.contains("-timeout=10"));
    }
}

#[test]
fn should_cover_every_fuzz_target_when_scheduled_smokes_run() {
    // Arrange
    let manifest = read_workflow("fuzz/Cargo.toml");
    let registered = named_targets(&manifest, "bin");
    let workflow = read_workflow(".github/workflows/fuzz.yml");

    // Act
    let exercised: BTreeSet<_> = command_argument(&workflow, "cargo fuzz run", "run")
        .into_iter()
        .collect();

    // Assert
    assert_eq!(exercised, registered);
}

#[test]
fn should_validate_benchmark_contract_when_ci_runs() {
    // Arrange
    let qualification = read_workflow(".github/workflows/qualification.yml");
    // Act
    // Assert
    assert!(
        qualification.contains("cntryl-tools validate-benchmarks --config .cntryl/repository.toml")
    );
}

#[test]
fn should_run_provider_feature_matrix_when_ci_runs() {
    // Arrange
    let compatibility = read_workflow(".github/workflows/compatibility.yml");

    // Act
    // Assert
    assert!(compatibility.contains("provider: [cloud-aws, cloud-azure, cloud-gcp, cloud-oci]"));
    assert!(compatibility.contains(
        "cargo check --workspace --all-targets --no-default-features --features ${{ matrix.provider }}"
    ));
}

#[test]
fn should_run_repository_qualification_when_ci_runs() {
    // Arrange
    let qualification = read_workflow(".github/workflows/qualification.yml");

    // Act
    let required_commands = [
        "cargo install --git https://github.com/cntryl/tools --rev 1ceecf1a6501793080235d2d46b2982f1424727c --locked",
        "cargo install cargo-machete --version 0.9.2 --locked",
        "cntryl-tools validate-tests",
        "cntryl-tools validate-docs --config .cntryl/repository.toml",
        "cntryl-tools validate-benchmarks --config .cntryl/repository.toml",
        "cntryl-tools check-module-sizes --config .cntryl/repository.toml",
        "cargo machete",
        "cargo test --workspace --all-features --doc",
        "cargo check --example documented_quick_start --all-features",
        "cargo package --locked",
    ];

    // Assert
    for command in required_commands {
        assert!(
            qualification.contains(command),
            "CI is missing qualification command: {command}"
        );
    }
}

#[test]
fn should_run_repository_qualification_before_publish() {
    // Arrange
    let publish = read_workflow(".github/workflows/publish.yml");

    // Act
    let required_commands = [
        "cargo install cargo-machete --version 0.9.2 --locked",
        "cargo machete",
        "cargo test --workspace --all-features --doc",
        "cargo check --example documented_quick_start --all-features",
        "cargo package --locked",
    ];

    // Assert
    for command in required_commands {
        assert!(
            publish.contains(command),
            "publish gate is missing qualification command: {command}"
        );
    }
}

#[test]
fn should_not_reference_retired_python_watchdog_when_documenting_repository_checks() {
    // Arrange
    let agent_guidance = read_workflow("AGENTS.md");
    let contributor_guidance = read_workflow("CONTRIBUTING.md");

    // Act
    let documented_guidance = format!("{agent_guidance}\n{contributor_guidance}");

    // Assert
    assert!(!documented_guidance.contains("scripts/test_watchdog.py"));
    assert!(documented_guidance.contains("Python 3"));
    assert!(documented_guidance.contains("coverage-tier governance"));
    assert!(documented_guidance.contains("cntryl-tools validate-tests"));
}

#[test]
fn should_build_test_image_when_ci_runs() {
    // Arrange
    let docker = read_workflow(".github/workflows/docker.yml");

    // Act
    // Assert
    assert!(docker.contains("docker build --file Dockerfile.tests --tag midge-tests:ci ."));
}

#[test]
fn should_include_repository_contract_inputs_when_test_image_builds() {
    // Arrange
    let dockerignore = read_workflow(".dockerignore");

    // Act
    let ignored_paths: BTreeSet<_> = dockerignore.lines().map(str::trim).collect();

    // Assert
    assert!(ignored_paths.contains("target/"));
    assert!(ignored_paths.contains(".git/"));
    assert!(!ignored_paths.contains(".github/"));
}

#[test]
fn should_trigger_ci_when_lockfile_changes() {
    // Arrange
    let ci = read_workflow(".github/workflows/ci.yml");

    // Act
    // Assert
    assert_eq!(ci.matches("\"Cargo.lock\"").count(), 2);
}

#[test]
fn should_trigger_ci_when_docker_context_changes() {
    // Arrange
    let docker = read_workflow(".github/workflows/docker.yml");

    // Act
    // Assert
    assert_eq!(docker.matches("\".dockerignore\"").count(), 2);
}

#[test]
fn should_limit_validation_workflow_permissions_to_read_only() {
    // Arrange
    let ci = read_workflow(".github/workflows/ci.yml");
    let bench = read_workflow(".github/workflows/bench.yml");

    // Act
    // Assert
    assert!(ci.contains("permissions:\n  contents: read"));
    assert!(bench.contains("permissions:\n  contents: read"));
}

#[test]
fn should_compile_canonical_example_when_repository_qualifies() {
    // Arrange
    let example = repository_root().join("examples/documented_quick_start.rs");
    let qualification = read_workflow(".github/workflows/qualification.yml");
    let publish = read_workflow(".github/workflows/publish.yml");

    // Act
    // Assert
    assert!(example.is_file());
    assert!(qualification.contains("cargo check --example documented_quick_start --all-features"));
    assert!(publish.contains("cargo check --example documented_quick_start --all-features"));
}

#[test]
fn should_document_transaction_owned_commit_in_canonical_guides() {
    // Arrange
    let guides = [
        "README.md",
        "docs/user-guides/quick-start.md",
        "docs/user-guides/api-guide.md",
        "docs/user-guides/faq.md",
        "docs/user-guides/troubleshooting.md",
        "docs/operations/performance-tuning.md",
        "docs/operations/migration-guide.md",
        "docs/transactions-and-mvcc.md",
        "docs/development/architecture.md",
        "docs/development/architecture-diagrams.md",
    ];

    // Act
    // Assert
    for guide in guides {
        let document = read_workflow(guide);
        assert!(
            !document.contains("engine.commit("),
            "{guide} still documents the removed Engine::commit API"
        );
        assert!(
            !document.contains(".commit(tx,"),
            "{guide} still commits by passing a transaction into an engine"
        );
        assert!(
            !document.contains("Engine::commit"),
            "{guide} still names the removed Engine::commit API"
        );
        assert!(
            !document.contains("drop(engine)"),
            "{guide} treats Drop as successful bounded shutdown"
        );
    }
}

#[test]
fn should_document_bounded_shutdown_in_quick_start() {
    // Arrange
    let quick_start = read_workflow("docs/user-guides/quick-start.md");

    // Act
    // Assert
    assert!(quick_start.contains("engine.shutdown(Duration::from_secs("));
    assert!(!quick_start.contains("drop(engine)"));
}

#[test]
fn should_remove_dependencies_rejected_by_machete() {
    // Arrange
    let manifest = read_workflow("Cargo.toml");

    // Act
    // Assert
    assert!(!manifest.contains("anyhow ="));
    assert!(!manifest.contains("once_cell ="));
}
