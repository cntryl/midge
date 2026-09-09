//! Repository Governance Tests
//!
//! Consolidated from: `repository_gates.rs`, `testing_governance.rs`, `coverage_manifests.rs`, `architecture_ladder.rs`, `external_adopter_smoke.rs`, `failpoints_contract.rs`

mod common;

mod repository_gates {
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

    /// Every declared feature except `default` and `failpoints`, in manifest
    /// order. CI must name these explicitly because the injection target needs
    /// a separate, single-threaded run and cannot share `--all-features`.
    fn non_failpoint_features() -> Vec<String> {
        let manifest = fs::read_to_string(repository_root().join("Cargo.toml"))
            .expect("failed to read Cargo.toml");
        let features = manifest
            .split("[features]")
            .nth(1)
            .expect("manifest declares a [features] table");
        let features = features.split("\n[").next().unwrap_or(features);
        features
            .lines()
            .filter_map(|line| line.split_once('=').map(|(name, _)| name.trim()))
            .filter(|name| !name.is_empty() && !name.starts_with('#'))
            .filter(|name| !matches!(*name, "default" | "failpoints"))
            .map(ToString::to_string)
            .collect()
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
        assert!(qualification
            .contains("cntryl-tools check-module-sizes --config .cntryl/repository.toml"));
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
        let has_cloud_or_docker_gate =
            ci.contains("docker compose up") || ci.contains("docker build");

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
    fn should_document_sqrzl_as_self_contained_cloud_qualification_authority() {
        // Arrange
        let readme = read_workflow("README.md");
        let policy = read_workflow("docs/development/cloud-qualification-policy.md");
        let support_matrix = read_workflow("docs/development/support-matrix.md");
        let cloud_setup = read_workflow("docs/operations/cloud-setup.md");

        // Act
        let documents = [&support_matrix, &cloud_setup];
        let normalized_readme = readme.split_whitespace().collect::<Vec<_>>().join(" ");

        // Assert
        assert!(normalized_readme.contains("supported pre-1.0 capability"));
        assert!(normalized_readme.contains("Sqrzl multi-provider emulator"));
        assert!(policy.contains("authoritative continuous qualification environment"));
        assert!(policy.contains("deterministic, credential-free"));
        assert!(policy.contains("Manual real-cloud integration testing has a different purpose"));
        assert!(policy
            .contains("absence of live-provider credentials in CI is not a cloud-maturity defect"));
        for document in documents {
            assert!(document.contains("cloud qualification policy"));
        }
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
    fn should_publish_only_version_matching_manual_tag_dispatch_with_least_privilege() {
        // Arrange
        let publish = read_workflow(".github/workflows/publish.yml");

        // Act
        // Assert
        assert!(publish.contains("workflow_dispatch:"));
        assert!(publish.contains("test \"${GITHUB_REF_TYPE}\" = \"tag\""));
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
            read_workflow(".github/workflows/benchmark-guard.yml"),
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
        // Failpoints are a process-global registry, so the injection target
        // runs on its own, single-threaded. That splits the former
        // `--all-features` run in two, and the explicit feature list is only
        // safe while it still names every non-failpoints feature.
        assert!(ci.contains(&format!(
            "cargo test --workspace --features {}",
            non_failpoint_features().join(",")
        )));
        assert!(ci.contains("cargo test --test fault_injection --all-features -- --test-threads=1"));
        assert!(compatibility.contains("rustup toolchain install 1.97"));
        assert!(compatibility.contains("rustup run 1.97 cargo check --workspace --all-targets"));
        assert!(compatibility.contains(
            "rustup run 1.97 cargo clippy --workspace --all-targets --no-default-features"
        ));
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
        assert!(
            dockerfile.contains("CMD [\"cargo\", \"bench\", \"--workspace\", \"--all-features\"]")
        );
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
    fn should_group_benchmark_artifacts_into_one_fresh_summary_run() {
        // Arrange
        let workflow = read_workflow(".github/workflows/bench.yml");

        // Act
        let uses_shared_run_id = workflow.contains(
            "STRESS_RUN_ID: midge-${{ github.run_id }}-${{ github.run_attempt }}-${{ matrix.os }}",
        );
        let removes_previous_summary =
            workflow.contains("rm -f target/bench_results.json target/bench_summary.md");

        // Assert
        assert!(
            uses_shared_run_id,
            "all benchmark tiers in one job must publish the same stress run ID"
        );
        assert!(
            removes_previous_summary,
            "bootstrap validation must not accept a stale summary manifest"
        );
        assert!(workflow.contains("if ! cntryl-tools summarize-benchmarks; then"));
    }

    #[test]
    fn should_size_recovery_benchmark_memtable_for_explicit_flush_fixture() {
        // Arrange
        let benchmark = read_workflow("benches/tier4_system_recovery_throughput.rs");

        // Act
        // Assert
        assert!(benchmark.contains("RECOVERY_FIXTURE_MEMTABLE_SIZE_BYTES"));
        assert!(benchmark.contains(".max(RECOVERY_FIXTURE_MEMTABLE_SIZE_BYTES)"));
        assert!(benchmark.contains("fixture_memtable_size_bytes"));
        assert!(benchmark.contains("recovery_opts_for_mode(\"local\")"));
        assert!(benchmark.contains("recovery_opts_for_mode(\"cloud\")"));
    }

    #[test]
    fn should_describe_bounded_pr_guard_when_benchmark_automation_documented() {
        // Arrange
        let document = read_workflow("docs/development/benchmarks.md");
        let benchmark_workflow = read_workflow(".github/workflows/bench.yml");
        let guard_workflow = read_workflow(".github/workflows/benchmark-guard.yml");

        // Act
        let normalized_document = document.split_whitespace().collect::<Vec<_>>().join(" ");

        // Assert
        assert!(benchmark_workflow.contains("workflow_dispatch:"));
        assert!(benchmark_workflow.contains("schedule:"));
        assert!(!benchmark_workflow.contains("pull_request:"));
        assert!(benchmark_workflow.contains("timeout-minutes: 90"));
        assert!(benchmark_workflow.contains("scripts/validate_benchmark_summary_bootstrap.py"));
        assert!(guard_workflow.contains("pull_request:"));
        assert!(guard_workflow.contains("Performance regression guard"));
        assert!(document.contains("bounded Ubuntu A/B guard"));
        assert!(normalized_document.contains("regression greater than 15%"));
    }

    #[test]
    fn should_run_diagnostic_pr_probe_without_internal_gate_obligation() {
        // Arrange
        let guard_workflow = read_workflow(".github/workflows/benchmark-guard.yml");

        // Act
        let diagnostic_profile_count = guard_workflow
            .matches("--profile default --samples 10 --warmup-samples 1")
            .count();

        // Assert
        assert_eq!(diagnostic_profile_count, 2);
        assert!(!guard_workflow.contains("--profile release --workload \"$WORKLOAD\""));
        assert!(guard_workflow.contains("--max-regression 0.15"));
        assert!(!guard_workflow.contains("cp Cargo.lock ../midge-bench-base/Cargo.lock"));
        assert!(guard_workflow.contains("--manifest-path ../midge-bench-base/Cargo.toml"));
    }

    #[test]
    fn should_require_acceptance_evidence_when_pull_request_changes() {
        // Arrange
        let workflow = read_workflow(".github/workflows/pr-acceptance.yml");
        let template = read_workflow(".github/pull_request_template.md");

        // Act
        // Assert
        assert!(workflow.contains("pull_request:"));
        assert!(workflow.contains("scripts/validate_pr_acceptance.py"));
        assert!(template.contains("## Linked issues"));
        assert!(template.contains("## Acceptance audit"));
        assert!(template.contains("Production entry point:"));
        assert!(template.contains("Resolution:"));
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
    fn should_validate_benchmark_contract_when_repository_qualifies() {
        // Arrange
        let qualification = read_workflow(".github/workflows/qualification.yml");
        // Act
        // Assert
        assert!(qualification.contains("cargo test --test governance -- repository_gates"));
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
            "cargo test --test governance -- repository_gates",
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
        let benchmark_guard = read_workflow(".github/workflows/benchmark-guard.yml");

        // Act
        // Assert
        assert!(ci.contains("permissions:\n  contents: read"));
        assert!(bench.contains("permissions:\n  contents: read"));
        assert!(benchmark_guard.contains("permissions:\n  contents: read"));
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
        assert!(
            qualification.contains("cargo check --example documented_quick_start --all-features")
        );
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
}

mod testing_governance {
    use std::fs;
    use std::process::Command;

    fn benchmark_guardrail_args(kind: &str) -> Vec<String> {
        let mut args = Vec::new();
        for index in 1..=3 {
            args.push("--base".to_owned());
            args.push(format!(
                "tests/fixtures/benchmark_guardrail/base-{index}.json"
            ));
        }
        for index in 1..=3 {
            args.push("--candidate".to_owned());
            args.push(format!(
                "tests/fixtures/benchmark_guardrail/{kind}-{index}.json"
            ));
        }
        args.extend([
            "--benchmark".to_owned(),
            "memory_batched_write_throughput".to_owned(),
            "--max-regression".to_owned(),
            "0.15".to_owned(),
        ]);
        args
    }

    #[test]
    fn should_accept_benchmark_candidate_at_regression_budget() {
        // Arrange
        let args = benchmark_guardrail_args("good");

        // Act
        let output = Command::new("python3")
            .arg("scripts/compare_benchmark_guardrail.py")
            .args(args)
            .output()
            .expect("run benchmark guardrail");

        // Assert
        assert!(
            output.status.success(),
            "guardrail failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn should_reject_benchmark_candidate_beyond_regression_budget() {
        // Arrange
        let args = benchmark_guardrail_args("bad");

        // Act
        let output = Command::new("python3")
            .arg("scripts/compare_benchmark_guardrail.py")
            .args(args)
            .output()
            .expect("run benchmark guardrail");

        // Assert
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("exceeds allowed 15%"));
    }

    #[test]
    fn should_accept_initial_benchmark_summary_without_baseline() {
        // Arrange
        let temp = tempfile::tempdir().expect("create benchmark summary fixture directory");
        let manifest = temp.path().join("bench_results.json");
        fs::write(
            &manifest,
            r#"{"comparison_summary":{"baseline_available":false,"critical":0,"new":517,"missing":0}}"#,
        )
        .expect("write initial benchmark summary fixture");

        // Act
        let output = Command::new("python3")
            .arg("scripts/validate_benchmark_summary_bootstrap.py")
            .arg(&manifest)
            .output()
            .expect("validate initial benchmark summary");

        // Assert
        assert!(
            output.status.success(),
            "initial benchmark summary was rejected: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn should_reject_failed_benchmark_summary_when_baseline_exists() {
        // Arrange
        let temp = tempfile::tempdir().expect("create benchmark summary fixture directory");
        let manifest = temp.path().join("bench_results.json");
        fs::write(
            &manifest,
            r#"{"comparison_summary":{"baseline_available":true,"critical":0,"new":1,"missing":0}}"#,
        )
        .expect("write baseline benchmark summary fixture");

        // Act
        let output = Command::new("python3")
            .arg("scripts/validate_benchmark_summary_bootstrap.py")
            .arg(&manifest)
            .output()
            .expect("validate baseline benchmark summary");

        // Assert
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("not an initial no-baseline report")
        );
    }

    #[test]
    fn should_use_typed_roles_when_benchmark_is_non_gating() {
        // Arrange
        let bench_paths = fs::read_dir("benches").expect("read benchmark directory");
        let mut legacy_role_authors = Vec::new();

        // Act
        for entry in bench_paths {
            let path = entry.expect("read benchmark entry").path();
            if path.extension().is_none_or(|extension| extension != "rs") {
                continue;
            }
            let source = fs::read_to_string(&path).expect("read benchmark source");
            if source.contains("trust_class") {
                legacy_role_authors.push(path);
            }
        }

        // Assert
        assert!(
            legacy_role_authors.is_empty(),
            "benchmark roles must use #[stress(role = ...)] or the typed row builder: {legacy_role_authors:?}"
        );
    }

    #[test]
    fn should_enforce_focused_benchmark_observation_contract() {
        // Arrange
        let transaction = fs::read_to_string("benches/tier2_subsystem_transaction_latency.rs")
            .expect("read transaction benchmark");
        let durability = fs::read_to_string("benches/tier2_subsystem_durability_commit_latency.rs")
            .expect("read durability benchmark");
        let compression = fs::read_to_string("benches/tier4_system_compression_policy.rs")
            .expect("read compression benchmark");
        let strict = fs::read_to_string("benches/tier4_system_strict_group_commit.rs")
            .expect("read strict system benchmark");

        // Act
        let required_compression_shapes = [
            "Repeated",
            "Structured",
            "Mixed",
            "PrefixRandomTail",
            "LowCardinality",
        ];

        // Assert
        assert!(transaction.contains("avg_txn_records_per_append"));
        assert!(transaction.contains("validation_errors"));
        assert!(!transaction.contains("format!(\"{scenario}_{phase}\")"));
        assert!(!durability.contains("record_commit_percentiles"));
        assert!(durability.contains("commits_per_fsync"));
        for shape in required_compression_shapes {
            assert!(
                compression.contains(shape),
                "missing compression shape {shape}"
            );
        }
        assert!(transaction.contains("role = \"diagnostic\""));
        assert!(strict.contains("role = \"diagnostic\""));
        assert!(strict.contains("final_sst_bytes"));
        assert!(strict.contains("write_stall_recoveries"));
        assert!(strict.contains("MAX_WRITE_STALL_RECOVERIES_PER_COMMIT"));
    }

    #[test]
    fn should_require_complete_acceptance_audit_given_pull_request_body() {
        // Arrange
        let temp = tempfile::tempdir().expect("create PR body fixture directory");
        let body = temp.path().join("body.md");
        fs::write(
            &body,
            "## Linked issues\nCloses #214\n\n## Acceptance audit\n- [x] Criterion: Enforce the Tier 4 budget.\n  Evidence: A/B fixture rejects a 22% regression.\n  Production entry point: Bench workflow executes the registered target.\n  Resolution: Matches the requested guardrail.\n\n## Verification\nFocused contracts passed.\n",
        )
        .expect("write PR body fixture");

        // Act
        let output = Command::new("python3")
            .arg("scripts/validate_pr_acceptance.py")
            .arg(&body)
            .output()
            .expect("validate PR acceptance body");

        // Assert
        assert!(
            output.status.success(),
            "acceptance validation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn should_reject_incomplete_acceptance_audit_given_renamed_test_claim() {
        // Arrange
        let temp = tempfile::tempdir().expect("create PR body fixture directory");
        let body = temp.path().join("body.md");
        fs::write(
            &body,
            "## Linked issues\nCloses #214\n\n## Acceptance audit\n- [ ] Criterion: Covered by a renamed old test.\n\n## Verification\nNone.\n",
        )
        .expect("write incomplete PR body fixture");

        // Act
        let output = Command::new("python3")
            .arg("scripts/validate_pr_acceptance.py")
            .arg(&body)
            .output()
            .expect("validate incomplete PR acceptance body");

        // Assert
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("unchecked acceptance"));
        assert!(error.contains("Production entry point"));
    }

    #[test]
    fn should_keep_expensive_testing_governance_scheduled_or_manual() {
        // Arrange
        let workflow = fs::read_to_string(".github/workflows/testing-governance.yml")
            .expect("read testing governance workflow");
        // Collapse all whitespace (indentation, line wraps, blank lines) to single
        // spaces so the assertions below tolerate harmless YAML/shell reformatting
        // (e.g. re-indenting a step, wrapping a long `run:` command onto another
        // line) without losing the ability to catch a real change to which jobs
        // exist, how they trigger, or what commands/flags they run.
        let normalized = workflow.split_whitespace().collect::<Vec<_>>().join(" ");

        // Act
        let has_expensive_jobs = normalized.contains("coverage-tier-diff:")
            && normalized.contains("mutation-pilot:")
            && normalized.contains("cargo llvm-cov")
            && normalized.contains("cargo mutants");

        // Assert
        assert!(normalized.contains("workflow_dispatch:"));
        assert!(normalized.contains("schedule:"));
        assert!(!normalized.contains("pull_request:"));
        assert!(!normalized.contains("workflow_run:"));
        assert!(normalized.contains("find tests -maxdepth 1"));
        assert!(!normalized.contains("cargo llvm-cov --tests"));
        assert!(normalized.contains("--shard 1/512 --sharding round-robin --jobs 2"));
        assert!(!normalized.contains("continue-on-error: true"));
        assert!(normalized.contains("scripts/mutation_report.py"));
        assert!(has_expensive_jobs);
    }

    #[test]
    fn should_reject_empty_mutation_pilot_given_no_viable_outcome() {
        // Arrange
        let temp = tempfile::tempdir().expect("create mutation fixture directory");
        let invalid = temp.path().join("invalid.json");
        fs::write(
            &invalid,
            r#"{"outcomes":[{"scenario":"Baseline","summary":"Success"}],"total_mutants":0,"caught":0,"missed":0,"timeout":0,"unviable":0}"#,
        )
        .expect("write invalid mutation report");

        // Act
        let output = Command::new("python3")
            .arg("scripts/mutation_report.py")
            .arg(&invalid)
            .output()
            .expect("validate mutation report");

        // Assert
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("produced no mutants"));
    }

    #[test]
    fn should_summarize_mutation_pilot_given_caught_and_surviving_mutants() {
        // Arrange
        let temp = tempfile::tempdir().expect("create mutation fixture directory");
        let valid = temp.path().join("valid.json");
        fs::write(
            &valid,
            r#"{"outcomes":[{"scenario":"Baseline","summary":"Success"},{"scenario":{"Mutant":{"name":"replace + with *"}},"summary":"CaughtMutant"},{"scenario":{"Mutant":{"name":"delete durability guard"}},"summary":"MissedMutant"}],"total_mutants":2,"caught":1,"missed":1,"timeout":0,"unviable":0}"#,
        )
        .expect("write valid mutation report");

        // Act
        let output = Command::new("python3")
            .arg("scripts/mutation_report.py")
            .arg(&valid)
            .output()
            .expect("validate mutation report");
        let summary = String::from_utf8(output.stdout).expect("mutation summary is UTF-8");

        // Assert
        assert!(output.status.success());
        assert!(summary.contains("Caught: 1"));
        assert!(summary.contains("Survived: 1"));
        assert!(summary.contains("delete durability guard"));
    }

    #[test]
    fn should_require_explicit_sqrzl_selection_without_silent_skip() {
        // Arrange
        let qualification = fs::read_to_string("tests/cloud_provider_engine_qualification.rs")
            .expect("read provider qualification tests");
        let provider_qualification = fs::read_to_string("src/storage/providers/qualification.rs")
            .expect("read provider-level qualification tests");
        let workflow =
            fs::read_to_string(".github/workflows/cloud.yml").expect("read cloud workflow");

        // Act
        let explicitly_selected = qualification.contains("#[ignore = \"requires Sqrzl")
            && provider_qualification.contains("#[ignore = \"requires Sqrzl")
            && workflow.matches("-- --ignored --test-threads=1").count() == 2;

        // Assert
        assert!(explicitly_selected);
        assert!(workflow.contains("workflow_dispatch:"));
        assert!(workflow.contains("schedule:"));
        assert!(!workflow.contains("workflow_run:"));
        assert!(qualification.contains("fn require_sqrzl"));
        assert!(provider_qualification.contains("fn require_sqrzl"));
        assert!(provider_qualification.contains("-- --ignored --test-threads=1"));
        assert!(!qualification.contains("sqrzl_available_or_skip"));
        assert!(!provider_qualification.contains("sqrzl_available_or_skip"));
        assert!(!qualification.contains("skipping Sqrzl qualification"));
        assert!(!provider_qualification.contains("skipping Sqrzl qualification"));
    }

    #[test]
    fn should_isolate_failpoint_sqrzl_qualification_from_ordinary_test_runs() {
        // Arrange
        let qualification = fs::read_to_string("tests/cloud_provider_engine_qualification.rs")
            .expect("read provider qualification tests");
        let cloud_workflow =
            fs::read_to_string(".github/workflows/cloud.yml").expect("read cloud workflow");
        let publish_workflow =
            fs::read_to_string(".github/workflows/publish.yml").expect("read publish workflow");
        let ignore_marker =
            "#[ignore = \"requires Sqrzl; run the scheduled/manual Cloud Qualification workflow\"]";
        let selected_command = "cargo test --test cloud_provider_engine_qualification --features \
                                sqrzl-tests,failpoints -- --ignored --test-threads=1";

        // Act
        let recovery_test_is_ignored = qualification.contains(&format!(
            "{ignore_marker}\nfn \
             should_recover_partitioned_compaction_from_sqrzl_s3_after_local_cache_loss"
        ));
        let partial_upload_test_is_ignored = qualification.contains(&format!(
            "{ignore_marker}\nfn should_rollback_partition_set_after_partial_sqrzl_compaction_upload"
        ));

        // Assert
        assert!(recovery_test_is_ignored);
        assert!(partial_upload_test_is_ignored);
        assert!(cloud_workflow.contains(selected_command));
        assert!(publish_workflow.contains(selected_command));
    }

    #[test]
    fn should_document_testing_review_contracts() {
        // Arrange
        let guide = fs::read_to_string("docs/development/testing.md").expect("read testing guide");
        let template =
            fs::read_to_string(".github/pull_request_template.md").expect("read PR template");

        // Act
        let guide_has_contracts = guide.contains("Test Through the Real Entry Point")
            && guide.contains("would the test still prove the behavior")
            && guide.contains("Shared Test Infrastructure Review")
            && guide.contains("durability_waiters_fanned_out_total")
            && guide.contains("sst_bloom_checks_total")
            && guide.contains("sst_bloom_rejects_total");

        // Assert
        assert!(guide_has_contracts);
        assert!(template.contains("real production entry point"));
        assert!(template.contains("poison-tolerant locks"));
        assert!(template.contains("mechanical call-site discovery"));
    }

    fn write_coverage_tier_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf)
    {
        let temp = tempfile::tempdir().expect("create coverage fixture directory");
        let unit = temp.path().join("unit.json");
        let integration = temp.path().join("integration.json");
        let wal_source = temp.path().join("src/wal/recovery.rs");
        let test_only_source = temp.path().join("src/compaction/test_only.rs");
        let field_source = temp.path().join("src/compaction/fields.rs");
        let shared_source = temp.path().join("src/runtime/live.rs");
        let external_test_source = temp.path().join("src/runtime/external_tests.rs");
        fs::create_dir_all(wal_source.parent().expect("WAL source parent"))
            .expect("create WAL source parent");
        fs::create_dir_all(test_only_source.parent().expect("compaction source parent"))
            .expect("create compaction source parent");
        fs::create_dir_all(shared_source.parent().expect("runtime source parent"))
            .expect("create runtime source parent");
        fs::write(
            &wal_source,
            "pub fn production() {\n    let value = \"not a }\";\n    drop(value);\n}\n\n#[cfg(test)]\nfn test_helper() {\n    let _ = r#\"{ raw }\"#;\n}\n\n#[cfg(test)]\nmod tests {\n    // A comment brace must not end the module: }\n    #[test]\n    fn test_only() {\n        assert!(true);\n    }\n}\n",
        )
        .expect("write mixed production/test source fixture");
        fs::write(
            &test_only_source,
            "#[cfg(test)]\nfn helper() {\n    let _ = '{';\n}\n\n#[cfg(test)]\nmod tests {\n    #[test]\n    fn covered_only_by_unit_tests() {\n        assert!(true);\n    }\n}\n",
        )
        .expect("write test-only source fixture");
        fs::write(
            &field_source,
            "pub struct Plan {\n    #[cfg(test)]\n    pub output_files: Vec<String>,\n    pub source_level: u32,\n}\n\npub fn production_plan() -> Plan {\n    Plan {\n        #[cfg(all(test, feature = \"proof\"))]\n        output_files: Vec::new(),\n        source_level: 1,\n    }\n}\n",
        )
        .expect("write cfg-gated field source fixture");
        fs::write(&shared_source, "pub fn shared() {}\n").expect("write shared source fixture");
        fs::write(
            temp.path().join("src/runtime/mod.rs"),
            "pub mod live;\n#[cfg(test)]\nmod external_tests;\n",
        )
        .expect("write external test module declaration");
        fs::write(
            &external_test_source,
            "#[test]\nfn external_test_body() {\n    assert!(true);\n}\n",
        )
        .expect("write externally guarded test module");

        let wal = wal_source.to_string_lossy();
        let test_only = test_only_source.to_string_lossy();
        let fields = field_source.to_string_lossy();
        let shared = shared_source.to_string_lossy();
        let external_test = external_test_source.to_string_lossy();
        let unit_report = serde_json::json!({"data": [{
            "files": [
                {"filename": wal, "segments": [[1,1,1,true,true,false],[4,2,0,false,false,false],[7,1,3,true,true,false],[9,2,0,false,false,false],[15,5,1,true,true,false],[17,6,0,false,false,false]]},
                {"filename": test_only, "segments": [[2,1,1,true,true,false],[4,2,0,false,false,false],[9,5,1,true,true,false],[11,6,0,false,false,false]]},
                {"filename": fields, "segments": [[3,5,1,true,true,false],[3,40,0,false,false,false],[7,1,2,true,true,false],[10,9,1,true,true,false],[10,40,0,false,false,false],[11,9,2,true,true,false],[13,2,0,false,false,false]]},
                {"filename": external_test, "segments": [[2,1,1,true,true,false],[4,2,0,false,false,false]]},
                {"filename": shared, "segments": [[1,1,2,true,true,false],[1,20,0,false,false,false]]}
            ],
            "functions": [
                {"name": "_RNvCsUnitHash_production", "filenames": [wal], "regions": [[1,1,4,2,1,0,0,0]]},
                {"name": "_RNvCsUnitHash_test_helper", "filenames": [wal], "regions": [[7,1,9,2,3,0,0,0]]},
                {"name": "_RNvCsUnitHash_test_only", "filenames": [wal], "regions": [[15,5,17,6,1,0,0,0]]},
                {"name": "_RNvCsUnitHash_only_test_code", "filenames": [test_only], "regions": [[2,1,11,6,1,0,0,0]]},
                {"name": "_RNvCsUnitHash_test_field", "filenames": [fields], "regions": [[3,5,3,40,1,0,0,0]]},
                {"name": "_RNvCsUnitHash_test_literal_field", "filenames": [fields], "regions": [[10,9,10,40,1,0,0,0]]},
                {"name": "_RNvCsUnitHash_production_plan", "filenames": [fields], "regions": [[7,1,13,2,2,0,0,0]]},
                {"name": "_RNvCsUnitHash_external_test_code", "filenames": [external_test], "regions": [[2,1,4,2,1,0,0,0]]},
                {"name": "_RNvCsUnitHash_shared", "filenames": [shared], "regions": [[1,1,1,20,2,0,0,0]]}
            ]
        }]});
        let integration_report = serde_json::json!({"data": [{
            "files": [{"filename": shared, "segments": [[1,1,1,true,true,false],[1,20,0,false,false,false]]}],
            "functions": [{"name": "_RNvCsIntegrationHash_shared", "filenames": [shared], "regions": [[1,1,1,20,1,0,0,0]]}]
        }]});
        fs::write(
            &unit,
            serde_json::to_vec(&unit_report).expect("serialize unit coverage fixture"),
        )
        .expect("write unit coverage fixture");
        fs::write(
            &integration,
            serde_json::to_vec(&integration_report)
                .expect("serialize integration coverage fixture"),
        )
        .expect("write integration coverage fixture");

        (temp, unit, integration)
    }

    #[test]
    fn should_report_unit_only_coverage_islands_given_distinct_tier_reports() {
        // Arrange
        let (_temp, unit, integration) = write_coverage_tier_fixture();

        // Act
        let output = Command::new("python3")
            .arg("scripts/coverage_tier_diff.py")
            .arg(&unit)
            .arg(&integration)
            .output()
            .expect("run coverage tier diff");
        let report = String::from_utf8(output.stdout).expect("coverage report is UTF-8");

        // Assert
        assert!(
            output.status.success(),
            "coverage tier diff failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(report.contains("src/wal/recovery.rs"));
        assert!(report.contains("src/wal/recovery.rs:1:1"));
        assert!(!report.contains("src/wal/recovery.rs:7:1"));
        assert!(!report.contains("src/wal/recovery.rs:15:5"));
        assert!(!report.contains("src/compaction/test_only.rs"));
        assert!(report.contains("src/compaction/fields.rs:7:1"));
        assert!(!report.contains("src/compaction/fields.rs:3:5"));
        assert!(!report.contains("src/compaction/fields.rs:10:9"));
        assert!(!report.contains("src/runtime/external_tests.rs"));
        assert!(!report.contains("src/runtime/live.rs"));
    }
}

mod coverage_manifests {
    //! Compile-enforced manifests for enum-shaped behavior axes.
    //! Internal `FsError` coverage lives in `src/io/traits.rs` unit tests because the
    //! filesystem module is intentionally private to library consumers.

    use cntryl_midge::sst::compression::{CompressionAlgo, CompressionPolicy};
    use cntryl_midge::{
        AzureCredentialSource, DurabilityPolicy, Engine, GcsCredentialSource,
        HybridStorageBudgetSnapshot, LocalStorageUsage, MidgeError, OpenOptions, RecoveryPolicy,
        S3CredentialSource, StorageAdmissionBlock, StorageAdmissionKind, StorageAdmissionReason,
        TransactionMode,
    };
    use std::time::Duration;

    fn s3_coverage(source: &S3CredentialSource) -> &'static str {
        match source {
            S3CredentialSource::Static { .. } => "provider request/qualification tests",
            S3CredentialSource::Environment => "configuration resolution tests",
            S3CredentialSource::SharedProfile { .. } => "profile parsing tests; real AWS scheduled",
            S3CredentialSource::AwsDefaultChain => "chain unit tests; real AWS scheduled",
        }
    }

    fn azure_coverage(source: &AzureCredentialSource) -> &'static str {
        match source {
            AzureCredentialSource::SharedKey { .. } => "Sqrzl qualification",
            AzureCredentialSource::SasToken { .. } => "request signing tests",
            AzureCredentialSource::ConnectionString { .. } => "configuration tests",
            AzureCredentialSource::StorageEnvironment => "environment resolution tests",
            AzureCredentialSource::EnvironmentClientSecret => "client-secret identity tests",
            AzureCredentialSource::WorkloadIdentity { .. } => "workload-identity tests",
            AzureCredentialSource::ManagedIdentity { .. } => "managed-identity tests",
            AzureCredentialSource::LightweightDefaultChain => {
                "chain unit tests; real Azure scheduled"
            }
        }
    }

    fn gcs_coverage(source: &GcsCredentialSource) -> &'static str {
        match source {
            GcsCredentialSource::BearerToken { .. } => "JSON request tests",
            GcsCredentialSource::HmacKey { .. } => "Sqrzl XML qualification",
            GcsCredentialSource::ApplicationDefault => "ADC unit tests; real GCS scheduled",
            GcsCredentialSource::ServiceAccountJsonFile { .. } => "service-account parsing tests",
            GcsCredentialSource::AuthorizedUserJsonFile { .. } => "authorized-user parsing tests",
            GcsCredentialSource::MetadataServer => "metadata transport tests",
        }
    }

    fn compression_coverage(algorithm: CompressionAlgo) -> &'static str {
        match algorithm {
            CompressionAlgo::None => "V4 raw-block roundtrip and integrity verification",
            CompressionAlgo::Lz4 => "V4 LZ4 roundtrip and adaptive selection",
            CompressionAlgo::Zstd3 => "V4 Zstd level-3 roundtrip and adaptive selection",
            CompressionAlgo::Zstd9 => "V4 Zstd level-9 roundtrip and adaptive selection",
        }
    }

    fn compression_policy_coverage(policy: &CompressionPolicy) -> &'static str {
        match policy {
            CompressionPolicy::None => "uncompressed production roundtrip",
            CompressionPolicy::Fixed(_) => "fixed-policy production roundtrip",
            CompressionPolicy::Adaptive { .. } => "adaptive production roundtrip",
        }
    }

    fn recovery_coverage(policy: RecoveryPolicy) -> &'static str {
        match policy {
            RecoveryPolicy::Strict => "strict corruption and recovery suites",
            RecoveryPolicy::Salvage => "salvage-prefix and degraded-health suites",
        }
    }

    fn durability_coverage(policy: DurabilityPolicy) -> &'static str {
        match policy {
            DurabilityPolicy::Sync => "local sync durability suites",
            DurabilityPolicy::Buffered => "local buffered recovery suites",
            DurabilityPolicy::BestEffort => "best-effort loss/flush suites",
            DurabilityPolicy::CloudAsync => "cloud async recovery suites",
            DurabilityPolicy::CloudStrict => "cloud strict qualification suites",
        }
    }

    fn storage_admission_kind_coverage(kind: StorageAdmissionKind) -> &'static str {
        match kind {
            StorageAdmissionKind::Wal => "cloud WAL admission and rollback accounting tests",
            StorageAdmissionKind::TransactionSpill => "public rejected-spill diagnostic snapshot",
            StorageAdmissionKind::Flush => "flush admission and publication reservation tests",
            StorageAdmissionKind::Compaction => "compaction admission and scratch cleanup tests",
            StorageAdmissionKind::FlushHeadroom => "shared reusable flush headroom tests",
            StorageAdmissionKind::StartupResidue => "startup residue reconciliation tests",
        }
    }

    fn storage_admission_reason_coverage(reason: StorageAdmissionReason) -> &'static str {
        match reason {
            StorageAdmissionReason::LocalCapacity => "public oversized-spill admission rejection",
            StorageAdmissionReason::CloudUpload => {
                "cloud upload pressure and admission history tests"
            }
            StorageAdmissionReason::Compaction => "high-watermark compaction pressure tests",
        }
    }

    #[test]
    fn should_keep_coverage_manifest_exhaustive_given_public_storage_admission_axes() {
        // Arrange
        let operations = [
            StorageAdmissionKind::Wal,
            StorageAdmissionKind::TransactionSpill,
            StorageAdmissionKind::Flush,
            StorageAdmissionKind::Compaction,
            StorageAdmissionKind::FlushHeadroom,
            StorageAdmissionKind::StartupResidue,
        ];
        let reasons = [
            StorageAdmissionReason::LocalCapacity,
            StorageAdmissionReason::CloudUpload,
            StorageAdmissionReason::Compaction,
        ];

        // Act
        let operations = operations.map(storage_admission_kind_coverage);
        let reasons = reasons.map(storage_admission_reason_coverage);

        // Assert
        for axis in [&operations[..], &reasons[..]] {
            assert!(axis.iter().all(|note| !note.is_empty()));
            let unique: std::collections::HashSet<_> = axis.iter().collect();
            assert_eq!(
                unique.len(),
                axis.len(),
                "distinct coverage notes: {axis:?}"
            );
        }
    }

    #[test]
    fn should_expose_typed_storage_pressure_when_transaction_spill_exceeds_local_capacity() {
        // Arrange
        let directory = tempfile::tempdir().expect("database directory");
        let local_budget = 1024 * 1024;
        let options = OpenOptions::cloud_simulated(directory.path(), "bucket", "typed-metrics")
            .local_storage_budget(local_budget)
            .transaction_memory_pool_size(8 * 1024)
            .background_compaction(false)
            .build()
            .expect("options");
        let mut engine = Engine::open(options).expect("engine");
        let cf = engine.create_column_family("data").expect("column family");
        let mut transaction = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("transaction");

        // Act
        let result = transaction.put(b"oversized".to_vec(), vec![1; 2 * 1024 * 1024], None);
        let snapshot = engine.get_runtime_metrics().expect("runtime metrics");
        let storage: HybridStorageBudgetSnapshot =
            snapshot.local_storage.expect("cloud disk budget");
        let usage: LocalStorageUsage = storage.usage;
        let pressure: StorageAdmissionBlock =
            storage.blocked_admission.expect("rejected admission");
        drop(transaction);
        engine.shutdown(Duration::from_secs(10)).expect("shutdown");

        // Assert
        assert!(matches!(result, Err(MidgeError::NoSpace(_))));
        assert_eq!(pressure.operation, StorageAdmissionKind::TransactionSpill);
        assert_eq!(pressure.reason, StorageAdmissionReason::LocalCapacity);
        assert!(pressure.requested_bytes > pressure.free_bytes_at_rejection);
        assert!(pressure.attempts > 0);
        assert!(storage.admission_rejections_total >= pressure.attempts);
        assert_eq!(storage.max_local_bytes, local_budget);
        assert_eq!(
            storage.total_committed_bytes,
            usage.wal_bytes
                + usage.transaction_spill_bytes
                + usage.resident_sst_bytes
                + usage.startup_residue_bytes
                + usage.flush_staging_reserved_bytes
                + usage.flush_headroom_reserved_bytes
                + usage.compaction_staging_reserved_bytes
                + usage.wal_headroom_reserved_bytes
        );
        assert_eq!(usage.transaction_spill_bytes, 0, "failed work owns no disk");
        assert_eq!(
            storage.free_bytes,
            local_budget.saturating_sub(storage.total_committed_bytes)
        );
    }

    #[test]
    fn should_keep_coverage_manifest_exhaustive_given_public_behavior_axes() {
        // Arrange
        // The real exhaustiveness guarantee comes from the non-wildcard `match`
        // arms in the `*_coverage` functions above: adding a new enum variant
        // without updating them fails to *compile*, not merely to pass this test.
        //
        // What this test adds at runtime is a check those compile-time-exhaustive
        // functions can't make for themselves: that every variant's description is
        // non-empty *and* distinct within its axis, catching a copy-pasted
        // coverage note left over from an adjacent match arm.
        let recovery = [
            recovery_coverage(RecoveryPolicy::Strict),
            recovery_coverage(RecoveryPolicy::Salvage),
        ];
        let durability = [
            durability_coverage(DurabilityPolicy::Sync),
            durability_coverage(DurabilityPolicy::Buffered),
            durability_coverage(DurabilityPolicy::BestEffort),
            durability_coverage(DurabilityPolicy::CloudAsync),
            durability_coverage(DurabilityPolicy::CloudStrict),
        ];
        let compression = [
            compression_coverage(CompressionAlgo::None),
            compression_coverage(CompressionAlgo::Lz4),
            compression_coverage(CompressionAlgo::Zstd3),
            compression_coverage(CompressionAlgo::Zstd9),
        ];
        let compression_policy = [
            compression_policy_coverage(&CompressionPolicy::None),
            compression_policy_coverage(&CompressionPolicy::Fixed(CompressionAlgo::Lz4)),
            compression_policy_coverage(&CompressionPolicy::Adaptive {
                min_savings_bytes: 64,
                min_ratio: 0.1,
                check_algorithms: vec![CompressionAlgo::Zstd3],
            }),
        ];
        let azure = [
            azure_coverage(&AzureCredentialSource::default_chain()),
            azure_coverage(&AzureCredentialSource::StorageEnvironment),
            azure_coverage(&AzureCredentialSource::EnvironmentClientSecret),
        ];
        let gcs = [
            gcs_coverage(&GcsCredentialSource::application_default()),
            gcs_coverage(&GcsCredentialSource::MetadataServer),
        ];
        let s3 = [
            s3_coverage(&S3CredentialSource::environment()),
            s3_coverage(&S3CredentialSource::AwsDefaultChain),
        ];

        // Act
        let axes = [
            &recovery[..],
            &durability[..],
            &compression[..],
            &compression_policy[..],
            &azure[..],
            &gcs[..],
            &s3[..],
        ];

        // Assert
        for axis in axes {
            assert!(
                axis.iter().all(|entry| !entry.is_empty()),
                "every variant must carry a non-empty coverage note: {axis:?}"
            );
            let mut unique = axis.to_vec();
            unique.sort_unstable();
            unique.dedup();
            assert_eq!(
                unique.len(),
                axis.len(),
                "coverage notes within one axis must be distinct per variant, found a duplicate in {axis:?}"
            );
        }
    }
}

mod architecture_ladder {
    use std::path::{Path, PathBuf};

    fn rust_sources_under(relative: &str) -> Vec<PathBuf> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
        if root.is_file() {
            return vec![root];
        }
        let mut pending = vec![root];
        let mut sources = Vec::new();
        while let Some(path) = pending.pop() {
            for entry in std::fs::read_dir(path).expect("read source directory") {
                let path = entry.expect("read source entry").path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    sources.push(path);
                }
            }
        }
        sources
    }

    fn production_source(path: &Path) -> String {
        if path.file_name().is_some_and(|name| name == "tests.rs") {
            return String::new();
        }
        let source = std::fs::read_to_string(path).expect("read Rust source");
        if source.trim_start().starts_with("#![cfg(test)]") {
            return String::new();
        }
        source
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .unwrap_or(&source)
            .to_string()
    }

    #[test]
    fn should_exclude_explicit_test_modules_from_production_dependency_checks() {
        // Arrange
        let directory = tempfile::tempdir().expect("source fixtures");
        let test_module = directory.path().join("test_fixture.rs");
        let production_module = directory.path().join("production.rs");
        let import = "use crate::runtime::Runtime;\n";
        std::fs::write(&test_module, format!("#![cfg(test)]\n{import}")).expect("test fixture");
        std::fs::write(&production_module, import).expect("production fixture");

        // Act
        let test_source = production_source(&test_module);
        let production = production_source(&production_module);

        // Assert
        assert!(test_source.is_empty());
        assert!(production.contains("crate::runtime"));
    }

    fn prohibited_edges_under(relative: &str, forbidden: &[&str]) -> Vec<String> {
        rust_sources_under(relative)
            .into_iter()
            .flat_map(|path| {
                let source = production_source(&path);
                forbidden
                    .iter()
                    .filter(move |edge| source.contains(**edge))
                    .map(move |edge| format!("{} imports {edge}", path.display()))
            })
            .collect()
    }

    #[test]
    fn should_keep_common_independent_from_higher_subsystems() {
        // Arrange
        let forbidden = [
            "crate::engine",
            "crate::lease",
            "crate::metadata",
            "crate::runtime",
            "crate::sst",
            "crate::storage",
            "crate::wal",
        ];

        // Act
        let violations: Vec<_> = rust_sources_under("src/common")
            .into_iter()
            .flat_map(|path| {
                let source = std::fs::read_to_string(&path).expect("read common source");
                forbidden
                    .iter()
                    .filter(move |edge| source.contains(**edge))
                    .map(move |edge| format!("{} imports {edge}", path.display()))
            })
            .collect();

        // Assert
        assert!(
            violations.is_empty(),
            "common must remain the bottom dependency layer: {violations:#?}"
        );
    }

    #[test]
    fn should_keep_provider_configuration_owned_by_config_layer() {
        // Arrange
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut sources = vec![manifest_dir.join("src/config.rs")];
        let config_dir = manifest_dir.join("src/config");
        if config_dir.exists() {
            sources.extend(rust_sources_under("src/config"));
        }

        // Act
        let violations: Vec<_> = sources
            .into_iter()
            .filter_map(|path| {
                let source = std::fs::read_to_string(&path).expect("read config source");
                source
                    .contains("crate::storage")
                    .then(|| format!("{} imports crate::storage", path.display()))
            })
            .collect();

        // Assert
        assert!(
            violations.is_empty(),
            "configuration DTOs must not be owned or re-exported by storage: {violations:#?}"
        );
    }

    #[test]
    fn should_keep_storage_as_raw_object_io() {
        // Arrange
        let forbidden = [
            "crate::engine",
            "crate::metadata",
            "crate::runtime",
            "crate::sst",
            "crate::wal",
        ];

        // Act
        let violations = prohibited_edges_under("src/storage", &forbidden);

        // Assert
        assert!(
            violations.is_empty(),
            "storage must provide raw bounded object I/O without format or runtime ownership: {violations:#?}"
        );
    }

    #[test]
    fn should_keep_sst_below_read_layers() {
        // Arrange
        let forbidden = [
            "crate::engine",
            "crate::iterators",
            "crate::metadata",
            "crate::runtime",
            "crate::storage",
            "crate::wal",
        ];

        // Act
        let violations = prohibited_edges_under("src/sst", &forbidden);

        // Assert
        assert!(
            violations.is_empty(),
            "SST format and readers must not depend on iterator facades or orchestration: {violations:#?}"
        );
    }

    #[test]
    fn should_keep_iterator_contracts_in_lower_layer() {
        // Arrange
        let forbidden = [
            "crate::engine",
            "crate::metadata",
            "crate::runtime",
            "crate::sst",
            "crate::storage",
            "crate::wal",
        ];

        // Act
        let violations = prohibited_edges_under("src/iterators", &forbidden);

        // Assert
        assert!(
            violations.is_empty(),
            "shared iterator contracts must be owned below SST and orchestration: {violations:#?}"
        );
    }

    #[test]
    fn should_keep_persistence_formats_below_storage_orchestration() {
        // Arrange
        let forbidden = ["crate::engine", "crate::runtime", "crate::storage"];

        // Act
        let mut violations = prohibited_edges_under("src/wal", &forbidden);
        violations.extend(prohibited_edges_under("src/metadata", &forbidden));

        // Assert
        assert!(
            violations.is_empty(),
            "WAL and metadata formats must not depend on storage or runtime orchestration: {violations:#?}"
        );
    }

    #[test]
    fn should_enforce_declared_architecture_boundaries_given_diagnostics_and_cli() {
        // Arrange
        let diagnostics_forbidden = [
            "crate::engine",
            "crate::metadata",
            "crate::runtime",
            "crate::storage",
            "crate::wal",
        ];
        let cli_forbidden = [
            "cntryl_midge::engine",
            "cntryl_midge::metadata",
            "cntryl_midge::runtime",
            "cntryl_midge::storage",
            "cntryl_midge::wal",
        ];

        // Act
        let mut violations = prohibited_edges_under("src/diagnostics.rs", &diagnostics_forbidden);
        violations.extend(prohibited_edges_under("src/bin/midge.rs", &cli_forbidden));

        // Assert
        assert!(
            violations.is_empty(),
            "diagnostics and the verify CLI must stay on their declared dependency boundaries: {violations:#?}"
        );
    }
}

mod failpoints_contract {
    #[cfg(feature = "failpoints")]
    use crate::common::crash;
    use std::path::PathBuf;

    fn repository_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    fn repository_file(path: &str) -> String {
        std::fs::read_to_string(repository_root().join(path))
            .unwrap_or_else(|error| panic!("read {path}: {error}"))
    }

    fn rust_files_below(root: &std::path::Path) -> Vec<PathBuf> {
        fn visit(directory: &std::path::Path, files: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(directory).expect("read source directory") {
                let path = entry.expect("read source entry").path();
                if path.is_dir() {
                    visit(&path, files);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    files.push(path);
                }
            }
        }

        let mut files = Vec::new();
        visit(root, &mut files);
        files.sort();
        files
    }

    fn direct_failpoint_bypasses(
        root: &std::path::Path,
        adapter: &std::path::Path,
    ) -> Vec<PathBuf> {
        rust_files_below(root)
            .into_iter()
            .filter(|path| path != adapter)
            .filter(|path| {
                let source = std::fs::read_to_string(path).expect("read Rust source");
                source.contains("fail::fail_point!") || source.contains("fail::eval(")
            })
            .collect()
    }

    fn poison_fragile_test_locks(roots: &[PathBuf]) -> Vec<PathBuf> {
        let expect_pattern = [".lock()", ".expect("].concat();
        let unwrap_pattern = [".lock()", ".unwrap("].concat();
        let mut fragile = Vec::new();
        for root in roots {
            for path in rust_files_below(root) {
                let source = std::fs::read_to_string(&path).expect("read Rust source");
                let compact: String = source
                    .chars()
                    .filter(|character| !character.is_whitespace())
                    .collect();
                if compact.contains(&expect_pattern) || compact.contains(&unwrap_pattern) {
                    fragile.push(path);
                }
            }
        }
        fragile.sort();
        fragile
    }

    #[test]
    fn should_exclude_fail_dependency_when_default_features_are_selected() {
        // Arrange
        let manifest = repository_file("Cargo.toml");
        let default_features = manifest
            .lines()
            .find(|line| line.starts_with("default = "))
            .expect("default feature declaration");

        // Act
        let fail_dependency_is_optional = manifest
            .contains("fail = { version = \"0.5\", features = [\"failpoints\"], optional = true }");
        let explicit_feature_exists = manifest.contains("failpoints = [\"dep:fail\"]");

        // Assert
        assert!(fail_dependency_is_optional);
        assert!(explicit_feature_exists);
        assert!(!default_features.contains("failpoints"));
    }

    #[test]
    fn should_require_failpoints_feature_when_injection_only_targets_are_selected() {
        // Arrange
        let manifest = repository_file("Cargo.toml");
        // Every injection-only suite lives in the single `fault_injection`
        // target, so one gated declaration keeps them all off default builds.
        let injection_targets = ["fault_injection"];

        // Act
        let missing_gate = injection_targets.iter().find(|target| {
            let declaration = format!(
                "name = \"{target}\"\npath = \"tests/{target}.rs\"\nrequired-features = [\"failpoints\"]"
            );
            !manifest.contains(&declaration)
        });

        // Assert
        assert_eq!(missing_gate, None);
    }

    #[test]
    fn should_route_production_injection_through_internal_adapter() {
        // Arrange
        let source_root = repository_root().join("src");
        let adapter = source_root.join("failpoints.rs");

        // Act
        let direct_production_references = direct_failpoint_bypasses(&source_root, &adapter);

        // Assert
        assert!(
            direct_production_references.is_empty(),
            "production code bypassed src/failpoints.rs: {direct_production_references:?}"
        );
    }

    #[test]
    fn should_flag_new_production_file_when_it_bypasses_internal_failpoint_adapter() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source_root = temp_dir.path().join("src");
        let nested = source_root.join("runtime/event_loop");
        std::fs::create_dir_all(&nested).expect("create nested source directory");
        let adapter = source_root.join("failpoints.rs");
        std::fs::write(&adapter, "macro_rules! fail_point { () => {} }\n")
            .expect("write adapter fixture");
        let bypass = nested.join("bypass.rs");
        std::fs::write(&bypass, "fn inject() { fail::fail_point!(\"raw\"); }\n")
            .expect("write bypass fixture");

        // Act
        let detected = direct_failpoint_bypasses(&source_root, &adapter);

        // Assert
        assert_eq!(detected, vec![bypass]);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_report_specific_failpoint_marker_when_child_process_aborts_at_intended_boundary() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let marker = temp_dir.path().join("trigger.sentinel");

        // Act
        let missing = crash::validate_trigger_sentinel(&marker, "scenario", "expected-trigger");
        std::fs::write(&marker, "scenario=scenario\ntrigger=wrong-trigger\n")
            .expect("write wrong trigger sentinel");
        let wrong = crash::validate_trigger_sentinel(&marker, "scenario", "expected-trigger");
        std::fs::write(&marker, "scenario=scenario\ntrigger=expected-trigger\n")
            .expect("write expected trigger sentinel");
        let exact = crash::validate_trigger_sentinel(&marker, "scenario", "expected-trigger");

        // Assert
        assert!(missing.is_err());
        assert!(wrong.is_err());
        assert_eq!(exact, Ok(()));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_reject_non_abort_child_failure_even_when_trigger_marker_matches() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let marker = temp_dir.path().join("trigger.sentinel");
        std::fs::write(&marker, "scenario=scenario\ntrigger=expected-trigger\n")
            .expect("write exact trigger sentinel");
        let output = std::process::Command::new(
            std::env::current_exe().expect("locate failpoint contract test executable"),
        )
        .arg("--definitely-not-a-valid-test-harness-option")
        .output()
        .expect("run ordinary failing child");

        // Act
        let validation =
            crash::validate_child_crash(&output, &marker, "scenario", "expected-trigger");

        // Assert
        assert!(validation
            .expect_err("ordinary failure must not count as an abort")
            .contains("failed without process abort"));
    }

    #[test]
    fn should_use_poison_tolerant_shared_test_locks_across_repository() {
        // Arrange
        let root = repository_root();
        let source_roots = [root.join("src"), root.join("tests")];

        // Act
        let fragile = poison_fragile_test_locks(&source_roots);

        // Assert
        assert!(
            fragile.is_empty(),
            "shared test locks must recover poisoned guards: {fragile:?}"
        );
    }

    #[test]
    fn should_cascade_no_further_test_failures_when_prior_failpoint_guard_panics() {
        // Arrange: this exercises the poison-tolerant lock pattern that
        // `should_use_poison_tolerant_shared_test_locks_across_repository`
        // enforces repository-wide for locks shared across tests (including
        // failpoint test guards, which are `pub(crate)` and so cannot be driven
        // directly from an external integration-test binary). A panic while
        // holding such a lock must not cascade into a failure for whoever
        // acquires it next.
        let lock = std::sync::Arc::new(std::sync::Mutex::new(0u32));
        let poisoner_lock = std::sync::Arc::clone(&lock);
        let poisoner = std::thread::spawn(move || {
            let mut guard = poisoner_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard += 1;
            panic!("synthetic assertion failure while holding test guard");
        });
        assert!(poisoner.join().is_err());
        assert!(lock.is_poisoned());

        // Act: a second, independent acquisition - standing in for the next
        // test in the suite grabbing the same shared lock - must still succeed
        // and observe the poisoner's partial work, rather than cascading.
        let follower_lock = std::sync::Arc::clone(&lock);
        let follower = std::thread::spawn(move || {
            let guard = follower_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard
        });
        let value_seen_by_follower = follower
            .join()
            .expect("follower must not cascade the panic");

        // Assert
        assert_eq!(
            value_seen_by_follower, 1,
            "follower must observe the poisoner's work rather than a reset/lost state"
        );
    }

    #[test]
    fn should_verify_default_release_graph_excludes_failpoints_in_workflows() {
        // Arrange
        let ci = repository_file(".github/workflows/ci.yml");
        let publish = repository_file(".github/workflows/publish.yml");
        let graph_check = "cargo tree --edges normal | grep -E '(^|[[:space:]])fail v'";

        // Act
        let ci_has_gate = ci.contains(graph_check) && ci.contains("cargo check --release");
        let publish_has_gate =
            publish.contains(graph_check) && publish.contains("cargo check --release");

        // Assert
        assert!(ci_has_gate);
        assert!(publish_has_gate);
    }

    #[test]
    fn should_enable_failpoints_when_release_runs_injection_suites() {
        // Arrange
        let publish = repository_file(".github/workflows/publish.yml");
        let injection_commands = [
            "cargo test --test fault_injection --features failpoints -- --test-threads=1 external_adopter_smoke",
            "cargo test --test fault_injection --features failpoints -- --test-threads=1 failure_injection",
            "cargo test --test fault_injection --features failpoints -- --test-threads=1 chaos_compaction",
        ];

        // Act
        let missing_feature = injection_commands
            .iter()
            .find(|command| !publish.contains(*command));

        // Assert
        assert_eq!(missing_feature, None);
    }
}
