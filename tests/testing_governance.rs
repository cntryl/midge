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
    assert!(String::from_utf8_lossy(&output.stderr).contains("not an initial no-baseline report"));
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
    let workflow = fs::read_to_string(".github/workflows/cloud.yml").expect("read cloud workflow");

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

fn write_coverage_tier_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
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
        serde_json::to_vec(&integration_report).expect("serialize integration coverage fixture"),
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
