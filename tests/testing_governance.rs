use std::fs;
use std::process::Command;

#[test]
fn should_keep_expensive_testing_governance_scheduled_or_manual() {
    // Arrange
    let workflow = fs::read_to_string(".github/workflows/testing-governance.yml")
        .expect("read testing governance workflow");

    // Act
    let has_expensive_jobs = workflow.contains("coverage-tier-diff:")
        && workflow.contains("mutation-pilot:")
        && workflow.contains("cargo llvm-cov")
        && workflow.contains("cargo mutants");

    // Assert
    assert!(workflow.contains("workflow_dispatch:"));
    assert!(workflow.contains("schedule:"));
    assert!(!workflow.contains("pull_request:"));
    assert!(!workflow.contains("workflow_run:"));
    assert!(workflow.contains("find tests -maxdepth 1"));
    assert!(!workflow.contains("cargo llvm-cov --tests"));
    assert!(has_expensive_jobs);
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

#[test]
fn should_report_unit_only_coverage_islands_given_distinct_tier_reports() {
    // Arrange
    let temp = tempfile::tempdir().expect("create coverage fixture directory");
    let unit = temp.path().join("unit.json");
    let integration = temp.path().join("integration.json");
    fs::write(
        &unit,
        r#"{"data":[{"files":[{"filename":"/repo/src/wal/recovery.rs","summary":{"lines":{"covered":17}}},{"filename":"/repo/src/runtime/live.rs","summary":{"lines":{"covered":8}}}],"functions":[{"name":"_RNvCsUnitHash_unit_only","filenames":["/repo/src/wal/recovery.rs"],"regions":[[1,1,2,1,3,0,0,0]]},{"name":"_RNvCsUnitHash_shared","filenames":["/repo/src/runtime/live.rs"],"regions":[[1,1,2,1,2,0,0,0]]}]}]}"#,
    )
    .expect("write unit coverage fixture");
    fs::write(
        &integration,
        r#"{"data":[{"files":[{"filename":"/repo/src/runtime/live.rs","summary":{"lines":{"covered":3}}}],"functions":[{"name":"_RNvCsIntegrationHash_shared","filenames":["/repo/src/runtime/live.rs"],"regions":[[1,1,2,1,1,0,0,0]]}]}]}"#,
    )
    .expect("write integration coverage fixture");

    // Act
    let output = Command::new("python3")
        .arg("scripts/coverage_tier_diff.py")
        .arg(&unit)
        .arg(&integration)
        .output()
        .expect("run coverage tier diff");
    let report = String::from_utf8(output.stdout).expect("coverage report is UTF-8");

    // Assert
    assert!(output.status.success());
    assert!(report.contains("src/wal/recovery.rs"));
    assert!(report.contains("src/wal/recovery.rs:1:1"));
    assert!(!report.contains("src/runtime/live.rs"));
}
