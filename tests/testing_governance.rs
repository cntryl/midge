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
