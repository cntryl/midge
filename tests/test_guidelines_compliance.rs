//! Meta-test that validates all tests follow guidelines in docs/dev/test_guidelines.md
//!
//! This meta-test ensures consistency, clarity, and maintainability across all test files.
//! It automatically fails if tests violate required conventions.
//!
//! ## Validation Rules
//!
//! 1. ❌ **Naming Violations** — Tests use `test_*` instead of `should_*`
//! 2. ⚠️ **AAA Violations** — Missing or combined Arrange/Act/Assert comments in tests >5 lines
//! 3. ⚠️ **Behavior Violations** — Multiple `// Act` sections or `_and_` in name (multi-behavior)
//! 4. 📁 **Organization Violations** — Test file naming or structure issues
//!
//! ## Run Locally
//! ```bash
//! cargo test test_guidelines_compliance
//! ```
//!
//! For context on rules, see: `docs/dev/test_guidelines.md`

#[path = "../testutils/validate_tests.rs"]
mod validate_tests;

use validate_tests::{check_test_organization, get_all_test_results, TestResult};

#[test]
fn should_enforce_test_naming_convention() {
    // Arrange
    let all_results = get_all_test_results();

    // Act
    let violations: Vec<&TestResult> = all_results
        .iter()
        .filter(|r| r.issues.iter().any(|i| i.starts_with("NAMING:")))
        .collect();

    // Assert
    if !violations.is_empty() {
        let mut message = String::from("\n\n❌ TEST NAMING CONVENTION VIOLATIONS\n");
        message.push_str("───────────────────────────────────────────────\n\n");
        message.push_str("Each test must use the `should_*` naming pattern.\n");
        message.push_str("This improves readability and documents expected behavior.\n");
        message.push_str("Rename tests using `test_*` → `should_*`.\n\n");

        for result in &violations {
            message.push_str(&format!(
                "• {}:{} → '{}' should be renamed to 'should_*'\n",
                result.file, result.line, result.test_name
            ));
        }

        message.push_str(&format!(
            "\nTotal Violations: {}\n\nSee: docs/dev/test_guidelines.md#1-naming-convention",
            violations.len()
        ));
        panic!("{}", message);
    }
}

#[test]
fn should_enforce_arrange_act_assert_structure() {
    // Arrange
    let all_results = get_all_test_results();

    // Act
    let violations: Vec<&TestResult> = all_results
        .iter()
        .filter(|r| r.issues.iter().any(|i| i.starts_with("AAA:")))
        .collect();

    // Assert
    if !violations.is_empty() {
        let mut message = String::from("\n\n⚠️  AAA STRUCTURE VIOLATIONS\n");
        message.push_str("─────────────────────────────\n\n");
        message.push_str("All tests longer than 5 lines must clearly separate stages using:\n");
        message.push_str("  // Arrange\n  // Act\n  // Assert\n\n");
        message.push_str("This helps future readers immediately understand the test flow.\n\n");

        for result in &violations {
            message.push_str(&format!(
                "• {}:{} — '{}'\n",
                result.file, result.line, result.test_name
            ));
            for issue in &result.issues {
                if issue.starts_with("AAA:") {
                    message.push_str(&format!("    ↳ {}\n", issue));
                }
            }
            message.push('\n');
        }

        message.push_str(&format!(
            "Found {} tests missing proper AAA structure.\n\n",
            violations.len()
        ));
        message.push_str("💡 Example of correct format:\n\n");
        message.push_str("  #[test]\n");
        message.push_str("  fn should_do_something() {\n");
        message.push_str("      // Arrange\n");
        message.push_str("      let setup = create_test_data();\n\n");
        message.push_str("      // Act\n");
        message.push_str("      let result = perform_operation(setup);\n\n");
        message.push_str("      // Assert\n");
        message.push_str("      assert_eq!(result, expected);\n");
        message.push_str("  }\n");

        panic!("{}", message);
    }
}

#[test]
fn should_enforce_single_behavior_principle() {
    // Arrange
    let all_results = get_all_test_results();

    // Act
    let violations: Vec<&TestResult> = all_results
        .iter()
        .filter(|r| {
            r.issues.iter().any(|i| {
                i.starts_with("MULTI-BEHAVIOR:")
                    && (i.contains("'// Act' sections") || i.contains("_and_"))
            })
        })
        .collect();

    // Assert
    if !violations.is_empty() {
        let mut message = String::from("\n\n⚠️  SINGLE-BEHAVIOR VIOLATIONS\n");
        message.push_str("───────────────────────────────\n\n");
        message.push_str("Each test should verify ONE specific behavior.\n");
        message.push_str("Multiple '// Act' sections or names like 'should_upload_and_download'\n");
        message.push_str("indicate more than one behavior is being tested.\n\n");
        message.push_str("Split such tests into focused, independent ones.\n\n");

        for result in &violations {
            message.push_str(&format!(
                "• {}:{} — '{}'\n",
                result.file, result.line, result.test_name
            ));
            for issue in &result.issues {
                if issue.starts_with("MULTI-BEHAVIOR:") {
                    message.push_str(&format!("    ↳ {}\n", issue));
                }
            }
            message.push('\n');
        }

        message.push_str(&format!(
            "Found {} multi-behavior tests.\n\n",
            violations.len()
        ));
        message.push_str("💡 Example of correct structure:\n\n");
        message.push_str("  #[test]\n");
        message.push_str("  fn should_upload_data_successfully() { ... }\n\n");
        message.push_str("  #[test]\n");
        message.push_str("  fn should_download_uploaded_data() { ... }\n");

        panic!("{}", message);
    }
}

#[test]
fn should_enforce_proper_test_file_organization() {
    // Arrange
    let issues = check_test_organization();

    // Act
    if !issues.is_empty() {
        let mut message = String::from("\n\n📁 TEST ORGANIZATION VIOLATIONS\n");
        message.push_str("────────────────────────────────\n\n");
        message.push_str(
            "Ensure test files follow Rust conventions for clarity and discoverability.\n",
        );
        message.push_str("Guidelines:\n");
        message.push_str("  • Files under `tests/` are auto-discovered by Cargo.\n");
        message.push_str("  • Nested tests require `mod.rs` or explicit `mod` imports.\n");
        message.push_str("  • Use descriptive, snake_case filenames (e.g., `engine_scans.rs`).\n");
        message.push_str("  • Avoid redundant prefixes like `test_` in filenames.\n\n");

        for issue in &issues {
            message.push_str(&format!("• {}\n    {}\n\n", issue.file_path, issue.issue));
        }

        message.push_str(&format!("Found {} organization issues.\n\n", issues.len()));
        message.push_str("💡 Fix by renaming or restructuring test files per above rules.\n");

        panic!("{}", message);
    }
}

// Note: The meta-tests above are intentionally exempt from AAA enforcement
// because they are internal compliance verifiers, not behavior tests.
