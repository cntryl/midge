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

use validate_tests::{get_all_test_results, TestResult};

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
        let mut msg = String::from("\n\n❌ TEST NAMING CONVENTION VIOLATIONS\n");
        msg.push_str("───────────────────────────────────────────────\n\n");
        msg.push_str("Each test must use the `should_*` naming pattern.\n");
        msg.push_str("Rename tests using `test_*` → `should_*` for clarity.\n\n");

        for r in &violations {
            msg.push_str(&format!(
                "• {}:{} → '{}' should be renamed to 'should_*'\n",
                r.file, r.line, r.test_name
            ));
        }

        msg.push_str(&format!(
            "\nTotal Violations: {}\n\nSee: docs/dev/test_guidelines.md#naming",
            violations.len()
        ));
        panic!("{}", msg);
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
        let mut msg = String::from("\n\n⚠️  AAA STRUCTURE VIOLATIONS\n");
        msg.push_str("─────────────────────────────\n\n");
        msg.push_str("Tests longer than 5 lines must contain:\n");
        msg.push_str("  // Arrange\n  // Act\n  // Assert\n\n");
        msg.push_str("These comments clarify structure and intention.\n\n");

        for r in &violations {
            msg.push_str(&format!("• {}:{} — '{}'\n", r.file, r.line, r.test_name));
            for issue in &r.issues {
                if issue.starts_with("AAA:") {
                    msg.push_str(&format!("    ↳ {}\n", issue));
                }
            }
            msg.push('\n');
        }

        msg.push_str(&format!(
            "Found {} tests missing proper AAA structure.\n\n",
            violations.len()
        ));
        msg.push_str("💡 Example of correct format:\n\n");
        msg.push_str("  #[test]\n");
        msg.push_str("  fn should_perform_action() {\n");
        msg.push_str("      // Arrange\n");
        msg.push_str("      let setup = create_fixture();\n\n");
        msg.push_str("      // Act\n");
        msg.push_str("      let result = run_operation(setup);\n\n");
        msg.push_str("      // Assert\n");
        msg.push_str("      assert_eq!(result, expected);\n");
        msg.push_str("  }\n");

        panic!("{}", msg);
    }
}

#[test]
fn should_enforce_single_behavior_per_test() {
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
        let mut msg = String::from("\n\n⚠️  SINGLE-BEHAVIOR VIOLATIONS\n");
        msg.push_str("───────────────────────────────\n\n");
        msg.push_str("Each test should verify ONE behavior only.\n");
        msg.push_str("Multiple '// Act' blocks or '_and_' in names imply multi-behavior.\n\n");

        for r in &violations {
            msg.push_str(&format!("• {}:{} — '{}'\n", r.file, r.line, r.test_name));
            for issue in &r.issues {
                if issue.starts_with("MULTI-BEHAVIOR:") {
                    msg.push_str(&format!("    ↳ {}\n", issue));
                }
            }
            msg.push('\n');
        }

        msg.push_str(&format!(
            "Found {} multi-behavior tests.\n\n",
            violations.len()
        ));
        msg.push_str("💡 Split into separate tests:\n\n");
        msg.push_str("  #[test]\n");
        msg.push_str("  fn should_upload_file_successfully() { ... }\n\n");
        msg.push_str("  #[test]\n");
        msg.push_str("  fn should_download_uploaded_file() { ... }\n");

        panic!("{}", msg);
    }
}

// Meta-tests are exempt from AAA rules
