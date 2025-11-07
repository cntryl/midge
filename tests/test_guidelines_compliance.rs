//! Meta-test that validates all tests follow guidelines in docs/dev/test_guidelines.md
//!
//! This test fails if:
//! 1. Tests use deprecated `test_*` naming instead of `should_*`
//! 2. Tests are missing Arrange/Act/Assert comments (for tests >5 lines)
//! 3. Tests have combined AAA comments (e.g., "// Arrange + Act")
//! 4. Tests violate single behavior principle (multiple Acts or "_and_" in name)
//!
//! Run with: cargo test test_guidelines_compliance

// Include the validation module directly
#[path = "../testutils/validate_tests.rs"]
mod validate_tests;

use validate_tests::{check_test_organization, get_all_test_results, TestResult};

#[test]
#[ignore = "Test guidelines enforcement - run explicitly with --ignored"]
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
        let mut message = String::from("\n\n❌ Test Naming Convention Violations:\n\n");
        message.push_str("Tests should use 'should_*' naming, not 'test_*'\n");
        message.push_str("See docs/dev/test_guidelines.md for details\n\n");

        for result in &violations {
            message.push_str(&format!(
                "  • {}:{} - '{}' → should use 'should_*' pattern\n",
                result.file, result.line, result.test_name
            ));
        }

        message.push_str(&format!("\nFound {} violations\n", violations.len()));
        panic!("{}", message);
    }
}

#[test]
#[ignore = "Test guidelines enforcement - run explicitly with --ignored"]
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
        let mut message = String::from("\n\n⚠️  Test Structure Violations:\n\n");
        message.push_str("All tests >5 lines should have clear Arrange/Act/Assert comments\n");
        message.push_str("This applies to both unit tests (src/) and integration tests (tests/)\n");
        message.push_str("This helps readability and maintains consistency\n\n");

        for result in &violations {
            message.push_str(&format!(
                "  • {} - test '{}' (line {})\n",
                result.file, result.test_name, result.line
            ));
            for issue in &result.issues {
                if issue.starts_with("AAA:") {
                    message.push_str(&format!("    - {}\n", issue));
                }
            }
        }

        message.push_str(&format!(
            "\nFound {} tests without proper AAA structure\n",
            violations.len()
        ));
        message.push_str("\nExample:\n");
        message.push_str("  #[test]\n");
        message.push_str("  fn should_do_something() {\n");
        message.push_str("      // Arrange\n");
        message.push_str("      let setup = ...;\n\n");
        message.push_str("      // Act\n");
        message.push_str("      let result = setup.do_something();\n\n");
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
            r.issues
                .iter()
                .any(|i| i.starts_with("MULTI-BEHAVIOR:") && i.contains("'// Act' sections"))
        })
        .collect();

    // Assert
    if !violations.is_empty() {
        let mut message = String::from("\n\n⚠️  Multi-Behavior Violations:\n\n");
        message.push_str("Each test should verify ONE specific behavior\n");
        message.push_str("Tests with multiple '// Act' sections likely test multiple behaviors\n");
        message.push_str("Consider splitting into separate focused tests\n\n");

        for result in &violations {
            message.push_str(&format!(
                "  • {} - test '{}' (line {})\n",
                result.file, result.test_name, result.line
            ));
            for issue in &result.issues {
                if issue.starts_with("MULTI-BEHAVIOR:") && issue.contains("'// Act' sections") {
                    message.push_str(&format!("    - {}\n", issue));
                }
            }
        }

        message.push_str(&format!(
            "\nFound {} tests with multiple behaviors\n",
            violations.len()
        ));

        panic!("{}", message);
    }
}

#[test]
#[ignore = "Test guidelines enforcement - run explicitly with --ignored"]
fn should_enforce_proper_test_file_organization() {
    // Arrange
    let issues = check_test_organization();

    // Act - Check for any organization violations

    // Assert
    if !issues.is_empty() {
        let mut message = String::from("\n\n📁 Test Organization Violations:\n\n");
        message.push_str("Integration tests should follow Rust conventions:\n");
        message.push_str("  • Test files in tests/*.rs are auto-discovered by Cargo\n");
        message.push_str("  • Files in tests/subdirectories/ are modules (must be imported)\n");
        message.push_str("  • Use descriptive names: 'engine.rs', not 'test_engine.rs'\n");
        message.push_str("  • Use snake_case for all filenames\n\n");

        for issue in &issues {
            message.push_str(&format!("  • {}\n", issue.file_path));
            message.push_str(&format!("    {}\n\n", issue.issue));
        }

        message.push_str(&format!("Found {} organization issues\n", issues.len()));
        panic!("{}", message);
    }
}

// Note: The meta-tests above are intentionally simple and don't need AAA structure
// They are excluded from the compliance checks by being under 5 lines
