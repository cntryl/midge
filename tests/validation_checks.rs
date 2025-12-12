#[path = "../testutils/validate_tests.rs"]
mod validate_tests;
use validate_tests::{get_all_test_results, TestResult};

// ============================================================================
// TEST QUALITY VALIDATION
// ============================================================================
// These tests enforce code quality standards across the entire test suite.
// They scan all test files and report violations of naming conventions,
// structure requirements, and best practices.
//
// If any validation test fails, fix the reported issues before committing.
// ============================================================================

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
        let mut msg = String::from("\n\nâŒ TEST NAMING CONVENTION VIOLATIONS\n");
        msg.push_str("â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•\n\n");
        msg.push_str("REQUIRED: All tests must use 'should_{action}_when_{context}' pattern.\n");
        msg.push_str("REASON: Makes test intent immediately clear and improves readability.\n\n");
        msg.push_str("VIOLATIONS FOUND:\n\n");

        for r in &violations {
            msg.push_str(&format!("  â€¢ [{}:{}] '{}'\n", r.file, r.line, r.test_name));
            msg.push_str("    â””â”€ Rename to 'should_[action]_when_[context]'\n\n");
        }

        msg.push_str(&format!("Total: {} violation(s)\n\n", violations.len()));
        msg.push_str("ðŸ“– See: docs/dev/test_guidelines.md#naming\n");
        panic!("{}", msg);
    }
}

#[test]
fn should_enforce_aaa_structure() {
    // Arrange
    let all_results = get_all_test_results();

    // Act
    let violations: Vec<&TestResult> = all_results
        .iter()
        .filter(|r| r.issues.iter().any(|i| i.starts_with("AAA:")))
        .collect();

    // Assert
    if !violations.is_empty() {
        let mut msg = String::from("\n\nâš ï¸  AAA STRUCTURE VIOLATIONS\n");
        msg.push_str("â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•\n\n");
        msg.push_str("REQUIRED: Tests >5 lines must include:\n");
        msg.push_str("  // Arrange\n");
        msg.push_str("  // Act\n");
        msg.push_str("  // Assert\n\n");
        msg.push_str("REASON: Clear structure makes tests easier to understand and maintain.\n\n");
        msg.push_str("VIOLATIONS FOUND:\n\n");

        for r in &violations {
            msg.push_str(&format!("  â€¢ [{}:{}] '{}'\n", r.file, r.line, r.test_name));
            for issue in &r.issues {
                if issue.starts_with("AAA:") {
                    let detail = issue.strip_prefix("AAA: ").unwrap_or(issue);
                    msg.push_str(&format!("    â””â”€ {}\n", detail));
                }
            }
            msg.push('\n');
        }

        msg.push_str(&format!("Total: {} violation(s)\n\n", violations.len()));
        msg.push_str("ðŸ’¡ CORRECT FORMAT:\n\n");
        msg.push_str("  #[test]\n");
        msg.push_str("  fn should_perform_action_when_condition() {\n");
        msg.push_str("      // Arrange\n");
        msg.push_str("      let fixture = setup();\n\n");
        msg.push_str("      // Act\n");
        msg.push_str("      let result = operation(fixture);\n\n");
        msg.push_str("      // Assert\n");
        msg.push_str("      assert_eq!(result, expected);\n");
        msg.push_str("  }\n\n");
        msg.push_str("ðŸ“– See: docs/dev/test_guidelines.md#aaa-pattern\n");
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
        let mut msg = String::from("\n\nâš ï¸  SINGLE-BEHAVIOR VIOLATIONS\n");
        msg.push_str("â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•\n\n");
        msg.push_str("REQUIRED: Each test verifies exactly ONE behavior.\n");
        msg.push_str("REASON: Focused tests are easier to debug and maintain.\n\n");
        msg.push_str("INDICATORS OF MULTI-BEHAVIOR:\n");
        msg.push_str("  â€¢ Multiple '// Act' comments\n");
        msg.push_str("  â€¢ '_and_' in test name\n");
        msg.push_str("  â€¢ Unrelated assertions\n\n");
        msg.push_str("VIOLATIONS FOUND:\n\n");

        for r in &violations {
            msg.push_str(&format!("  â€¢ [{}:{}] '{}'\n", r.file, r.line, r.test_name));
            for issue in &r.issues {
                if issue.starts_with("MULTI-BEHAVIOR:") {
                    let detail = issue.strip_prefix("MULTI-BEHAVIOR: ").unwrap_or(issue);
                    msg.push_str(&format!("    â””â”€ {}\n", detail));
                }
            }
            msg.push('\n');
        }

        msg.push_str(&format!("Total: {} violation(s)\n\n", violations.len()));
        msg.push_str("ðŸ’¡ SPLIT INTO SEPARATE TESTS:\n\n");
        msg.push_str("  âŒ BAD:\n");
        msg.push_str("  fn should_upload_and_download_file() { ... }\n\n");
        msg.push_str("  âœ… GOOD:\n");
        msg.push_str("  fn should_upload_file_when_valid() { ... }\n");
        msg.push_str("  fn should_download_file_when_exists() { ... }\n\n");
        msg.push_str("ðŸ“– See: docs/dev/test_guidelines.md#single-behavior\n");
        panic!("{}", msg);
    }
}

#[test]
fn should_report_test_quality_summary() {
    // Arrange
    let all_results = get_all_test_results();
    let total_tests = all_results.len();

    // Act
    let naming_violations = all_results
        .iter()
        .filter(|r| r.issues.iter().any(|i| i.starts_with("NAMING:")))
        .count();

    let aaa_violations = all_results
        .iter()
        .filter(|r| r.issues.iter().any(|i| i.starts_with("AAA:")))
        .count();

    let behavior_violations = all_results
        .iter()
        .filter(|r| {
            r.issues.iter().any(|i| {
                i.starts_with("MULTI-BEHAVIOR:")
                    && (i.contains("'// Act' sections") || i.contains("_and_"))
            })
        })
        .count();

    let total_violations = naming_violations + aaa_violations + behavior_violations;
    let clean_tests = total_tests - total_violations;

    // Assert
    let mut msg = String::new();
    msg.push_str("\nâ•”â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•—\n");
    msg.push_str("â•‘         TEST QUALITY VALIDATION SUMMARY          â•‘\n");
    msg.push_str("â•šâ•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•\n\n");
    msg.push_str(&format!("  Total Tests Scanned:     {}\n", total_tests));
    msg.push_str(&format!(
        "  Tests Passing Standards: {} ({:.1}%)\n",
        clean_tests,
        (clean_tests as f64 / total_tests as f64) * 100.0
    ));
    msg.push_str("\n  Violations by Category:\n");
    msg.push_str(&format!(
        "    â€¢ Naming Convention:   {}\n",
        naming_violations
    ));
    msg.push_str(&format!("    â€¢ AAA Structure:       {}\n", aaa_violations));
    msg.push_str(&format!(
        "    â€¢ Single Behavior:     {}\n",
        behavior_violations
    ));
    msg.push_str("    â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€\n");
    msg.push_str(&format!(
        "    Total Violations:      {}\n",
        total_violations
    ));

    if total_violations == 0 {
        msg.push_str("\n  âœ… All tests meet quality standards!\n");
        println!("{}", msg);
    } else {
        msg.push_str(&format!(
            "\n  âš ï¸  {} test(s) need attention\n",
            total_violations
        ));
        msg.push_str("     Run individual validation tests for detailed reports:\n");
        msg.push_str(
            "       cargo test --test validation_checks should_enforce_test_naming_convention\n",
        );
        msg.push_str("       cargo test --test validation_checks should_enforce_aaa_structure\n");
        msg.push_str(
            "       cargo test --test validation_checks should_enforce_single_behavior_per_test\n",
        );
        panic!("{}", msg);
    }
}
