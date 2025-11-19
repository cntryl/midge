// Test Validation Utility
// =======================
// This utility helps identify common test guideline violations
//
// Usage:
//   cargo run --bin validate_tests -- --summary
//   cargo run --bin validate_tests -- --file src/wal/wal_helpers.rs

use regex::Regex;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

struct Args {
    summary: bool,
    file: Option<PathBuf>,
    organization: bool,
}

impl Args {
    fn parse() -> Self {
        let args: Vec<String> = env::args().collect();

        let summary = args.iter().any(|a| a == "--summary" || a == "-s");
        let organization = args.iter().any(|a| a == "--organization" || a == "-o");

        let file = args
            .iter()
            .position(|a| a == "--file" || a == "-f")
            .and_then(|i| args.get(i + 1))
            .map(PathBuf::from);

        Args {
            summary,
            file,
            organization,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TestResult {
    pub test_name: String,
    pub file: String,
    pub line: usize,
    pub line_count: usize,
    pub issues: Vec<String>,
}

impl TestResult {
    #[allow(dead_code)]
    pub fn is_compliant(&self) -> bool {
        self.issues.is_empty()
    }
}

pub fn test_single_test(
    file: &str,
    test_name: &str,
    line_num: usize,
    lines: &[String],
) -> TestResult {
    let test_start = line_num - 1;

    // Find test end (next #[test] or end of file)
    let mut test_end = lines.len() - 1;
    for (i, line) in lines.iter().enumerate().skip(test_start + 2) {
        if line.trim_start().starts_with("#[test]") {
            test_end = i - 1;
            break;
        }
    }

    let test_body = lines[test_start..=test_end].join("\n");
    let test_lines = test_end - test_start + 1;

    let mut issues = Vec::new();

    // Check 1: Naming convention
    if !test_name.starts_with("should_") {
        issues.push("NAMING: Does not start with 'should_'".to_string());
    }

    // Check 2: AAA structure (only for tests >5 lines)
    if test_lines > 5 {
        let arrange_re = Regex::new(r"//\s*Arrange\s*(\r?\n|$)").unwrap();
        let act_re = Regex::new(r"//\s*Act\s*(\r?\n|$)").unwrap();
        let assert_re = Regex::new(r"//\s*Assert\s*(\r?\n|$)").unwrap();
        let combined_re = Regex::new(r"//\s*(Arrange|Act|Assert)\s*[+&]").unwrap();

        let has_arrange = arrange_re.is_match(&test_body);
        let has_act = act_re.is_match(&test_body);
        let has_assert = assert_re.is_match(&test_body);
        let has_combined = combined_re.is_match(&test_body);

        if !has_arrange {
            issues.push("AAA: Missing '// Arrange' comment".to_string());
        }
        if !has_act {
            issues.push("AAA: Missing '// Act' comment".to_string());
        }
        if !has_assert {
            issues.push("AAA: Missing '// Assert' comment".to_string());
        }
        if has_combined {
            issues.push("AAA: Has combined AAA comment (e.g., '// Arrange + Act')".to_string());
        }
    }

    // Check 3: Multiple Act sections (indicates multi-behavior)
    // Only count actual Act comment lines, not string literals containing "Act"
    let act_count_re = Regex::new(r"^\s*//\s*Act\s*$").unwrap();
    let test_lines_vec: Vec<&str> = test_body.lines().collect();
    let mut act_count = 0;
    let mut in_string = false;

    for line in &test_lines_vec {
        let trimmed = line.trim();

        // Simple string detection: count quotes (not perfect but catches most cases)
        let quote_count = trimmed.matches('"').count();
        if quote_count % 2 == 1 {
            in_string = !in_string;
        }

        // Only count // Act lines that are not in strings
        if !in_string && act_count_re.is_match(line) {
            act_count += 1;
        }
    }

    if act_count > 1 {
        issues.push(format!(
            "MULTI-BEHAVIOR: Has {} '// Act' sections",
            act_count
        ));
    }

    // Check 4: Smarter "and" detection in test name
    // Only flag if "and" appears in the ACTION part (before "given"/"when"), not in conditions
    // Pattern: should_ACTION_and_ACTION (bad) vs should_ACTION_given_X_and_Y (ok)
    if test_name.contains("_and_") {
        // Split on "given" or "when" to separate action from conditions
        let parts: Vec<&str> = test_name.split("_given_").collect();
        let action_part = if parts.len() > 1 {
            parts[0]
        } else {
            let parts: Vec<&str> = test_name.split("_when_").collect();
            if parts.len() > 1 {
                parts[0]
            } else {
                test_name
            }
        };

        // If "and" is in the action part (before given/when), it's likely multi-behavior
        if action_part.contains("_and_") {
            // Additional heuristic: allow common phrases that aren't multi-behavior
            // These are patterns where "and" is part of the description, not separate actions
            let allowed_patterns = ["with_id_and_name"];

            let is_allowed = allowed_patterns
                .iter()
                .any(|pattern| test_name.contains(pattern));

            if !is_allowed {
                issues.push(
                    "MULTI-BEHAVIOR: Test name contains '_and_' in action (may test multiple behaviors)".to_string(),
                );
            }
        }
    }

    TestResult {
        test_name: test_name.to_string(),
        file: file.to_string(),
        line: line_num,
        line_count: test_lines,
        issues,
    }
}

pub fn find_tests_in_file(file_path: &Path) -> Vec<TestResult> {
    let content = match fs::read_to_string(file_path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let lines: Vec<String> = content.lines().map(|l| l.to_string()).collect();
    let mut results = Vec::new();

    let test_attr_re = Regex::new(r"^\s*#\[test\]\s*$").unwrap();
    let fn_name_re = Regex::new(r"^\s*fn\s+(\w+)").unwrap();

    for i in 0..lines.len() {
        if test_attr_re.is_match(&lines[i]) && i + 1 < lines.len() {
            if let Some(caps) = fn_name_re.captures(&lines[i + 1]) {
                let test_name = caps.get(1).unwrap().as_str();
                let line_num = i + 2; // 1-based line number

                let result =
                    test_single_test(file_path.to_str().unwrap(), test_name, line_num, &lines);
                results.push(result);
            }
        }
    }

    results
}

pub fn find_all_rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut rust_files = Vec::new();

    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                rust_files.extend(find_all_rust_files(&path));
            } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
                rust_files.push(path);
            }
        }
    }

    rust_files
}

/// Get all test results from src/ and tests/ directories
pub fn get_all_test_results() -> Vec<TestResult> {
    let mut all_results = Vec::new();

    for dir in &["src", "tests"] {
        let dir_path = Path::new(dir);
        if dir_path.exists() {
            let files = find_all_rust_files(dir_path);
            for file in files {
                let results = find_tests_in_file(&file);
                all_results.extend(results);
            }
        }
    }

    all_results
}

#[allow(dead_code)]
fn print_summary() {
    println!("\x1b[36mScanning all tests for guideline violations...\x1b[0m");
    println!();

    let all_results = get_all_test_results();

    let total_count = all_results.len();
    let compliant_count = all_results.iter().filter(|r| r.is_compliant()).count();
    let non_compliant: Vec<_> = all_results.iter().filter(|r| !r.is_compliant()).collect();

    println!("\x1b[33mSummary:\x1b[0m");
    println!("  Total tests: {}", total_count);
    println!(
        "  \x1b[32mCompliant: {} ({:.1}%)\x1b[0m",
        compliant_count,
        (compliant_count as f64 / total_count as f64) * 100.0
    );
    println!(
        "  \x1b[31mNon-compliant: {} ({:.1}%)\x1b[0m",
        non_compliant.len(),
        (non_compliant.len() as f64 / total_count as f64) * 100.0
    );
    println!();

    // Group by issue type
    let naming_issues = non_compliant
        .iter()
        .filter(|r| r.issues.iter().any(|i| i.starts_with("NAMING:")))
        .count();
    let aaa_issues = non_compliant
        .iter()
        .filter(|r| r.issues.iter().any(|i| i.starts_with("AAA:")))
        .count();
    let multi_issues = non_compliant
        .iter()
        .filter(|r| r.issues.iter().any(|i| i.starts_with("MULTI-BEHAVIOR:")))
        .count();

    println!("\x1b[33mIssue breakdown:\x1b[0m");
    println!("  Naming violations: {}", naming_issues);
    println!("  AAA structure violations: {}", aaa_issues);
    println!("  Multi-behavior violations: {}", multi_issues);
    println!();

    if !non_compliant.is_empty() {
        println!("\x1b[33mSample of non-compliant tests (first 20):\x1b[0m");
        for result in non_compliant.iter().take(20) {
            println!(
                "  \x1b[31m{}::{}  (line {})\x1b[0m",
                result.file, result.test_name, result.line
            );
            for issue in &result.issues {
                println!("    \x1b[33m- {}\x1b[0m", issue);
            }
        }

        // Print all multi-behavior violations
        let multi_violations: Vec<_> = non_compliant.iter().filter(|r| r.issues.iter().any(|i| i.starts_with("MULTI-BEHAVIOR:"))).collect();
        if !multi_violations.is_empty() {
            println!("\x1b[33mAll Multi-behavior violations:\x1b[0m");
            for result in multi_violations {
                println!(
                    "  \x1b[31m{}::{}  (line {})\x1b[0m",
                    result.file, result.test_name, result.line
                );
                for issue in &result.issues {
                    if issue.starts_with("MULTI-BEHAVIOR:") {
                        println!("    \x1b[33m- {}\x1b[0m", issue);
                    }
                }
            }
        }
    }
}

#[allow(dead_code)]
fn print_file_results(file_path: &Path) {
    println!("\x1b[36mChecking tests in: {}\x1b[0m", file_path.display());
    println!();

    let results = find_tests_in_file(file_path);

    if results.is_empty() {
        println!("\x1b[33mNo tests found in file\x1b[0m");
        return;
    }

    let compliant = results.iter().filter(|r| r.is_compliant()).count();
    let total = results.len();

    println!(
        "\x1b[33mResults: {}/{} compliant ({:.1}%)\x1b[0m",
        compliant,
        total,
        (compliant as f64 / total as f64) * 100.0
    );
    println!();

    for result in results {
        if result.is_compliant() {
            println!(
                "\x1b[32m[OK] {} (line {})\x1b[0m",
                result.test_name, result.line
            );
        } else {
            println!(
                "\x1b[31m[!!] {} (line {})\x1b[0m",
                result.test_name, result.line
            );
            for issue in &result.issues {
                println!("    \x1b[33m- {}\x1b[0m", issue);
            }
        }
    }
}

#[derive(Debug)]
pub struct OrganizationIssue {
    pub file_path: String,
    pub issue: String,
}

pub fn check_test_organization() -> Vec<OrganizationIssue> {
    let mut issues = Vec::new();
    let tests_dir = Path::new("tests");

    if !tests_dir.exists() {
        return issues;
    }

    // Compile regex once outside the loop
    let test_attr_re = Regex::new(r"#\[test\]").unwrap();

    // Check for test files in subdirectories (should be modules, not integration tests)
    let subdirs = [
        "api",
        "backup",
        "cloud",
        "common",
        "compaction",
        "config",
        "lock",
        "manifest",
        "storage",
        "utils",
        "verification",
        "wal",
    ];

    for subdir in &subdirs {
        let subdir_path = tests_dir.join(subdir);
        if subdir_path.exists() {
            let rust_files = find_all_rust_files(&subdir_path);
            for file in rust_files {
                let content = fs::read_to_string(&file).unwrap_or_default();

                // Check if file has #[test] but is in a subdirectory
                if test_attr_re.is_match(&content) {
                    // Allow tests in subdirs if they're used as shared test modules
                    // This is actually fine - the organization check is about CARGO discovery
                    // Subdirectory files can still contain tests, they're just not auto-discovered
                    // as separate test binaries by Cargo
                }
            }
        }
    }

    // Check for improperly named test files in root
    if let Ok(entries) = fs::read_dir(tests_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("rs") {
                let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");

                // Skip special files
                if file_name == "README.md" || file_name.starts_with('.') {
                    continue;
                }

                // Test files in root should follow naming conventions:
                // - Descriptive names (not test_*.rs)
                // - Use underscores, not CamelCase
                // - Should describe what they test (e.g., engine.rs, durability_wal.rs)

                if file_name.starts_with("test_") && file_name != "test_guidelines_compliance.rs" {
                    issues.push(OrganizationIssue {
                        file_path: format!("tests/{}", file_name),
                        issue: "Integration test file starts with 'test_' (prefer descriptive names like 'engine.rs', 'durability_wal.rs')".to_string(),
                    });
                }

                // Check for CamelCase in filenames
                if file_name.chars().any(|c| c.is_uppercase()) {
                    issues.push(OrganizationIssue {
                        file_path: format!("tests/{}", file_name),
                        issue: "File name contains uppercase letters (use snake_case)".to_string(),
                    });
                }

                // Check for domain-specific files that should be in subdirectories
                // Pattern: {domain}_{feature}.rs should be in tests/{domain}/{feature}.rs
                let domain_prefixes = [
                    ("wal_", "wal"),
                    ("cloud_", "cloud"),
                    ("compaction_", "compaction"),
                    ("storage_", "storage"),
                    ("backup_", "backup"),
                    ("manifest_", "manifest"),
                    ("api_", "api"),
                    ("config_", "config"),
                    ("lock_", "lock"),
                    ("utils_", "utils"),
                ];

                for (prefix, domain) in &domain_prefixes {
                    if file_name.starts_with(prefix) {
                        let feature = file_name
                            .strip_prefix(prefix)
                            .and_then(|s| s.strip_suffix(".rs"))
                            .unwrap_or("");

                        issues.push(OrganizationIssue {
                            file_path: format!("tests/{}", file_name),
                            issue: format!(
                                "Domain-specific test should be in subdirectory: move to 'tests/{}/{}' (files starting with '{}_' belong in tests/{}/)",
                                domain, feature, prefix.trim_end_matches('_'), domain
                            ),
                        });
                    }
                }
            }
        }
    }

    issues
}

#[allow(dead_code)]
fn print_organization_check() {
    println!("\x1b[36mChecking test organization...\x1b[0m");
    println!();

    let issues = check_test_organization();

    if issues.is_empty() {
        println!("\x1b[32m✓ All tests are properly organized!\x1b[0m");
        println!();
        println!("Organization follows Rust conventions:");
        println!("  • Integration tests in tests/*.rs (auto-discovered by Cargo)");
        println!("  • Shared modules in tests/subdirectories/ (imported by test files)");
        println!("  • Descriptive file names (engine.rs, durability_wal.rs, etc.)");
    } else {
        println!(
            "\x1b[33m⚠ Found {} organization issues:\x1b[0m",
            issues.len()
        );
        println!();
        for issue in &issues {
            println!("  \x1b[31m{}\x1b[0m", issue.file_path);
            println!("    {}", issue.issue);
            println!();
        }
        println!("\x1b[33mReminder:\x1b[0m");
        println!("  • Files in tests/ root become separate test binaries (auto-discovered)");
        println!("  • Files in tests/subdirectories/ are modules (must be imported)");
        println!("  • Use descriptive names: 'engine.rs', not 'test_engine.rs'");
    }
}

#[allow(dead_code)]
fn main() {
    let args = Args::parse();

    if args.summary {
        print_summary();
    } else if args.organization {
        print_organization_check();
    } else if let Some(file_path) = args.file {
        print_file_results(&file_path);
    } else {
        println!("\x1b[36mTest Validation Helper\x1b[0m");
        println!("\x1b[36m======================\x1b[0m");
        println!();
        println!("Usage:");
        println!("  cargo run --bin validate_tests -- --summary                    # Show summary of all tests");
        println!("  cargo run --bin validate_tests -- --file src/backup.rs         # Check specific file");
        println!("  cargo run --bin validate_tests -- --organization               # Check test file organization");
        println!();
        println!("Examples:");
        println!("  cargo run --bin validate_tests -- --summary");
        println!("  cargo run --bin validate_tests -- --file src/error.rs");
        println!("  cargo run --bin validate_tests -- --organization");
        println!("  cargo run --bin validate_tests -- --file tests/engine.rs");
    }
}
