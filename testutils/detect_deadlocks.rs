// Deadlock Pattern Detection Utility
// ==================================
// Scans for common deadlock and concurrency antipatterns in Rust code.
//
// Usage:
//   cargo run --bin detect_deadlocks -- --summary
//   cargo run --bin detect_deadlocks -- --file src/wal/fs/batched_sync.rs

use regex::Regex;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

// -----------------------------------------------------------------------------
// CLI Args
// -----------------------------------------------------------------------------

struct Args {
    summary: bool,
    file: Option<PathBuf>,
}

impl Args {
    fn parse() -> Self {
        let args: Vec<String> = env::args().collect();

        let summary = args.iter().any(|a| a == "--summary" || a == "-s");

        let file = args
            .iter()
            .position(|a| a == "--file" || a == "-f")
            .and_then(|i| args.get(i + 1))
            .map(PathBuf::from);

        Args { summary, file }
    }
}

// -----------------------------------------------------------------------------
// Issue Model
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct DeadlockIssue {
    file: String,
    line: usize,
    pattern: String,
    severity: Severity,
    description: String,
    fix_suggestion: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Severity {
    High,
    Medium,
    Low,
}

impl Severity {
    fn color(&self) -> &str {
        match self {
            Severity::High => "\x1b[31m",
            Severity::Medium => "\x1b[33m",
            Severity::Low => "\x1b[36m",
        }
    }

    fn label(&self) -> &str {
        match self {
            Severity::High => "HIGH",
            Severity::Medium => "MEDIUM",
            Severity::Low => "LOW",
        }
    }
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn get_indentation(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

/// Detect whether `.wait()` is inside an immediately enclosing `loop` block.
fn has_enclosing_loop(lines: &[String], idx: usize) -> bool {
    let mut brace_depth = 0;

    for line in lines[..idx].iter().rev() {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }

        // Track brace depth to properly handle nested constructs
        for ch in t.chars().rev() {
            match ch {
                '}' => brace_depth += 1,
                '{' => {
                    if brace_depth > 0 {
                        brace_depth -= 1;
                    } else {
                        // We're at the opening brace of the current block
                        // Check if this block is a loop
                        if t == "loop" || t.starts_with("loop {") || t.ends_with("loop {") {
                            return true;
                        }
                        // If it's another construct (if/while/for/match), keep looking
                        // but only if we're at the same brace level
                        if t.starts_with("if ")
                            || t.starts_with("while ")
                            || t.starts_with("for ")
                        {
                            return false;
                        }
                        // For match, continue looking - it could be inside a loop
                    }
                }
                _ => {}
            }
        }

        // Also check for standalone "loop" keyword
        if brace_depth == 0 && (t == "loop" || t == "loop {") {
            return true;
        }
    }
    false
}

#[derive(Debug, Clone)]
struct LockRegion {
    guard_name: String,
    mutex_name: String,
    start_line: usize,
    start_idx: usize,
    end_idx: usize,
}

/// Try to extract `let guard = mutex.lock()` pattern.
fn extract_lock_region(
    lines: &[String],
    idx: usize,
    line: &str,
) -> Option<LockRegion> {
    let re =
        Regex::new(r"let\s+mut?\s*(\w+)\s*=\s*(\w+)\.lock\(").unwrap();
    let caps = re.captures(line)?;
    let guard_name = caps.get(1)?.as_str().to_string();
    let mutex_name = caps.get(2)?.as_str().to_string();

    let base_indent = get_indentation(line);

    // Region ends at first blank line or dedent
    let mut end_idx = idx + 1;
    while end_idx < lines.len() {
        let next = &lines[end_idx];
        if next.trim().is_empty() {
            break;
        }
        let next_indent = get_indentation(next);
        if next_indent < base_indent {
            break;
        }
        end_idx += 1;
    }

    Some(LockRegion {
        guard_name,
        mutex_name,
        start_line: idx + 1,
        start_idx: idx,
        end_idx,
    })
}

/// Is a while-line an atomic spin with empty body?
fn is_empty_atomic_spin(lines: &[String], idx: usize, line: &str) -> bool {
    let spin_re = Regex::new(r"while\s+[\w\.]+\s*\.load\(").unwrap();
    if !spin_re.is_match(line) {
        return false;
    }

    // Handle `while cond {}` on one line.
    if line.contains('{') && line.contains('}') {
        let body = line
            .split('{')
            .nth(1)
            .and_then(|s| s.split('}').next())
            .unwrap_or("")
            .trim();
        return body.is_empty() || body.starts_with("//");
    }

    // Otherwise, look at subsequent lines until the closing `}`.
    let mut seen_open = line.contains('{');
    let mut j = idx + 1;

    while j < lines.len() {
        let l = lines[j].trim();
        if l.is_empty() {
            j += 1;
            continue;
        }

        if !seen_open {
            if l.starts_with('{') {
                seen_open = true;
                // If the same line also closes, check body.
                if l.ends_with('}') {
                    let body = l
                        .trim_start_matches('{')
                        .trim_end_matches('}')
                        .trim();
                    return body.is_empty() || body.starts_with("//");
                }
                j += 1;
                continue;
            } else {
                // not a braced loop body
                return false;
            }
        } else {
            // Inside body, stop at closing brace.
            if l == "}" {
                return true;
            }
            if !l.starts_with("//") && !l.is_empty() {
                return false;
            }
        }

        j += 1;
    }

    false
}

// -----------------------------------------------------------------------------
// File Scanner
// -----------------------------------------------------------------------------

fn scan_file(file_path: &Path) -> Vec<DeadlockIssue> {
    let content = match fs::read_to_string(file_path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let lines: Vec<String> = content.lines().map(|l| l.to_string()).collect();
    let mut issues = Vec::new();
    let file_str = file_path.to_string_lossy().to_string();

    // Common regexes
    let wait_re = Regex::new(r"\.wait\(").unwrap();
    let wait_mut_re = Regex::new(r"\.wait\(\s*&?mut").unwrap();
    let wait_while_re = Regex::new(r"\.wait_while\(").unwrap();
    let condvar_re = Regex::new(r"\bcondvar\b|\bcond\b|\bcv\b").unwrap();
    let lock_call_re = Regex::new(r"(\w+)\.lock\(").unwrap();
    let await_re = Regex::new(r"\.await\b").unwrap();
    let io_re = Regex::new(
        r"(read|write|fsync|flush|recv|send|accept|connect|sleep|park_timeout)",
    )
    .unwrap();

    // A small pre-scan for while lines (atomic spin)
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();

        // A-1: Condvar.wait without loop/wait_while
        if wait_mut_re.is_match(trimmed)
            && !wait_while_re.is_match(trimmed)
            && condvar_re.is_match(trimmed)
        {
            if !has_enclosing_loop(&lines, i) {
                issues.push(DeadlockIssue {
                    file: file_str.clone(),
                    line: i + 1,
                    pattern: "condvar.wait() without loop".into(),
                    severity: Severity::High,
                    description:
                        "Condvar.wait() outside a loop can miss notifications and deadlock."
                            .into(),
                    fix_suggestion:
                        "Wrap condvar.wait() in a loop or use wait_while with a predicate."
                            .into(),
                });
            }
        }

        // A-4 / C-11: Empty atomic spin-loop
        if trimmed.contains("while")
            && trimmed.contains(".load(")
            && is_empty_atomic_spin(&lines, i, trimmed)
        {
            issues.push(DeadlockIssue {
                file: file_str.clone(),
                line: i + 1,
                pattern: "empty atomic spin-loop".into(),
                severity: Severity::High,
                description:
                    "Tight atomic spin loop with empty body can starve other threads and deadlock."
                        .into(),
                fix_suggestion:
                    "Add backoff, condvar parking, or a bounded retry strategy instead of tight spinning."
                        .into(),
            });
        }
    }

    // Lock-region based analysis (A, B, C)
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.starts_with("let ")
            && trimmed.contains(".lock(")
            && trimmed.contains('=')
        {
            if let Some(region) = extract_lock_region(&lines, i, line) {
                // A-2: Double-lock same mutex in same region
                let mut double_lock_reported = false;

                for j in (region.start_idx + 1)..region.end_idx {
                    let l = &lines[j];
                    if l.contains(&format!("{}.lock(", region.mutex_name)) {
                        issues.push(DeadlockIssue {
                            file: file_str.clone(),
                            line: region.start_line,
                            pattern: "double-lock of same mutex".into(),
                            severity: Severity::High,
                            description: format!(
                                "Mutex '{}' appears to be locked again before guard '{}' is dropped.",
                                region.mutex_name, region.guard_name
                            ),
                            fix_suggestion:
                                "Ensure the first guard is dropped (or scope exited) before acquiring the lock again."
                                    .into(),
                        });
                        double_lock_reported = true;
                        break;
                    }
                }

                // A-3: Nested A.lock -> B.lock -> A.lock (heuristic)
                if !double_lock_reported {
                    let mut seen_other_lock = false;
                    let mut other_mutexes: Vec<String> = Vec::new();

                    for j in (region.start_idx + 1)..region.end_idx {
                        let l = &lines[j];
                        if let Some(caps) = lock_call_re.captures(l) {
                            let name = caps.get(1).unwrap().as_str().to_string();
                            if name != region.mutex_name {
                                seen_other_lock = true;
                                if !other_mutexes.contains(&name) {
                                    other_mutexes.push(name);
                                }
                            } else if seen_other_lock {
                                issues.push(DeadlockIssue {
                                    file: file_str.clone(),
                                    line: region.start_line,
                                    pattern: "nested lock reacquire pattern".into(),
                                    severity: Severity::High,
                                    description: format!(
                                        "Mutex '{}' is locked, then other mutex(es) {:?}, then '{}' appears to be locked again in the same region.",
                                        region.mutex_name, other_mutexes, region.mutex_name
                                    ),
                                    fix_suggestion:
                                        "Avoid re-acquiring the same mutex inside another mutex's critical section. Consider lock ordering or splitting regions."
                                            .into(),
                                });
                                break;
                            }
                        }
                    }
                }

                // B-7 / C-12: Long lock-held region
                let region_len = region.end_idx.saturating_sub(region.start_idx);
                if region_len > 20 {
                    issues.push(DeadlockIssue {
                        file: file_str.clone(),
                        line: region.start_line,
                        pattern: "long critical section".into(),
                        severity: Severity::Medium,
                        description: format!(
                            "Lock guard '{}' for mutex '{}' is held across ~{} lines.",
                            region.guard_name, region.mutex_name, region_len
                        ),
                        fix_suggestion:
                            "Consider reducing the amount of work in this critical section or splitting it into smaller regions."
                                .into(),
                    });
                }

                // B-8: Blocking I/O while holding lock
                for j in (region.start_idx + 1)..region.end_idx {
                    let l = &lines[j];
                    if io_re.is_match(l) {
                        issues.push(DeadlockIssue {
                            file: file_str.clone(),
                            line: j + 1,
                            pattern: "blocking I/O while holding lock".into(),
                            severity: Severity::Medium,
                            description: format!(
                                "Potential blocking I/O (read/write/fsync/sleep/etc) while holding guard '{}' on mutex '{}'.",
                                region.guard_name, region.mutex_name
                            ),
                            fix_suggestion:
                                "Perform blocking I/O outside the critical section or redesign to minimize lock-hold time."
                                    .into(),
                        });
                        break;
                    }
                }

                // B-9: .await while holding lock (async)
                for j in (region.start_idx + 1)..region.end_idx {
                    let l = &lines[j];
                    if await_re.is_match(l) {
                        issues.push(DeadlockIssue {
                            file: file_str.clone(),
                            line: j + 1,
                            pattern: "await while holding lock".into(),
                            severity: Severity::Medium,
                            description: format!(
                                "Async .await encountered while guard '{}' for mutex '{}' is in scope.",
                                region.guard_name, region.mutex_name
                            ),
                            fix_suggestion:
                                "Release the lock before awaiting, or use async-aware synchronization primitives."
                                    .into(),
                        });
                        break;
                    }
                }

                // C-13: Guard clone / copy (perf smell)
                for j in (region.start_idx + 1)..region.end_idx {
                    let l = &lines[j];
                    if l.contains(&format!("{}.clone()", region.guard_name)) {
                        issues.push(DeadlockIssue {
                            file: file_str.clone(),
                            line: j + 1,
                            pattern: "lock guard clone".into(),
                            severity: Severity::Low,
                            description: format!(
                                "Lock guard '{}' is cloned while holding mutex '{}'.",
                                region.guard_name, region.mutex_name
                            ),
                            fix_suggestion:
                                "Prefer borrowing the guard rather than cloning it; if you need shared data, clone the inner value instead."
                                    .into(),
                        });
                        break;
                    }
                }

                // B-10: condvar.wait(...) without reassigning guard (suspicious)
                for j in (region.start_idx + 1)..region.end_idx {
                    let l = &lines[j];
                    if wait_re.is_match(l) && l.contains(&region.guard_name) {
                        // If the line doesn't contain '=' it likely isn't reassigning the guard
                        if !l.contains('=') {
                            issues.push(DeadlockIssue {
                                file: file_str.clone(),
                                line: j + 1,
                                pattern: "condvar.wait without guard reassignment".into(),
                                severity: Severity::Medium,
                                description: format!(
                                    "condvar.wait(...) uses guard '{}' without reassigning the returned guard, which may lose wakeups or hold stale state.",
                                    region.guard_name
                                ),
                                fix_suggestion:
                                    "Assign the result of condvar.wait(...) back to the guard: `guard = condvar.wait(guard)?;`"
                                        .into(),
                            });
                        }
                    }
                }

                // A-6 / B-6: Notify while holding lock (only if guard is still live)
                for j in (region.start_idx + 1)..region.end_idx {
                    let l = &lines[j];
                    if (l.contains(".notify_all()") || l.contains(".notify_one()"))
                        && condvar_re.is_match(l)
                    {
                        issues.push(DeadlockIssue {
                            file: file_str.clone(),
                            line: j + 1,
                            pattern: "notify while holding lock".into(),
                            severity: Severity::Low,
                            description: format!(
                                "Condvar notify is called while guard '{}' on mutex '{}' is still in scope.",
                                region.guard_name, region.mutex_name
                            ),
                            fix_suggestion:
                                "Typically drop the guard before calling notify_* to reduce contention and thundering herd.".into(),
                        });
                        break;
                    }
                }
            }
        }
    }

    issues
}

// -----------------------------------------------------------------------------
// Directory scanning
// -----------------------------------------------------------------------------

fn find_all_rust_files(dir: &Path) -> Vec<PathBuf> {
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

fn get_all_issues() -> Vec<DeadlockIssue> {
    let mut all = Vec::new();

    for dir in &["src", "tests"] {
        let root = Path::new(dir);
        if root.exists() {
            for file in find_all_rust_files(root) {
                all.extend(scan_file(&file));
            }
        }
    }

    all
}

// -----------------------------------------------------------------------------
// Output
// -----------------------------------------------------------------------------

fn print_summary() {
    println!("\x1b[36mScanning for deadlock patterns...\x1b[0m\n");

    let issues = get_all_issues();

    if issues.is_empty() {
        println!("\x1b[32m✓ No deadlock patterns detected!\x1b[0m");
        return;
    }

    let high = issues.iter().filter(|i| i.severity == Severity::High).count();
    let medium = issues.iter().filter(|i| i.severity == Severity::Medium).count();
    let low = issues.iter().filter(|i| i.severity == Severity::Low).count();

    println!("\x1b[33mSummary:\x1b[0m");
    println!("  Total issues: {}", issues.len());
    println!("  \x1b[31mHigh severity: {}\x1b[0m", high);
    println!("  \x1b[33mMedium severity: {}\x1b[0m", medium);
    println!("  \x1b[36mLow severity: {}\x1b[0m", low);
    println!();

    for severity in &[Severity::High, Severity::Medium, Severity::Low] {
        let group: Vec<_> = issues.iter().filter(|i| &i.severity == severity).collect();
        if group.is_empty() {
            continue;
        }

        println!(
            "{}━━━ {} SEVERITY ━━━\x1b[0m",
            severity.color(),
            severity.label()
        );

        for issue in group {
            println!(
                "{}  📍 {}:{}\x1b[0m",
                severity.color(),
                issue.file,
                issue.line
            );
            println!("     Pattern: {}", issue.pattern);
            println!("     Issue: {}", issue.description);
            println!("     Fix: {}", issue.fix_suggestion);
            println!();
        }
    }

    println!("\x1b[36mFor details, see docs/DEADLOCK_DETECTION.md\x1b[0m");
}

fn print_file_results(path: &Path) {
    println!("\x1b[36mChecking: {}\x1b[0m\n", path.display());

    let issues = scan_file(path);

    if issues.is_empty() {
        println!("\x1b[32m✓ No deadlock patterns detected\x1b[0m");
        return;
    }

    println!("\x1b[33mFound {} potential issue(s):\x1b[0m\n", issues.len());

    for issue in issues {
        println!(
            "{}[{}] Line {}\x1b[0m",
            issue.severity.color(),
            issue.severity.label(),
            issue.line
        );
        println!("  Pattern: {}", issue.pattern);
        println!("  Issue: {}", issue.description);
        println!("  Fix: {}", issue.fix_suggestion);
        println!();
    }
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

fn main() {
    let args = Args::parse();

    if args.summary {
        print_summary();
    } else if let Some(file) = args.file {
        print_file_results(&file);
    } else {
        println!("\x1b[36mDeadlock Pattern Detection\x1b[0m");
        println!("\x1b[36m=========================\x1b[0m\n");
        println!("Usage:");
        println!("  cargo run --bin detect_deadlocks -- --summary");
        println!("  cargo run --bin detect_deadlocks -- --file <PATH>\n");
        println!("Patterns detected (high-level):");
        println!("  • condvar.wait() without loop or wait_while (HIGH)");
        println!("  • Double-lock and nested lock reacquire patterns (HIGH)");
        println!("  • Empty atomic spin-loops (HIGH)");
        println!("  • Long critical sections and blocking I/O under lock (MEDIUM)");
        println!("  • Await while holding lock (MEDIUM)");
        println!("  • Suspicious condvar.wait usage (MEDIUM)");
        println!("  • Notify while holding lock, guard clones (LOW)\n");
    }
}
