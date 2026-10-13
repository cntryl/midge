//! Run the actual named WAL regressions; matching RED baselines are not acceptance.
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

type ToolResult<T> = Result<T, Box<dyn std::error::Error>>;
const REGISTRY: &str = "docs/development/wal-redesign-regressions-808.json";

#[derive(Deserialize)]
struct Registry {
    schema_version: u32,
    regressions: Vec<Case>,
    #[serde(default)]
    supplemental_regressions: Vec<Case>,
}

#[derive(Deserialize, Serialize)]
struct Case {
    id: String,
    target: String,
    test: String,
    source: String,
    baseline: String,
    #[serde(default)]
    failure_marker: String,
    #[serde(default)]
    requires_sqrzl: bool,
    #[serde(flatten)]
    extra: serde_json::Map<String, Value>,
}

#[derive(PartialEq, Eq, Serialize)]
struct Provenance {
    source_sha: String,
    source_changes_sha256: String,
    clean_source: bool,
}

struct Options {
    baseline: bool,
    sqrzl: bool,
    timeout: Duration,
    output: PathBuf,
}

fn options() -> ToolResult<Options> {
    let mut args = std::env::args().skip(1);
    let mut options = Options {
        baseline: false,
        sqrzl: false,
        timeout: Duration::from_secs(300),
        output: PathBuf::new(),
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--baseline" => options.baseline = true,
            "--with-sqrzl" => options.sqrzl = true,
            "--output" => options.output = args.next().ok_or("--output needs a path")?.into(),
            "--timeout-seconds" => {
                let seconds: f64 = args
                    .next()
                    .ok_or("--timeout-seconds needs a value")?
                    .parse()?;
                options.timeout = Duration::try_from_secs_f64(seconds)?;
                if options.timeout.is_zero() {
                    return Err("--timeout-seconds must be positive".into());
                }
            }
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    if options.output.as_os_str().is_empty() {
        return Err("--output is required (outside the checkout)".into());
    }
    Ok(options)
}

fn git(root: &Path, arguments: &[&str]) -> ToolResult<Vec<u8>> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .output()?;
    if !output.status.success() {
        return Err(format!("git {arguments:?} failed").into());
    }
    Ok(output.stdout)
}

fn source_identity(root: &Path) -> ToolResult<Provenance> {
    let source_sha = String::from_utf8(git(root, &["rev-parse", "HEAD"])?)?
        .trim()
        .to_owned();
    let diff = git(root, &["diff", "HEAD", "--"])?;
    let untracked = String::from_utf8(git(root, &["ls-files", "--others", "--exclude-standard"])?)?;
    let mut names: Vec<_> = untracked.lines().collect();
    names.sort_unstable();
    let clean_source = diff.is_empty() && names.is_empty();
    let mut digest = Sha256::new();
    digest.update(diff);
    for name in names {
        digest.update(name.as_bytes());
        digest.update(fs::read(root.join(name))?);
    }
    Ok(Provenance {
        source_sha,
        source_changes_sha256: hex::encode(digest.finalize()),
        clean_source,
    })
}

fn load_cases(root: &Path) -> ToolResult<Vec<Case>> {
    let registry: Registry = serde_json::from_slice(&fs::read(root.join(REGISTRY))?)?;
    if registry.schema_version != 1 {
        return Err("unsupported registry schema".into());
    }
    let mut cases = registry.regressions;
    cases.extend(registry.supplemental_regressions);
    if cases.is_empty() {
        return Err("empty regression registry".into());
    }
    let mut selectors = HashSet::new();
    for case in &cases {
        if [&case.id, &case.target, &case.test, &case.source]
            .iter()
            .any(|field| field.trim().is_empty())
            || !case
                .id
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err("missing or unsafe regression identity".into());
        }
        if !matches!(case.baseline.as_str(), "red" | "green")
            || (case.baseline == "red" && case.failure_marker.trim().is_empty())
        {
            return Err("baseline needs RED/GREEN and a nonempty RED failure marker".into());
        }
        if !selectors.insert((&case.target, &case.test)) {
            return Err("duplicate test selector".into());
        }
    }
    Ok(cases)
}

fn selected(name: &str, expected: &str) -> bool {
    name == expected
        || name
            .strip_suffix(expected)
            .is_some_and(|prefix| prefix.ends_with("::"))
}

fn classify(log: &str, exit_code: i32, case: &Case) -> (bool, bool, bool) {
    let summaries: Vec<_> = log
        .lines()
        .filter_map(|line| {
            let summary = line.strip_prefix("test result: ")?;
            let (status, counts) = summary.split_once(". ")?;
            if !matches!(status, "ok" | "FAILED") {
                return None;
            }
            let numbers: Vec<usize> = counts
                .split(';')
                .take(3)
                .filter_map(|field| field.split_whitespace().next()?.parse().ok())
                .collect();
            (numbers.len() == 3).then_some(numbers)
        })
        .collect();
    let executed =
        summaries.len() == 1 && summaries[0][0] + summaries[0][1] == 1 && summaries[0][2] == 0;
    let passed = log.lines().any(|line| {
        line.strip_prefix("test ")
            .and_then(|line| line.strip_suffix(" ... ok"))
            .is_some_and(|name| selected(name, &case.test))
    });
    let mut failure_block = false;
    let mut marker_found = false;
    for line in log.lines() {
        if let Some(name) = line
            .strip_prefix("---- ")
            .and_then(|line| line.strip_suffix(" stdout ----"))
        {
            failure_block = selected(name, &case.test);
        } else if line.trim() == "failures:" || line.starts_with("test result:") {
            failure_block = false;
        } else if failure_block && line.contains(&case.failure_marker) {
            marker_found = true;
        }
    }
    let green = executed && passed && exit_code == 0 && summaries[0][0] == 1;
    let red = executed
        && exit_code != 0
        && summaries[0][1] == 1
        && !case.failure_marker.trim().is_empty()
        && marker_found;
    (executed, green, red)
}

fn run_test(
    root: &Path,
    command: &[String],
    log: &Path,
    timeout: Duration,
) -> ToolResult<(i32, bool)> {
    let output = fs::File::create(log)?;
    let mut process = Command::new(&command[0]);
    process
        .args(&command[1..])
        .current_dir(root)
        .stdin(Stdio::null())
        .stdout(output.try_clone()?)
        .stderr(output);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        process.process_group(0);
    }
    let mut child = process.spawn()?;
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok((status.code().unwrap_or(-1), false));
        }
        if started.elapsed() >= timeout {
            #[cfg(unix)]
            {
                let _ = Command::new("kill")
                    .args(["-KILL", "--", &format!("-{}", child.id())])
                    .status();
            }
            #[cfg(windows)]
            {
                let _ = Command::new("taskkill")
                    .args(["/PID", &child.id().to_string(), "/T", "/F"])
                    .status();
            }
            let _ = child.kill();
            let status = child.wait()?;
            return Ok((status.code().unwrap_or(-1), true));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn sqrzl_available() -> bool {
    let address = SocketAddr::from(([127, 0, 0, 1], 9000));
    std::env::var_os("SQRZL_SECRET_ACCESS_KEY").is_some_and(|key| !key.is_empty())
        && TcpStream::connect_timeout(&address, Duration::from_secs(1)).is_ok()
}

fn skip_reason(case: &Case, options: &Options) -> Option<&'static str> {
    (case.requires_sqrzl && !options.sqrzl).then_some("sqrzl-not-selected")
}

struct Campaign<'a> {
    source: &'a Provenance,
    options: &'a Options,
    results: Vec<Value>,
    accepted: bool,
    complete: bool,
    executed: usize,
    case_count: usize,
    run_complete: bool,
}

impl Campaign<'_> {
    fn write(&self, stable: bool) -> ToolResult<()> {
        let receipt = json!({
            "schema_version": 2, "source_sha": self.source.source_sha,
            "source_changes_sha256": self.source.source_changes_sha256,
            "clean_source": self.source.clean_source, "source_stable": stable,
            "baseline_mode": self.options.baseline, "coverage_complete": self.complete,
            "run_complete": self.run_complete,
            "acceptance_passed": self.run_complete && !self.options.baseline && self.accepted
                && self.complete && self.executed == self.case_count && stable,
            "results": self.results,
        });
        fs::write(
            self.options.output.join("receipt.json"),
            serde_json::to_vec_pretty(&receipt)?,
        )?;
        Ok(())
    }

    fn execute(&mut self, root: &Path, index: usize, case: &Case) -> ToolResult<()> {
        if let Some(reason) = skip_reason(case, self.options) {
            self.complete = false;
            self.accepted &= self.options.baseline;
            let mut result = serde_json::to_value(case)?;
            result["status"] = json!(reason);
            self.results.push(result);
            return Ok(());
        }
        if case.requires_sqrzl && !sqrzl_available() {
            self.complete = false;
            self.accepted = false;
            let mut result = serde_json::to_value(case)?;
            result["status"] = json!("sqrzl-unavailable");
            self.results.push(result);
            return Err("selected Sqrzl provider is unavailable".into());
        }
        let mut command: Vec<String> = ["cargo", "test", "--locked", "--all-features"]
            .map(str::to_owned)
            .into();
        if case.target == "lib" {
            command.push("--lib".into());
        } else {
            command.extend(["--test".into(), case.target.clone()]);
        }
        command.extend([case.test.clone(), "--".into(), "--test-threads=1".into()]);
        let log_name = format!("{index:02}-{}.log", case.id);
        let path = self.options.output.join(&log_name);
        let (exit_code, timed_out) = run_test(root, &command, &path, self.options.timeout)?;
        let (executed, green, red) = classify(&fs::read_to_string(path)?, exit_code, case);
        let expected = if self.options.baseline {
            case.baseline.as_str()
        } else {
            "green"
        };
        let matched = !timed_out && if expected == "green" { green } else { red };
        self.accepted &= matched;
        self.executed += usize::from(executed);
        let mut result = serde_json::to_value(case)?;
        for (key, value) in [
            ("command", json!(command)),
            ("exit_code", json!(exit_code)),
            ("timed_out", json!(timed_out)),
            ("executed_exactly_one", json!(executed)),
            ("expected", json!(expected)),
            ("matched", json!(matched)),
            ("log", json!(log_name)),
        ] {
            result[key] = value;
        }
        self.results.push(result);
        Ok(())
    }
}

fn run() -> ToolResult<bool> {
    let mut options = options()?;
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).canonicalize()?;
    let cases = load_cases(&root)?;
    fs::create_dir_all(&options.output)?;
    options.output = options.output.canonicalize()?;
    if options.output.starts_with(&root) {
        return Err("--output must be outside the checkout (including target)".into());
    }
    let source = source_identity(&root)?;
    let mut campaign = Campaign {
        source: &source,
        options: &options,
        results: Vec::new(),
        accepted: true,
        complete: true,
        executed: 0,
        case_count: cases.len(),
        run_complete: false,
    };
    campaign.write(true)?;
    if !options.baseline && !source.clean_source {
        campaign.accepted = false;
        campaign.complete = false;
        campaign
            .results
            .push(json!({"status": "dirty-acceptance-source"}));
        campaign.write(true)?;
        return Ok(false);
    }
    for (index, case) in cases.iter().enumerate() {
        if source_identity(&root)? != source {
            campaign.write(false)?;
            return Ok(false);
        }
        if let Err(error) = campaign.execute(&root, index, case) {
            campaign.accepted = false;
            campaign.results.push(
                json!({"id": case.id, "status": "execution-error", "error": error.to_string()}),
            );
            campaign.write(source_identity(&root)? == source)?;
            return Err(error);
        }
        let stable = source_identity(&root)? == source;
        campaign.write(stable)?;
        if !stable {
            return Ok(false);
        }
    }
    campaign.run_complete = true;
    campaign.accepted &= campaign.executed > 0;
    let stable = source_identity(&root)? == source;
    campaign.write(stable)?;
    Ok(campaign.accepted && stable)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("WAL regression runner: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{classify, Case};

    fn case() -> Case {
        serde_json::from_str(
            r#"{"id":"regression", "target":"lib", "test":"should_preserve_wal",
                "source":"src/wal.rs", "baseline":"red", "failure_marker":"retired WAL replayed"}"#,
        )
        .unwrap()
    }

    #[test]
    fn should_match_red_when_selected_test_fails_with_its_assertion() {
        // Arrange
        let log = "test wal::should_preserve_wal ... FAILED\n\nfailures:\n\n---- wal::should_preserve_wal stdout ----\nretired WAL replayed\n\nfailures:\n    wal::should_preserve_wal\n\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 99 filtered out\n";
        // Act
        let result = classify(log, 101, &case());
        // Assert
        assert_eq!(result, (true, false, true));
    }

    #[test]
    fn should_reject_red_when_marker_is_outside_selected_failure() {
        // Arrange
        let log = "warning: retired WAL replayed\n---- wal::should_preserve_wal stdout ----\nsetup failed\n---- other::test stdout ----\nretired WAL replayed\n\nfailures:\n\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 99 filtered out\n";
        // Act
        let result = classify(log, 101, &case());
        // Assert
        assert_eq!(result, (true, false, false));
    }

    #[test]
    fn should_reject_baseline_when_test_is_ignored_or_not_selected() {
        // Arrange
        let logs = [
            "error: retired WAL replayed\n",
            "test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 99 filtered out\n",
            "test other::test ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 99 filtered out\n",
            "test wal::should_preserve_wal ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 99 filtered out\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n",
        ];
        for log in logs {
            // Act
            let (_, green, red) = classify(log, 0, &case());
            // Assert
            assert!(!green && !red, "unexpected baseline match: {log}");
        }
    }

    #[test]
    fn should_match_green_when_exactly_one_selected_test_passes() {
        // Arrange
        let log = "test wal::should_preserve_wal ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 99 filtered out\n";
        // Act
        let result = classify(log, 0, &case());
        // Assert
        assert_eq!(result, (true, true, false));
    }
}
