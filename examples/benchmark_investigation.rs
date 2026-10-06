//! Reproducible exploratory probes. These are not production policy acceptance.
use cntryl_midge::__internal::checkpoint::run_checkpoint_policy_probe;
use cntryl_midge::__internal::recovery::run_recovery_cost_probe;
use serde_json::json;
use sha2::Digest as _;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if arguments.len() != 2 || !["checkpoint", "recovery"].contains(&arguments[0].as_str()) {
        return Err("usage: benchmark_investigation checkpoint|recovery OUTPUT.json".into());
    }
    let sha = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()?;
    if !sha.status.success() {
        return Err("cannot capture source revision".into());
    }
    let clean = std::process::Command::new("git")
        .args(["diff", "HEAD", "--quiet"])
        .status()?;
    if !clean.success() {
        return Err("commit tracked source changes before recording probe evidence".into());
    }
    let binary = std::env::current_exe()?;
    let binary_sha256 = hex::encode(sha2::Sha256::digest(std::fs::read(&binary)?));
    let mut result = json!({
        "schema_version": 1, "git_commit": String::from_utf8(sha.stdout)?.trim(),
        "scope": "exploratory_probes_not_engine_ack_crash_or_production_provider_qualification",
        "mode": arguments[0], "binary_sha256": binary_sha256,
        "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
        "logical_cpus": std::thread::available_parallelism()?.get(),
        "arguments": arguments,
        "planned_rows": if arguments[0] == "checkpoint" { 27 } else { 24 },
        "complete": false, "rows": [],
    });
    write_receipt(&arguments[1], &result)?;
    if arguments[0] == "checkpoint" {
        for repeat in 1..=3 {
            for files in [16, 64, 256] {
                for interval in [1, 16, 64] {
                    let root = tempfile::tempdir()?;
                    let mut row =
                        run_checkpoint_policy_probe(root.path(), files, 512, interval, 16_384)?;
                    row["repeat"] = json!(repeat);
                    result["rows"]
                        .as_array_mut()
                        .expect("receipt rows")
                        .push(row);
                    write_receipt(&arguments[1], &result)?;
                }
            }
        }
    } else {
        for repeat in 1..=3 {
            for budget in [128 * 1_024, 2 * 1_024 * 1_024] {
                for interleaved in [false, true] {
                    for release_interval in [0, 256] {
                        let root = tempfile::tempdir()?;
                        let mut row = run_recovery_cost_probe(
                            root.path(),
                            16,
                            128,
                            budget,
                            interleaved,
                            release_interval,
                        )?;
                        row["repeat"] = json!(repeat);
                        result["rows"]
                            .as_array_mut()
                            .expect("receipt rows")
                            .push(row);
                        write_receipt(&arguments[1], &result)?;
                    }
                }
            }
        }
    }
    result["complete"] = json!(true);
    write_receipt(&arguments[1], &result)?;
    Ok(())
}

fn write_receipt(path: &str, result: &serde_json::Value) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::write(PathBuf::from(path), serde_json::to_vec_pretty(result)?)?;
    Ok(())
}
