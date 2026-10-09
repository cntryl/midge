//! Finite cold-Engine recovery attribution, with complete native receipts.
#[path = "recovery_attribution/fixture.rs"]
mod fixture;
use cntryl_midge::__internal::recovery::RecoveryProbeVariant;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::Path;
use tracing_subscriber::prelude::*;

fn command(args: &[&str]) -> fixture::Result<String> {
    let output = std::process::Command::new("git").args(args).output()?;
    if !output.status.success() {
        return Err("source identity command failed".into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}
fn run(args: &[String], capture: &fixture::Capture) -> fixture::Result<Value> {
    match args.get(1).map(String::as_str) {
        Some("seed") if args.len() == 4 => fixture::create(Path::new(&args[2]), 8192),
        Some("trial") if args.len() == 6 => {
            let variant = match args[4].as_str() {
                "baseline" => RecoveryProbeVariant::Baseline,
                "key_index" => RecoveryProbeVariant::KeyIndex,
                "single_reader" => RecoveryProbeVariant::SingleReader,
                "timers_off" => RecoveryProbeVariant::TimersOff,
                _ => return Err("unknown variant".into()),
            };
            fixture::trial(
                Path::new(&args[2]),
                Path::new(&args[3]),
                variant,
                256 * 1024,
                capture,
            )
        }
        _ => Err("usage: seed FIXTURE RECEIPT | trial FIXTURE TARGET VARIANT RECEIPT".into()),
    }
}
fn main() -> fixture::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let output = Path::new(args.last().ok_or("missing receipt")?);
    let capture = fixture::Capture::default();
    tracing_subscriber::registry()
        .with(
            capture.clone().with_filter(
                tracing_subscriber::filter::Targets::new()
                    .with_target("midge::recovery", tracing::Level::INFO),
            ),
        )
        .try_init()?;
    let source = command(&["rev-parse", "HEAD"])?;
    let clean = command(&["status", "--porcelain"])?.is_empty();
    let executable = std::env::current_exe()?;
    let digest = hex::encode(Sha256::digest(std::fs::read(&executable)?));
    let mut receipt = json!({"schema":1,"source_sha":source,"source_clean":clean,"binary_sha256":digest,
        "command":args,"os":std::env::consts::OS,"arch":std::env::consts::ARCH,
        "logical_cpus":std::thread::available_parallelism()?.get(),"complete":false,"phase":"started"});
    std::fs::write(output, serde_json::to_vec_pretty(&receipt)?)?;
    if !clean {
        return Err("campaign requires a clean immutable source".into());
    }
    match run(&args, &capture) {
        Ok(result) => {
            receipt["result"] = result;
            receipt["complete"] = json!(true);
            receipt["phase"] = json!("completed");
        }
        Err(error) => {
            receipt["phase"] = json!("failed");
            receipt["error"] = json!(error.to_string());
            receipt["native_events"] = json!(capture.drain());
            std::fs::write(output, serde_json::to_vec_pretty(&receipt)?)?;
            return Err(error);
        }
    }
    std::fs::write(output, serde_json::to_vec_pretty(&receipt)?)?;
    Ok(())
}
