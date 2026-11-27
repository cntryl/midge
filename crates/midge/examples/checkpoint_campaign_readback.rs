//! Bind downloaded hosted artifacts and read the preregistered checkpoint campaign.

#[path = "checkpoint_campaign_readback/mod.rs"]
mod checkpoint_campaign_readback;

use checkpoint_campaign_readback::{campaign, provenance, Identity, Result};
use std::path::PathBuf;

fn arguments() -> Result<(String, PathBuf, Identity)> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 9
        || args[1] != "--root"
        || args[3] != "--run-id"
        || args[5] != "--attempt"
        || args[7] != "--sha"
    {
        return Err("usage: <prepare-download|seal-download|readback|construction-smoke> --root DIR --run-id ID --attempt N --sha SHA".into());
    }
    let identity = Identity {
        run_id: args[4].parse().map_err(|_| "bad run ID")?,
        run_attempt: args[6].parse().map_err(|_| "bad attempt")?,
        sha: args[8].clone(),
    };
    identity.validate()?;
    Ok((args[0].clone(), PathBuf::from(&args[2]), identity))
}

fn run() -> Result<bool> {
    let (mode, root, identity) = arguments()?;
    match mode.as_str() {
        "prepare-download" => {
            provenance::prepare(&root, &identity)?;
            Ok(true)
        }
        "seal-download" => {
            provenance::seal(&root, &identity)?;
            Ok(true)
        }
        "readback" => {
            let report = campaign::evaluate(&root, &identity);
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
            );
            Ok(report["complete"] == true)
        }
        "construction-smoke" => {
            let report = campaign::evaluate_construction_smoke(&root, &identity);
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
            );
            Ok(report["construction_transport_valid"] == true)
        }
        _ => Err("unknown mode".into()),
    }
}

fn main() {
    match run() {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("checkpoint readback failed: {error}");
            std::process::exit(2);
        }
    }
}
