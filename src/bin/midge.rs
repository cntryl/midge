use cntryl_midge::{Engine, EngineHealth};

fn main() {
    let code = match run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("midge: {err}");
            2
        }
    };
    std::process::exit(code);
}

fn run() -> Result<i32, Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("verify") => {
            let mut emit_json = false;
            let mut path = None;

            for arg in args {
                if arg == "--json" {
                    emit_json = true;
                } else if path.is_none() {
                    path = Some(arg);
                } else {
                    return Err("usage: midge verify [--json] <db-path>".into());
                }
            }

            let path = path.ok_or("usage: midge verify [--json] <db-path>")?;
            let report = Engine::verify_path(path)?;
            if emit_json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!(
                    "health={:?} manifest_files_verified={} sst_files_verified={} wal_recovery_records_replayed={} wal_recovery_bytes_replayed={} intent_entries_loaded={}",
                    report.health,
                    report.manifest_files_verified,
                    report.sst_files_verified,
                    report.wal_recovery_records_replayed,
                    report.wal_recovery_bytes_replayed,
                    report.intent_entries_loaded
                );
            }
            Ok(exit_code_for_health(report.health))
        }
        _ => Err("usage: midge verify [--json] <db-path>".into()),
    }
}

fn exit_code_for_health(health: EngineHealth) -> i32 {
    match health {
        EngineHealth::Healthy => 0,
        EngineHealth::Corrupt => 2,
        EngineHealth::Degraded | EngineHealth::SalvageMode | EngineHealth::WriteStalled => 1,
    }
}
