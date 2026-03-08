use cntryl_midge::Engine;

fn main() {
    if let Err(err) = run() {
        eprintln!("midge: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("verify") => {
            let path = args.next().ok_or("usage: midge verify <db-path>")?;
            if args.next().is_some() {
                return Err("usage: midge verify <db-path>".into());
            }

            let report = Engine::verify_path(path)?;
            println!(
                "health={:?} manifest_files_verified={} sst_files_verified={} wal_recovery_records_replayed={} wal_recovery_bytes_replayed={} intent_entries_loaded={}",
                report.health,
                report.manifest_files_verified,
                report.sst_files_verified,
                report.wal_recovery_records_replayed,
                report.wal_recovery_bytes_replayed,
                report.intent_entries_loaded
            );
            Ok(())
        }
        _ => Err("usage: midge verify <db-path>".into()),
    }
}
