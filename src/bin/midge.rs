use cntryl_midge::{Engine, EngineHealth, MidgeError, StorageVerificationReport};
use serde::Serialize;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

const EXIT_HEALTHY: i32 = 0;
const EXIT_DEGRADED: i32 = 1;
const EXIT_USAGE: i32 = 2;
const EXIT_STORAGE: i32 = 3;
const EXIT_CORRUPTION: i32 = 4;
const EXIT_INTERNAL: i32 = 5;
const USAGE: &str = "usage: midge verify [--json] <db-path>";
const HELP: &str = "midge storage diagnostics\n\n\
Usage:\n  midge verify [--json] <db-path>\n\n\
Exit codes:\n  0  storage is healthy\n  1  storage is degraded, in salvage mode, or write-stalled\n  2  command-line usage error\n  3  storage path is missing or inaccessible\n  4  storage corruption or incompatible persisted state\n  5  unexpected internal failure";

fn main() {
    let outcome = run(std::env::args_os().skip(1));
    outcome.render();
    std::process::exit(outcome.exit_code);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Plain,
    Json,
}

#[derive(Debug)]
enum ParsedCommand {
    Help,
    Verify { path: PathBuf, format: OutputFormat },
}

#[derive(Debug)]
struct ParseFailure {
    message: String,
    format: OutputFormat,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ErrorKind {
    Usage,
    Storage,
    Corruption,
    Internal,
}

#[derive(Debug, Serialize)]
struct ErrorOutput {
    status: &'static str,
    error_kind: ErrorKind,
    message: String,
}

#[derive(Debug)]
enum OutcomePayload {
    Help,
    Report(StorageVerificationReport),
    Error(ErrorOutput),
}

#[derive(Debug)]
struct CliOutcome {
    exit_code: i32,
    format: OutputFormat,
    payload: OutcomePayload,
}

impl CliOutcome {
    fn help() -> Self {
        Self {
            exit_code: EXIT_HEALTHY,
            format: OutputFormat::Plain,
            payload: OutcomePayload::Help,
        }
    }

    fn report(report: StorageVerificationReport, format: OutputFormat) -> Self {
        Self {
            exit_code: exit_code_for_health(report.health),
            format,
            payload: OutcomePayload::Report(report),
        }
    }

    fn error(
        exit_code: i32,
        format: OutputFormat,
        error_kind: ErrorKind,
        message: impl Into<String>,
    ) -> Self {
        Self {
            exit_code,
            format,
            payload: OutcomePayload::Error(ErrorOutput {
                status: "error",
                error_kind,
                message: message.into(),
            }),
        }
    }

    fn render(&self) {
        match (&self.payload, self.format) {
            (OutcomePayload::Help, _) => println!("{HELP}"),
            (OutcomePayload::Report(report), OutputFormat::Json) => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(report)
                        .expect("storage verification reports must serialize")
                );
            }
            (OutcomePayload::Report(report), OutputFormat::Plain) => {
                println!(
                    "health={:?} authoritative={} manifest_epoch={} manifest_files_verified={} sst_files_verified={} bytes_verified={} data_blocks_verified={} wal_boundary={:?} wal_recovery_records_replayed={} wal_recovery_bytes_replayed={} intent_entries_loaded={}",
                    report.health,
                    report.authoritative,
                    report.manifest_epoch,
                    report.manifest_files_verified,
                    report.sst_files_verified,
                    report.bytes_verified,
                    report.data_blocks_verified,
                    report.wal_boundary,
                    report.wal_recovery_records_replayed,
                    report.wal_recovery_bytes_replayed,
                    report.intent_entries_loaded
                );
            }
            (OutcomePayload::Error(error), OutputFormat::Json) => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(error)
                        .expect("typed command errors must serialize")
                );
            }
            (OutcomePayload::Error(error), OutputFormat::Plain) => {
                eprintln!("midge: {}", error.message);
            }
        }
    }
}

fn run(args: impl IntoIterator<Item = OsString>) -> CliOutcome {
    let command = match parse_args(args) {
        Ok(command) => command,
        Err(error) => {
            return CliOutcome::error(EXIT_USAGE, error.format, ErrorKind::Usage, error.message);
        }
    };

    match command {
        ParsedCommand::Help => CliOutcome::help(),
        ParsedCommand::Verify { path, format } => {
            if let Err(message) = validate_storage_path(&path) {
                return CliOutcome::error(EXIT_STORAGE, format, ErrorKind::Storage, message);
            }
            match Engine::verify_path(&path) {
                Ok(report) => CliOutcome::report(report, format),
                Err(error) => outcome_for_verification_error(&error, format),
            }
        }
    }
}

fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<ParsedCommand, ParseFailure> {
    let args: Vec<OsString> = args.into_iter().collect();
    let format = if args.iter().any(|arg| arg == "--json") {
        OutputFormat::Json
    } else {
        OutputFormat::Plain
    };
    let Some(command) = args.first() else {
        return Err(ParseFailure {
            message: USAGE.to_string(),
            format,
        });
    };
    if command == "--help" || command == "-h" {
        return Ok(ParsedCommand::Help);
    }
    if command != "verify" {
        return Err(ParseFailure {
            message: format!("unknown command '{}'; {USAGE}", command.to_string_lossy()),
            format,
        });
    }

    let mut path = None;
    for arg in args.into_iter().skip(1) {
        if arg == "--json" {
            continue;
        }
        if arg == "--help" || arg == "-h" {
            return Ok(ParsedCommand::Help);
        }
        if arg.to_string_lossy().starts_with("--") {
            return Err(ParseFailure {
                message: format!("unknown flag '{}'; {USAGE}", arg.to_string_lossy()),
                format,
            });
        }
        if path.replace(PathBuf::from(arg)).is_some() {
            return Err(ParseFailure {
                message: USAGE.to_string(),
                format,
            });
        }
    }

    path.map(|path| ParsedCommand::Verify { path, format })
        .ok_or_else(|| ParseFailure {
            message: USAGE.to_string(),
            format,
        })
}

fn validate_storage_path(path: &Path) -> Result<(), String> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!("storage path '{}' does not exist", path.display())
        } else {
            format!("storage path '{}' is inaccessible: {error}", path.display())
        }
    })?;
    if !metadata.is_dir() {
        return Err(format!(
            "storage path '{}' is not a directory",
            path.display()
        ));
    }
    std::fs::read_dir(path)
        .map(|_| ())
        .map_err(|error| format!("storage path '{}' is inaccessible: {error}", path.display()))?;

    validate_optional_readable_file(&path.join("FORMAT"))?;
    let manifest_snapshot = path.join("manifest.snapshot.json");
    let manifest_file = if storage_entry_exists(&manifest_snapshot)? {
        manifest_snapshot
    } else {
        path.join("manifest.json")
    };
    for file_name in [
        manifest_file,
        path.join("manifest.journal"),
        path.join("intent_log.json"),
    ] {
        validate_optional_readable_file(&file_name)?;
    }
    Ok(())
}

fn storage_entry_exists(path: &Path) -> Result<bool, String> {
    path.try_exists().map_err(|error| {
        format!(
            "storage entry '{}' is inaccessible: {error}",
            path.display()
        )
    })
}

fn validate_optional_readable_file(path: &Path) -> Result<(), String> {
    match std::fs::File::open(path) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "storage file '{}' is inaccessible: {error}",
            path.display()
        )),
    }
}

fn outcome_for_verification_error(error: &MidgeError, format: OutputFormat) -> CliOutcome {
    let message = error.to_string();
    match error {
        MidgeError::Corruption(_)
        | MidgeError::RecoveryFailed(_)
        | MidgeError::CompatibilityError(_) => {
            CliOutcome::error(EXIT_CORRUPTION, format, ErrorKind::Corruption, message)
        }
        MidgeError::Io(_)
        | MidgeError::NotFound
        | MidgeError::InvalidPath
        | MidgeError::LeaseHeld(_)
        | MidgeError::LeaseUnavailable(_)
        | MidgeError::LeaseIndeterminate(_) => {
            CliOutcome::error(EXIT_STORAGE, format, ErrorKind::Storage, message)
        }
        MidgeError::Internal(_)
        | MidgeError::InvalidArgument(_)
        | MidgeError::NotSupported(_)
        | MidgeError::NoSpace(_)
        | MidgeError::WriteStall(_)
        | MidgeError::MemoryModeViolation(_)
        | MidgeError::Fenced(_)
        | MidgeError::LeaseEpochExhausted
        | MidgeError::WriteConflict(_)
        | MidgeError::Aborted(_)
        | MidgeError::Busy(_)
        | MidgeError::Timeout(_)
        | MidgeError::ResourceLimit(_) => {
            CliOutcome::error(EXIT_INTERNAL, format, ErrorKind::Internal, message)
        }
    }
}

/// Stable public process contract for successful verification reports.
///
/// `0` means healthy; `1` means degraded, salvage, or write-stalled. Parser,
/// storage-access, corruption, and unexpected internal failures use `2`, `3`,
/// `4`, and `5`, respectively.
fn exit_code_for_health(health: EngineHealth) -> i32 {
    match health {
        EngineHealth::Healthy => EXIT_HEALTHY,
        EngineHealth::Degraded | EngineHealth::SalvageMode | EngineHealth::WriteStalled => {
            EXIT_DEGRADED
        }
        EngineHealth::Corrupt => EXIT_CORRUPTION,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_preserve_json_format_given_usage_error_when_flag_follows_error() {
        // Arrange
        let args = [
            OsString::from("verify"),
            OsString::from("--bad"),
            OsString::from("--json"),
        ];

        // Act
        let error = parse_args(args).expect_err("unknown flag must fail parsing");

        // Assert
        assert_eq!(error.format, OutputFormat::Json);
        assert!(error.message.contains("unknown flag '--bad'"));
    }
}
