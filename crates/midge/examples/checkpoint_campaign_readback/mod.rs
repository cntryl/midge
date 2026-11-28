//! Campaign provenance and fixed-workload readback helpers; no benchmark execution.

pub mod accounting;
pub mod campaign;
pub mod commit_backpressure;
pub mod metadata_boundary;
pub mod native;
pub mod provenance;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Component, Path, PathBuf};

pub type Result<T> = std::result::Result<T, String>;
pub const SUITE: &str = "tier4-system-checkpoint-write-amplification";
pub const TARGET: &str = "tier4_system_checkpoint_write_amplification";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Identity {
    pub run_id: u64,
    pub run_attempt: u64,
    pub sha: String,
}

impl Identity {
    pub fn validate(&self) -> Result<()> {
        require(
            self.run_id > 0 && self.run_attempt > 0,
            "invalid run identity",
        )?;
        require(is_hash(&self.sha, 40), "invalid source SHA")
    }

    pub fn build_artifact(&self) -> String {
        format!(
            "checkpoint-build-{}-{}-a{}",
            self.sha, self.run_id, self.run_attempt
        )
    }

    pub fn artifact(&self, cell: Cell, repeat: u64) -> String {
        format!(
            "bench-tier4-checkpoint-{}-r{}-{}-{}-a{}",
            cell.id, repeat, self.sha, self.run_id, self.run_attempt
        )
    }

    pub fn native_run_id(&self, cell: Cell, repeat: u64) -> String {
        format!(
            "midge715-{}-a{}-{}-r{}",
            self.run_id, self.run_attempt, cell.id, repeat
        )
    }
}

#[derive(Clone, Copy)]
pub struct Cell {
    pub id: &'static str,
    pub workload: &'static str,
    pub cycles: u64,
    pub rows: u64,
    pub families: u64,
}

pub const CELLS: [Cell; 3] = [
    Cell {
        id: "A",
        workload: "checkpoint_local_256x1mib_1cf",
        cycles: 256,
        rows: 1024,
        families: 1,
    },
    Cell {
        id: "B",
        workload: "checkpoint_local_512x256kib_16cf",
        cycles: 512,
        rows: 256,
        families: 16,
    },
    Cell {
        id: "C",
        workload: "checkpoint_local_1024x64kib_1cf",
        cycles: 1024,
        rows: 64,
        families: 1,
    },
];

impl Cell {
    pub const fn warmup(self) -> u64 {
        self.cycles.div_ceil(10)
    }
    pub const fn measured(self) -> u64 {
        self.cycles - self.warmup()
    }
    pub fn job_name(self, repeat: u64) -> String {
        format!("checkpoint-{}-r{repeat}", self.id)
    }
}

pub fn load(path: &Path) -> Result<Value> {
    let bytes = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("{}: {error}", path.display()))
}

pub fn require(ok: bool, error: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(error.to_owned())
    }
}

pub fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing/string field {key}"))
}

pub fn number(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("missing/u64 field {key}"))
}

pub fn array<'a>(value: &'a Value, key: &str) -> Result<&'a [Value]> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("missing/array field {key}"))
}

pub fn is_hash(value: &str, length: usize) -> bool {
    value.len() == length && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub fn safe_relative(value: &str) -> Result<PathBuf> {
    let path = Path::new(value);
    require(
        !value.is_empty()
            && path
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "unsafe relative evidence path",
    )?;
    // unzip -p interprets member patterns; these evidence names must be literal.
    require(
        !value
            .chars()
            .any(|ch| matches!(ch, '*' | '?' | '[' | ']' | '\\' | '\n' | '\r')),
        "nonliteral archive member",
    )?;
    Ok(path.to_path_buf())
}

pub fn json_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut output = Vec::new();
    walk(root, &mut output, true)?;
    output.sort();
    Ok(output)
}

pub fn files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut output = Vec::new();
    walk(root, &mut output, false)?;
    output.sort();
    Ok(output)
}

fn walk(root: &Path, output: &mut Vec<PathBuf>, json_only: bool) -> Result<()> {
    let metadata = std::fs::symlink_metadata(root).map_err(|error| error.to_string())?;
    require(
        !metadata.file_type().is_symlink(),
        "symlink evidence is not accepted",
    )?;
    require(metadata.is_dir(), "evidence root is not a directory")?;
    for entry in std::fs::read_dir(root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let kind = entry.file_type().map_err(|error| error.to_string())?;
        require(!kind.is_symlink(), "symlink evidence is not accepted")?;
        if kind.is_dir() {
            walk(&entry.path(), output, json_only)?;
        } else if kind.is_file() {
            if !json_only || entry.path().extension().is_some_and(|ext| ext == "json") {
                output.push(entry.path());
            }
        } else {
            return Err("nonregular evidence file".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod contract_tests;
