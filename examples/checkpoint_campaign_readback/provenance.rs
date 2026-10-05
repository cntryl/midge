//! Fresh download fence, provider archive digest and exact extracted-content binding.

use super::{
    array, files, is_hash, load, number, require, safe_relative, string, Identity, Result, CELLS,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Artifact {
    pub id: u64,
    pub name: String,
    pub digest: String,
    pub size_in_bytes: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Download {
    pub schema_version: String,
    pub identity: Identity,
    pub complete: bool,
    pub artifacts: Vec<Artifact>,
    pub hosted_sha256: BTreeMap<String, String>,
    pub extracted_sha256: BTreeMap<String, String>,
}

pub fn hash_reader(reader: &mut impl Read) -> Result<String> {
    let mut hash = Sha256::new();
    let mut bytes = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = reader.read(&mut bytes).map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        hash.update(&bytes[..read]);
    }
    Ok(hash
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut encoded, byte| {
            write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
            encoded
        }))
}

pub fn hash_file(path: &Path) -> Result<String> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    require(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "archive/evidence is not a regular file",
    )?;
    hash_reader(&mut std::fs::File::open(path).map_err(|error| error.to_string())?)
}

pub fn hosted(root: &Path, identity: &Identity) -> Result<(Value, Value, Vec<Artifact>)> {
    identity.validate()?;
    let run = load(&root.join("hosted/run.json"))?;
    require(
        number(&run, "id")? == identity.run_id
            && number(&run, "run_attempt")? == identity.run_attempt
            && string(&run, "head_sha")? == identity.sha,
        "REST run identity differs from requested campaign",
    )?;
    let jobs = load(&root.join("hosted/jobs.json"))?;
    require(
        number(&jobs, "total_count")? == array(&jobs, "jobs")?.len() as u64,
        "hosted jobs capture is not fully paginated",
    )?;
    let uploaded = load(&root.join("hosted/artifacts.json"))?;
    require(
        number(&uploaded, "total_count")? == array(&uploaded, "artifacts")?.len() as u64,
        "hosted artifact capture is not fully paginated",
    )?;
    let mut names = BTreeSet::from([identity.build_artifact()]);
    for cell in CELLS {
        for repeat in 1..=3 {
            names.insert(identity.artifact(cell, repeat));
        }
    }
    let mut artifacts = Vec::new();
    let mut found = BTreeSet::new();
    for raw in array(&uploaded, "artifacts")? {
        let name = string(raw, "name")?;
        if !names.contains(name) {
            continue;
        }
        require(
            found.insert(name.to_owned()),
            "duplicate artifact name for exact campaign",
        )?;
        require(
            raw["workflow_run"]["id"] == identity.run_id
                && raw["workflow_run"]["head_sha"] == identity.sha,
            "artifact is from another run/source",
        )?;
        let digest = string(raw, "digest")?.to_owned();
        require(
            digest
                .strip_prefix("sha256:")
                .is_some_and(|hash| is_hash(hash, 64)),
            "artifact provider SHA256 absent",
        )?;
        artifacts.push(Artifact {
            id: number(raw, "id")?,
            name: name.to_owned(),
            digest,
            size_in_bytes: number(raw, "size_in_bytes")?,
        });
    }
    artifacts.sort_by(|left, right| left.name.cmp(&right.name));
    Ok((run, jobs, artifacts))
}

fn empty_or_absent(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    require(
        files(path)?.is_empty(),
        "prepare refuses an existing nonempty download destination",
    )?;
    require(
        std::fs::read_dir(path)
            .map_err(|error| error.to_string())?
            .next()
            .is_none(),
        "prepare refuses existing artifact subdirectories",
    )
}

fn new_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("refuses existing receipt {}: {error}", path.display()))?;
    file.write_all(bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

fn hosted_hashes(root: &Path) -> Result<BTreeMap<String, String>> {
    ["run.json", "jobs.json", "artifacts.json"]
        .into_iter()
        .map(|name| Ok((name.to_owned(), hash_file(&root.join("hosted").join(name))?)))
        .collect()
}

pub fn prepare(root: &Path, identity: &Identity) -> Result<()> {
    empty_or_absent(&root.join("archives"))?;
    empty_or_absent(&root.join("artifacts"))?;
    let (_, _, artifacts) = hosted(root, identity)?;
    let receipt = Download {
        schema_version: "midge715-download.v1".into(),
        identity: identity.clone(),
        complete: false,
        artifacts,
        hosted_sha256: hosted_hashes(root)?,
        extracted_sha256: BTreeMap::new(),
    };
    new_file(
        &root.join("download-provenance.json"),
        &serde_json::to_vec_pretty(&receipt).map_err(|error| error.to_string())?,
    )
}

fn load_download(root: &Path, identity: &Identity) -> Result<Download> {
    let download: Download = serde_json::from_value(load(&root.join("download-provenance.json"))?)
        .map_err(|error| error.to_string())?;
    require(
        download.schema_version == "midge715-download.v1" && download.identity == *identity,
        "existing download belongs to another run/attempt/source",
    )?;
    let (_, _, current) = hosted(root, identity)?;
    require(
        current == download.artifacts,
        "uploaded artifact identities/digests changed",
    )?;
    require(
        hosted_hashes(root)? == download.hosted_sha256,
        "original hosted capture changed; preserve it and start a fresh download",
    )?;
    Ok(download)
}

fn archive_members(archive: &Path) -> Result<BTreeSet<String>> {
    let output = Command::new("unzip")
        .arg("-Z1")
        .arg(archive)
        .stdin(Stdio::null())
        .output()
        .map_err(|error| format!("unzip unavailable: {error}"))?;
    require(output.status.success(), "archive listing failed")?;
    let output = String::from_utf8(output.stdout).map_err(|_| "archive member is not UTF8")?;
    let mut members = BTreeSet::new();
    for member in output.lines() {
        if member.ends_with('/') {
            safe_relative(member.trim_end_matches('/'))?;
            continue;
        }
        safe_relative(member)?;
        require(
            members.insert(member.to_owned()),
            "duplicate archive member",
        )?;
    }
    Ok(members)
}

fn archive_member_hash(archive: &Path, member: &str) -> Result<String> {
    safe_relative(member)?;
    let mut child = Command::new("unzip")
        .arg("-p")
        .arg(archive)
        .arg(member)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| error.to_string())?;
    let result = child
        .stdout
        .take()
        .ok_or_else(|| "unzip stdout unavailable".to_owned())
        .and_then(|mut reader| hash_reader(&mut reader));
    // Reap even if hashing fails; no background archive process is abandoned.
    if result.is_err() {
        let _ = child.kill();
    }
    let status = child.wait().map_err(|error| error.to_string())?;
    require(status.success(), "archive member read failed")?;
    result
}

fn directory_names(root: &Path) -> Result<BTreeSet<String>> {
    if !root.exists() {
        return Ok(BTreeSet::new());
    }
    let mut names = BTreeSet::new();
    for entry in std::fs::read_dir(root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let kind = entry.file_type().map_err(|error| error.to_string())?;
        require(
            kind.is_dir() && !kind.is_symlink(),
            "artifact root contains non-directory/symlink",
        )?;
        names.insert(
            entry
                .file_name()
                .into_string()
                .map_err(|_| "nonUTF8 artifact directory")?,
        );
    }
    Ok(names)
}

fn content_hashes(root: &Path, artifacts: &[Artifact]) -> Result<BTreeMap<String, String>> {
    let expected = artifacts
        .iter()
        .map(|artifact| artifact.name.clone())
        .collect::<BTreeSet<_>>();
    require(
        directory_names(&root.join("artifacts"))? == expected,
        "artifact directories missing or unexpected",
    )?;
    let mut hashes = BTreeMap::new();
    let mut archives = BTreeSet::new();
    for artifact in artifacts {
        let archive = root.join("archives").join(format!("{}.zip", artifact.id));
        archives.insert(archive.clone());
        require(
            std::fs::metadata(&archive)
                .map_err(|error| error.to_string())?
                .len()
                == artifact.size_in_bytes,
            "provider archive size differs",
        )?;
        require(
            format!("sha256:{}", hash_file(&archive)?) == artifact.digest,
            "provider archive SHA256 differs",
        )?;
        let artifact_root = root.join("artifacts").join(&artifact.name);
        let local = files(&artifact_root)?;
        let members = archive_members(&archive)?;
        let mut seen = BTreeSet::new();
        for path in local {
            let member = path
                .strip_prefix(&artifact_root)
                .map_err(|error| error.to_string())?
                .to_str()
                .ok_or("nonUTF8 extracted path")?
                .replace(std::path::MAIN_SEPARATOR, "/");
            safe_relative(&member)?;
            seen.insert(member.clone());
            let local_hash = hash_file(&path)?;
            require(
                members.contains(&member) && archive_member_hash(&archive, &member)? == local_hash,
                "extracted file does not match downloaded archive",
            )?;
            hashes.insert(format!("{}/{}", artifact.name, member), local_hash);
        }
        require(
            seen == members,
            "extracted artifact missing/extra archive member",
        )?;
    }
    let found = if root.join("archives").exists() {
        files(&root.join("archives"))?.into_iter().collect()
    } else {
        BTreeSet::new()
    };
    require(
        found == archives,
        "download contains missing/unexpected archive",
    )?;
    Ok(hashes)
}

pub fn seal(root: &Path, identity: &Identity) -> Result<()> {
    let mut receipt = load_download(root, identity)?;
    require(!receipt.complete, "refuses to reseal completed provenance")?;
    receipt.extracted_sha256 = content_hashes(root, &receipt.artifacts)?;
    receipt.complete = true;
    let temporary = root.join("download-provenance.complete.tmp");
    new_file(
        &temporary,
        &serde_json::to_vec_pretty(&receipt).map_err(|error| error.to_string())?,
    )?;
    std::fs::rename(temporary, root.join("download-provenance.json"))
        .map_err(|error| error.to_string())
}

pub fn verify(root: &Path, identity: &Identity) -> Result<Download> {
    let receipt = load_download(root, identity)?;
    require(receipt.complete, "download receipt incomplete")?;
    require(
        content_hashes(root, &receipt.artifacts)? == receipt.extracted_sha256,
        "downloaded content drifted after original seal",
    )?;
    Ok(receipt)
}
