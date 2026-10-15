//! Saved crash images and invariant failure diagnostics.

use crate::harness::{Finding, Image};
use std::path::{Path, PathBuf};

/// Write `image` below `directory`: its files under `files/`, the rest as
/// JSON. An earlier image there is replaced.
fn save_image(directory: &Path, image: &Image) {
    if directory.exists() {
        std::fs::remove_dir_all(directory).expect("replace earlier image");
    }
    for (path, bytes) in &image.files {
        let target = directory.join("files").join(path);
        std::fs::create_dir_all(target.parent().expect("image file parent"))
            .expect("image directory");
        std::fs::write(target, bytes).expect("write image file");
    }
    std::fs::create_dir_all(directory).expect("image directory");
    std::fs::write(
        directory.join("image.json"),
        serde_json::to_vec_pretty(image).expect("encode image"),
    )
    .expect("write image");
}

/// Read an image `save_image` wrote.
pub(crate) fn load_image(directory: &Path) -> Image {
    let mut image: Image =
        serde_json::from_slice(&std::fs::read(directory.join("image.json")).expect("read image"))
            .expect("decode image");
    let root = directory.join("files");
    let mut pending = vec![root.clone()];
    while let Some(folder) = pending.pop() {
        let entries = match std::fs::read_dir(&folder) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && folder == root => {
                continue;
            }
            Err(error) => panic!("read crash image {}: {error}", folder.display()),
        };
        for entry in entries {
            let path = entry.expect("image entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let relative = path
                    .strip_prefix(&root)
                    .expect("image file below root")
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                image
                    .files
                    .insert(relative, std::fs::read(&path).expect("read image file"));
            }
        }
    }
    image
}

/// Where `finding`'s image is kept, below `MIDGE_CAMPAIGN_IMAGES`.
fn kept_image_directory(case: &str, finding: &Finding) -> Option<PathBuf> {
    std::env::var_os("MIDGE_CAMPAIGN_IMAGES")
        .map(|root| Path::new(&root).join(format!("{case}-seed-{}", finding.seed)))
}

/// Write `finding`'s image to `directory`, with a readable account of it.
fn save_finding(directory: &Path, finding: &Finding) {
    save_image(directory, &finding.image);
    std::fs::write(
        directory.join("finding.txt"),
        format!("{:#?}\n\nworkload: {:#?}\n", finding.kind, finding.workload),
    )
    .expect("write finding");
}

/// Assert a control campaign found nothing. A finding's image is kept below
/// `MIDGE_CAMPAIGN_IMAGES` first, so CI uploads it for replay.
pub(crate) fn assert_clean(case: &str, finding: Option<Finding>) {
    let Some(finding) = finding else {
        return;
    };
    let directory = kept_image_directory(case, &finding);
    if let Some(directory) = &directory {
        save_finding(directory, &finding);
    }
    panic!("{case}: the control found {finding:#?}\nimage: {directory:?}");
}
