//! Saving findings as crash images, loading them back, and the assertions
//! that prove a saved image replays its finding.

use crate::harness::{Campaign, Finding, Image};
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
        let Ok(entries) = std::fs::read_dir(&folder) else {
            continue;
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

/// Save `finding`'s image below `MIDGE_CAMPAIGN_IMAGES`, or a temporary
/// directory, and load it back: replays run from what was saved.
fn saved_and_loaded(case: &str, finding: &Finding) -> Image {
    let temporary = tempfile::tempdir().expect("image directory");
    let directory =
        kept_image_directory(case, finding).unwrap_or_else(|| temporary.path().join(case));
    save_finding(&directory, finding);
    load_image(&directory)
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

/// Assert `finding` matches `signature`, save its image, and prove the saved
/// image replays the same class of finding.
pub(crate) fn assert_reproduced(
    case: &str,
    finding: Option<Finding>,
    signature: impl Fn(&Finding) -> bool,
) {
    let finding = finding.unwrap_or_else(|| panic!("{case}: the campaign found nothing"));
    assert!(signature(&finding), "{case}: found {finding:#?}");
    let image = saved_and_loaded(case, &finding);
    let replayed = Campaign::replay(&image);
    assert!(
        replayed
            .as_ref()
            .is_err_and(|kind| kind.same_class(&finding.kind)),
        "{case}: the saved image did not reproduce {:?}: {replayed:?}",
        finding.kind
    );
}
