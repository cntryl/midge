use std::path::Path;

#[test]
fn should_reference_existing_test_sources_in_current_storage_guides() {
    // Arrange
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let guides = [
        (
            "docs/development/storage-invariants.md",
            include_str!("../docs/development/storage-invariants.md"),
        ),
        (
            "docs/development/testing.md",
            include_str!("../docs/development/testing.md"),
        ),
    ];

    // Act
    let missing: Vec<_> = guides
        .into_iter()
        .flat_map(|(guide, body)| {
            body.split('`')
                .skip(1)
                .step_by(2)
                .filter(|reference| {
                    (reference.starts_with("src/") || reference.starts_with("tests/"))
                        && Path::new(reference)
                            .extension()
                            .is_some_and(|extension| extension.eq_ignore_ascii_case("rs"))
                })
                .filter(move |reference| !root.join(reference).is_file())
                .map(move |reference| format!("{guide}: {reference}"))
        })
        .collect();

    // Assert
    assert!(missing.is_empty(), "missing source references: {missing:?}");
}

const CURRENT_VERSION_GUIDES: [(&str, &str, &str); 5] = [
    ("README.md", include_str!("../README.md"), "Midge `"),
    (
        "docs/README.md",
        include_str!("../docs/README.md"),
        "This documentation describes Midge `",
    ),
    (
        "docs/user-guides/overview.md",
        include_str!("../docs/user-guides/overview.md"),
        "Version `",
    ),
    (
        "docs/operations/operator-runbook.md",
        include_str!("../docs/operations/operator-runbook.md"),
        "\n\nMidge ",
    ),
    (
        "docs/user-guides/faq.md",
        include_str!("../docs/user-guides/faq.md"),
        "Midge `",
    ),
];

fn claimed_current_version<'a>(body: &'a str, introduction: &str) -> Result<&'a str, String> {
    let (_, claim) = body
        .split_once(introduction)
        .ok_or_else(|| format!("missing current-version introduction {introduction:?}"))?;
    let actual = claim
        .split(|character: char| character == '`' || character == ',' || character.is_whitespace())
        .next()
        .unwrap_or_default();
    Ok(actual)
}

fn validate_current_version(body: &str, introduction: &str, expected: &str) -> Result<(), String> {
    let actual = claimed_current_version(body, introduction)?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "current-version introduction claims {actual:?}; package is {expected:?}"
        ))
    }
}

#[test]
fn should_match_package_version_when_guides_describe_current_release() {
    // Arrange
    let expected = env!("CARGO_PKG_VERSION");
    // Act
    let errors: Vec<_> = CURRENT_VERSION_GUIDES
        .into_iter()
        .filter_map(|(path, body, introduction)| {
            validate_current_version(body, introduction, expected)
                .err()
                .map(|error| format!("{path}: {error}"))
        })
        .collect();
    // Assert
    assert!(
        errors.is_empty(),
        "stale current-version claims: {errors:?}"
    );
}

#[test]
fn should_reject_obsolete_version_when_current_introduction_drifts() {
    // Arrange: disposable content; preserve the actual repository documents.
    let expected = env!("CARGO_PKG_VERSION");
    let (path, body, introduction) = CURRENT_VERSION_GUIDES[0];
    let actual = claimed_current_version(body, introduction).expect("actual current claim");
    let actual_claim = format!("{introduction}{actual}");
    let stale = body.replacen(&actual_claim, &format!("{introduction}0.0.0"), 1);
    assert_ne!(
        stale, body,
        "actual current introduction must be present: {path}"
    );
    // Act
    let result = validate_current_version(&stale, introduction, expected);
    // Assert
    assert!(result.is_err(), "an obsolete current claim must fail");
}

#[test]
fn should_allow_historical_version_when_current_introduction_matches_package() {
    // Arrange
    let (_, body, introduction) = CURRENT_VERSION_GUIDES[0];
    let expected = env!("CARGO_PKG_VERSION");
    let actual = claimed_current_version(body, introduction).expect("actual current claim");
    let current = body.replacen(
        &format!("{introduction}{actual}"),
        &format!("{introduction}{expected}"),
        1,
    );
    let historical = format!("{current}\nHistorical migration used registry `=0.3.0`; that prior contract remains historical.\n");
    // Act
    let result = validate_current_version(&historical, introduction, env!("CARGO_PKG_VERSION"));
    // Assert
    assert!(
        result.is_ok(),
        "historical references remain valid: {result:?}"
    );
}

#[test]
fn should_reject_missing_introduction_when_current_version_claim_is_removed() {
    // Arrange
    let body = "Historical migration used registry `=0.3.0`.";
    // Act
    let result = validate_current_version(body, "Midge `", env!("CARGO_PKG_VERSION"));
    // Assert
    assert!(result.is_err());
}
const HISTORICAL_STATUS_ENTRY_POINTS: [&str; 3] = [
    "# Midge 0.3.1 Bughunt and Roadmap",
    "## Backup/restore repair evidence (in progress)",
    "## 0.3.1 release preparation (2026-10-02)",
];
const FINAL_RELEASE_HEADING: &str = "## Final release qualification (2026-10-02)";
const FINAL_RELEASE_ANCHOR: &str = "#final-release-qualification-2026-10-02";
const HISTORICAL_STATUS_NOTICE: &str = "Historical status record: superseded by the [final release qualification](#final-release-qualification-2026-10-02).";

fn introductory_paragraph<'a>(body: &'a str, heading: &str) -> Option<&'a str> {
    let after = body.split_once(&format!("{heading}\n"))?.1;
    let paragraph = after
        .split("\n\n")
        .find(|paragraph| !paragraph.trim().is_empty())?
        .trim();
    (!paragraph.starts_with('#')).then_some(paragraph)
}

#[test]
fn should_mark_historical_status_when_roadmap_entry_points_precede_final_qualification() {
    // Arrange: test the actual dated roadmap; a final-section notice alone is
    // insufficient to orient a reader entering at an earlier status heading.
    let body = include_str!("../docs/development/roadmap-0.3.1.md");

    // Act: resolve the notices' target to the actual final heading and inspect
    // the first paragraph at each of the three separately named entry points.
    let final_headings = body
        .lines()
        .filter(|line| *line == FINAL_RELEASE_HEADING)
        .count();
    let final_slug: String = FINAL_RELEASE_HEADING
        .trim_start_matches('#')
        .trim()
        .to_ascii_lowercase()
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || *character == ' ' || *character == '-'
        })
        .map(|character| if character == ' ' { '-' } else { character })
        .collect();
    let missing_notices: Vec<_> = HISTORICAL_STATUS_ENTRY_POINTS
        .into_iter()
        .filter(|heading| {
            body.lines().filter(|line| line == heading).count() != 1
                || introductory_paragraph(body, heading) != Some(HISTORICAL_STATUS_NOTICE)
        })
        .collect();

    // Assert: keep the original headings/anchors and dated evidence; require a
    // visible superseding link before readers consume each historical status.
    assert_eq!(
        final_headings, 1,
        "the linked final release heading must exist"
    );
    assert_eq!(format!("#{final_slug}"), FINAL_RELEASE_ANCHOR);
    assert!(
        missing_notices.is_empty(),
        "historical entry points lack their superseding notice: {missing_notices:?}"
    );
}
