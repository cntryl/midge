//! Live, read-only deployment preflight for configured cloud locations.
//!
//! Preflight performs provider I/O, so it lives with the providers. `config`
//! keeps only static validation and the report types, which keeps the
//! foundation layer free of any dependency on storage.

#![allow(clippy::wildcard_imports)]

use std::time::{Duration, Instant};

use crate::config::cloud_validation::*;
use crate::config::{CloudProviderConfig, CloudStorageLocation, CloudStorageTopology};

impl CloudStorageLocation {
    /// Run an explicit, read-only deployment preflight.
    #[must_use]
    pub fn preflight(&self, options: CloudPreflightOptions) -> CloudValidationReport {
        preflight_location(self, &[CloudStorageRole::Standalone], options)
    }
}

impl CloudStorageTopology {
    /// Run an explicit, read-only deployment preflight.
    #[must_use]
    pub fn preflight(&self, options: CloudPreflightOptions) -> CloudValidationReport {
        preflight_topology(self, options)
    }
}

fn preflight_topology(
    topology: &CloudStorageTopology,
    options: CloudPreflightOptions,
) -> CloudValidationReport {
    let started = Instant::now();
    let locations = unique_locations(topology);
    let (sender, receiver) = std::sync::mpsc::channel();
    for (index, (location, roles)) in locations.iter().enumerate() {
        let sender = sender.clone();
        let location = location.clone();
        let roles = roles.clone();
        let remaining = options.deadline.saturating_sub(started.elapsed());
        std::thread::spawn(move || {
            let report =
                preflight_location_inner(&location, &roles, CloudPreflightOptions::new(remaining));
            let _ = sender.send((index, report));
        });
    }
    drop(sender);
    let mut reports = vec![None; locations.len()];
    for _ in 0..locations.len() {
        let remaining = options.deadline.saturating_sub(started.elapsed());
        let Ok((index, report)) = receiver.recv_timeout(remaining) else {
            break;
        };
        reports[index] = Some(report);
    }
    let reports = reports
        .into_iter()
        .zip(locations)
        .map(|(report, (location, roles))| {
            report.unwrap_or_else(|| preflight_timeout_report(&location, &roles))
        });
    report_from_findings(reports.flat_map(|r| r.findings).collect())
}

fn preflight_location(
    location: &CloudStorageLocation,
    roles: &[CloudStorageRole],
    options: CloudPreflightOptions,
) -> CloudValidationReport {
    let structural = validate_location(location, roles);
    if !structural.is_valid {
        return structural;
    }
    let worker_location = location.clone();
    let worker_roles = roles.to_vec();
    bounded_location_preflight(location, roles, options.deadline, move || {
        preflight_location_inner(&worker_location, &worker_roles, options)
    })
}

fn bounded_location_preflight<F>(
    location: &CloudStorageLocation,
    roles: &[CloudStorageRole],
    deadline: Duration,
    worker: F,
) -> CloudValidationReport
where
    F: FnOnce() -> CloudValidationReport + Send + 'static,
{
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = sender.send(worker());
    });
    receiver
        .recv_timeout(deadline)
        .unwrap_or_else(|_| preflight_timeout_report(location, roles))
}

fn preflight_timeout_report(
    location: &CloudStorageLocation,
    roles: &[CloudStorageRole],
) -> CloudValidationReport {
    let mut report = validate_location(location, roles);
    finding(
        &mut report.findings,
        location.provider(),
        roles,
        CloudValidationMode::LivePreflight,
        CloudCheckCode::FeatureAvailability,
        CloudCheckOutcome::Unverified,
        "provider feature check did not complete before the preflight deadline",
    );
    finding(
        &mut report.findings,
        location.provider(),
        roles,
        CloudValidationMode::LivePreflight,
        CloudCheckCode::BackendResolution,
        CloudCheckOutcome::Failed,
        "preflight deadline exceeded during credential or backend resolution",
    );
    unverified_reads(
        &mut report.findings,
        location.provider(),
        roles,
        "dependent read checks were not completed before the preflight deadline",
    );
    report_from_findings(report.findings)
}

fn preflight_location_inner(
    location: &CloudStorageLocation,
    roles: &[CloudStorageRole],
    options: CloudPreflightOptions,
) -> CloudValidationReport {
    let mut report = validate_location(location, roles);
    let started = Instant::now();
    #[cfg(feature = "cloud-common")]
    {
        if !provider_feature_available(location.provider()) {
            finding(
                &mut report.findings,
                location.provider(),
                roles,
                CloudValidationMode::LivePreflight,
                CloudCheckCode::FeatureAvailability,
                CloudCheckOutcome::Failed,
                "required provider feature is not compiled",
            );
            unverified_reads(
                &mut report.findings,
                location.provider(),
                roles,
                "dependent read checks were not attempted",
            );
            return report_from_findings(report.findings);
        }
        let remaining = options.deadline.saturating_sub(started.elapsed());
        if let Ok(backend) = crate::storage::providers::build_cloud_backend(location.provider()) {
            backend.set_request_timeout(remaining);
            finding(
                &mut report.findings,
                location.provider(),
                roles,
                CloudValidationMode::LivePreflight,
                CloudCheckCode::FeatureAvailability,
                CloudCheckOutcome::Passed,
                "provider feature is available",
            );
            finding(
                &mut report.findings,
                location.provider(),
                roles,
                CloudValidationMode::LivePreflight,
                CloudCheckCode::BackendResolution,
                CloudCheckOutcome::Passed,
                "credentials and backend resolved",
            );
            run_read_checks(
                &mut report.findings,
                location,
                roles,
                backend.as_ref(),
                started,
                options.deadline,
            );
        } else {
            finding(
                &mut report.findings,
                location.provider(),
                roles,
                CloudValidationMode::LivePreflight,
                CloudCheckCode::BackendResolution,
                CloudCheckOutcome::Failed,
                "provider feature, credentials, or backend could not be resolved",
            );
            unverified_reads(
                &mut report.findings,
                location.provider(),
                roles,
                "dependent read checks were not attempted",
            );
        }
    }
    #[cfg(not(feature = "cloud-common"))]
    {
        let _ = (started, options);
        finding(
            &mut report.findings,
            location.provider(),
            roles,
            CloudValidationMode::LivePreflight,
            CloudCheckCode::FeatureAvailability,
            CloudCheckOutcome::Failed,
            "cloud-common provider support is not compiled",
        );
        unverified_reads(
            &mut report.findings,
            location.provider(),
            roles,
            "dependent read checks were not attempted",
        );
    }
    report_from_findings(report.findings)
}

#[cfg(feature = "cloud-common")]
fn provider_feature_available(provider: &CloudProviderConfig) -> bool {
    match provider {
        CloudProviderConfig::AwsS3(_) => cfg!(feature = "cloud-aws"),
        CloudProviderConfig::AzureBlob(_) => cfg!(feature = "cloud-azure"),
        CloudProviderConfig::Gcs(_) => cfg!(feature = "cloud-gcp"),
        CloudProviderConfig::OciObjectStorage(_) => cfg!(feature = "cloud-oci"),
        CloudProviderConfig::S3Compatible(_) => {
            cfg!(any(feature = "cloud-aws", feature = "cloud-oci"))
        }
    }
}

#[cfg(feature = "cloud-common")]
fn run_read_checks(
    findings: &mut Vec<CloudValidationFinding>,
    location: &CloudStorageLocation,
    roles: &[CloudStorageRole],
    backend: &dyn crate::storage::cloud::CloudBackend,
    started: Instant,
    deadline: Duration,
) {
    let remaining = || deadline.saturating_sub(started.elapsed());
    let Some(objects) = preflight_list(findings, location, roles, backend, remaining()) else {
        return;
    };
    finding(
        findings,
        location.provider(),
        roles,
        CloudValidationMode::LivePreflight,
        CloudCheckCode::NamespaceList,
        CloudCheckOutcome::Passed,
        "namespace LIST passed",
    );
    let Some(key) = objects.first() else {
        finding(
            findings,
            location.provider(),
            roles,
            CloudValidationMode::LivePreflight,
            CloudCheckCode::ObjectHead,
            CloudCheckOutcome::Warning,
            "namespace is empty; object read capabilities cannot be verified",
        );
        finding(
            findings,
            location.provider(),
            roles,
            CloudValidationMode::LivePreflight,
            CloudCheckCode::ObjectHead,
            CloudCheckOutcome::Unverified,
            "namespace is empty; HEAD was not verified",
        );
        finding(
            findings,
            location.provider(),
            roles,
            CloudValidationMode::LivePreflight,
            CloudCheckCode::RangedRead,
            CloudCheckOutcome::Unverified,
            "namespace is empty; read was not verified",
        );
        return;
    };
    let Some(size) = preflight_head(findings, location, roles, backend, key, remaining()) else {
        return;
    };
    finding(
        findings,
        location.provider(),
        roles,
        CloudValidationMode::LivePreflight,
        CloudCheckCode::ObjectHead,
        CloudCheckOutcome::Passed,
        "object HEAD passed",
    );
    preflight_read(findings, location, roles, backend, key, size, remaining());
}

#[cfg(feature = "cloud-common")]
fn preflight_read(
    findings: &mut Vec<CloudValidationFinding>,
    location: &CloudStorageLocation,
    roles: &[CloudStorageRole],
    backend: &dyn crate::storage::cloud::CloudBackend,
    key: &str,
    size: u64,
    timeout: Duration,
) {
    use crate::storage::cloud::CloudEvent;
    let (tx, rx) = std::sync::mpsc::channel();
    if size == 0 {
        backend.submit_get(key, tx);
    } else {
        backend.submit_get_range(key, 0, Some(1), tx);
    }
    let passed = match rx.recv_timeout(timeout) {
        Ok(CloudEvent::Get {
            key: event_key,
            result: Ok(bytes),
        }) => size == 0 && event_key == key && bytes.is_empty(),
        Ok(CloudEvent::GetRange {
            key: event_key,
            start: 0,
            end: Some(1),
            result: Ok(bytes),
        }) => size > 0 && event_key == key && bytes.len() == 1,
        _ => false,
    };
    finding(
        findings,
        location.provider(),
        roles,
        CloudValidationMode::LivePreflight,
        CloudCheckCode::RangedRead,
        if passed {
            CloudCheckOutcome::Passed
        } else {
            CloudCheckOutcome::Failed
        },
        if passed {
            "bounded object read passed"
        } else {
            "bounded object read failed or timed out"
        },
    );
}

#[cfg(feature = "cloud-common")]
fn preflight_list(
    findings: &mut Vec<CloudValidationFinding>,
    location: &CloudStorageLocation,
    roles: &[CloudStorageRole],
    backend: &dyn crate::storage::cloud::CloudBackend,
    timeout: Duration,
) -> Option<Vec<String>> {
    use crate::storage::cloud::CloudEvent;
    let list_prefix = if location.prefix().is_empty() {
        String::new()
    } else {
        format!("{}/", location.prefix())
    };
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_list(&list_prefix, tx);
    let Ok(CloudEvent::List {
        result: Ok(keys), ..
    }) = rx.recv_timeout(timeout)
    else {
        finding(
            findings,
            location.provider(),
            roles,
            CloudValidationMode::LivePreflight,
            CloudCheckCode::NamespaceList,
            CloudCheckOutcome::Failed,
            "namespace LIST failed or timed out",
        );
        unverified_reads(
            findings,
            location.provider(),
            roles,
            "HEAD and ranged read depend on LIST",
        );
        return None;
    };
    Some(
        keys.into_iter()
            .filter(|key| list_prefix.is_empty() || key.starts_with(&list_prefix))
            .collect(),
    )
}

#[cfg(feature = "cloud-common")]
fn preflight_head(
    findings: &mut Vec<CloudValidationFinding>,
    location: &CloudStorageLocation,
    roles: &[CloudStorageRole],
    backend: &dyn crate::storage::cloud::CloudBackend,
    key: &str,
    timeout: Duration,
) -> Option<u64> {
    use crate::storage::cloud::CloudEvent;
    let (tx, rx) = std::sync::mpsc::channel();
    backend.submit_head(key, tx);
    let Ok(CloudEvent::Head {
        result: Ok(metadata),
        ..
    }) = rx.recv_timeout(timeout)
    else {
        finding(
            findings,
            location.provider(),
            roles,
            CloudValidationMode::LivePreflight,
            CloudCheckCode::ObjectHead,
            CloudCheckOutcome::Failed,
            "object HEAD failed or timed out",
        );
        finding(
            findings,
            location.provider(),
            roles,
            CloudValidationMode::LivePreflight,
            CloudCheckCode::RangedRead,
            CloudCheckOutcome::Unverified,
            "ranged read depends on HEAD",
        );
        return None;
    };
    Some(metadata.size)
}

fn unverified_reads(
    findings: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    message: &str,
) {
    for code in [
        CloudCheckCode::NamespaceList,
        CloudCheckCode::ObjectHead,
        CloudCheckCode::RangedRead,
    ] {
        finding(
            findings,
            provider,
            roles,
            CloudValidationMode::LivePreflight,
            code,
            CloudCheckOutcome::Unverified,
            message,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AwsS3Config;

    #[cfg(feature = "cloud-common")]
    #[test]
    fn should_verify_only_one_byte_given_nonempty_namespace() {
        use crate::storage::cloud::{CloudBackend, CloudEvent, MockCloudBackend};

        // Arrange
        let backend = MockCloudBackend::new();
        let (sender, receiver) = std::sync::mpsc::channel();
        backend.submit_put("prefix/object", vec![1, 2, 3], Vec::new(), sender);
        assert!(matches!(
            receiver.recv(),
            Ok(CloudEvent::Put { result: Ok(()), .. })
        ));
        let location =
            CloudStorageLocation::new(AwsS3Config::new("valid-bucket", "us-east-1"), "prefix");
        let mut findings = Vec::new();

        // Act
        run_read_checks(
            &mut findings,
            &location,
            &[CloudStorageRole::Standalone],
            &backend,
            Instant::now(),
            Duration::from_secs(1),
        );

        // Assert
        assert!(findings.iter().any(|finding| {
            finding.code == CloudCheckCode::RangedRead
                && finding.outcome == CloudCheckOutcome::Passed
        }));
    }
    #[cfg(feature = "cloud-common")]
    #[test]
    fn should_keep_preflight_inside_configured_namespace() {
        use crate::storage::cloud::{CloudBackend, CloudEvent, MockCloudBackend};

        // Arrange
        let backend = MockCloudBackend::new();
        for key in ["db", "db-old/object"] {
            let (sender, receiver) = std::sync::mpsc::channel();
            backend.submit_put(key, vec![1], Vec::new(), sender);
            assert!(matches!(
                receiver.recv(),
                Ok(CloudEvent::Put { result: Ok(()), .. })
            ));
        }
        let location =
            CloudStorageLocation::new(AwsS3Config::new("valid-bucket", "us-east-1"), "db");
        let mut findings = Vec::new();

        // Act
        run_read_checks(
            &mut findings,
            &location,
            &[CloudStorageRole::Standalone],
            &backend,
            Instant::now(),
            Duration::from_secs(1),
        );

        // Assert
        assert!(findings.iter().any(|finding| {
            finding.code == CloudCheckCode::NamespaceList
                && finding.outcome == CloudCheckOutcome::Passed
        }));
        assert!(!findings.iter().any(|finding| {
            matches!(
                finding.code,
                CloudCheckCode::ObjectHead | CloudCheckCode::RangedRead
            ) && finding.outcome == CloudCheckOutcome::Passed
        }));
    }

    #[test]
    fn should_return_preflight_report_before_deadline() {
        // Arrange
        let location =
            CloudStorageLocation::new(AwsS3Config::new("valid-bucket", "us-east-1"), "database");
        let roles = [CloudStorageRole::Standalone];
        let started = Instant::now();

        // Act
        let report =
            bounded_location_preflight(&location, &roles, Duration::from_millis(10), || {
                std::thread::sleep(Duration::from_secs(1));
                report_from_findings(Vec::new())
            });
        let elapsed = started.elapsed();

        // Assert
        assert!(elapsed < Duration::from_millis(500));
        assert!(!report.is_ready);
        assert!(report.findings.iter().any(|finding| {
            finding.code == CloudCheckCode::BackendResolution
                && finding.outcome == CloudCheckOutcome::Failed
        }));
    }
}
