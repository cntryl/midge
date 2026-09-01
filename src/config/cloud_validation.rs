//! Provider-neutral structural validation and read-only cloud preflight reports.

use std::net::IpAddr;
use std::time::{Duration, Instant};

use serde::Serialize;

use super::{
    AzureCredentialSource, CloudProviderConfig, CloudStorageLocation, CloudStorageTopology,
    GcsCredentialSource, S3CredentialSource,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CloudProviderKind {
    AwsS3,
    AzureBlob,
    Gcs,
    OciObjectStorage,
    S3Compatible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CloudStorageRole {
    Wal,
    Sst,
    Control,
    Standalone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CloudValidationMode {
    Structural,
    LivePreflight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CloudCheckCode {
    Configuration,
    FeatureAvailability,
    BackendResolution,
    NamespaceList,
    ObjectHead,
    RangedRead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CloudCheckOutcome {
    Passed,
    Failed,
    Warning,
    Unverified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CloudValidationFinding {
    pub provider: CloudProviderKind,
    pub roles: Vec<CloudStorageRole>,
    pub mode: CloudValidationMode,
    pub code: CloudCheckCode,
    pub outcome: CloudCheckOutcome,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CloudValidationReport {
    pub is_valid: bool,
    pub is_ready: bool,
    pub is_fully_verified: bool,
    pub findings: Vec<CloudValidationFinding>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloudPreflightOptions {
    deadline: Duration,
}

impl CloudPreflightOptions {
    #[must_use]
    pub fn new(deadline: Duration) -> Self {
        Self { deadline }
    }
    #[must_use]
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }
    #[must_use]
    pub fn deadline(self) -> Duration {
        self.deadline
    }
}

impl Default for CloudPreflightOptions {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

impl CloudProviderConfig {
    /// Validate provider structure without consulting environment variables, files, or services.
    #[must_use]
    pub fn validate(&self) -> CloudValidationReport {
        report_from_findings(validate_provider(self, &[CloudStorageRole::Standalone]))
    }

    pub(crate) fn provider_kind(&self) -> CloudProviderKind {
        match self {
            Self::AwsS3(_) => CloudProviderKind::AwsS3,
            Self::AzureBlob(_) => CloudProviderKind::AzureBlob,
            Self::Gcs(_) => CloudProviderKind::Gcs,
            Self::OciObjectStorage(_) => CloudProviderKind::OciObjectStorage,
            Self::S3Compatible(_) => CloudProviderKind::S3Compatible,
        }
    }
}

pub(super) fn validate_location(
    location: &CloudStorageLocation,
    roles: &[CloudStorageRole],
) -> CloudValidationReport {
    let mut findings = validate_provider(location.provider(), roles);
    if location
        .prefix()
        .split('/')
        .any(|part| matches!(part, "." | ".."))
    {
        fail(
            &mut findings,
            location.provider(),
            roles,
            "cloud prefix must not contain dot segments",
        );
    } else {
        pass(
            &mut findings,
            location.provider(),
            roles,
            CloudCheckCode::Configuration,
            "cloud prefix is structurally valid",
        );
    }
    report_from_findings(findings)
}

pub(super) fn validate_topology(topology: &CloudStorageTopology) -> CloudValidationReport {
    let mut findings = Vec::new();
    for (location, roles) in unique_locations(topology) {
        findings.extend(validate_location(&location, &roles).findings);
    }
    report_from_findings(findings)
}

fn validate_provider(
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
) -> Vec<CloudValidationFinding> {
    let mut out = Vec::new();
    match provider {
        CloudProviderConfig::AwsS3(config) => validate_aws(&mut out, provider, roles, config),
        CloudProviderConfig::S3Compatible(config) => validate_s3(&mut out, provider, roles, config),
        CloudProviderConfig::OciObjectStorage(config) => {
            validate_oci(&mut out, provider, roles, config);
        }
        CloudProviderConfig::AzureBlob(config) => validate_azure(&mut out, provider, roles, config),
        CloudProviderConfig::Gcs(config) => validate_gcs(&mut out, provider, roles, config),
    }
    if !out
        .iter()
        .any(|finding| finding.outcome == CloudCheckOutcome::Failed)
    {
        pass(
            &mut out,
            provider,
            roles,
            CloudCheckCode::Configuration,
            "provider configuration is structurally valid",
        );
    }
    out
}

fn validate_aws(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    config: &super::AwsS3Config,
) {
    required(out, provider, roles, "AWS bucket", &config.bucket);
    required(out, provider, roles, "AWS region", &config.region);
    if !valid_aws_bucket(&config.bucket) {
        fail(
            out,
            provider,
            roles,
            "AWS bucket must satisfy native S3 naming rules",
        );
    }
    if !valid_lower_dns_label(&config.region) {
        fail(
            out,
            provider,
            roles,
            "AWS region must be a lowercase DNS label",
        );
    }
    validate_s3_credentials(out, provider, roles, &config.credentials, true);
}

fn validate_s3(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    config: &super::S3CompatibleConfig,
) {
    required(out, provider, roles, "S3-compatible bucket", &config.bucket);
    required(out, provider, roles, "S3-compatible region", &config.region);
    if !valid_s3_compatible_bucket(&config.bucket) {
        fail(
            out,
            provider,
            roles,
            "S3-compatible bucket must be one transport-safe path or host component",
        );
    }
    if !valid_signing_component(&config.region) {
        fail(
            out,
            provider,
            roles,
            "S3-compatible region must be a transport-safe signing component",
        );
    }
    endpoint(out, provider, roles, &config.endpoint);
    validate_s3_credentials(out, provider, roles, &config.credentials, false);
}

fn validate_oci(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    config: &super::OciObjectStorageConfig,
) {
    required(out, provider, roles, "OCI namespace", &config.namespace);
    required(out, provider, roles, "OCI bucket", &config.bucket);
    required(out, provider, roles, "OCI region", &config.region);
    if config.bucket.len() > 256
        || !config
            .bucket
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        fail(
            out,
            provider,
            roles,
            "OCI bucket must be 1-256 ASCII letters, digits, hyphens, underscores, or periods",
        );
    }
    if !safe_dns_component(&config.namespace) || !safe_dns_component(&config.region) {
        fail(
            out,
            provider,
            roles,
            "OCI namespace and region must be safe DNS components",
        );
    }
    if let Some(value) = &config.endpoint {
        endpoint(out, provider, roles, value);
    }
    validate_s3_credentials(out, provider, roles, &config.credentials, false);
}

fn validate_azure(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    config: &super::AzureBlobConfig,
) {
    let connection_string = matches!(
        config.credential,
        AzureCredentialSource::ConnectionString { .. }
    );
    if !connection_string {
        required(out, provider, roles, "Azure account", &config.account);
    }
    required(out, provider, roles, "Azure container", &config.container);
    if !config.account.is_empty()
        && (!(3..=24).contains(&config.account.len())
            || !config
                .account
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()))
    {
        fail(
            out,
            provider,
            roles,
            "Azure account must be 3-24 lowercase ASCII letters or digits",
        );
    }
    if !valid_azure_container(&config.container) {
        fail(out, provider, roles, "Azure container must be 3-63 lowercase letters, digits, or nonconsecutive interior hyphens");
    }
    if let Some(value) = &config.endpoint {
        if azure_identity_credentials(&config.credential) {
            azure_identity_endpoint(out, provider, roles, value);
        } else {
            endpoint(out, provider, roles, value);
        }
    }
    validate_azure_credentials(out, provider, roles, &config.credential);
}

fn validate_gcs(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    config: &super::GcsConfig,
) {
    required(out, provider, roles, "GCS bucket", &config.bucket);
    if !valid_gcs_bucket(&config.bucket) {
        fail(
            out,
            provider,
            roles,
            "GCS bucket violates published length, character, IP-address, or reserved-name rules",
        );
    }
    if let Some(value) = &config.endpoint {
        endpoint(out, provider, roles, value);
    }
    validate_gcs_credentials(out, provider, roles, &config.credential);
}

fn validate_s3_credentials(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    credentials: &S3CredentialSource,
    aws: bool,
) {
    match credentials {
        S3CredentialSource::Static {
            access_key,
            secret_key,
            session_token,
        } => {
            required(out, provider, roles, "S3 access key", access_key);
            required(out, provider, roles, "S3 secret key", secret_key);
            if let Some(token) = session_token {
                required(out, provider, roles, "S3 session token", token);
            }
        }
        S3CredentialSource::SharedProfile {
            profile,
            credentials_file,
            config_file,
        } => {
            if profile.as_ref().is_some_and(|v| v.trim().is_empty())
                || credentials_file
                    .as_ref()
                    .is_some_and(|v| v.as_os_str().is_empty())
                || config_file
                    .as_ref()
                    .is_some_and(|v| v.as_os_str().is_empty())
            {
                fail(
                    out,
                    provider,
                    roles,
                    "S3 profile names and explicit credential paths must not be empty",
                );
            }
        }
        S3CredentialSource::AwsDefaultChain if !aws => fail(
            out,
            provider,
            roles,
            "AWS default credentials are incompatible with this S3-compatible provider",
        ),
        S3CredentialSource::Environment | S3CredentialSource::AwsDefaultChain => {}
    }
}

fn validate_azure_credentials(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    credential: &AzureCredentialSource,
) {
    match credential {
        AzureCredentialSource::SharedKey { account_key } => {
            required(out, provider, roles, "Azure account key", account_key);
        }
        AzureCredentialSource::SasToken { token } => {
            required(out, provider, roles, "Azure SAS token", token);
        }
        AzureCredentialSource::ConnectionString { connection_string } => {
            required(
                out,
                provider,
                roles,
                "Azure connection string",
                connection_string,
            );
            validate_azure_connection_string(out, provider, roles, connection_string);
        }
        AzureCredentialSource::WorkloadIdentity {
            tenant_id,
            client_id,
            token_file,
        } => {
            if tenant_id.as_ref().is_some_and(|v| v.trim().is_empty())
                || client_id.as_ref().is_some_and(|v| v.trim().is_empty())
                || token_file
                    .as_ref()
                    .is_some_and(|v| v.as_os_str().is_empty())
            {
                fail(
                    out,
                    provider,
                    roles,
                    "Azure workload identity fields and paths must not be empty",
                );
            }
        }
        AzureCredentialSource::ManagedIdentity { client_id }
            if client_id.as_ref().is_some_and(|v| v.trim().is_empty()) =>
        {
            fail(
                out,
                provider,
                roles,
                "Azure managed identity client ID must not be empty",
            );
        }
        _ => {}
    }
}

fn validate_azure_connection_string(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    connection_string: &str,
) {
    let account = azure_connection_string_field(connection_string, "accountname");
    let account_key = azure_connection_string_field(connection_string, "accountkey");
    let sas = azure_connection_string_field(connection_string, "sharedaccesssignature");
    let blob_endpoint = azure_connection_string_field(connection_string, "blobendpoint");
    let development = azure_connection_string_field(connection_string, "usedevelopmentstorage")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let usable = development
        || (account_key.is_some() && account.is_some())
        || (sas.is_some() && (account.is_some() || blob_endpoint.is_some()));
    if !usable {
        fail(
            out,
            provider,
            roles,
            "Azure connection string must contain a supported account-key or SAS configuration",
        );
    }
    if let Some(value) = blob_endpoint {
        endpoint(out, provider, roles, value);
    }
}

fn azure_connection_string_field<'a>(connection_string: &'a str, name: &str) -> Option<&'a str> {
    connection_string.split(';').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        (key.trim().eq_ignore_ascii_case(name) && !value.trim().is_empty()).then(|| value.trim())
    })
}

fn validate_gcs_credentials(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    credential: &GcsCredentialSource,
) {
    match credential {
        GcsCredentialSource::BearerToken { token } => {
            required(out, provider, roles, "GCS bearer token", token);
        }
        GcsCredentialSource::HmacKey { access_id, secret } => {
            required(out, provider, roles, "GCS HMAC access ID", access_id);
            required(out, provider, roles, "GCS HMAC secret", secret);
        }
        GcsCredentialSource::ServiceAccountJsonFile { path }
        | GcsCredentialSource::AuthorizedUserJsonFile { path }
            if path.as_os_str().is_empty() =>
        {
            fail(
                out,
                provider,
                roles,
                "GCS credential file path must not be empty",
            );
        }
        _ => {}
    }
}

fn valid_aws_bucket(value: &str) -> bool {
    (3..=63).contains(&value.len())
        && !value.starts_with("xn--")
        && !value.starts_with("sthree-")
        && !value.starts_with("amzn-s3-demo-")
        && !value.ends_with("-s3alias")
        && !value.ends_with("--ol-s3")
        && !value.as_bytes().ends_with(b".mrap")
        && !value.ends_with("--x-s3")
        && !value.ends_with("--table-s3")
        && !value.contains("..")
        && value.parse::<IpAddr>().is_err()
        && value
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && value
            .chars()
            .last()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '-' | '.'))
}

fn valid_lower_dns_label(value: &str) -> bool {
    (1..=63).contains(&value.len())
        && value
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && value
            .chars()
            .last()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

fn valid_s3_compatible_bucket(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

fn valid_signing_component(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}
fn valid_azure_container(value: &str) -> bool {
    (3..=63).contains(&value.len())
        && !value.starts_with('-')
        && !value.ends_with('-')
        && !value.contains("--")
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}
fn valid_gcs_bucket(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    let length_ok = if value.contains('.') {
        (3..=222).contains(&value.len())
            && value
                .split('.')
                .all(|part| part.len() <= 63 && valid_dns_bucket(part))
    } else {
        (3..=63).contains(&value.len())
    };
    length_ok
        && valid_dns_bucket(value)
        && value.parse::<IpAddr>().is_err()
        && !lower.starts_with("goog")
        && !lower.contains("google")
}
fn valid_dns_bucket(value: &str) -> bool {
    value
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && value
            .chars()
            .last()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '-' | '_' | '.'))
}
fn safe_dns_component(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

fn azure_identity_credentials(credentials: &AzureCredentialSource) -> bool {
    matches!(
        credentials,
        AzureCredentialSource::EnvironmentClientSecret
            | AzureCredentialSource::WorkloadIdentity { .. }
            | AzureCredentialSource::ManagedIdentity { .. }
            | AzureCredentialSource::LightweightDefaultChain
    )
}

fn azure_identity_endpoint(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    value: &str,
) {
    let valid = url::Url::parse(value).is_ok_and(|parsed| {
        parsed.scheme() == "https"
            && parsed.host_str().is_some()
            && parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.query().is_none()
            && parsed.fragment().is_none()
            && parsed.path() == "/"
    });
    if !valid {
        fail(
            out,
            provider,
            roles,
            "Azure identity endpoint must be a pathless HTTPS origin without userinfo, query, or fragment",
        );
    }
}

fn endpoint(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    value: &str,
) {
    let Ok(parsed) = url::Url::parse(value) else {
        fail(
            out,
            provider,
            roles,
            "endpoint must be an absolute HTTP(S) base URL",
        );
        return;
    };
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        fail(
            out,
            provider,
            roles,
            "endpoint must be an absolute HTTP(S) base URL without userinfo, query, or fragment",
        );
    }
}

fn required(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    label: &str,
    value: &str,
) {
    if value.trim().is_empty() {
        fail(out, provider, roles, &format!("{label} must not be empty"));
    }
}
fn fail(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    message: &str,
) {
    finding(
        out,
        provider,
        roles,
        CloudValidationMode::Structural,
        CloudCheckCode::Configuration,
        CloudCheckOutcome::Failed,
        message,
    );
}
fn pass(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    code: CloudCheckCode,
    message: &str,
) {
    finding(
        out,
        provider,
        roles,
        CloudValidationMode::Structural,
        code,
        CloudCheckOutcome::Passed,
        message,
    );
}
fn finding(
    out: &mut Vec<CloudValidationFinding>,
    provider: &CloudProviderConfig,
    roles: &[CloudStorageRole],
    mode: CloudValidationMode,
    code: CloudCheckCode,
    outcome: CloudCheckOutcome,
    message: &str,
) {
    out.push(CloudValidationFinding {
        provider: provider.provider_kind(),
        roles: roles.to_vec(),
        mode,
        code,
        outcome,
        message: message.to_string(),
    });
}

fn report_from_findings(findings: Vec<CloudValidationFinding>) -> CloudValidationReport {
    let is_valid = !findings.iter().any(|f| {
        f.mode == CloudValidationMode::Structural && f.outcome == CloudCheckOutcome::Failed
    });
    let live = findings
        .iter()
        .any(|f| f.mode == CloudValidationMode::LivePreflight);
    let is_ready = is_valid
        && live
        && !findings
            .iter()
            .any(|f| f.outcome == CloudCheckOutcome::Failed)
        && findings.iter().any(|f| {
            f.code == CloudCheckCode::NamespaceList && f.outcome == CloudCheckOutcome::Passed
        });
    let is_fully_verified = is_ready
        && findings.iter().all(|f| {
            !matches!(
                f.outcome,
                CloudCheckOutcome::Unverified | CloudCheckOutcome::Warning
            )
        });
    CloudValidationReport {
        is_valid,
        is_ready,
        is_fully_verified,
        findings,
    }
}

fn unique_locations(
    topology: &CloudStorageTopology,
) -> Vec<(CloudStorageLocation, Vec<CloudStorageRole>)> {
    let mut unique: Vec<(CloudStorageLocation, Vec<CloudStorageRole>)> = Vec::new();
    for (location, role) in [
        (topology.wal(), CloudStorageRole::Wal),
        (topology.sst(), CloudStorageRole::Sst),
        (topology.control(), CloudStorageRole::Control),
    ] {
        if let Some((_, roles)) = unique
            .iter_mut()
            .find(|(candidate, _)| candidate == location)
        {
            roles.push(role);
        } else {
            unique.push((location.clone(), vec![role]));
        }
    }
    unique
}

pub(super) fn preflight_topology(
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

pub(super) fn preflight_location(
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
        if let Ok(backend) = crate::cloud_preflight_backend::build(location.provider()) {
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
    backend: &dyn crate::cloud_preflight_backend::CloudBackend,
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
    backend: &dyn crate::cloud_preflight_backend::CloudBackend,
    key: &str,
    size: u64,
    timeout: Duration,
) {
    use crate::cloud_preflight_backend::CloudEvent;
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
    backend: &dyn crate::cloud_preflight_backend::CloudBackend,
    timeout: Duration,
) -> Option<Vec<String>> {
    use crate::cloud_preflight_backend::CloudEvent;
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
    backend: &dyn crate::cloud_preflight_backend::CloudBackend,
    key: &str,
    timeout: Duration,
) -> Option<u64> {
    use crate::cloud_preflight_backend::CloudEvent;
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
    use crate::config::{
        AwsS3Config, AzureBlobConfig, AzureCredentialSource, CloudStorageLocation,
        CloudStorageTopology, OciObjectStorageConfig, S3CompatibleConfig,
    };

    #[test]
    fn should_report_multiple_structural_failures_without_secrets() {
        // Arrange: preserve a wide gap between the 10 ms preflight deadline
        // and the one-second slow path for loaded cross-platform runners.
        let aws = AwsS3Config::new("Invalid_Bucket", "us-east-1");
        let generic = S3CompatibleConfig::new(
            "tenant/bucket",
            "region",
            "https://user:secret@example.test/path?token=secret",
            S3CredentialSource::environment(),
        );

        // Act
        let aws_report = CloudProviderConfig::from(aws).validate();
        let generic_report = CloudProviderConfig::from(generic).validate();

        // Assert
        assert!(!aws_report.is_valid);
        assert!(!generic_report.is_valid);
        let serialized = serde_json::to_string(&generic_report).expect("serialize report");
        assert!(!serialized.contains("secret"));
        assert!(!serialized.contains("token="));
    }

    #[test]
    fn should_accept_dotted_native_aws_bucket() {
        // Arrange
        let provider = CloudProviderConfig::from(AwsS3Config::new("database.example", "us-east-1"));

        // Act
        let report = provider.validate();
        let options = crate::engine::OpenOptions::cloud(
            "/tmp/midge-dotted-aws-validation",
            CloudStorageLocation::new(provider, "database"),
        )
        .build();

        // Assert
        assert!(report.is_valid, "dotted AWS bucket should use path style");
        assert!(
            options.is_ok(),
            "OpenOptions should accept dotted AWS bucket"
        );
    }

    #[test]
    fn should_reject_malformed_endpoint_authorities() {
        // Arrange
        let endpoints = ["http://:9000", "https://bad host.example"];

        // Act
        let reports = endpoints.map(|endpoint| {
            CloudProviderConfig::from(S3CompatibleConfig::new(
                "bucket",
                "region",
                endpoint,
                S3CredentialSource::environment(),
            ))
            .validate()
        });

        // Assert
        assert!(reports.iter().all(|report| !report.is_valid));
    }

    #[test]
    fn should_accept_accountless_sas_connection_string() {
        // Arrange
        let connection_string = "BlobEndpoint=https://account.blob.core.usgovcloudapi.net;SharedAccessSignature=sv=2024-11-04&sig=sensitive";
        let provider =
            CloudProviderConfig::azure_blob_connection_string("container", connection_string);
        let location = CloudStorageLocation::new(provider.clone(), "database");

        // Act
        let report = provider.validate();
        let options = crate::engine::OpenOptions::cloud(
            "/tmp/midge-accountless-azure-sas-validation",
            location,
        )
        .build();
        let serialized = serde_json::to_string(&report).expect("serialize report");

        // Assert
        assert!(report.is_valid);
        assert!(options.is_ok());
        assert!(!serialized.contains("sensitive"));
    }

    #[test]
    fn should_reject_non_origin_azure_identity_endpoints() {
        // Arrange
        let credentials = [
            AzureCredentialSource::environment_client_secret(),
            AzureCredentialSource::workload_identity(),
            AzureCredentialSource::managed_identity(),
            AzureCredentialSource::default_chain(),
        ];
        let endpoints = [
            "http://account.blob.core.windows.net",
            "https://account.blob.core.windows.net/path",
        ];

        // Act
        let reports = credentials.into_iter().flat_map(|credential| {
            endpoints.map(move |endpoint| {
                CloudProviderConfig::from(
                    AzureBlobConfig::new("account", "container")
                        .with_credentials(credential.clone())
                        .with_endpoint(endpoint),
                )
                .validate()
            })
        });

        // Assert
        assert!(reports.into_iter().all(|report| !report.is_valid));
    }

    #[test]
    fn should_reject_invalid_native_aws_identifiers() {
        // Arrange
        let invalid_buckets = [
            "foo_bar",
            "foo..bar",
            "sthree-bucket",
            "amzn-s3-demo-bucket",
        ];
        let invalid_regions = ["us-east-1/path", "US-EAST-1"];

        // Act
        let bucket_reports = invalid_buckets.map(|bucket| {
            CloudProviderConfig::from(AwsS3Config::new(bucket, "us-east-1")).validate()
        });
        let region_reports = invalid_regions.map(|region| {
            CloudProviderConfig::from(AwsS3Config::new("valid-bucket", region)).validate()
        });

        // Assert
        assert!(bucket_reports.iter().all(|report| !report.is_valid));
        assert!(region_reports.iter().all(|report| !report.is_valid));
    }

    #[test]
    fn should_reject_s3_routing_delimiters() {
        // Arrange
        let provider_with_bucket_path = S3CompatibleConfig::new(
            "tenant/bucket",
            "custom_region.1",
            "https://objects.example.test",
            S3CredentialSource::environment(),
        );
        let provider_with_region_path = S3CompatibleConfig::new(
            "Tenant_BUCKET.1",
            "region/scope",
            "https://objects.example.test",
            S3CredentialSource::environment(),
        );
        let flexible_provider = S3CompatibleConfig::new(
            "Tenant_BUCKET.1",
            "custom_region.1",
            "https://objects.example.test",
            S3CredentialSource::environment(),
        );

        // Act
        let bucket_report = CloudProviderConfig::from(provider_with_bucket_path).validate();
        let region_report = CloudProviderConfig::from(provider_with_region_path).validate();
        let flexible_report = CloudProviderConfig::from(flexible_provider).validate();

        // Assert
        assert!(!bucket_report.is_valid);
        assert!(!region_report.is_valid);
        assert!(flexible_report.is_valid);
    }

    #[test]
    fn should_use_explicit_oci_realm_endpoint() {
        // Arrange
        let endpoint = "https://namespace.compat.objectstorage.us-gov-ashburn-1.oraclegovcloud.com";
        let config = OciObjectStorageConfig::new(
            "namespace",
            "bucket",
            "us-gov-ashburn-1",
            S3CredentialSource::access_key("access", "secret"),
        )
        .with_endpoint(endpoint);

        // Act
        let report = CloudProviderConfig::from(config.clone()).validate();

        // Assert
        assert!(report.is_valid);
        assert_eq!(config.endpoint_override(), Some(endpoint));
        assert_eq!(config.endpoint(), endpoint);
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

    #[test]
    fn should_validate_deduplicated_shared_topology() {
        // Arrange
        let location =
            CloudStorageLocation::new(AwsS3Config::new("valid-bucket", "us-east-1"), "/database/");
        let topology = CloudStorageTopology::new(location.clone());

        // Act
        let report = topology.validate();

        // Assert
        assert_eq!(location.prefix(), "database");
        assert!(report.is_valid);
        assert!(report.findings.iter().all(|finding| finding.roles
            == [
                CloudStorageRole::Wal,
                CloudStorageRole::Sst,
                CloudStorageRole::Control,
            ]));
    }

    #[cfg(feature = "cloud-common")]
    #[test]
    fn should_verify_only_one_byte_given_nonempty_namespace() {
        use crate::cloud_preflight_backend::{CloudBackend, CloudEvent, MockCloudBackend};

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
        use crate::cloud_preflight_backend::{CloudBackend, CloudEvent, MockCloudBackend};

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
}
