use super::*;

// ========== Goal Enum Tests ==========

#[test]
fn should_have_latency_as_default_goal() {
    assert_eq!(Goal::default(), Goal::Latency);
}

// ========== MemoryBudget Enum Tests ==========

#[test]
fn should_have_auto_as_default_memory_budget() {
    assert_eq!(MemoryBudget::default(), MemoryBudget::Auto);
}

// ========== WorkloadProfile Enum Tests ==========

#[test]
fn should_have_mixed_as_default_workload() {
    assert_eq!(WorkloadProfile::default(), WorkloadProfile::Mixed);
}

// ========== Cloud Provider Constructor Tests ==========

#[test]
fn should_create_aws_s3_with_default_chain() {
    // Arrange
    let provider = CloudProviderConfig::aws_s3("bucket", "us-east-1");

    // Act
    // Assert
    assert!(
        matches!(provider, CloudProviderConfig::AwsS3(config) if config.bucket() == "bucket" && config.region() == "us-east-1" && matches!(config.credentials(), S3CredentialSource::AwsDefaultChain))
    );
}

#[test]
fn should_create_s3_compatible_env_with_safe_defaults() {
    // Arrange
    let provider = CloudProviderConfig::s3_compatible_env("bucket", "http://localhost:9000");

    // Act
    // Assert
    assert!(
        matches!(provider, CloudProviderConfig::S3Compatible(config) if config.bucket() == "bucket" && config.region() == "us-east-1" && config.endpoint() == "http://localhost:9000" && config.path_style() && matches!(config.credentials(), S3CredentialSource::Environment))
    );
}

#[test]
fn should_create_s3_compatible_config_with_explicit_overrides() {
    // Arrange
    let provider = CloudProviderConfig::s3_compatible(
        "bucket",
        "us-phoenix-1",
        "https://object.example.com",
        "key",
        "secret",
    )
    .with_path_style(false)
    .expect("path-style override");

    // Act
    // Assert
    assert!(
        matches!(provider, CloudProviderConfig::S3Compatible(config) if config.bucket() == "bucket" && config.region() == "us-phoenix-1" && config.endpoint() == "https://object.example.com" && !config.path_style() && config.credentials() == &S3CredentialSource::access_key("key", "secret"))
    );
}

#[test]
fn should_create_azure_configs_for_supported_credentials() {
    // Arrange
    let identity = CloudProviderConfig::azure_blob("account", "container");
    let shared_key =
        CloudProviderConfig::azure_blob_shared_key("account", "container", "account-key");
    let sas = CloudProviderConfig::azure_blob_sas("account", "container", "?sig=token");
    let conn = CloudProviderConfig::azure_blob_connection_string(
        "container",
        "AccountName=account;AccountKey=key",
    );

    // Act
    // Assert
    assert!(matches!(
        identity,
        CloudProviderConfig::AzureBlob(config) if matches!(config.credentials(), AzureCredentialSource::LightweightDefaultChain)
    ));
    assert!(matches!(
        shared_key,
        CloudProviderConfig::AzureBlob(config) if matches!(config.credentials(), AzureCredentialSource::SharedKey { .. })
    ));
    assert!(matches!(
        sas,
        CloudProviderConfig::AzureBlob(config) if matches!(config.credentials(), AzureCredentialSource::SasToken { .. })
    ));
    assert!(matches!(
        conn,
        CloudProviderConfig::AzureBlob(config) if config.account() == "account" && matches!(config.credentials(), AzureCredentialSource::ConnectionString { .. })
    ));
}

#[test]
fn should_create_gcs_configs_with_matching_api_styles() {
    // Arrange
    let adc = CloudProviderConfig::gcs("bucket");
    let hmac = CloudProviderConfig::gcs_hmac("bucket", "access", "secret");
    let bearer = CloudProviderConfig::gcs_bearer_token("bucket", "token");

    // Act
    // Assert
    assert!(matches!(
        adc,
        CloudProviderConfig::Gcs(config) if config.api_style() == GcsApiStyle::Json && matches!(config.credentials(), GcsCredentialSource::ApplicationDefault)
    ));
    assert!(matches!(
        hmac,
        CloudProviderConfig::Gcs(config) if config.api_style() == GcsApiStyle::Xml && matches!(config.credentials(), GcsCredentialSource::HmacKey { .. })
    ));
    assert!(matches!(
        bearer,
        CloudProviderConfig::Gcs(config) if config.api_style() == GcsApiStyle::Json && matches!(config.credentials(), GcsCredentialSource::BearerToken { .. })
    ));
}

#[test]
fn should_resolve_three_cloud_storage_locations() {
    // Arrange
    let wal = crate::config::CloudStorageLocation::new(
        CloudProviderConfig::aws_s3("wal-bucket", "us-east-1"),
        "database-a",
    );
    let sst = crate::config::CloudStorageLocation::new(
        CloudProviderConfig::aws_s3("sst-bucket", "us-east-1"),
        "database-a",
    );
    let control = CloudProviderConfig::aws_s3("control-bucket", "us-east-1");
    let topology = crate::config::CloudStorageTopology::new(wal.clone())
        .with_sst(sst.clone())
        .with_control(crate::config::CloudStorageLocation::new(
            control.clone(),
            "database-a",
        ));

    // Act
    let options = OpenOptions::cloud_multi("/tmp/midge-control-options", topology)
        .build()
        .expect("build separated cloud options");

    // Assert
    let configured = options
        .cloud_storage_topology()
        .expect("cloud topology should be retained");
    assert_eq!(configured.wal(), &wal);
    assert_eq!(configured.sst(), &sst);
    assert_eq!(configured.control().provider(), &control);
    assert_eq!(configured.control().prefix(), "database-a");
}

#[test]
fn should_resolve_shared_cloud_topology() {
    // Arrange
    let shared = crate::config::CloudStorageLocation::new(
        CloudProviderConfig::aws_s3("shared-bucket", "us-east-1"),
        "database-a",
    );
    // Act
    let shared_options = OpenOptions::cloud("/tmp/midge-shared-options", shared.clone())
        .build()
        .expect("build shared cloud options");

    // Assert
    let shared_topology = shared_options
        .cloud_storage_topology()
        .expect("shared topology");
    assert_eq!(shared_topology.wal(), &shared);
    assert_eq!(shared_topology.sst(), &shared);
    assert_eq!(shared_topology.control(), &shared);
}

#[test]
fn should_resolve_two_location_cloud_topology() {
    // Arrange
    let shared = crate::config::CloudStorageLocation::new(
        CloudProviderConfig::aws_s3("shared-bucket", "us-east-1"),
        "database-a",
    );
    let control = crate::config::CloudStorageLocation::new(
        CloudProviderConfig::aws_s3("control-bucket", "us-east-1"),
        "database-a",
    );

    // Act
    let two_location_options = OpenOptions::cloud_multi(
        "/tmp/midge-two-location-options",
        crate::config::CloudStorageTopology::new(shared.clone()).with_control(control.clone()),
    )
    .build()
    .expect("build two-location cloud options");

    // Assert
    let two_location_topology = two_location_options
        .cloud_storage_topology()
        .expect("two-location topology");
    assert_eq!(two_location_topology.wal(), &shared);
    assert_eq!(two_location_topology.sst(), &shared);
    assert_eq!(two_location_topology.control(), &control);
}

#[test]
fn should_apply_fluent_cloud_modifiers() {
    // Arrange
    let s3 = CloudProviderConfig::s3_compatible_env("bucket", "http://old")
        .with_endpoint("http://new")
        .expect("endpoint override")
        .with_s3_region("eu-west-1")
        .expect("region override")
        .with_path_style(false)
        .expect("path-style override")
        .with_s3_credentials(S3CredentialSource::access_key("key", "secret"))
        .expect("s3 credentials");
    let gcs = CloudProviderConfig::gcs_hmac("bucket", "access", "secret")
        .with_gcs_credentials(GcsCredentialSource::application_default())
        .expect("gcs credentials");

    // Act
    // Assert
    assert!(
        matches!(s3, CloudProviderConfig::S3Compatible(config) if config.bucket() == "bucket" && config.region() == "eu-west-1" && config.endpoint() == "http://new" && !config.path_style() && config.credentials() == &S3CredentialSource::access_key("key", "secret"))
    );
    assert!(matches!(
        gcs,
        CloudProviderConfig::Gcs(config) if config.api_style() == GcsApiStyle::Json && matches!(config.credentials(), GcsCredentialSource::ApplicationDefault)
    ));
}

#[test]
fn should_reject_unsupported_cloud_modifiers() {
    // Arrange
    // Act
    // Assert
    assert!(CloudProviderConfig::aws_s3("bucket", "us-east-1")
        .with_endpoint("http://localhost:9000")
        .is_err());
    assert!(CloudProviderConfig::gcs("bucket")
        .with_path_style(true)
        .is_err());
    assert!(CloudProviderConfig::azure_blob("account", "container")
        .with_s3_region("us-west-2")
        .is_err());
}

#[test]
fn should_parse_azure_account_from_connection_string_config() {
    // Arrange
    let provider = CloudProviderConfig::azure_blob_connection_string(
        "container",
        "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=key",
    );

    // Act
    // Assert
    assert!(matches!(
        provider,
        CloudProviderConfig::AzureBlob(config) if config.account() == "myaccount"
    ));
}

#[test]
fn should_reject_connection_string_credential_override_without_account() {
    // Arrange
    let provider: CloudProviderConfig = crate::config::AzureBlobConfig::new("", "container")
        .with_credentials(AzureCredentialSource::connection_string("AccountKey=key"))
        .into();

    let result = provider.with_azure_credentials(AzureCredentialSource::shared_key("key"));

    // Act
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_create_workload_identity_credentials_without_none_annotations() {
    // Arrange
    let client = AzureCredentialSource::workload_identity_for_client("client-id");
    let file = AzureCredentialSource::workload_identity_from_file("/var/run/token");
    let full = AzureCredentialSource::workload_identity_with(
        Some("tenant-id".to_string()),
        None,
        Some(PathBuf::from("/var/run/token")),
    );

    // Act
    // Assert
    assert!(matches!(
        client,
        AzureCredentialSource::WorkloadIdentity {
            client_id: Some(_),
            ..
        }
    ));
    assert!(matches!(
        file,
        AzureCredentialSource::WorkloadIdentity {
            token_file: Some(_),
            ..
        }
    ));
    assert!(matches!(
        full,
        AzureCredentialSource::WorkloadIdentity {
            tenant_id: Some(_),
            client_id: None,
            token_file: Some(_),
        }
    ));
}

#[test]
fn should_clamp_memtable_for_small_explicit_budget() {
    // Arrange
    let budget = 64 * 1024 * 1024;

    // Act
    let opts = OpenOptions::in_memory()
        .goal(Goal::Throughput)
        .memory_budget(MemoryBudget::Bytes(budget))
        .build()
        .expect("build options");

    // Assert
    assert_eq!(opts.memory_budget_bytes(), budget);
    assert!(opts.memtable_size_limit() <= budget / 2);
    assert!(opts.block_cache_size() <= budget);
}

#[test]
fn should_use_explicit_memtable_size_for_flush_threshold_when_only_size_override_is_set() {
    // Arrange
    let size_limit = 128 * 1024;

    // Act
    let opts = OpenOptions::in_memory()
        .with_memtable_size_limit(size_limit)
        .build()
        .expect("build options");

    // Assert
    assert_eq!(opts.memtable_size_limit(), size_limit);
    assert_eq!(opts.memtable_flush_threshold(), size_limit);
}

#[test]
fn should_preserve_explicit_memtable_limits_when_both_are_set() {
    // Arrange
    let size_limit = 256 * 1024;
    let flush_threshold = 128 * 1024;

    // Act
    let opts = OpenOptions::in_memory()
        .with_memtable_size_limit(size_limit)
        .with_memtable_flush_threshold(flush_threshold)
        .build()
        .expect("build options");

    // Assert
    assert_eq!(opts.memtable_size_limit(), size_limit);
    assert_eq!(opts.memtable_flush_threshold(), flush_threshold);
}

#[test]
fn should_reject_zero_memtable_overrides_when_building() {
    // Arrange
    // (no setup required)

    // Act
    let result = OpenOptions::in_memory()
        .with_memtable_size_limit(0)
        .with_memtable_flush_threshold(0)
        .build();

    // Assert
    assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
}

#[test]
fn should_reject_flush_threshold_exceeding_size_limit_given_both_explicit_overrides_when_building()
{
    // Arrange

    // Act
    let result = OpenOptions::in_memory()
        .with_memtable_size_limit(1024)
        .with_memtable_flush_threshold(2048)
        .build();

    // Assert
    assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
}

#[test]
fn should_reject_memory_budget_given_value_below_minimum_when_building() {
    // Arrange

    // Act
    let result = OpenOptions::in_memory()
        .memory_budget(MemoryBudget::Bytes(2))
        .build();

    // Assert
    assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
}

#[test]
fn should_reject_zero_transaction_memory_pool_size_when_building() {
    // Arrange

    // Act
    let result = OpenOptions::in_memory()
        .transaction_memory_pool_size(0)
        .build();

    // Assert
    assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
}

#[test]
fn should_reject_degenerate_cloud_write_policy_given_zero_fields_when_building() {
    // Arrange
    let default = CloudWritePolicy::default();
    let invalid_policies = [
        CloudWritePolicy {
            eventual_flush_segment_gap: 0,
            ..default.clone()
        },
        CloudWritePolicy {
            wal_seal_min_segment_bytes: 0,
            ..default.clone()
        },
        CloudWritePolicy {
            wal_seal_max_flush_delay: Duration::ZERO,
            ..default.clone()
        },
        CloudWritePolicy {
            wal_seal_max_pending_writes: 0,
            ..default
        },
    ];

    // Act
    let results: Vec<_> = invalid_policies
        .into_iter()
        .map(|policy| OpenOptions::in_memory().cloud_write_policy(policy).build())
        .collect();

    // Assert
    assert!(
        results
            .iter()
            .all(|result| matches!(result, Err(MidgeError::InvalidArgument(_)))),
        "every zero cloud write-policy field must be rejected"
    );
}

// ========== OpenOptions Builder Tests ==========

#[test]
fn should_create_in_memory_options() {
    // Arrange
    // (no setup required)

    // Act
    let opts = OpenOptions::in_memory();

    // Assert
    assert_eq!(opts.storage, Storage::InMemory);
    assert_eq!(opts.goal, Goal::Latency);
    assert_eq!(opts.memory_budget, MemoryBudget::Auto);
    assert_eq!(opts.workload, WorkloadProfile::Mixed);
}

#[test]
fn should_create_local_options_with_path() {
    // Arrange
    // (no setup required)

    // Act
    let opts = OpenOptions::local("./test_db");

    // Assert
    assert_eq!(
        opts.storage,
        Storage::Local {
            path: PathBuf::from("./test_db")
        }
    );
}

#[test]
fn should_set_goal_when_calling_goal() {
    // Arrange
    // Act
    let opts = OpenOptions::in_memory().goal(Goal::Throughput);

    // Assert
    assert_eq!(opts.goal, Goal::Throughput);
}

#[test]
fn should_use_inclusive_worth_compressing_ratio_for_throughput_goal() {
    // Arrange
    let options = OpenOptions::in_memory()
        .goal(Goal::Throughput)
        .build()
        .expect("build throughput options");

    // Act
    let policy = options.compression_policy();

    // Assert
    assert!(matches!(
        policy,
        CompressionPolicy::Adaptive {
            min_savings_bytes: 256,
            min_ratio,
            check_algorithms,
        } if (*min_ratio - 0.95).abs() < f32::EPSILON
            && check_algorithms
                == &[CompressionAlgo::Lz4, CompressionAlgo::Zstd3]
    ));
}

#[test]
fn should_set_memory_budget_when_calling_memory_budget() {
    // Arrange
    let budget = MemoryBudget::Bytes(2 * 1024 * 1024 * 1024);

    // Act
    let opts = OpenOptions::in_memory().memory_budget(budget);

    // Assert
    assert_eq!(opts.memory_budget, budget);
}

#[test]
fn should_set_workload_when_calling_workload() {
    let opts = OpenOptions::in_memory().workload(WorkloadProfile::WriteHeavy);
    assert_eq!(opts.workload, WorkloadProfile::WriteHeavy);
}

#[test]
fn should_support_fluent_builder_chain() {
    // Arrange
    // (no setup required)

    // Act
    let opts = OpenOptions::local("./db")
        .goal(Goal::Latency)
        .workload(WorkloadProfile::ReadMostly)
        .build()
        .expect("build options");

    // Assert
    assert_eq!(
        opts.storage,
        Storage::Local {
            path: PathBuf::from("./db")
        }
    );
    assert_eq!(opts.goal, Goal::Latency);
    assert_eq!(opts.workload, WorkloadProfile::ReadMostly);
}

#[test]
fn should_derive_parameters_when_building() {
    // Arrange
    // (no setup required)

    // Act
    let opts = OpenOptions::in_memory()
        .goal(Goal::Latency)
        .build()
        .expect("build options");

    // Assert
    assert!(opts.block_size() > 0);
    assert!(opts.memtable_size_limit() > 0);
    assert!(opts.target_sst_size() > 0);
    assert!(opts.block_cache_size() > 0);
}

#[test]
fn should_use_different_block_sizes_for_different_goals() {
    // Arrange
    // (no setup required)

    // Act
    let latency_opts = OpenOptions::in_memory()
        .goal(Goal::Latency)
        .build()
        .expect("build latency options");
    let throughput_opts = OpenOptions::in_memory()
        .goal(Goal::Throughput)
        .build()
        .expect("build throughput options");

    // Assert
    assert_ne!(latency_opts.block_size(), throughput_opts.block_size());
}

#[test]
fn should_use_different_memtable_sizes_for_different_workloads() {
    // Arrange
    // (no setup required)

    // Act
    let normal = OpenOptions::in_memory()
        .workload(WorkloadProfile::Mixed)
        .build()
        .expect("build normal options");
    let write_heavy = OpenOptions::in_memory()
        .workload(WorkloadProfile::WriteHeavy)
        .build()
        .expect("build write-heavy options");

    // Assert
    assert!(write_heavy.memtable_size_limit() >= normal.memtable_size_limit());
}

#[test]
fn should_provide_getter_methods() {
    // Arrange: pin an explicit memtable size limit so we can assert the
    // getter reflects caller intent, not just a non-panicking default.
    let explicit_limit = 8 * 1024 * 1024;

    // Act
    let opts = OpenOptions::in_memory()
        .with_memtable_size_limit(explicit_limit)
        .build()
        .expect("build options");

    // Assert - getters return concrete, falsifiable values
    assert_eq!(opts.memtable_size_limit(), explicit_limit);
    assert!(opts.block_size() > 0);
    assert!(opts.target_sst_size() > 0);
    assert!(opts.block_cache_size() > 0);
    assert!(opts.wal_buffer_size() > 0);
    assert!(opts.l0_compaction_trigger() > 0);
}

#[test]
fn should_respect_explicit_memory_budget() {
    // Arrange
    // Use a realistic budget larger than 2x memtable size to have cache allocation
    let budget = MemoryBudget::Bytes(512 * 1024 * 1024); // 512MB

    // Act
    let opts = OpenOptions::in_memory()
        .memory_budget(budget)
        .build()
        .expect("build options");

    // Assert
    assert!(opts.block_cache_size() > 0);
}

#[test]
fn should_clone_options() {
    // Arrange: set several distinct fields so a shallow/partial clone would
    // be caught, then build both to compare via public getters (the builder
    // itself doesn't derive PartialEq).
    let original = OpenOptions::in_memory()
        .goal(Goal::Throughput)
        .workload(WorkloadProfile::WriteHeavy)
        .with_memtable_size_limit(16 * 1024 * 1024)
        .transaction_memory_pool_size(2 * 1024 * 1024);

    // Act
    let cloned = original.clone();
    let original_opts = original.build().expect("build original");
    let cloned_opts = cloned.build().expect("build cloned");

    // Assert: clone carries every field forward independently
    assert_eq!(cloned_opts.goal(), original_opts.goal());
    assert_eq!(cloned_opts.workload(), original_opts.workload());
    assert_eq!(
        cloned_opts.memtable_size_limit(),
        original_opts.memtable_size_limit()
    );
    assert_eq!(
        cloned_opts.transaction_memory_pool_size(),
        original_opts.transaction_memory_pool_size()
    );
    assert_eq!(cloned_opts.goal(), Goal::Throughput);
    assert_eq!(cloned_opts.workload(), WorkloadProfile::WriteHeavy);
    assert_eq!(cloned_opts.memtable_size_limit(), 16 * 1024 * 1024);
}

#[test]
fn should_use_half_ttl_clock_skew_tolerance_by_default() {
    // Arrange
    // (default configuration)

    // Act
    let options = OpenOptions::in_memory().build().expect("build options");

    // Assert
    assert_eq!(
        options.lease_clock_skew_tolerance(),
        Duration::from_secs(15)
    );
}

#[test]
fn should_reject_clock_skew_tolerance_larger_than_lease_ttl() {
    // Arrange
    let tolerance = Duration::from_secs(31);

    // Act
    let result = OpenOptions::in_memory()
        .lease_clock_skew_tolerance(tolerance)
        .build();

    // Assert
    assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
}

#[test]
fn should_allow_explicit_lease_profile() {
    // Arrange
    let ttl = Duration::from_secs(30);
    let skew = Duration::from_secs(5);

    // Act
    let options = OpenOptions::in_memory()
        .lease_ttl(ttl)
        .lease_clock_skew_tolerance(skew)
        .build()
        .expect("valid lease timing profile");

    // Assert
    assert_eq!(options.lease_ttl(), ttl);
    assert_eq!(options.lease_clock_skew_tolerance(), skew);
}

#[test]
fn should_reject_zero_lease_ttl() {
    // Arrange
    let ttl = Duration::ZERO;

    // Act
    let result = OpenOptions::in_memory().lease_ttl(ttl).build();

    // Assert
    assert!(
        matches!(result, Err(MidgeError::InvalidArgument(message)) if message.contains("lease TTL"))
    );
}
