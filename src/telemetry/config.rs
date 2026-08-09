//! Telemetry configuration

use serde::{Deserialize, Serialize};

/// OTLP exporter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpConfig {
    /// OTLP collector endpoint (e.g., "<http://localhost:4317>")
    pub endpoint: String,
}

/// Telemetry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryFeatures {
    /// Enable structured logging
    pub enable_logging: bool,

    /// Enable tracing spans
    pub enable_tracing: bool,

    /// Enable metrics collection
    pub enable_metrics: bool,
}

/// Telemetry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// Enable/disable all telemetry
    pub enabled: bool,

    #[serde(flatten)]
    pub features: TelemetryFeatures,

    /// Sampling rate for traces; must be finite and in `(0.0, 1.0]`.
    pub trace_sample_rate: f64,

    /// Service name for traces
    pub service_name: String,

    /// OTLP exporter configuration (if using OTLP)
    pub otlp_config: Option<OtlpConfig>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: cfg!(debug_assertions), // Only enable in dev by default
            features: TelemetryFeatures {
                enable_logging: true,
                enable_tracing: true,
                enable_metrics: true,
            },
            trace_sample_rate: 1.0,
            service_name: "midge".to_string(),
            otlp_config: None,
        }
    }
}

impl TelemetryConfig {
    /// Create a new configuration (disabled by default)
    pub fn new() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Enable/disable telemetry
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set service name
    pub fn with_service_name(mut self, name: String) -> Self {
        self.service_name = name;
        self
    }

    /// Set trace sampling rate for tests.
    ///
    /// Validation deliberately happens at telemetry initialization so that
    /// deserialized and directly-constructed configurations follow the same
    /// fail-closed path as builder-created configurations.
    #[cfg(test)]
    pub fn with_sample_rate(mut self, rate: f64) -> Self {
        self.trace_sample_rate = rate;
        self
    }

    pub(crate) fn validate(&self) -> crate::common::MidgeResult<()> {
        if !self.trace_sample_rate.is_finite()
            || self.trace_sample_rate <= 0.0
            || self.trace_sample_rate > 1.0
        {
            return Err(crate::common::MidgeError::InvalidArgument(
                "telemetry trace sample rate must be finite and in (0.0, 1.0]".to_string(),
            ));
        }

        Ok(())
    }

    /// Set OTLP exporter configuration
    #[cfg(test)]
    pub fn with_otlp(mut self, endpoint: String) -> Self {
        self.otlp_config = Some(OtlpConfig { endpoint });
        self
    }

    /// Enable/disable logging
    #[cfg(test)]
    pub fn with_logging(mut self, enabled: bool) -> Self {
        self.features.enable_logging = enabled;
        self
    }

    /// Enable/disable tracing
    #[cfg(test)]
    pub fn with_tracing(mut self, enabled: bool) -> Self {
        self.features.enable_tracing = enabled;
        self
    }

    /// Enable/disable metrics
    #[cfg(test)]
    pub fn with_metrics(mut self, enabled: bool) -> Self {
        self.features.enable_metrics = enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_default_config() {
        let config = TelemetryConfig::default();
        assert!((config.trace_sample_rate - 1.0).abs() <= f64::EPSILON);
        assert_eq!(config.service_name, "midge");
    }

    #[test]
    fn should_reject_zero_or_nonfinite_sample_rate_given_telemetry_config_when_initializing() {
        // Arrange
        let invalid_rates = [0.0, -0.5, 1.5, f64::NAN, f64::INFINITY];

        // Act
        let errors = invalid_rates.map(|rate| {
            super::super::Telemetry::init(&TelemetryConfig::new().with_sample_rate(rate))
                .expect_err("invalid sample rate must fail before global initialization")
        });

        // Assert
        for error in errors {
            assert!(matches!(
                error,
                crate::common::MidgeError::InvalidArgument(_)
            ));
        }
    }

    #[test]
    fn should_set_otlp_config() {
        // Arrange
        let endpoint = "http://localhost:4317".to_string();

        // Act
        let config = TelemetryConfig::new().with_otlp(endpoint.clone());
        let otlp = config.otlp_config;

        // Assert
        assert!(otlp.is_some());
        assert_eq!(otlp.unwrap().endpoint, endpoint);
    }

    #[test]
    fn should_configure_feature_flags_independently() {
        // Arrange

        // Act
        let config = TelemetryConfig::new()
            .with_logging(false)
            .with_tracing(false)
            .with_metrics(false);

        // Assert
        assert!(!config.features.enable_logging);
        assert!(!config.features.enable_tracing);
        assert!(!config.features.enable_metrics);
    }
}
