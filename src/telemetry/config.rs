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
pub struct TelemetryConfig {
    /// Enable/disable all telemetry
    pub enabled: bool,

    /// Enable structured logging
    pub enable_logging: bool,

    /// Enable tracing spans
    pub enable_tracing: bool,

    /// Enable metrics collection
    pub enable_metrics: bool,

    /// Sampling rate for traces (0.0..=1.0)
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
            enable_logging: true,
            enable_tracing: true,
            enable_metrics: true,
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

    /// Set trace sampling rate (0.0 = none, 1.0 = all)
    pub fn with_sample_rate(mut self, rate: f64) -> Self {
        self.trace_sample_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Set OTLP exporter configuration
    pub fn with_otlp(mut self, endpoint: String) -> Self {
        self.otlp_config = Some(OtlpConfig { endpoint });
        self
    }

    /// Enable/disable logging
    pub fn with_logging(mut self, enabled: bool) -> Self {
        self.enable_logging = enabled;
        self
    }

    /// Enable/disable tracing
    pub fn with_tracing(mut self, enabled: bool) -> Self {
        self.enable_tracing = enabled;
        self
    }

    /// Enable/disable metrics
    pub fn with_metrics(mut self, enabled: bool) -> Self {
        self.enable_metrics = enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_default_config() {
        let config = TelemetryConfig::default();
        assert_eq!(config.trace_sample_rate, 1.0);
        assert_eq!(config.service_name, "midge");
    }

    #[test]
    fn should_clamp_sample_rate() {
        // Arrange

        // Act
        let high = TelemetryConfig::new().with_sample_rate(1.5);
        let low = TelemetryConfig::new().with_sample_rate(-0.5);

        // Assert
        assert_eq!(high.trace_sample_rate, 1.0);
        assert_eq!(low.trace_sample_rate, 0.0);
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
}
