//! OpenTelemetry instrumentation for Midge
//!
//! Provides tracing, metrics, and structured logging with semantic attributes.
//! Fully configurable and low-overhead on hot paths.

pub mod config;
pub mod metrics;
pub mod spans;

pub use config::TelemetryConfig;
pub use metrics::Metrics;
pub use spans::MidgeSpan;

use std::sync::Arc;
use std::sync::OnceLock;

/// Global telemetry instance
static TELEMETRY: OnceLock<Option<Arc<Telemetry>>> = OnceLock::new();

/// Central telemetry coordinator
pub struct Telemetry {
    #[allow(dead_code)]
    config: TelemetryConfig,
    metrics: Metrics,
    enabled: bool,
}

impl Telemetry {
    /// Initialize global telemetry
    pub fn init(config: TelemetryConfig) -> crate::common::MidgeResult<()> {
        let enabled = config.enabled;
        let metrics = Metrics::new(&config)?;

        #[cfg(feature = "telemetry")]
        if enabled {
            Self::setup_tracing(&config)?;
        }

        let telemetry = Arc::new(Telemetry {
            config,
            metrics,
            enabled,
        });

        TELEMETRY
            .set(if enabled { Some(telemetry) } else { None })
            .map_err(|_| {
                crate::common::MidgeError::Internal("Telemetry already initialized".to_string())
            })?;

        Ok(())
    }

    /// Get global telemetry instance (if enabled)
    pub fn global() -> Option<Arc<Telemetry>> {
        TELEMETRY.get_or_init(|| None).clone()
    }

    /// Check if telemetry is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get metrics collector
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    #[cfg(feature = "telemetry")]
    #[allow(dead_code)]
    fn setup_tracing(_config: &TelemetryConfig) -> crate::common::MidgeResult<()> {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        use tracing_subscriber::Layer;

        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            );

        let registry = tracing_subscriber::registry().with(fmt_layer);

        #[cfg(feature = "telemetry-otlp")]
        let registry = if let Some(otel_config) = &config.otlp_config {
            let tracer = opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint(&otel_config.endpoint),
                )
                .install_batch(opentelemetry::runtime::Tokio)
                .map_err(|e| {
                    crate::common::MidgeError::Internal(format!(
                        "Failed to initialize OTLP tracer: {}",
                        e
                    ))
                })?;

            registry.with(tracing_opentelemetry::layer().with_tracer(tracer))
        } else {
            #[allow(unreachable_code)]
            registry
        };

        registry.init();
        Ok(())
    }

    #[cfg(not(feature = "telemetry"))]
    #[allow(dead_code)]
    fn setup_tracing(_config: &TelemetryConfig) -> crate::common::MidgeResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_initialize_telemetry_when_enabled() {
        let config = TelemetryConfig::default().with_enabled(true);
        assert!(config.enabled);
    }

    #[test]
    fn should_support_disabled_telemetry() {
        let config = TelemetryConfig::default().with_enabled(false);
        assert!(!config.enabled);
    }
}
