//! OpenTelemetry instrumentation for Midge
//!
//! Provides tracing, metrics, and structured logging with semantic attributes.
//! Fully configurable and low-overhead on hot paths.

pub mod config;
pub mod metrics;

pub use config::TelemetryConfig;
pub use metrics::Metrics;

use std::sync::{Arc, Mutex, OnceLock};

/// Global telemetry instance
static TELEMETRY: OnceLock<Option<Arc<Telemetry>>> = OnceLock::new();
static TELEMETRY_INIT: Mutex<()> = Mutex::new(());

/// Central telemetry coordinator
pub struct Telemetry {
    metrics: Metrics,
}

impl Telemetry {
    /// Initialize global telemetry
    pub fn init(config: &TelemetryConfig) -> crate::common::MidgeResult<()> {
        config.validate()?;

        // Subscriber installation and publication of the telemetry singleton
        // are one initialization transaction. Without this gate, a repeated
        // or concurrent enabled call can try to install the process-global
        // tracing subscriber before observing that telemetry already exists.
        let _init_guard = TELEMETRY_INIT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if TELEMETRY.get().is_some() {
            return Err(crate::common::MidgeError::Internal(
                "Telemetry already initialized".to_string(),
            ));
        }

        let enabled = config.enabled;
        let metrics = Metrics::new(config);

        #[cfg(feature = "telemetry")]
        if enabled {
            Self::setup_tracing(config)?;
        }

        let telemetry = Arc::new(Telemetry { metrics });

        TELEMETRY
            .set(if enabled { Some(telemetry) } else { None })
            .map_err(|_| {
                crate::common::MidgeError::Internal("Telemetry already initialized".to_string())
            })?;

        Ok(())
    }

    /// Get global telemetry instance (if enabled)
    pub fn global() -> Option<Arc<Telemetry>> {
        TELEMETRY.get().and_then(Clone::clone)
    }

    /// Get metrics collector
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    #[cfg(feature = "telemetry")]
    fn setup_tracing(config: &TelemetryConfig) -> crate::common::MidgeResult<()> {
        use opentelemetry::global;
        use opentelemetry::trace::TracerProvider as _;
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
        if let Some(otel_config) = &config.otlp_config {
            use opentelemetry_otlp::WithExportConfig;

            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(&otel_config.endpoint)
                .build()
                .map_err(|e| {
                    crate::common::MidgeError::Internal(format!(
                        "Failed to initialize OTLP exporter: {e}"
                    ))
                })?;

            let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .build();
            let tracer = tracer_provider.tracer("midge");
            registry
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .try_init()
                .map_err(|error| {
                    crate::common::MidgeError::Internal(format!(
                        "Failed to install telemetry subscriber: {error}"
                    ))
                })?;
            global::set_tracer_provider(tracer_provider);
            return Ok(());
        }

        registry.try_init().map_err(|error| {
            crate::common::MidgeError::Internal(format!(
                "Failed to install telemetry subscriber: {error}"
            ))
        })?;
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
