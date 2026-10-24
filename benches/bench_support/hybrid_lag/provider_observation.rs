//! Counts existing native transport completion events within one engine window.
use super::Traffic;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tracing::field::{Field, Visit};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[derive(Default)]
pub(super) struct Counters {
    active: AtomicBool,
    requests: AtomicU64,
    puts: AtomicU64,
    bytes: AtomicU64,
    response_bytes: AtomicU64,
    transport_errors: AtomicU64,
    cancelled: AtomicU64,
    malformed: AtomicU64,
}

pub(super) struct Window(Arc<Counters>);
impl Drop for Window {
    fn drop(&mut self) {
        self.0.active.store(false, Ordering::Release);
    }
}

impl Counters {
    pub(super) fn begin(self: &Arc<Self>) -> Window {
        assert!(
            !self.active.swap(true, Ordering::AcqRel),
            "provider measurement windows must not overlap"
        );
        Window(self.clone())
    }
    pub(super) fn snapshot(&self) -> Traffic {
        Traffic {
            requests: self.requests.load(Ordering::Relaxed),
            puts: self.puts.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            response_bytes: self.response_bytes.load(Ordering::Relaxed),
            transport_errors: self.transport_errors.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
            malformed: self.malformed.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct Fields {
    method: Option<bool>,
    bytes: Option<u64>,
    response_bytes: Option<u64>,
    transport_error: Option<bool>,
    cancelled: Option<bool>,
}
impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "request_body_bytes" => self.bytes = Some(value),
            "response_body_bytes" => self.response_bytes = Some(value),
            _ => {}
        }
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        match field.name() {
            "transport_error" => self.transport_error = Some(value),
            "cancelled" => self.cancelled = Some(value),
            _ => {}
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "method" {
            self.method = Some(value == "PUT");
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

#[derive(Clone)]
struct CostLayer(Arc<Counters>);
impl<S: tracing::Subscriber> Layer<S> for CostLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "midge::cloud_io" || !self.0.active.load(Ordering::Acquire)
        {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0.requests.fetch_add(1, Ordering::Relaxed);
        self.0
            .puts
            .fetch_add(u64::from(fields.method.unwrap_or(false)), Ordering::Relaxed);
        self.0
            .bytes
            .fetch_add(fields.bytes.unwrap_or(0), Ordering::Relaxed);
        self.0
            .response_bytes
            .fetch_add(fields.response_bytes.unwrap_or(0), Ordering::Relaxed);
        self.0.transport_errors.fetch_add(
            u64::from(fields.transport_error.unwrap_or(false)),
            Ordering::Relaxed,
        );
        self.0.cancelled.fetch_add(
            u64::from(fields.cancelled.unwrap_or(true)),
            Ordering::Relaxed,
        );
        if fields.method.is_none()
            || fields.bytes.is_none()
            || fields.response_bytes.is_none()
            || fields.transport_error.is_none()
            || fields.cancelled.is_none()
        {
            self.0.malformed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(super) fn install() -> Arc<Counters> {
    static COUNTERS: OnceLock<Arc<Counters>> = OnceLock::new();
    COUNTERS
        .get_or_init(|| {
            let counters = Arc::new(Counters::default());
            let layer =
                CostLayer(counters.clone()).with_filter(tracing_subscriber::filter::filter_fn(
                    |metadata| metadata.target() == "midge::cloud_io",
                ));
            tracing_subscriber::registry()
                .with(layer)
                .try_init()
                .expect(
                    "provider cost subscriber unavailable; diagnostic cannot infer request counts",
                );
            counters
        })
        .clone()
}
