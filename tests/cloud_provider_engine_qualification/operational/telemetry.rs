//! Process-wide, bounded-cardinality aggregation for one-engine child campaigns.

use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[derive(Clone, Default)]
pub(super) struct Recorder(Arc<Mutex<Snapshot>>);

#[derive(Clone, Default, Serialize)]
pub(super) struct Snapshot {
    http: BTreeMap<String, BTreeMap<String, u64>>,
    recovery_phases: BTreeMap<String, BTreeMap<String, u64>>,
}

#[derive(Default)]
struct Fields {
    method: String,
    phase: String,
    range: bool,
    values: BTreeMap<String, u64>,
}

impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.values.insert(field.name().into(), value);
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "range" {
            self.range = value;
        } else {
            self.values.insert(field.name().into(), u64::from(value));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "method" => self.method = value.into(),
            "phase" => self.phase = value.into(),
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> Layer<S> for Recorder {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let mut snapshot = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let totals = if event.metadata().target() == "midge::cloud_io" {
            let method = if fields.range {
                format!("{}_range", fields.method)
            } else {
                fields.method
            };
            let status = fields.values.remove("status").unwrap_or(0);
            fields
                .values
                .insert("http_errors".into(), u64::from(status >= 400));
            snapshot.http.entry(method).or_default()
        } else {
            snapshot.recovery_phases.entry(fields.phase).or_default()
        };
        fields.values.insert("count".into(), 1);
        for (name, value) in fields.values {
            let current = totals.entry(name).or_default();
            *current = current.saturating_add(value);
        }
    }
}

impl Recorder {
    pub fn install() -> Self {
        let recorder = Self::default();
        let layer = recorder
            .clone()
            .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                matches!(metadata.target(), "midge::cloud_io" | "midge::recovery")
            }));
        tracing_subscriber::registry()
            .with(layer)
            .try_init()
            .expect("qualification tracing subscriber");
        recorder
    }

    pub fn snapshot(&self) -> Snapshot {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}
