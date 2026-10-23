//! Tracing spans on the engine API (#930). A scoped subscriber records the
//! spans opened on the calling thread, so this does not touch the process
//! global subscriber that another test in this binary installs.
use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::sync::{Arc, Mutex};
use tracing_subscriber::layer::SubscriberExt;

#[derive(Default)]
struct SpanNames(Arc<Mutex<Vec<&'static str>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SpanNames {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.0.lock().unwrap().push(attrs.metadata().name());
    }
}

#[test]
fn should_open_one_span_per_public_operation_given_scoped_subscriber_when_api_is_called() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(
        OpenOptions::local(dir.path())
            .background_compaction(false)
            .build()
            .unwrap(),
    )
    .unwrap();
    let cf = engine.create_column_family("spans").unwrap();
    let recorder = SpanNames::default();
    let names = Arc::clone(&recorder.0);
    let subscriber = tracing_subscriber::registry().with(recorder);

    // Act
    tracing::subscriber::with_default(subscriber, || {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"k".to_vec(), b"v".to_vec(), None).unwrap();
        tx.commit(WriteOptions::buffered()).unwrap();
        engine.flush_cf(&cf).unwrap();
        engine.compact_all().unwrap();
    });

    // Assert: each public operation opened its span on the caller's thread.
    let names = names.lock().unwrap();
    for expected in ["commit", "flush_cf", "compact_all"] {
        assert!(
            names.contains(&expected),
            "missing span {expected:?}; recorded: {names:?}"
        );
    }
}
