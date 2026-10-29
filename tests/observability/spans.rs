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
    // Keep registrar aggregation active while an unscoped thread reaches the
    // same callsites. This second dispatcher is never installed as a default.
    let _other_registrar = tracing::Dispatch::new(tracing_subscriber::registry());

    // Act. Other tests in this binary hit these callsites first, with no or a
    // filtering subscriber, so their cached interest must be recomputed
    // against the scoped subscriber or a span can be skipped.
    tracing::subscriber::with_default(subscriber, || {
        tracing::callsite::rebuild_interest_cache();
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"k".to_vec(), b"v".to_vec(), None).unwrap();
        tx.commit(WriteOptions::buffered()).unwrap();
        // First reach the actual flush callsite on another thread while this
        // scoped recorder remains live, after the explicit cache rebuild.
        let unscoped_default = std::thread::scope(|workers| {
            workers
                .spawn(|| {
                    let no_subscriber = tracing::dispatcher::get_default(|dispatch| {
                        dispatch.is::<tracing::subscriber::NoSubscriber>()
                    });
                    engine.flush_cf(&cf).unwrap();
                    no_subscriber
                })
                .join()
                .unwrap()
        });
        eprintln!("unscoped flush registration: no_subscriber={unscoped_default}");
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
