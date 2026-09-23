//! The manifest checkpoint horizon across the public API (#493).
//!
//! Every manifest persist compares the in-memory checkpoint horizon with the
//! durable snapshot's. If a journal writer appends without advancing the
//! horizon, every later persist takes the "stale caller" branch that is meant
//! for a rare race. That branch only logs, so the test counts its log lines.
//! It lives in its own binary because it installs a global subscriber: the
//! runtime thread does not see a thread-local one.

use cntryl_midge::{Engine, OpenOptions, TransactionMode, WriteOptions};
use std::io::Write as _;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

#[derive(Clone, Default)]
struct LogSink(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn logs() -> &'static LogSink {
    static SINK: OnceLock<LogSink> = OnceLock::new();
    SINK.get_or_init(|| {
        let sink = LogSink::default();
        tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::WARN)
            .with_writer(sink.clone())
            .init();
        sink
    })
}

fn stale_caller_lines() -> usize {
    let mut sink = logs().clone();
    sink.flush().expect("flush log sink");
    let bytes = sink
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    String::from_utf8_lossy(&bytes)
        .matches("stale caller")
        .count()
}

fn flush_and_compact(engine: &Engine, write: WriteOptions) {
    let cf = engine.create_column_family("data").expect("column family");
    for round in 0..3 {
        for index in 0..4 {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("transaction");
            tx.put(
                format!("key-{round}-{index}").into_bytes(),
                b"value".to_vec(),
                None,
            )
            .expect("put");
            tx.commit(write).expect("commit");
            engine.flush_cf(&cf).expect("flush");
        }
        engine.compact_all().expect("compact");
    }
}

#[test]
fn should_not_take_stale_caller_branch_when_compacting_after_flushes() {
    for mode in ["local", "cloud"] {
        // Arrange
        let directory = tempfile::tempdir().expect("database directory");
        let options = match mode {
            "local" => OpenOptions::local(directory.path()),
            _ => OpenOptions::cloud_simulated(directory.path(), "bucket", "checkpoint"),
        }
        .background_compaction(false)
        .build()
        .expect("options");
        let before = stale_caller_lines();
        let mut engine = Engine::open(options).expect("open");

        // Act
        let write = if mode == "local" {
            WriteOptions::sync()
        } else {
            WriteOptions::cloud_async()
        };
        flush_and_compact(&engine, write);
        engine.shutdown(Duration::from_secs(30)).expect("shutdown");

        // Assert
        let stale = stale_caller_lines() - before;
        assert_eq!(
            stale, 0,
            "{mode} mode took the stale-caller branch {stale} times"
        );
    }
}
