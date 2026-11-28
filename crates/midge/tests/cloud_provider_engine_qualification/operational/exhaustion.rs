//! External acknowledgment ledger for kernel disk-exhaustion qualification.

use super::fixture::{self, Campaign};
use cntryl_midge::{Engine, Query, TransactionMode, WriteOptions};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

const FAMILY: &str = "resource-exhaustion";

#[derive(Default, Serialize, Deserialize)]
struct Ledger {
    accepted: u64,
    attempted: u64,
}

fn save(campaign: &Campaign, ledger: &Ledger) {
    std::fs::write(
        campaign.artifacts.join("exhaustion-ledger.json"),
        serde_json::to_vec(ledger).unwrap(),
    )
    .expect("external acknowledgment ledger");
}

pub(super) fn prepare(engine: &Engine) {
    engine
        .create_column_family(FAMILY)
        .expect("exhaustion family");
}

pub(super) fn exercise(mut engine: Engine, campaign: &Campaign) {
    std::fs::write(
        campaign.artifacts.join("ready-for-disk-exhaustion"),
        b"opened",
    )
    .expect("exhaustion barrier");
    let deadline = Instant::now() + Duration::from_secs(campaign.profile.timeout_seconds);
    while !campaign.artifacts.join("disk-filled").exists() {
        assert!(
            Instant::now() < deadline,
            "external disk filler did not reach its barrier"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let cf = engine.get_column_family(FAMILY).expect("exhaustion family");
    let mut ledger = Ledger::default();
    let failure = loop {
        assert!(
            Instant::now() < deadline && ledger.attempted < 10_000,
            "disk exhaustion did not produce bounded backpressure or an explicit error"
        );
        let index = ledger.attempted;
        ledger.attempted += 1;
        save(campaign, &ledger);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("exhaustion transaction");
        tx.put(
            index.to_be_bytes().to_vec(),
            fixture::value(index, 2048),
            None,
        )
        .expect("exhaustion value");
        match tx.commit(WriteOptions::cloud_strict()) {
            Ok(()) => {
                ledger.accepted += 1;
                save(campaign, &ledger);
            }
            Err(error) => break error,
        }
    };
    assert!(
        matches!(
            failure,
            cntryl_midge::MidgeError::NoSpace(_) | cntryl_midge::MidgeError::WriteStall(_)
        ) || failure.to_string().contains("No space left on device"),
        "unexpected exhaustion outcome: {failure}"
    );
    let shutdown = engine.shutdown(Duration::from_secs(5));
    std::fs::write(
        campaign.artifacts.join("disk-exhausted-observed"),
        format!("write: {failure}\nshutdown: {shutdown:?}\n"),
    )
    .expect("exhaustion evidence");
}

pub(super) fn verify(engine: &Engine, campaign: &Campaign) {
    let ledger_path = campaign.artifacts.join("exhaustion-ledger.json");
    let ledger = if ledger_path.exists() {
        serde_json::from_slice::<Ledger>(&std::fs::read(ledger_path).expect("external ledger"))
            .expect("ledger JSON")
    } else {
        Ledger::default()
    };
    let cf = engine
        .get_column_family(FAMILY)
        .expect("persisted exhaustion family");
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("exhaustion recovery snapshot");
    let mut observed = 0_u64;
    for entry in tx.scan(&Query::new()).expect("complete exhaustion keyset") {
        let (key, value) = entry.expect("exhaustion recovered entry");
        assert_eq!(key.as_ref(), observed.to_be_bytes());
        assert_eq!(value.as_ref(), fixture::value(observed, 2048));
        observed += 1;
    }
    // An operation whose error raced durability can also be present. Every ACK
    // must survive, and no unknown key/value is admitted by this oracle.
    assert!(observed >= ledger.accepted && observed <= ledger.attempted);
}
