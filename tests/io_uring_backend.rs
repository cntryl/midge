#![cfg(all(target_os = "linux", feature = "uring"))]

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use cntryl_midge::common::MidgeResult;
use cntryl_midge::sst::fs::FsSstFactoryIo;
use cntryl_midge::sst::traits::SstFactory;
use cntryl_midge::wal::fs::{FsWalReaderIo, FsWalWriterIo};
use cntryl_midge::wal::traits::{WalReader, WalWriter};
use cntryl_midge::wal::{WalOpKind, WalRecord};
use cntryl_midge::{Fs, FsDurability, FsOpenMode, FsOpenOptions, FsPath, UringFs};
use tempfile::TempDir;

#[test]
fn should_roundtrip_wal_through_io_uring_fs() -> MidgeResult<()> {
    let temp = TempDir::new().map_err(cntryl_midge::common::MidgeError::Io)?;
    let fs: Arc<dyn Fs> = Arc::new(UringFs::new(temp.path())?);

    let writer = FsWalWriterIo::new("wal.log", Arc::clone(&fs))?;
    let records = vec![
        WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"alpha"),
            Some(Bytes::from_static(b"one")),
            1,
            7,
        ),
        WalRecord::new(WalOpKind::Delete, Bytes::from_static(b"beta"), None, 2, 7),
    ];
    writer.append_batch(&records)?;
    writer.flush()?;
    drop(writer);

    let mut reader = FsWalReaderIo::open("wal.log", fs)?;
    let mut seen = Vec::new();
    reader.replay(0, |record| {
        seen.push((
            record.op,
            record.key.clone(),
            record.value.clone(),
            record.seq,
            record.writer_epoch,
        ));
        Ok(())
    })?;

    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].0, WalOpKind::Put);
    assert_eq!(seen[0].1, Bytes::from_static(b"alpha"));
    assert_eq!(seen[0].2, Some(Bytes::from_static(b"one")));
    assert_eq!(seen[0].3, 1);
    assert_eq!(seen[0].4, 7);
    assert_eq!(seen[1].0, WalOpKind::Delete);
    assert_eq!(seen[1].1, Bytes::from_static(b"beta"));
    assert_eq!(seen[1].2, None);
    assert_eq!(seen[1].3, 2);
    assert_eq!(seen[1].4, 7);

    Ok(())
}

#[test]
fn should_roundtrip_sst_through_io_uring_fs() -> MidgeResult<()> {
    let temp = TempDir::new().map_err(cntryl_midge::common::MidgeError::Io)?;
    let fs: Arc<dyn Fs> = Arc::new(UringFs::new(temp.path())?);
    let factory = FsSstFactoryIo::new(Arc::clone(&fs), 4096);

    let writer = factory.create()?;
    let mut writer = writer;
    writer.add_with_meta(b"alpha", Some(b"one"), 10, 0, None)?;
    writer.add_with_meta(b"beta", Some(b"two"), 11, 0, None)?;
    let bytes = writer.finish_bytes()?;

    let tmp_path = FsPath::new("table.sst.tmp");
    let final_path = FsPath::new("table.sst");

    let mut tmp = fs.open(
        &tmp_path,
        FsOpenOptions {
            mode: FsOpenMode::ReadWrite,
            create: true,
            create_new: false,
            truncate: true,
        },
    )?;
    tmp.write_at(0, Bytes::from(bytes.clone()))?;
    tmp.sync(FsDurability::Durable)?;
    drop(tmp);

    fs.rename_atomic(&tmp_path, &final_path)?;
    fs.sync_dir(&FsPath::new(""), FsDurability::Durable)?;

    let reader = factory.open(Path::new("table.sst"))?;
    let states = reader.scan_range_state(None, None)?;

    assert_eq!(states.len(), 2);
    assert_eq!(states[0].0, Bytes::from_static(b"alpha"));
    assert_eq!(states[1].0, Bytes::from_static(b"beta"));

    Ok(())
}
