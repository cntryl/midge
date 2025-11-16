#[test]
#[ignore]
fn debug_sst_open() {
    use cntryl_midge::{sst::MemSstFactory, sst::fs::SstFile, sst::SstFactory};
    use cntryl_midge::sst::traits::SstStateReader;
    use std::fs;
    let factory = MemSstFactory; 
    let mut writer = factory.create(cntryl_midge::common::codec::CompressionType::None, 4096, false);
    writer.add(b"a", b"A").unwrap();
    writer.add(b"b", b"B").unwrap();
    writer.add(b"c", b"C").unwrap();
    let bytes = writer.finish_bytes().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("test.sst");
    fs::write(&path, &bytes).unwrap();
    let sstfile = SstFile::open(&path).unwrap();
    let all = sstfile.scan_range_state(None, None).unwrap();
    let keys: Vec<String> = all.into_iter().map(|(k, _)| String::from_utf8_lossy(&k).to_string()).collect();
    println!("keys from fs reader: {:?}", keys);
}
