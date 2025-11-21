use cntryl_midge::sst::mem::SstMemWriter;
use cntryl_midge::common::codec::CompressionType;

let mut writer = SstMemWriter::new_with_internal(CompressionType::None, 4096, true);
let keys: Vec<String> = (0..10).map(|i| format!(\"key_{:06}\", i)).collect();
let value = b\"value\";

for i in 0..10 {
    println!(\"Adding key {}: {}\", i, keys[i]);
    writer.add_with_meta(keys[i].as_bytes(), Some(value), i as u64, false, None).unwrap();
}

let bytes = writer.finish_bytes().unwrap();
println!(\"Finished! {} bytes\", bytes.len());
