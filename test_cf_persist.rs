// Quick test to debug CF persistence
use cntryl_midge::{MidgeEngine, testkit::*};

fn main() {
    let opts = opts_for_mode("local");
    
    println!("Phase 1: Creating CF");
    {
        let engine = open_with_mode(opts.clone(), "local");
        println!("  Created engine, creating CF...");
        engine.create_column_family("test_cf").unwrap();
        println!("  CF created");
        let cfs = engine.list_column_families().unwrap();
        println!("  CFs before restart: {}", cfs.len());
        for cf in &cfs {
            println!("    - {}", cf.name());
        }
    }
    
    println!("\nPhase 2: Reopening");
    {
        let engine = open_with_mode(opts, "local");
        let cfs = engine.list_column_families().unwrap();
        println!("  CFs after restart: {}", cfs.len());
        for cf in &cfs {
            println!("    - {}", cf.name());
        }
        
        let names: Vec<&str> = cfs.iter().map(|cf| cf.name()).collect();
        if names.contains(&"test_cf") {
            println!("\n✓ SUCCESS: test_cf persisted!");
        } else {
            println!("\n✗ FAILURE: test_cf not found!");
        }
    }
}
