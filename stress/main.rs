mod harness;
mod ycsb;
mod soak;

use harness::{Config, run};

fn main() {
    let cfg = Config::from_env_or_args();

    if let Err(e) = run(&cfg.workload, &cfg) {
        eprintln!("stress harness failed: {:#}", e);
        std::process::exit(1);
    }
}
