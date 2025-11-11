mod common;

#[test]
fn should_reject_config_given_memtable_size_exceeds_memory_budget_when_open_called() {
    panic!("TODO: implement test - invalid config rejected at startup");
}

#[test]
fn should_apply_new_cache_size_given_runtime_config_reload_when_requested() {
    panic!("TODO: implement test - supported settings update at runtime");
}

#[test]
fn should_not_restart_components_given_same_config_reapplied_when_reload() {
    panic!("TODO: implement test - config reapplication is idempotent");
}
