use cntryl_stress::StressContext;

pub fn measure_hot_path_batch(
    ctx: &mut StressContext,
    name: impl Into<String>,
    operations: u64,
    f: impl FnMut(),
) {
    let _completed = ctx.measure_batch(name, operations, f);
}

pub fn mark_validated_micro(ctx: &mut StressContext, unit: &'static str) {
    ctx.parameter("logical_unit", unit);
    ctx.metadata("validated_micro", "true");
}

pub fn mark_local_rsd_diagnostic(ctx: &mut StressContext) {
    ctx.metadata("diagnostic_reason", "local_rsd_above_5pct");
    ctx.parameter("local_gate_rsd_limit_pct", 5);
}
