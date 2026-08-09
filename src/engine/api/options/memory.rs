#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::path::Path;

pub fn auto_memory_budget_bytes() -> Option<usize> {
    let limit = effective_memory_limit_bytes()?;
    Some(budget_from_limit_bytes(limit))
}

fn budget_from_limit_bytes(limit: u64) -> usize {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut budget = limit / 2;
    let min_budget: usize = 64 * 1024 * 1024;
    if limit >= min_budget.saturating_mul(2) {
        budget = budget.max(min_budget);
    } else {
        budget = budget.max(1024 * 1024);
    }
    budget.min(limit).max(1)
}

fn effective_memory_limit_bytes() -> Option<u64> {
    let host_total = host_total_memory_bytes();
    let cgroup_limit = cgroup_memory_limit_bytes();

    match (host_total, cgroup_limit) {
        (Some(host), Some(limit)) => Some(host.min(limit)),
        (Some(host), None) => Some(host),
        (None, Some(limit)) => Some(limit),
        (None, None) => None,
    }
}

fn host_total_memory_bytes() -> Option<u64> {
    let mut system = sysinfo::System::new();
    system.refresh_memory();
    let total_bytes = system.total_memory();
    if total_bytes == 0 {
        return None;
    }
    Some(total_bytes)
}

#[cfg(target_os = "linux")]
fn cgroup_memory_limit_bytes() -> Option<u64> {
    let v2_limit = cgroup_v2_limit_bytes();
    if v2_limit.is_some() {
        return v2_limit;
    }
    cgroup_v1_limit_bytes()
}

#[cfg(not(target_os = "linux"))]
fn cgroup_memory_limit_bytes() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn cgroup_v2_limit_bytes() -> Option<u64> {
    let controllers = Path::new("/sys/fs/cgroup/cgroup.controllers");
    if !controllers.exists() {
        return None;
    }
    let max_path = Path::new("/sys/fs/cgroup/memory.max");
    let value = fs::read_to_string(max_path).ok()?;
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("max") {
        return None;
    }
    trimmed.parse::<u64>().ok().filter(|v| *v > 0)
}

#[cfg(target_os = "linux")]
fn cgroup_v1_limit_bytes() -> Option<u64> {
    let max_path = Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    let value = fs::read_to_string(max_path).ok()?;
    let limit = value.trim().parse::<u64>().ok()?;
    if limit == 0 {
        return None;
    }
    Some(limit)
}

#[cfg(test)]
mod tests {
    use super::host_total_memory_bytes;

    #[test]
    fn should_treat_sysinfo_total_memory_as_bytes() {
        // Arrange
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        let expected_bytes = system.total_memory();

        // Act
        let detected_bytes = host_total_memory_bytes();

        // Assert
        if expected_bytes == 0 {
            assert_eq!(detected_bytes, None);
        } else {
            assert_eq!(detected_bytes, Some(expected_bytes));
        }
    }
}
