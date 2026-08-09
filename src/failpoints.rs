//! Compile-time boundary for deterministic fault injection.
//!
//! Production and default builds do not link the `fail` crate. Call sites use
//! this adapter so the injection branches disappear unless the explicitly
//! non-production `failpoints` feature is enabled.

macro_rules! fail_point {
    ($name:expr) => {{
        #[cfg(feature = "failpoints")]
        {
            let _midge_failpoint_read_guard = $crate::failpoints::read_gate();
            fail::fail_point!($name);
        }
    }};
    ($name:expr, $($rest:tt)*) => {{
        #[cfg(feature = "failpoints")]
        {
            let _midge_failpoint_read_guard = $crate::failpoints::read_gate();
            fail::fail_point!($name, $($rest)*);
        }
    }};
}

pub(crate) use fail_point;

/// Return whether a boolean failpoint is active.
#[inline]
pub(crate) fn is_active(name: &str) -> bool {
    #[cfg(feature = "failpoints")]
    {
        fail::eval(name, |_| true).unwrap_or(false)
    }

    #[cfg(not(feature = "failpoints"))]
    {
        let _ = name;
        false
    }
}

#[cfg(feature = "failpoints")]
use std::cell::Cell;
#[cfg(all(test, feature = "failpoints"))]
use std::sync::RwLockWriteGuard;
#[cfg(feature = "failpoints")]
use std::sync::{OnceLock, RwLock, RwLockReadGuard};

#[cfg(feature = "failpoints")]
static FAILPOINT_GATE: OnceLock<RwLock<()>> = OnceLock::new();

#[cfg(feature = "failpoints")]
thread_local! {
    static OWNS_FAILPOINT_GATE: Cell<bool> = const { Cell::new(false) };
    static FAILPOINT_READ_SCOPE_DEPTH: Cell<u32> = const { Cell::new(0) };
}

#[cfg(feature = "failpoints")]
fn gate() -> &'static RwLock<()> {
    FAILPOINT_GATE.get_or_init(|| RwLock::new(()))
}

/// Read-side gate held while production code evaluates a failpoint.
///
/// Failpoint configuration is process-global. The test guard below takes the
/// write side so unrelated tests cannot observe a configured failure while a
/// fault-injection scenario is running. The owner thread skips the read lock
/// because it already holds the write side.
#[cfg(feature = "failpoints")]
pub(crate) fn read_gate() -> Option<RwLockReadGuard<'static, ()>> {
    if OWNS_FAILPOINT_GATE.with(Cell::get) || FAILPOINT_READ_SCOPE_DEPTH.with(Cell::get) > 0 {
        None
    } else {
        Some(
            gate()
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

#[cfg(feature = "failpoints")]
struct FailpointReadScope;

#[cfg(feature = "failpoints")]
impl FailpointReadScope {
    fn enter() -> Self {
        FAILPOINT_READ_SCOPE_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }
}

#[cfg(feature = "failpoints")]
impl Drop for FailpointReadScope {
    fn drop(&mut self) {
        FAILPOINT_READ_SCOPE_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// Keep a multi-step operation inside one failpoint read-side scope.
///
/// This establishes a consistent lock order for operations that also acquire
/// subsystem mutexes. Individual failpoint evaluations inside the scope reuse
/// the outer read lock instead of recursively acquiring the global gate.
pub(crate) fn with_read_gate<T>(operation: impl FnOnce() -> T) -> T {
    #[cfg(feature = "failpoints")]
    {
        if OWNS_FAILPOINT_GATE.with(Cell::get) || FAILPOINT_READ_SCOPE_DEPTH.with(Cell::get) > 0 {
            return operation();
        }
        let _guard = gate()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _scope = FailpointReadScope::enter();
        operation()
    }

    #[cfg(not(feature = "failpoints"))]
    {
        operation()
    }
}

/// Guard used by failpoint tests to isolate process-global configurations.
#[cfg(all(test, feature = "failpoints"))]
pub(crate) struct TestFailpointGuard {
    guard: Option<RwLockWriteGuard<'static, ()>>,
}

#[cfg(all(test, feature = "failpoints"))]
impl Drop for TestFailpointGuard {
    fn drop(&mut self) {
        self.guard.take();
        OWNS_FAILPOINT_GATE.with(|owner| owner.set(false));
    }
}

/// Enter an isolated failpoint test scope.
#[cfg(all(test, feature = "failpoints"))]
pub(crate) fn test_failpoint_guard() -> TestFailpointGuard {
    let guard = gate()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    OWNS_FAILPOINT_GATE.with(|owner| owner.set(true));
    TestFailpointGuard { guard: Some(guard) }
}

#[cfg(all(test, feature = "failpoints"))]
mod tests {
    fn injected_result() -> Result<(), &'static str> {
        super::fail_point!("midge::adapter::return_error", |_| Err("injected"));
        Ok(())
    }

    #[test]
    fn should_activate_adapter_paths_given_configured_failpoints() {
        // Arrange
        let _test_guard = super::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::adapter::boolean", "return").expect("configure boolean failpoint");
        fail::cfg("midge::adapter::return_error", "return").expect("configure result failpoint");

        // Act
        let active = super::is_active("midge::adapter::boolean");
        let result = injected_result();

        // Assert
        assert!(active);
        assert_eq!(result, Err("injected"));
        fail::remove("midge::adapter::boolean");
        fail::remove("midge::adapter::return_error");
        scenario.teardown();
    }
}
