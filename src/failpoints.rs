//! Compile-time boundary for deterministic fault injection.
//!
//! Production and default builds do not link the `fail` crate. Call sites use
//! this adapter so the injection branches disappear unless the explicitly
//! non-production `failpoints` feature is enabled.

macro_rules! fail_point {
    ($name:expr) => {{
        #[cfg(feature = "failpoints")]
        {
            fail::fail_point!($name);
        }
    }};
    ($name:expr, $($rest:tt)*) => {{
        #[cfg(feature = "failpoints")]
        {
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

#[cfg(all(test, feature = "failpoints"))]
mod tests {
    fn injected_result() -> Result<(), &'static str> {
        super::fail_point!("midge::adapter::return_error", |_| Err("injected"));
        Ok(())
    }

    #[test]
    fn should_activate_adapter_when_failpoints_feature_is_enabled() {
        // Arrange
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
