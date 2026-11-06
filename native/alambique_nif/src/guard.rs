//! Panic barrier for code that C calls.
//!
//! A panic must not unwind into SQLite or ERTS: since Rust 1.81 it aborts the
//! process when it leaves an `extern "C"` function, which stops the whole VM.
//! Every callback runs its body through `guard`, which turns a panic into the
//! callback's error value. The same applies to `Drop` of NIF resources, which
//! ERTS runs from a C callback that rustler does not protect.

use std::panic::{catch_unwind, AssertUnwindSafe};

/// Runs `f`; if it panics, returns `on_panic`. `site` names the callback for
/// the panic probe of the tests (`test` and `test-panic` builds only).
#[inline]
pub fn guard<T>(site: &'static str, on_panic: T, f: impl FnOnce() -> T) -> T {
    catch_unwind(AssertUnwindSafe(|| {
        #[cfg(any(test, feature = "test-panic"))]
        probe::hit(site);
        #[cfg(not(any(test, feature = "test-panic")))]
        let _ = site;
        f()
    }))
    .unwrap_or(on_panic)
}

/// Test hook: makes `guard` panic once at one named site on the current
/// thread.
#[cfg(any(test, feature = "test-panic"))]
pub mod probe {
    use std::cell::Cell;

    thread_local! {
        static SITE: Cell<Option<&'static str>> = const { Cell::new(None) };
    }

    pub(super) fn hit(site: &'static str) {
        if SITE.get() == Some(site) {
            SITE.set(None);
            panic!("injected panic at {site}");
        }
    }

    /// Panics at `site` on its next hit, or never if the guard is dropped
    /// first.
    pub fn panic_at(site: &'static str) -> impl Drop {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                SITE.set(None);
            }
        }
        SITE.set(Some(site));
        Reset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_panic_becomes_the_fallback_value() {
        assert_eq!(guard("t", -1, || panic!("boom")), -1);
        assert_eq!(guard("t", -1, || 7), 7);
    }

    #[test]
    fn the_probe_fires_only_at_its_site() {
        let _p = probe::panic_at("a");
        assert_eq!(guard("b", -1, || 7), 7);
        assert_eq!(guard("a", -1, || 7), -1);
        assert_eq!(guard("a", -1, || 7), 7);
    }
}
