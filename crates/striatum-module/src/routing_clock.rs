//! The routing clock used by handlers and refresh tasks.
//! Replay tests can pin this clock on their own thread without changing the clocks of other requests.

use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
thread_local! {
    /// The wall clock a test injected on this thread (see [`InjectedClock`]).
    pub(super) static INJECTED_NOW_MS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

pub(super) fn now_ms() -> u64 {
    #[cfg(test)]
    if let Some(now) = INJECTED_NOW_MS.with(std::cell::Cell::get) {
        return now;
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Holds this thread's [`now_ms`] at a fixed time until dropped, then restores
/// whatever clock was in force before. A test that replays frozen evidence
/// (quota readings, a vault listing) sets the time that evidence was captured
/// at, so freshness and reset checks answer the same on every run. The clock
/// is per thread: a `#[tokio::test]` runs its tasks on the test's own thread.
#[cfg(test)]
pub(crate) struct InjectedClock {
    pub(super) previous: Option<u64>,
}

#[cfg(test)]
impl InjectedClock {
    pub(crate) fn set(now_ms: u64) -> Self {
        Self {
            previous: INJECTED_NOW_MS.with(|clock| clock.replace(Some(now_ms))),
        }
    }
}

#[cfg(test)]
impl Drop for InjectedClock {
    fn drop(&mut self) {
        INJECTED_NOW_MS.with(|clock| clock.set(self.previous));
    }
}
