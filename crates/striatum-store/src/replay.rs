//! Test-only clock and ID inputs for replaying routing requests without masking
//! generated fields in the recorded replies or persisted state.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

thread_local! {
    static INPUTS: RefCell<Option<Inputs>> = const { RefCell::new(None) };
}

/// Clones share the ID sequence, including when a request moves to a blocking
/// thread. Separate replay cases start their own sequence at zero.
#[derive(Clone)]
pub struct Inputs {
    pub now_ms: i64,
    next: Arc<AtomicU64>,
    decisions: Arc<Mutex<VecDeque<String>>>,
}

impl Inputs {
    pub fn new(now_ms: i64) -> Self {
        Self {
            now_ms,
            next: Arc::new(AtomicU64::new(0)),
            decisions: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn install(&self) -> Guard {
        Guard(INPUTS.with(|inputs| inputs.replace(Some(self.clone()))))
    }

    /// An owner-supplied report can name a synthetic decision. Mint that exact
    /// identity during setup instead of rewriting the recorded request body.
    pub fn with_decisions(self, ids: impl IntoIterator<Item = String>) -> Self {
        self.decisions.lock().unwrap().extend(ids);
        self
    }

    pub(crate) fn decision_id(&self) -> Option<String> {
        self.decisions.lock().unwrap().pop_front()
    }

    pub fn next_id(&self) -> String {
        format!("{:012x}", self.next.fetch_add(1, Ordering::Relaxed))
    }
}

pub fn current() -> Option<Inputs> {
    INPUTS.with(|inputs| inputs.borrow().clone())
}

/// Restores the previous inputs, so nested fixtures and later tests do not
/// inherit a replay's clock or ID sequence.
pub struct Guard(Option<Inputs>);

impl Drop for Guard {
    fn drop(&mut self) {
        INPUTS.with(|inputs| inputs.replace(self.0.take()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_inputs_restore_nested_clocks_and_share_ids_across_threads() {
        assert!(current().is_none());
        let outer = Inputs::new(1_800_000_000_000);
        let outer = outer.with_decisions(["dec_owner_supplied".into()]);
        let guard = outer.install();
        assert_eq!(crate::now_ms(), 1_800_000_000_000);
        assert_eq!(crate::short_id(), "000000000000");
        assert_eq!(crate::new_decision_id(), "dec_owner_supplied");
        {
            let _nested = Inputs::new(42).install();
            assert_eq!(crate::now_ms(), 42);
            assert_eq!(crate::short_id(), "000000000000");
        }
        assert_eq!(crate::now_ms(), 1_800_000_000_000);
        let carried = current().unwrap();
        std::thread::spawn(move || {
            assert!(current().is_none());
            let _installed = carried.install();
            assert_eq!(crate::now_ms(), 1_800_000_000_000);
            assert_eq!(crate::short_id(), "000000000001");
        })
        .join()
        .unwrap();
        assert_eq!(crate::short_id(), "000000000002");
        drop(guard);
        assert!(current().is_none());
    }
}
