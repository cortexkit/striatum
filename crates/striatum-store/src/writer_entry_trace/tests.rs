//! Test-only observations of the producer that actually ran, not labels supplied
//! by a command generator. The handle follows the injected test context across
//! handler and writer threads; unrelated tests have independent handles.

use std::cell::RefCell;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub name: &'static str,
    pub account_rows_written: Option<u64>,
}

#[derive(Default)]
struct State {
    rows: u64,
    entries: Vec<(Entry, u64)>,
}

#[derive(Clone, Default)]
pub struct Trace(Arc<Mutex<State>>);

thread_local! {
    static CURRENT: RefCell<Option<Trace>> = const { RefCell::new(None) };
}

pub fn current() -> Option<Trace> {
    CURRENT.with(|current| current.borrow().clone())
}

impl Trace {
    pub fn install(&self) -> Installed {
        Installed(CURRENT.with(|current| current.replace(Some(self.clone()))))
    }

    pub fn entries(&self) -> Vec<Entry> {
        self.0
            .lock()
            .unwrap()
            .entries
            .iter()
            .map(|(entry, _)| entry.clone())
            .collect()
    }
}

pub struct Installed(Option<Trace>);

impl Drop for Installed {
    fn drop(&mut self) {
        CURRENT.with(|current| *current.borrow_mut() = self.0.take());
    }
}

pub struct Entered(Option<(Trace, usize)>);

pub fn enter(name: &'static str) -> Entered {
    Entered(current().map(|trace| {
        let index = {
            let mut state = trace.0.lock().unwrap();
            let index = state.entries.len();
            let before = state.rows;
            state.entries.push((
                Entry {
                    name,
                    account_rows_written: None,
                },
                before,
            ));
            index
        };
        (trace, index)
    }))
}

impl Drop for Entered {
    fn drop(&mut self) {
        if let Some((trace, index)) = self.0.take() {
            let mut state = trace.0.lock().unwrap();
            let rows = state.rows - state.entries[index].1;
            state.entries[index].0.account_rows_written = Some(rows);
        }
    }
}

pub(crate) fn account_row_written() {
    if let Some(trace) = current() {
        trace.0.lock().unwrap().rows += 1;
    }
}

#[test]
fn nested_entry_scopes_observe_real_writes_and_restore_the_previous_trace() {
    assert!(current().is_none());
    let first = Trace::default();
    let first_install = first.install();
    let parent = enter("parent");
    account_row_written();
    let child = enter("child");
    account_row_written();
    drop(child);
    drop(parent);
    assert_eq!(
        first.entries(),
        vec![
            Entry {
                name: "parent",
                account_rows_written: Some(2)
            },
            Entry {
                name: "child",
                account_rows_written: Some(1)
            }
        ]
    );
    let second = Trace::default();
    let second_install = second.install();
    account_row_written();
    drop(second_install);
    let restored = enter("restored");
    account_row_written();
    drop(restored);
    assert_eq!(first.entries()[2].account_rows_written, Some(1));
    assert!(second.entries().is_empty());
    drop(first_install);
    assert!(current().is_none());
}
