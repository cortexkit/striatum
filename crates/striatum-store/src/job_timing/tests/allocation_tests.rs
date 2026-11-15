//! Count Rust allocations, not SQLite's C allocator, on the request thread.

use super::*;
use std::alloc::{GlobalAlloc, Layout, System};

struct CountingAllocator;
thread_local! {
    static COUNT: Cell<Option<usize>> = const { Cell::new(None) };
}

fn allocated() {
    let _ = COUNT.try_with(|count| {
        if let Some(n) = count.get() {
            count.set(Some(n + 1));
        }
    });
}

// SAFETY: every operation delegates unchanged pointers/layouts to System;
// accounting uses only a thread-local Cell and never allocates or panics.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        allocated();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        allocated();
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn count(f: impl FnOnce()) -> usize {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            COUNT.with(|count| count.set(None));
        }
    }
    COUNT.with(|count| assert!(count.replace(Some(0)).is_none()));
    let _reset = Reset;
    f();
    COUNT.with(|count| count.get().unwrap())
}

#[test]
fn warmed_request_writer_telemetry_adds_no_allocations() {
    use striatum_core::RouteStateStore;
    let scratch = cortexkit_test_support::ScratchDir::new("job-allocations");
    let store = crate::SqliteRouteStateStore::open(&crate::dev_descriptor(
        "job-allocations",
        scratch.join("store.db").to_string_lossy(),
    ))
    .unwrap();
    let id = store
        .record_decision(&striatum_core::DecisionInput::default())
        .unwrap();
    let update = striatum_core::DecisionOutcomeUpdate {
        provider_outcome: None,
        outcome: striatum_core::RouteOutcome::Completed,
        last_error: None,
        observed_at_ms: 2000,
        failure_backoff: Default::default(),
    };
    let request_op = || store.set_decision_outcome(&id, &update).unwrap();
    // Both modes see the same prepared statements and warmed pages.
    request_op();
    WRITER_TELEMETRY.with(|enabled| enabled.set(false));
    let baseline = count(request_op);
    WRITER_TELEMETRY.with(|enabled| enabled.set(true));
    let measured = count(request_op);
    assert_eq!(
        measured, baseline,
        "writer telemetry allocated on a fast request"
    );
    assert!(
        count(|| {
            std::hint::black_box(Box::new(42));
        }) > 0,
        "allocator control must detect allocation"
    );
    eprintln!(
        "warmed set_decision_outcome Rust allocations: bypass={baseline} telemetry={measured}"
    );
}

#[test]
fn fast_job_and_fixed_batch_labels_allocate_nothing() {
    let allocations = count(|| {
        let mut labels = [None; 32];
        labels[0] = Some("observe");
        labels[31] = Some("quota_feed");
        let _batch = BatchScope::enter(labels);
        sync("fast_fake", || {
            writer_held(Duration::from_micros(1), Location::caller());
            rows(3);
        });
    });
    assert_eq!(allocations, 0);
}
