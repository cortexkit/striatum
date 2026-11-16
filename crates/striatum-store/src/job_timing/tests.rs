use super::*;

mod allocation_tests;

fn capture(f: impl FnOnce()) -> Vec<String> {
    LOG_LINES.with(|lines| *lines.borrow_mut() = Some(Vec::new()));
    f();
    LOG_LINES.with(|lines| lines.borrow_mut().take().unwrap())
}

fn assert_pass(line: &str, name: &str) {
    assert!(
        line.starts_with(&format!("[routing] slow job job={name} total_ms=")),
        "{line}"
    );
    for field in ["cpu_ms=7.000", "writer_hold_ms=", "rows=13"] {
        assert!(line.contains(field), "missing {field}: {line}");
    }
    assert_eq!(line.lines().count(), 1);
}

#[test]
fn slow_pass_total_threshold_logs_exactly_one_line() {
    let work = Work {
        cpu_ns: 7_000_000,
        ..Work::default()
    };
    let lines = capture(|| emit_pass("fake_total", Duration::from_micros(250_001), work, Some(13)));
    assert_eq!(lines.len(), 1, "a slow pass must emit exactly once");
    assert_pass(&lines[0], "fake_total");
    assert!(lines[0].contains("total_ms=250.001"));
    assert!(lines[0].contains("writer_hold_ms=0.000"));
}

#[test]
fn slow_pass_writer_threshold_logs_exactly_one_line() {
    let work = Work {
        cpu_ns: 7_000_000,
        writer_ns: 50_001_000,
        ..Work::default()
    };
    let lines = capture(|| emit_pass("fake_writer", Duration::from_millis(60), work, Some(13)));
    assert_eq!(lines.len(), 1, "a writer-heavy pass must emit exactly once");
    assert_pass(&lines[0], "fake_writer");
    assert!(lines[0].contains("writer_hold_ms=50.001"));
}

#[test]
fn fast_pass_and_exact_thresholds_log_nothing() {
    assert!(capture(|| {
        emit_pass("fast", Duration::from_millis(1), Work::default(), None);
        emit_pass(
            "boundary",
            SLOW_PASS,
            Work {
                writer_ns: 50_000_000,
                ..Work::default()
            },
            None,
        );
        writer_held(SLOW_WRITER, Location::caller());
    })
    .is_empty());
}

#[test]
fn slow_writer_names_holder_through_existing_acquisition_helper() {
    let scratch = cortexkit_test_support::ScratchDir::new("slow-writer-holder");
    let store = crate::SqliteRouteStateStore::open(&crate::dev_descriptor(
        "slow-writer-holder",
        scratch.join("store.db").to_string_lossy(),
    ))
    .unwrap();
    let lines = capture(|| {
        let _scope = Scope::enter(Some("retention.test"));
        store
            .store
            .with_conn(|_| {
                std::thread::sleep(Duration::from_millis(120));
                Ok(())
            })
            .unwrap();
    });
    assert_eq!(lines.len(), 1, "the acquisition helper must emit once");
    assert!(
        lines[0].starts_with("[routing] slow writer holder=retention.test writer_hold_ms="),
        "{:?}",
        lines
    );
    let held: f64 = lines[0]
        .split("writer_hold_ms=")
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    assert!(held >= 100.0);
}

#[test]
fn batch_holder_names_all_static_ops_and_unlabelled_writer_has_source() {
    let mut labels = [None; 32];
    labels[0] = Some("catalog_apply");
    labels[1] = Some("observe");
    let lines = capture(|| {
        {
            let _batch = BatchScope::enter(labels);
            writer_held(Duration::from_millis(101), Location::caller());
        }
        writer_held(Duration::from_millis(101), Location::caller());
    });
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("holder=account_batch[catalog_apply,observe]"));
    assert!(lines[1].contains("holder=crates/striatum-store/src/job_timing/tests.rs:"));
}

#[test]
fn blocking_and_account_writer_counters_return_to_the_named_pass() {
    let scratch = cortexkit_test_support::ScratchDir::new("job-account-counters");
    let store = crate::SqliteRouteStateStore::open(&crate::dev_descriptor(
        "job-account-counters",
        scratch.join("store.db").to_string_lossy(),
    ))
    .unwrap();
    let scope = Scope::enter(Some("quota_feed"));
    let worker = std::thread::spawn(blocking(move || {
        store
            .account_command(crate::AccountCommandKind::QuotaDelta, |_| {
                assert_eq!(BATCH.with(Cell::get)[0], Some("quota_feed"));
                let started = Instant::now();
                while started.elapsed() < Duration::from_millis(4) {
                    std::hint::spin_loop();
                }
                Ok(())
            })
            .unwrap();
    }));
    let (_, work) = worker.join().unwrap();
    record(work);
    assert!(
        scope.work().cpu_ns >= 1_000_000,
        "writer CPU must reach the job"
    );
    assert!(
        scope.work().writer_ns >= 4_000_000,
        "writer hold must reach the job"
    );
}

#[test]
fn future_scope_restores_label_on_pending_ready_panic_and_cancel() {
    use std::{
        pin::pin,
        task::{Context as TaskContext, Poll, Waker},
    };
    let outer = Scope::enter(Some("outer"));
    let mut polls = 0;
    let mut pass = pin!(run(
        "inner",
        std::future::poll_fn(|_| {
            assert_eq!(label(), Some("inner"));
            record(Work {
                cpu_ns: 1_000_000,
                writer_ns: 2_000_000,
                rows: Some(1),
            });
            polls += 1;
            if polls == 1 {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
    ));
    let mut cx = TaskContext::from_waker(Waker::noop());
    assert!(pass.as_mut().poll(&mut cx).is_pending());
    assert_eq!(label(), Some("outer"));
    assert!(pass.as_mut().poll(&mut cx).is_ready());
    assert_eq!(outer.work().writer_ns, 4_000_000);
    assert_eq!(outer.work().cpu_ns, 2_000_000);
    assert_eq!(outer.work().rows, Some(2));
    let mut panics = pin!(run("panic", async { panic!("fake pass") }));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| panics
            .as_mut()
            .poll(&mut cx)))
        .is_err()
    );
    assert_eq!(label(), Some("outer"));
    {
        let mut cancelled = pin!(run("cancelled", std::future::pending::<()>()));
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(label(), Some("outer"));
}
