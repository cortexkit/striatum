//! Slow background passes and writer attribution, without statement tracing.
//!
//! CPU is CLOCK_THREAD_CPUTIME_ID on Linux/macOS, sampled around each future
//! poll (never across an await). Blocking work and account-writer replies carry
//! their counters back to the polling scope. CPU excludes time asleep or queued.

use std::{
    cell::Cell,
    future::Future,
    panic::Location,
    time::{Duration, Instant},
};

const SLOW_PASS: Duration = Duration::from_millis(250);
const SLOW_PASS_WRITER: Duration = Duration::from_millis(50);
const SLOW_WRITER: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Default)]
pub struct Work {
    pub cpu_ns: u64,
    pub writer_ns: u64,
    pub rows: Option<u64>,
}

#[derive(Clone, Copy, Default)]
struct Context {
    name: Option<&'static str>,
    work: Work,
}

thread_local! {
    static CONTEXT: Cell<Context> = const { Cell::new(Context { name: None, work: Work { cpu_ns: 0, writer_ns: 0, rows: None } }) };
    static BATCH: Cell<[Option<&'static str>; 32]> = const { Cell::new([None; 32]) };
}

pub(crate) fn label() -> Option<&'static str> {
    CONTEXT.with(|context| context.get().name)
}

#[doc(hidden)]
pub fn record(work: Work) {
    CONTEXT.with(|context| {
        let mut current = context.get();
        current.work.cpu_ns += work.cpu_ns;
        current.work.writer_ns += work.writer_ns;
        if let Some(rows) = work.rows {
            current.work.rows = Some(current.work.rows.unwrap_or(0) + rows);
        }
        context.set(current);
    });
}

/// Add an already known row count; never query the store just for logging.
pub fn rows(rows: u64) {
    record(Work {
        rows: Some(rows),
        ..Work::default()
    });
}

/// Synchronous subpass on the same thread. Nested scopes forward remote CPU
/// and writer counters, but not local CPU already sampled by the outer scope.
pub fn sync<T>(name: &'static str, f: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let scope = Scope::enter(Some(name));
    let cpu = thread_cpu_ns();
    let result = f();
    let remote = scope.work();
    let work = Work {
        cpu_ns: remote.cpu_ns + thread_cpu_ns().saturating_sub(cpu),
        ..remote
    };
    drop(scope);
    record(remote);
    emit_pass(name, started.elapsed(), work, work.rows);
    result
}

struct Scope(Context);
impl Scope {
    fn enter(name: Option<&'static str>) -> Self {
        Self(CONTEXT.with(|context| {
            context.replace(Context {
                name,
                ..Context::default()
            })
        }))
    }
    fn work(&self) -> Work {
        CONTEXT.with(|context| context.get().work)
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        CONTEXT.with(|context| context.set(self.0));
    }
}

/// Measure one pass, excluding the timer wait between passes. Static labels must
/// describe work, not an account, credential, or other request-supplied value.
pub async fn run<F: Future>(name: &'static str, future: F) -> F::Output {
    let started = Instant::now();
    let mut future = std::pin::pin!(future);
    let mut work = Work::default();
    let mut remote_cpu = 0;
    let result = std::future::poll_fn(|cx| {
        let scope = Scope::enter(Some(name));
        let cpu = thread_cpu_ns();
        let result = future.as_mut().poll(cx);
        work.cpu_ns += thread_cpu_ns().saturating_sub(cpu) + scope.work().cpu_ns;
        remote_cpu += scope.work().cpu_ns;
        work.writer_ns += scope.work().writer_ns;
        if let Some(rows) = scope.work().rows {
            work.rows = Some(work.rows.unwrap_or(0) + rows);
        }
        result
    })
    .await;
    // Local CPU is already included in an enclosing scope's poll sample.
    record(Work {
        cpu_ns: remote_cpu,
        ..work
    });
    emit_pass(name, started.elapsed(), work, work.rows);
    result
}

/// The same scope on a blocking worker. The returned counters are merged only
/// after the worker finishes, so unrelated work on its thread is not charged.
#[doc(hidden)]
pub fn blocking<F: FnOnce() -> T, T>(f: F) -> impl FnOnce() -> (T, Work) {
    let name = label();
    move || {
        let scope = Scope::enter(name);
        let cpu = thread_cpu_ns();
        let result = f();
        let mut work = scope.work();
        work.cpu_ns += thread_cpu_ns().saturating_sub(cpu);
        (result, work)
    }
}

pub(crate) struct BatchScope([Option<&'static str>; 32]);
impl BatchScope {
    pub(crate) fn enter(labels: [Option<&'static str>; 32]) -> Self {
        Self(BATCH.with(|batch| batch.replace(labels)))
    }
}
impl Drop for BatchScope {
    fn drop(&mut self) {
        BATCH.with(|batch| batch.set(self.0));
    }
}

/// Called inside the existing acquisition helper, after the connection callback
/// (including commit) returns. Queue wait is deliberately not part of the hold.
pub(crate) fn writer_held(held: Duration, caller: &'static Location<'static>) {
    #[cfg(test)]
    {
        // Take the probe before calling it so it can measure this same entry
        // point with telemetry enabled and bypassed, without recursing.
        let probe = WRITER_PROBE.with(|probe| probe.borrow_mut().take());
        if let Some(probe) = probe {
            probe(held, caller);
            return;
        }
    }
    #[cfg(test)]
    if !WRITER_TELEMETRY.with(Cell::get) {
        return;
    }
    record(Work {
        writer_ns: held.as_nanos() as u64,
        ..Work::default()
    });
    if held <= SLOW_WRITER {
        return;
    }
    struct Holder(&'static Location<'static>);
    impl std::fmt::Display for Holder {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let labels = BATCH.with(Cell::get);
            if labels[0].is_some() {
                f.write_str("account_batch[")?;
                for (index, label) in labels.iter().flatten().enumerate() {
                    if index != 0 {
                        f.write_str(",")?;
                    }
                    f.write_str(label)?;
                }
                f.write_str("]")
            } else if let Some(name) = label() {
                f.write_str(name)
            } else {
                write!(f, "{}:{}", self.0.file(), self.0.line())
            }
        }
    }
    emit(format!(
        "[routing] slow writer holder={} writer_hold_ms={:.3}",
        Holder(caller),
        held.as_secs_f64() * 1000.0
    ));
}

fn emit_pass(name: &'static str, total: Duration, work: Work, rows: Option<u64>) {
    if total <= SLOW_PASS && Duration::from_nanos(work.writer_ns) <= SLOW_PASS_WRITER {
        return;
    }
    // Formatting and allocation happen only after a threshold is crossed.
    emit(format!(
        "[routing] slow job job={name} total_ms={:.3} cpu_ms={:.3} writer_hold_ms={:.3} rows={}",
        total.as_secs_f64() * 1000.0,
        work.cpu_ns as f64 / 1_000_000.0,
        work.writer_ns as f64 / 1_000_000.0,
        rows.map_or_else(|| "unknown".to_owned(), |n| n.to_string())
    ));
}

fn emit(line: String) {
    #[cfg(test)]
    LOG_LINES.with(|lines| {
        if let Some(lines) = lines.borrow_mut().as_mut() {
            lines.push(line.clone());
        }
    });
    eprintln!("{line}");
}

// POSIX thread CPU clocks use nanoseconds and include user and system CPU. The
// clock IDs differ between Darwin and Linux; no process-wide fallback is used.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn thread_cpu_ns() -> u64 {
    #[repr(C)]
    struct Timespec {
        sec: std::os::raw::c_long,
        nsec: std::os::raw::c_long,
    }
    unsafe extern "C" {
        fn clock_gettime(clock: std::os::raw::c_int, time: *mut Timespec) -> std::os::raw::c_int;
    }
    #[cfg(target_os = "linux")]
    const THREAD_CPU: std::os::raw::c_int = 3;
    #[cfg(target_os = "macos")]
    const THREAD_CPU: std::os::raw::c_int = 16;
    let mut time = Timespec { sec: 0, nsec: 0 };
    // SAFETY: time points to an initialized timespec with the platform C layout.
    if unsafe { clock_gettime(THREAD_CPU, &mut time) } == 0 {
        time.sec as u64 * 1_000_000_000 + time.nsec as u64
    } else {
        0
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn thread_cpu_ns() -> u64 {
    0
}

#[cfg(test)]
type WriterProbe = Box<dyn FnOnce(Duration, &'static Location<'static>) + Send>;

#[cfg(test)]
thread_local! {
    static LOG_LINES: std::cell::RefCell<Option<Vec<String>>> = const { std::cell::RefCell::new(None) };
    static WRITER_TELEMETRY: Cell<bool> = const { Cell::new(true) };
    static WRITER_PROBE: std::cell::RefCell<Option<WriterProbe>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
mod tests;
