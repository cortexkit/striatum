use super::{OperatorWarnings, INJECTED_NOW_MS, OPERATOR_INFOS, OPERATOR_WARNINGS};
use std::sync::Arc;

/// The per-thread test state a route handler reads: the injected clocks, ID
/// sequence and diagnostics buffer. Route handlers run on a blocking thread (see
/// [`super::run_route_handler_off_async_workers`]), so the state of the thread that
/// dispatched the handler is installed there for the handler's lifetime.
#[derive(Clone)]
pub(super) struct TestThreadState {
    now_ms: Option<u64>,
    warnings: OperatorWarnings,
    infos: OperatorWarnings,
    replay: Option<striatum_store::replay::Inputs>,
    writer_trace: Option<striatum_store::writer_entry_trace::Trace>,
}

impl TestThreadState {
    pub(super) fn capture() -> Self {
        Self {
            now_ms: INJECTED_NOW_MS.with(std::cell::Cell::get),
            warnings: OPERATOR_WARNINGS.with(|sink| Arc::clone(&sink.borrow())),
            infos: OPERATOR_INFOS.with(|sink| Arc::clone(&sink.borrow())),
            replay: striatum_store::replay::current(),
            writer_trace: striatum_store::writer_entry_trace::current(),
        }
    }

    /// Make this the current thread's state until the returned guard drops,
    /// which puts back what the thread had before. Blocking threads are reused,
    /// so a handler's state must not outlive it.
    pub(super) fn install(self) -> InstalledTestThreadState {
        let replay = self.replay.as_ref().map(|inputs| inputs.install());
        let writer_trace = self.writer_trace.as_ref().map(|trace| trace.install());
        InstalledTestThreadState {
            previous: Some(TestThreadState {
                now_ms: INJECTED_NOW_MS.with(|clock| clock.replace(self.now_ms)),
                warnings: OPERATOR_WARNINGS.with(|sink| sink.replace(self.warnings)),
                infos: OPERATOR_INFOS.with(|sink| sink.replace(self.infos)),
                replay: None,
                writer_trace: None,
            }),
            _replay: replay,
            _writer_trace: writer_trace,
        }
    }
}

/// Holds the state a thread had before [`TestThreadState::install`].
pub(super) struct InstalledTestThreadState {
    previous: Option<TestThreadState>,
    _replay: Option<striatum_store::replay::Guard>,
    _writer_trace: Option<striatum_store::writer_entry_trace::Installed>,
}

impl Drop for InstalledTestThreadState {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            INJECTED_NOW_MS.with(|clock| clock.set(previous.now_ms));
            OPERATOR_WARNINGS.with(|sink| *sink.borrow_mut() = previous.warnings);
            OPERATOR_INFOS.with(|sink| *sink.borrow_mut() = previous.infos);
        }
    }
}
