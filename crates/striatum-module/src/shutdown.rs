use std::{
    sync::{mpsc, Arc, OnceLock},
    time::{Duration, Instant},
};

use crate::{async_main, ModuleError};

const CLOSE_LIMIT: Duration = Duration::from_secs(5);

pub(super) fn run_main() -> Result<(), ModuleError> {
    let shutdown = Shutdown::default();
    let mut store = None;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| ModuleError::Message(error.to_string()))?;
    let result = runtime.block_on(async_main(&shutdown, &mut store));
    // Aborting an async task does not stop its in-flight blocking job. Join
    // the runtime before closing SQLite, while main still owns the store.
    drop(runtime);
    if let Some(store) = store {
        drop(Arc::try_unwrap(store).map_err(|_| {
            ModuleError::Message("routing shutdown left a live store owner".into())
        })?);
    }
    result
}

/// Keep the deadline outside Tokio: a blocking job or the store's destructor
/// can stall runtime teardown itself, after async timers stop being polled.
#[derive(Default)]
pub(super) struct Shutdown {
    completed: OnceLock<mpsc::Sender<()>>,
}

impl Shutdown {
    pub(super) fn begin(&self) {
        self.completed.get_or_init(|| {
            let deadline = Instant::now() + CLOSE_LIMIT;
            let (completed, wait) = mpsc::channel();
            if let Err(error) = std::thread::Builder::new()
                .name("routing-shutdown".into())
                .spawn(move || {
                    if let Err(mpsc::RecvTimeoutError::Timeout) =
                        wait.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    {
                        eprintln!(
                            "[striatum] WARN: shutdown timed out after {CLOSE_LIMIT:?}; \
                             SQLite files may need recovery"
                        );
                        std::process::exit(1);
                    }
                })
            {
                eprintln!("[striatum] WARN: could not start shutdown watchdog: {error}");
                std::process::exit(1);
            }
            completed
        });
    }
}
