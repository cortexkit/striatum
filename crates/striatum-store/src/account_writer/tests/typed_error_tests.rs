use super::*;

#[derive(Debug, PartialEq)]
enum TestError {
    Store(RouteStateStoreError),
    Refused(String),
}

impl From<RouteStateStoreError> for TestError {
    fn from(error: RouteStateStoreError) -> Self {
        Self::Store(error)
    }
}

impl AccountCommandError for TestError {
    fn store_error(&self) -> Option<&RouteStateStoreError> {
        match self {
            Self::Store(error) => Some(error),
            Self::Refused(_) => None,
        }
    }
}

fn typed_queued(
    mut run: impl FnMut(&AccountTx<'_>) -> Result<String, TestError> + Send + 'static,
) -> (Queued, mpsc::Receiver<Result<String, TestError>>) {
    let (reply, receiver) = mpsc::channel();
    (Queued {
        enqueued: Instant::now(), initialize_only: false, history_maintenance: false,
        label: "test_command", background: false,
        command: Box::new(TypedCommand {
            run: move |tx: &AccountTx<'_>, _: Option<&mut (dyn crate::AccountWorkingState + 'static)>| run(tx),
            result: None, reply, timing: Arc::new(Mutex::new(ReplyTiming::default())),
        }),
    }, receiver)
}

#[test]
fn typed_refusal_rolls_back_only_its_command_and_commits_batch_sibling() {
    let (_scratch, store) = fixture("typed-refusal-sibling");
    let (failed, failure) = typed_queued(|tx| {
        insert(tx, "refused")?;
        // A domain message must never trigger the store's divergence retry.
        Err(TestError::Refused(STATE_DIVERGED.into()))
    });
    let (sibling, success) = queued(|tx| insert(tx, "sibling"));
    let mut batch = vec![failed, sibling];
    store
        .store
        .with_conn(|conn| {
            assert!(run_batch(
                conn,
                &mut batch,
                &mut stateless(),
                &store.account_write_transactions
            )
            .unwrap());
            Ok(())
        })
        .unwrap();
    for command in batch {
        command.command.publish(None, ReplyTiming::default());
    }
    assert_eq!(
        failure.recv().unwrap(),
        Err(TestError::Refused(STATE_DIVERGED.into()))
    );
    assert_eq!(success.recv().unwrap().unwrap(), "sibling");
    assert_eq!(
        store
            .store
            .with_read(|conn| conn.query_row(
                "SELECT group_concat(id) FROM writer_test",
                [],
                |row| row.get::<_, String>(0),
            ))
            .unwrap(),
        "sibling"
    );
}

#[test]
fn typed_store_divergence_still_reloads_and_reruns_the_batch_once() {
    let (_scratch, store) = fixture("typed-store-divergence");
    let attempts = Arc::new(AtomicU64::new(0));
    let count = attempts.clone();
    let (command, result) = typed_queued(move |tx| {
        let answer = insert(tx, "eventual")?;
        if count.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(TestError::Store(err(STATE_DIVERGED)));
        }
        Ok(answer)
    });
    let mut batch = vec![command];
    store
        .store
        .with_conn(|conn| {
            assert!(run_batch(
                conn,
                &mut batch,
                &mut stateless(),
                &store.account_write_transactions
            )
            .unwrap());
            Ok(())
        })
        .unwrap();
    for command in batch {
        command.command.publish(None, ReplyTiming::default());
    }
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(result.recv().unwrap().unwrap(), "eventual");
}
