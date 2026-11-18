use std::sync::{Arc, Mutex};

#[derive(Debug, PartialEq)]
pub(crate) struct Evidence {
    pub iq: Option<f64>,
    pub top: Option<f64>,
    pub bar: Option<f64>,
    pub adequate: usize,
}

tokio::task_local! {
    static EVIDENCE: Arc<Mutex<Vec<Evidence>>>;
}

pub(super) fn record(iq: Option<f64>, top: Option<f64>, bar: Option<f64>, adequate: usize) {
    let _ = EVIDENCE.try_with(|evidence| {
        evidence.lock().unwrap().push(Evidence {
            iq,
            top,
            bar,
            adequate,
        })
    });
}

pub(crate) async fn capture<T>(future: impl std::future::Future<Output = T>) -> (T, Vec<Evidence>) {
    let evidence = Arc::new(Mutex::new(Vec::new()));
    let result = EVIDENCE.scope(evidence.clone(), future).await;
    let captured = std::mem::take(&mut *evidence.lock().unwrap());
    (result, captured)
}

pub(crate) fn inherit<F: std::future::Future>(
    future: F,
) -> impl std::future::Future<Output = F::Output> {
    let evidence = EVIDENCE.try_with(Clone::clone).ok();
    async move {
        match evidence {
            Some(evidence) => EVIDENCE.scope(evidence, future).await,
            None => future.await,
        }
    }
}
