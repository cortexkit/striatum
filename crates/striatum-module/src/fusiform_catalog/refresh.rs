use std::time::Duration;

use striatum_store::FusiformCatalogCursor;
use tokio::time::Instant;

use super::{error, FusiformCatalogError};
use crate::SharedBrocaCredentials;

pub(super) fn needs_catalog_get(
    credentials: Option<&SharedBrocaCredentials>,
    cursor: Option<FusiformCatalogCursor>,
    version: i64,
) -> Result<bool, FusiformCatalogError> {
    let published_differs = match credentials {
        Some(cache) => cache
            .lock()
            .map_err(|_| error("credential cache poisoned"))?
            .catalog_snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.version() != version),
        None => false,
    };
    // The facts transaction advances the cursor before operator grades are
    // committed and the catalog used by request handlers is published.
    Ok(published_differs || cursor.is_none_or(|cursor| cursor.catalog_version != version))
}

pub(super) fn poll_delay(credentials: &SharedBrocaCredentials, cadence: Duration) -> Duration {
    let retry = credentials.lock().ok().and_then(|guard| {
        guard
            .billing_catalog
            .retry_at
            .values()
            .copied()
            .chain(
                guard
                    .catalog_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.local_grades.retry_at),
            )
            .min()
    });
    retry.map_or(cadence, |due| {
        cadence.min(due.saturating_duration_since(Instant::now()))
    })
}
