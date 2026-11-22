//! Keep nonce-based vault access on legacy launches and use the SDK's bound
//! connection for both the scoped listing and held watch after adoption.

use std::{path::PathBuf, sync::Arc, time::Duration};

use cortexkit_claustrum_client::{
    ClaustrumCredentialResolver, CredentialResolver, CredentialResolverError,
    CredentialResolverFuture, ResolvedCredential, ScopedCredentialListing, ScopedWatch,
};
use subc_client_rs::SubcConsumer;
use tokio::{sync::OnceCell, time::timeout};

pub(super) struct VaultResolver {
    connection_file: PathBuf,
    legacy_module_id: &'static str,
    resolver: OnceCell<ClaustrumCredentialResolver>,
}

impl VaultResolver {
    pub(super) fn new(connection_file: PathBuf, legacy_module_id: &'static str) -> Self {
        Self {
            connection_file,
            legacy_module_id,
            resolver: OnceCell::new(),
        }
    }

    async fn get(&self) -> Result<&ClaustrumCredentialResolver, CredentialResolverError> {
        // Both paths open lazily, so a slow vault cannot hold up module readiness.
        // Cache only success; the watch and listing can retry a failed initial dial.
        self.resolver
            .get_or_try_init(|| async {
                if !crate::registration_session::claim_bound() {
                    return Ok(ClaustrumCredentialResolver::new(
                        self.connection_file.clone(),
                        self.legacy_module_id,
                    ));
                }
                let consumer = timeout(
                    Duration::from_secs(10),
                    SubcConsumer::connect(&self.connection_file, Default::default()),
                )
                .await
                .map_err(|_| {
                    CredentialResolverError::transport("credential SDK connection timed out")
                })?
                .map_err(|_| {
                    CredentialResolverError::transport("credential SDK connection failed")
                })?;
                Ok(ClaustrumCredentialResolver::with_consumer(Arc::new(
                    consumer,
                )))
            })
            .await
    }
}

impl CredentialResolver for VaultResolver {
    fn resolve<'a>(
        &'a self,
        raw_handle: &'a str,
        min_ttl_ms: Option<u64>,
        force_refresh: bool,
    ) -> CredentialResolverFuture<'a, Result<ResolvedCredential, CredentialResolverError>> {
        Box::pin(async move {
            self.get()
                .await?
                .resolve(raw_handle, min_ttl_ms, force_refresh)
                .await
        })
    }

    fn get_scoped<'a>(
        &'a self,
        credential_id: &'a str,
    ) -> CredentialResolverFuture<'a, Result<ResolvedCredential, CredentialResolverError>> {
        Box::pin(async move { self.get().await?.get_scoped(credential_id).await })
    }

    fn list_scoped(
        &self,
    ) -> CredentialResolverFuture<'_, Result<ScopedCredentialListing, CredentialResolverError>>
    {
        Box::pin(async move { self.get().await?.list_scoped().await })
    }

    fn watch_scoped<'a>(
        &'a self,
        enrollment_token: Option<&'a str>,
    ) -> CredentialResolverFuture<'a, Result<ScopedWatch, CredentialResolverError>> {
        Box::pin(async move { self.get().await?.watch_scoped(enrollment_token).await })
    }
}
