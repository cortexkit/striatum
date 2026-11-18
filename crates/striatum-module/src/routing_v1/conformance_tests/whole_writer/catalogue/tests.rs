use super::*;
use crate::account_writer as writer;
use std::collections::HashSet;
use striatum_core::{FactSource, RouteStateStore};
use striatum_store::{FusiformCatalogCursor, FusiformFactUpdate, PersistedVaultRoster};

impl World {
    pub(super) fn catalogue_group(&mut self, nonce: u64) {
        let now = i64::try_from(self.now).unwrap();
        let route = format!("{}/model-c", fixture::SUBSCRIPTION_PROVIDER);
        let mut fact = self
            .harness
            .store()
            .get_fact_by_route_key(&route)
            .unwrap()
            .expect("the shared subscription model exists");
        fact.provider_id = "whole-writer-catalogue".into();
        fact.model_id = format!("generated-{nonce}");
        self.adapter("catalogue/upsert adapter", |store| {
            writer::upsert_fact(store, &fact, FactSource::Override)
        });
        let update = FusiformFactUpdate {
            auth_methods: None,
            login_methods_documented: false,
            provider_id: fact.provider_id.clone(),
            model_id: format!("{}-discovered", fact.model_id),
            present: true,
            context_window: Some(64_000.0),
            max_output_tokens: Some(4_096),
            cost_tier: Some(1.0),
            cost_per_mtok_out: Some(1.0),
            input_rate: None,
            cache_read_rate: None,
            cache_write_rate: None,
            reasoning_rate: None,
            supports_vision: Some(false),
            supports_tool_use: Some(true),
            reasoning_efforts: None,
            catalog_efforts: None,
            canonical_id: None,
            upstream_status: None,
            model_type: None,
            model_kind: None,
            lineage: None,
        };
        let one = update.clone();
        let version = i64::try_from(self.sequence).unwrap() + 1;
        self.command("catalogue/incremental", move |tx| {
            tx.apply_fusiform_catalog(
                &[one.clone()],
                FusiformCatalogCursor {
                    catalog_version: version,
                    resolved_at_ms: now,
                },
            )
        });
        let one = update.clone();
        let mapped = HashSet::from([fact.provider_id.clone()]);
        let providers = mapped.clone();
        self.command("catalogue/provider incremental", move |tx| {
            tx.apply_fusiform_catalog_for_providers(
                &[one.clone()],
                FusiformCatalogCursor {
                    catalog_version: version + 1,
                    resolved_at_ms: now,
                },
                &providers,
            )
        });
        let retained = HashSet::from([
            (fact.provider_id.clone(), fact.model_id.clone()),
            (update.provider_id.clone(), update.model_id.clone()),
        ]);
        self.adapter("catalogue/provider snapshot adapter", |store| {
            writer::apply_fusiform_catalog_snapshot_for_providers(
                store,
                &[update],
                FusiformCatalogCursor {
                    catalog_version: version + 2,
                    resolved_at_ms: now,
                },
                &mapped,
                &retained,
            )
        });
        let roster = PersistedVaultRoster {
            view: format!("whole-writer-{nonce}"),
            roster_json: "{}".into(),
            fetched_at: now,
        };
        self.adapter("listing/persisted roster", |store| {
            writer::put_account_vault_roster(store, &roster)
        });
    }
}
