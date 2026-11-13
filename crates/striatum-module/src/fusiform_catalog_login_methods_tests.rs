mod login_methods_tests {
    use super::*;
    use crate::broca_credentials::BrocaCredentials;
    use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};

    pub(crate) fn golden_reply() -> Value {
        let golden = fixture();
        let mut reply = json!({"catalog_version": 1, "resolved_at_ms": -1,
            "models": {}, "login_methods": []});
        for name in [
            "catalog.get — upstream ChatGPT login allowlist",
            "catalog.get — upstream API-key-only login allowlist",
            "catalog.get — upstream login availability not established",
        ] {
            let response = &golden[name]["response"];
            for (key, entry) in response["models"].as_object().unwrap() {
                let mut entry = entry.clone();
                // These producer reads request only rates. Supply existence for
                // the combined current-catalog fixture, retaining its policies.
                entry["existence"] = json!("present");
                reply["models"][key] = entry;
            }
            if let Some(rows) = response["login_methods"].as_array() {
                reply["login_methods"]
                    .as_array_mut()
                    .unwrap()
                    .extend(rows.clone());
            }
        }
        for key in ["anthropic/claude-control", "google/gemini-control"] {
            reply["models"][key] = json!({"existence":"present"});
        }
        reply
    }

    fn cache(method: &str) -> BrocaCredentials {
        let mut cache = BrocaCredentials::default();
        cache.apply_listing(
            &ScopedCredentialListing {
                view: "login-methods".into(),
                credentials: ["openai", "anthropic", "google"]
                    .into_iter()
                    .map(|provider| ListedCredential {
                        id: format!("{method}:{provider}"),
                        state: "active".into(),
                        categories: vec!["llm-provider".into()],
                        provider_ids: vec![provider.into()],
                        auth_method: Some(method.into()),
                        credential_type: if method == "apikey" {
                            "apikey"
                        } else {
                            "oauth"
                        }
                        .into(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
            1,
        );
        cache
    }

    fn apply(store: &SqliteRouteStateStore, reply: &Value) -> Vec<striatum_core::ModelFact> {
        let snapshot = decode_catalog_get(&serde_json::to_vec(reply).unwrap()).unwrap();
        store
            .apply_fusiform_catalog(&snapshot.facts, snapshot.cursor)
            .unwrap();
        store
            .get_facts()
            .unwrap()
            .into_iter()
            .filter(|fact| {
                reply["models"]
                    .get(format!("{}/{}", fact.provider_id, fact.model_id))
                    .is_some()
            })
            .collect()
    }

    #[test]
    fn positive_login_listing_denies_unlisted_openai_row() {
        let rig = Rig::new("positive-login");
        let facts = apply(&rig.store(), &golden_reply());
        assert!(!cache("chatgpt")
            .served_routes(&facts, 1)
            .0
            .contains(&"openai/gpt-4o".into()));
    }

    #[test]
    fn positive_login_listing_keeps_listed_row() {
        let rig = Rig::new("listed-login");
        let facts = apply(&rig.store(), &golden_reply());
        assert!(cache("chatgpt")
            .served_routes(&facts, 1)
            .0
            .contains(&"openai/gpt-6.1-sol".into()));
        assert_eq!(
            facts
                .iter()
                .find(|fact| fact.model_id == "gpt-6.1-sol")
                .unwrap()
                .auth_methods,
            Some(vec!["apikey".into(), "chatgpt".into()])
        );
    }

    #[test]
    fn apikey_only_login_row_denies_signin_but_accepts_key() {
        let rig = Rig::new("apikey-login");
        let facts = apply(&rig.store(), &golden_reply());
        assert!(!cache("chatgpt")
            .served_routes(&facts, 1)
            .0
            .contains(&"openai/gpt-5.5".into()));
        let keys = cache("apikey").served_routes(&facts, 1).0;
        assert!(keys.contains(&"openai/gpt-5.5".into()));
        assert!(
            keys.contains(&"openai/gpt-4o".into()),
            "an absent API-key policy remains unrestricted"
        );
    }

    #[test]
    fn providers_and_methods_without_login_listing_keep_legacy_reach() {
        let rig = Rig::new("unlisted-login");
        let facts = apply(&rig.store(), &golden_reply());
        assert!(cache("oauth")
            .served_routes(&facts, 1)
            .0
            .contains(&"anthropic/claude-control".into()));
        assert!(cache("antigravity")
            .served_routes(&facts, 1)
            .0
            .contains(&"google/gemini-control".into()));
        assert!(cache("another-signin")
            .served_routes(&facts, 1)
            .0
            .contains(&"openai/gpt-4o".into()));
    }

    #[test]
    fn later_poll_dropping_login_listing_restores_legacy_reach() {
        let rig = Rig::new("withdrawn-login");
        let store = rig.store();
        let mut reply = golden_reply();
        let before = apply(&store, &reply);
        let fingerprint = store.catalog_fingerprint().unwrap();
        assert!(!cache("chatgpt")
            .served_routes(&before, 1)
            .0
            .contains(&"openai/gpt-4o".into()));
        reply["login_methods"] = json!([]);
        reply["catalog_version"] = json!(2);
        let after = apply(&store, &reply);
        assert_ne!(fingerprint, store.catalog_fingerprint().unwrap());
        let routes = cache("chatgpt").served_routes(&after, 1).0;
        for model in ["gpt-4o", "gpt-5.5", "gpt-6.1-sol"] {
            assert!(routes.contains(&format!("openai/{model}")));
        }
    }

    #[test]
    fn producer_current_golden_login_policy_reaches_reader() {
        let snapshot =
            decode_catalog_get(&get_reply("catalog.get — provider default endpoint")).unwrap();
        assert_eq!(
            snapshot.model("openai/gpt-6.1-sol").unwrap().auth_methods,
            Some(vec!["apikey".into(), "chatgpt".into()])
        );
    }

    #[test]
    fn curated_and_login_methods_share_provider_positive_listing() {
        let rig = Rig::new("generic-login");
        for member in ["curated", "login_methods"] {
            let reply = json!({"catalog_version":1,"resolved_at_ms":-1,
                "models":{"vendor/listed":{"existence":"present"},
                          "vendor/unlisted":{"existence":"present"}},
                member: [{"model":"vendor/listed","auth_methods":["custom-signin","apikey"]}]});
            let facts = apply(&rig.store(), &reply);
            let catalog = facts.iter().collect();
            let row = ListedCredential {
                state: "active".into(),
                categories: vec!["llm-provider".into()],
                provider_ids: vec!["vendor".into()],
                auth_method: Some("custom-signin".into()),
                ..Default::default()
            };
            for fact in &facts {
                assert_eq!(
                    crate::broca_credentials::credential_serves_fact(
                        &row,
                        fact,
                        &catalog,
                        &Default::default()
                    ),
                    fact.model_id == "listed",
                    "{member}: {}",
                    fact.model_id
                );
            }
        }
    }

    #[test]
    fn login_policy_refresh_reprices_only_eligible_methods() {
        let rig = Rig::new("price-login");
        let store = rig.store();
        let mut reply = golden_reply();
        let mut facts = apply(&store, &reply);
        let mut cache = cache("chatgpt");
        cache.refresh_serving_methods(&Default::default(), &facts, 1, &[]);
        cache.price_facts(&mut facts, &Default::default());
        for fact in &facts {
            if fact.provider_id == "openai" {
                assert_eq!(
                    fact.billing_plane.as_ref().unwrap().auth_method.as_str(),
                    if fact.model_id == "gpt-6.1-sol" {
                        "chatgpt"
                    } else {
                        "ambiguous"
                    }
                );
            }
        }
        reply["login_methods"] = json!([]);
        reply["catalog_version"] = json!(2);
        let mut restored = apply(&store, &reply);
        cache.price_facts(&mut restored, &Default::default());
        assert!(restored
            .iter()
            .all(|fact| fact.billing_plane.as_ref().unwrap().auth_method == "chatgpt"));
    }
}
