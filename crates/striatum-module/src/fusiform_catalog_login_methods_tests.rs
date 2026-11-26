mod login_methods_tests {
    use super::*;
    use crate::broca_credentials::BrocaCredentials;
    use cortexkit_claustrum_client::{ListedCredential, ScopedCredentialListing};
    use std::collections::HashSet;

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
    fn curated_only_signin_preserves_unlisted_sibling_reach() {
        let rig = Rig::new("curated-sibling");
        let store = rig.store();
        for model in ["muse-wonder-full-1.0", "muse-spark-1.3-contributor"] {
            store
                .upsert_fact(
                    &crate::tests::fact("meta", model),
                    striatum_core::FactSource::Seed,
                )
                .unwrap();
        }
        let reply = json!({"catalog_version":1,"resolved_at_ms":-1,
            "models":{"meta/muse-wonder-full-1.0":{"existence":"present"},
                      "meta/muse-spark-1.3-contributor":{"existence":"present"}},
            "curated":[{"model":"meta/muse-wonder-full-1.0","auth_methods":["oauth"]}]});
        apply(&store, &reply);
        drop(store);
        let facts = rig.store().get_facts().unwrap();
        assert_eq!(
            facts
                .iter()
                .filter(|fact| fact.provider_id == "meta")
                .count(),
            2
        );
        let mut cache = BrocaCredentials::default();
        cache.apply_listing(
            &ScopedCredentialListing {
                view: "curated-sibling".into(),
                credentials: vec![muse_credential("oauth", true)],
                ..Default::default()
            },
            1,
        );
        let routes = cache.served_routes(&facts, 1).0;
        assert!(routes.contains(&"meta/muse-spark-1.3-contributor".into()),
            "a curated row restricts only itself; it does not document the sign-in's complete coverage");
        assert!(routes.contains(&"meta/muse-wonder-full-1.0".into()));
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
    fn antigravity_golden_login_policy_reaches_alias_routes_only() {
        const ALIASES: [&str; 7] = [
            "google/antigravity-claude-opus-5-5",
            "google/antigravity-claude-sonnet-5-5",
            "google/antigravity-gemini-3.8-flash",
            "google/antigravity-gemini-4-argon",
            "google/antigravity-gemini-4-carbon",
            "google/antigravity-gemini-4-hydrogen",
            "google/antigravity-gemini-4-neon",
        ];
        const PLAIN: &str = "google/gemini-4-hydrogen";
        let golden: BTreeMap<String, Value> = serde_json::from_str(include_str!(
            "../fixtures/served-payloads-dd928ca5.json"
        ))
        .unwrap();
        let mut aliases = BTreeMap::new();
        let mut checked = 0;
        for (name, case) in &golden {
            let arguments = &case["request"]["arguments"];
            let Some(id) = arguments["model_id"].as_str() else {
                continue;
            };
            let route = format!("google/{id}");
            if case["request"]["name"] != "catalog.get"
                || arguments["provider_id"] != "google"
                || !ALIASES.contains(&route.as_str())
            {
                continue;
            }
            let mut reply = case["response"].clone();
            let model = reply["models"][&route]
                .as_object_mut()
                .unwrap_or_else(|| panic!("{name}: missing {route}"));
            // Reads limited to selected facts can omit existence. Add it here
            // to check the login policy on a usable row; the reader matrix
            // checks the unchanged replies.
            model.entry("existence").or_insert(json!("present"));
            let snapshot = decode_catalog_get(serde_json::to_vec(&reply).unwrap()).unwrap();
            let fact = snapshot
                .model(&route)
                .unwrap_or_else(|| panic!("{name}: no decoded fact for {route}"));
            assert!(fact.login_methods_documented, "{name}: {route}");
            assert_eq!(fact.auth_methods, Some(vec!["antigravity".into()]), "{name}");
            aliases.insert(route, fact.clone());
            checked += 1;
        }
        assert_eq!(checked, 11);
        assert_eq!(aliases.keys().map(String::as_str).collect::<Vec<_>>(), ALIASES);

        let plain_reply = &golden["catalog.get — Gemini 4 Hydrogen full row"]["response"];
        let snapshot = decode_catalog_get(serde_json::to_vec(plain_reply).unwrap()).unwrap();
        let plain = snapshot.model(PLAIN).unwrap();
        assert!(!plain.login_methods_documented);
        assert_eq!(plain.auth_methods, None);

        let rig = Rig::new("antigravity-golden");
        let store = rig.store();
        let updates: Vec<_> = aliases.into_values().chain([plain.clone()]).collect();
        store.apply_fusiform_catalog(&updates, snapshot.cursor).unwrap();
        // The store opens with seeded facts. Test only the captured models.
        let facts: Vec<_> = store
            .get_facts()
            .unwrap()
            .into_iter()
            .filter(|fact| {
                updates.iter().any(|update| {
                    fact.provider_id == update.provider_id && fact.model_id == update.model_id
                })
            })
            .collect();

        // Do not add runner protocol restrictions: these assertions should
        // depend only on the login methods in the catalog.
        let signin = cache("antigravity").statically_served_routes(&facts);
        assert_eq!(signin, HashSet::from(ALIASES.map(str::to_owned)));
        let apikey = cache("apikey").statically_served_routes(&facts);
        assert_eq!(apikey, HashSet::from([PLAIN.to_owned()]));
    }

    #[test]
    fn only_login_methods_establish_provider_positive_listing() {
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
                    member == "curated" || fact.model_id == "listed",
                    "{member}: {}",
                    fact.model_id
                );
            }
        }
    }

    #[test]
    fn source_change_preserves_row_policy_but_withdraws_coverage_and_refreshes_variants() {
        use striatum_core::FactSource;
        let rig = Rig::new("login-source");
        let store = rig.store();
        let mut reply = golden_reply();
        let facts = apply(&store, &reply);
        let mut variant = facts
            .iter()
            .find(|fact| fact.model_id == "gpt-6.1-sol")
            .unwrap()
            .clone();
        variant.variant = Some("high".into());
        variant.login_methods_documented = false;
        store.upsert_fact(&variant, FactSource::Override).unwrap();
        assert!(
            store
                .get_fact_by_route_key("openai/gpt-6.1-sol/high")
                .unwrap()
                .unwrap()
                .login_methods_documented,
            "a score variant inherits the catalog's coverage source, not the caller's marker"
        );
        let fingerprint = store.catalog_fingerprint().unwrap();
        reply["curated"] = reply
            .as_object_mut()
            .unwrap()
            .remove("login_methods")
            .unwrap();
        reply["catalog_version"] = json!(2);
        apply(&store, &reply);
        assert_ne!(
            fingerprint,
            store.catalog_fingerprint().unwrap(),
            "changing only disclosure provenance still changes routing reachability"
        );
        drop(store);
        let store = rig.store();
        let listed = store
            .get_fact_by_route_key("openai/gpt-6.1-sol/high")
            .unwrap()
            .unwrap();
        assert!(!listed.login_methods_documented);
        assert_eq!(
            listed.auth_methods,
            Some(vec!["apikey".into(), "chatgpt".into()])
        );
        let facts = store.get_facts().unwrap();
        assert!(cache("chatgpt")
            .served_routes(&facts, 1)
            .0
            .contains(&"openai/gpt-4o".into()));
        assert!(
            !cache("chatgpt")
                .served_routes(&facts, 1)
                .0
                .contains(&"openai/gpt-5.5".into()),
            "the API-only curated row keeps its local restriction"
        );
    }

    #[test]
    fn additive_coverage_migration_keeps_existing_rows_and_curated_policy() {
        let rig = Rig::new("login-migration");
        let path = rig.dir.join("route.db");
        let descriptor = striatum_store::dev_descriptor(
            "fusiform-catalog-test",
            path.to_string_lossy().into_owned(),
        );
        let old = SqliteRouteStateStore::open_at_version_for_test(&descriptor, 86).unwrap();
        drop(old);
        let connection = rusqlite::Connection::open(&path).unwrap();
        let key: String = connection
            .query_row(
                "SELECT route_key FROM model_facts ORDER BY route_key LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        connection
            .execute(
                "UPDATE model_facts SET auth_methods_json='[\"oauth\"]' WHERE route_key=?1",
                [&key],
            )
            .unwrap();
        let count: i64 = connection
            .query_row("SELECT COUNT(*) FROM model_facts", [], |row| row.get(0))
            .unwrap();
        drop(connection);
        let store = rig.store();
        let fact = store.get_fact_by_route_key(&key).unwrap().unwrap();
        assert_eq!(fact.auth_methods, Some(vec!["oauth".into()]));
        assert!(
            !fact.login_methods_documented,
            "a migration cannot infer documented coverage from a curated restriction"
        );
        drop(store);
        let connection = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM model_facts", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            count
        );
        assert_eq!(
            connection
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
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
