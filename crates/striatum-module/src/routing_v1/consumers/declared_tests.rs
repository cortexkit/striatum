use serde_json::json;

use crate::routing_v1::consumers::Catalog;

#[tokio::test]
async fn declared_names_check_identity_after_schema_and_keep_null_transition_defaults() {
    crate::routing_v1::describe::with_candidate_capabilities(&["consumer_declarations"], async {
        for member in [
            json!({"iq": 0}),
            json!({"ladder": []}),
            json!({"placement": {"module": "broca"}}),
            json!({"selectors": []}),
            json!({"kind": "llm"}),
        ] {
            let mut outside = member.clone();
            outside["name"] = json!("callosum_task");
            outside["eq"] = json!(17);
            let catalog = Catalog::from_catalog(&json!([{
                "module_id": "callosum", "nonce_verified": true,
                "role_declarations": {"routing/v1": {"consumers": [
                    outside,
                    {"name": "callosum", "iq": 0},
                    {"name": "callosum.task", "selectors": []},
                    {"name": "phone.media", "eq": 22},
                    {"name": "null-legacy", "eq": 33, "iq": null, "ladder": null,
                     "placement": null, "selectors": null, "kind": null}
                ]}}
            }]));
            let names = &catalog.modules["callosum"];
            assert!(!names.contains_key("callosum_task"), "{member}");
            assert!(names.contains_key("callosum"));
            assert!(names.contains_key("callosum.task"));
            for (name, eq) in [("phone.media", 22), ("null-legacy", 33)] {
                assert!(
                    catalog.issues.iter().any(|issue| {
                        issue.contains("transitional")
                            && issue.contains(&format!("consumer {name:?}"))
                            && issue.contains("from module \"callosum\"")
                    }),
                    "{name}: {:?}",
                    catalog.issues
                );
                let defaults = &names[name];
                assert_eq!(defaults.eq, Some(eq));
                assert_eq!(defaults.iq, None);
                assert_eq!(defaults.ladder, None);
                assert_eq!(defaults.placement, None);
                assert_eq!(defaults.selectors, None);
                assert_eq!(defaults.kind, None);
            }
            assert_eq!(catalog.issues.len(), 3);
            assert!(catalog
                .issues
                .iter()
                .any(|issue| issue.contains("ignored consumer \"callosum_task\"")
                    && issue.contains("callosum")));
            assert_eq!(
                catalog
                    .issues
                    .iter()
                    .filter(|issue| issue.contains("transitional"))
                    .count(),
                2
            );
        }
        // An invalid schema still refuses the whole slot before identity checks.
        let invalid = Catalog::from_catalog(&json!([{
            "module_id": "callosum", "nonce_verified": true,
            "role_declarations": {"routing/v1": {"consumers": [
                {"name": "outside", "iq": 101}, {"name": "callosum.task", "iq": 40}
            ]}}
        }]));
        assert!(invalid.modules.is_empty());
        assert_eq!(invalid.issues.len(), 1);
        assert!(invalid.issues[0].contains("invalid consumer declaration"));
    })
    .await;
}
