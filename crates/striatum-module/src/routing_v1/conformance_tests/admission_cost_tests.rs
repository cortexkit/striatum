use super::*;

fn result(reply: (FrameType, Value)) -> Value {
    assert_eq!(reply.0, FrameType::Response, "{reply:?}");
    reply.1["result"].clone()
}

#[tokio::test]
async fn admission_snapshot_reuses_selection_without_extra_writer_acquisitions() {
    for legacy in [true, false] {
        for selection_kind in ["none", "unbound", "standard"] {
            let subject = KitSubject {
                observe_blocks: Mutex::new(false),
                media: Mutex::new(Default::default()),
                harness: Mutex::new(Harness::new("admission-cost")),
            };
            subject
                .configure_consumers(&fixture::consumers())
                .await
                .unwrap();
            let mut catalog = fixture::continue_catalog();
            catalog
                .accounts
                .retain(|account| account.account_id == "account-1");
            subject.configure_catalog(&catalog).await.unwrap();
            let harness = subject.harness();
            let runner = harness.bind(reserved(fixture::RUNNER_ONE), None).await;
            let mut admission = json!({
                "provider":fixture::SUBSCRIPTION_PROVIDER,"model":"model-c",
                "admission_id":"warm","conversation_key":"cost-conversation","initial_input_bytes":1024,
            });
            if legacy {
                admission["caller"] = json!({"runner":fixture::RUNNER_ONE});
            }
            if selection_kind != "none" {
                let consumer = harness.bind(reserved(fixture::CONSUMER), None).await;
                let mut request = json!({"consumer":fixture::SUMMARY,"pin":fixture::HOLD_FIRST,"requirements":{"iq":50}});
                if selection_kind == "standard" {
                    request["service_tier"] = json!("standard");
                }
                let selection = result(consumer.call("route.select", &request).await);
                admission["decision_id"] = selection["decision_id"].clone();
            }
            // Publish the account state and caches before measuring a fresh
            // admission. Setup, selection and trace installation are excluded.
            result(runner.call("route.account_for", &admission).await);
            let store = harness.store();
            let reads = store.trace_admission_group_reads_for_test().unwrap();
            reads.take();
            let before = store.writer_acquisition_count_for_test();
            let commits = store.writer_commit_count_for_test();
            admission["admission_id"] = json!("measured");
            let admitted = result(runner.call("route.account_for", &admission).await);
            let acquisitions = store.writer_acquisition_count_for_test() - before;
            let commits = store.writer_commit_count_for_test() - commits;
            let (ordinary, keyed) = reads.take();
            assert_eq!(admitted["admission"]["id"], "measured");
            println!("ADMISSION_COST surface={} selection={selection_kind} acquisitions={acquisitions} commits={commits} ordinary_reads={} keyed_reads={}",
                if legacy { "legacy" } else { "v1" }, ordinary.len(), keyed.len());
            // Bound the whole frame, including model-metadata checkouts and
            // its writer command. A bound tier reuses the class selection's
            // runner instead of checking the same decision out again.
            let maximum = match (legacy, selection_kind) {
                (true, "none") => 2,
                (false, "none") => 3,
                (true, _) => 8,
                (false, _) => 13,
            };
            assert!(acquisitions <= maximum,
                "admission exceeded its writer acquisition budget: legacy={legacy}, selection={selection_kind}, {acquisitions} > {maximum}");
            assert!(ordinary.is_empty(), "{ordinary:#?}");
            assert!(keyed.len() <= 1, "{keyed:#?}");
            assert_eq!(commits, 1);
        }
    }
}
