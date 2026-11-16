use super::*;

// The public outcome classes are independently pinned by the report contract's
// penalty matrix. Do not derive this obligation from the cases the loop drives.
const REPORT_OUTCOMES: [&str; 10] = [
    "completed",
    "cancelled",
    "contract_error",
    "session_error",
    "interrupted",
    "launch_failed",
    "failed",
    "refused",
    "timed_out",
    "vanished",
];

impl World {
    pub(super) async fn report_class_witnesses(&mut self, seed: u64) {
        for reload in [false, true] {
            let _clock = crate::InjectedClock::set(START);
            let mut world = World::new(&format!("report-classes-{seed}-{reload}")).await;
            world.all_report_classes_and_config(reload, seed).await;
            self.inherit_witnesses(&world);
        }
    }

    async fn all_report_classes_and_config(&mut self, reload: bool, seed: u64) {
        self.harness.edit_config(|config| {
            config["providers"][fixture::SUBSCRIPTION_PROVIDER]["accounts"]["strategy"] =
                json!("sticky");
        });
        self.compare("report class/initial policy publication");
        let route = self
            .catalog
            .models
            .iter()
            .find(|row| row.route_key().ends_with(&format!("/{POOL_MODEL}")))
            .unwrap()
            .route_key();
        let mut outcomes = vec![
            "failed",
            "completed",
            "refused",
            "timed_out",
            "cancelled",
            "contract_error",
            "vanished",
            "session_error",
            "launch_failed",
            "interrupted",
        ];
        let shift = seed as usize % outcomes.len();
        outcomes.rotate_left(shift);
        let mut reached = BTreeSet::new();
        let mut replies = Vec::new();
        for (index, outcome) in outcomes.into_iter().enumerate() {
            let prior = self
                .harness
                .operator_config()
                .allocator_policy_ref(fixture::SUBSCRIPTION_PROVIDER)
                .strategy
                .clone();
            self.harness.edit_config(|config| {
                config["providers"][fixture::SUBSCRIPTION_PROVIDER]["accounts"]["strategy"] =
                    json!(if index % 2 == 0 {
                        "round_robin"
                    } else {
                        "sticky"
                    });
            });
            let current = self
                .harness
                .operator_config()
                .allocator_policy_ref(fixture::SUBSCRIPTION_PROVIDER)
                .strategy
                .clone();
            assert_ne!(
                prior, current,
                "a config-file write must change the effective allocator input"
            );
            self.sequence += 1;
            self.commands.insert("config strategy publication".into());
            self.compare("report class/effective policy publication");
            self.witness(format!("config/reload={reload}"));
            if reload {
                self.reload();
            }
            let selected = self
                .ok(
                    Caller::Owner,
                    "route.select",
                    json!({"consumer":self.consumer,
                "requirements":{"iq":30},"pin":route,"affinity":format!("outcome-{outcome}")}),
                )
                .await;
            let body = json!({"decision_id":selected["decision_id"],"outcome":outcome});
            let answer = self.ok(Caller::Owner, "route.report", body.clone()).await;
            assert_eq!(
                answer["outcome"], outcome,
                "the requested public outcome must be acknowledged unchanged"
            );
            let stored = self
                .harness
                .store()
                .v1_report(selected["decision_id"].as_str().unwrap())
                .unwrap()
                .unwrap();
            let stored_body: Value = serde_json::from_str(&stored.body_json).unwrap();
            let stored_answer: Value = serde_json::from_str(&stored.answer_json).unwrap();
            assert_eq!(
                stored_body["outcome"], outcome,
                "the actual report ledger must retain its class"
            );
            assert_eq!(stored_answer, answer);
            if reload {
                self.reload();
            }
            assert_eq!(
                self.ok(Caller::Owner, "route.report", body.clone()).await,
                answer,
                "the real report delivery must replay rather than apply its outcome twice"
            );
            reached.insert(outcome);
            self.witness(format!("report/{outcome}/reload={reload}"));
            replies.push((body, answer));
        }
        assert_eq!(
            reached,
            REPORT_OUTCOMES.into_iter().collect(),
            "the run missed a declared report class"
        );
        self.reload();
        for (body, answer) in replies {
            assert_eq!(
                self.ok(Caller::Owner, "route.report", body).await,
                answer,
                "a report class must retain its original reply after reload"
            );
        }
    }
}

#[tokio::test]
async fn each_report_outcome_class_and_effective_config_publication_survives_reload() {
    let _clock = crate::InjectedClock::set(START);
    let mut world = World::new("report-class-witness-driver").await;
    world.report_class_witnesses(1).await;
}
