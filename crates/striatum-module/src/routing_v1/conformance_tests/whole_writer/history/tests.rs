use super::*;
use crate::account_writer as writer;
use striatum_core::route_state_store::{DecisionOutcomeUpdate, RouteOutcome};
use striatum_core::RouteStateStore;
use striatum_store::V1Recorded;

impl World {
    pub(super) async fn history_group(&mut self, nonce: u64) {
        let route = format!("{}/model-c", fixture::SUBSCRIPTION_PROVIDER);
        self.ok(
            Caller::Operator,
            "route.clear_cooldown",
            json!({"routeKey":route}),
        )
        .await;
        let selected = self
            .ok(
                Caller::Owner,
                "route.select",
                json!({"consumer":self.consumer,"requirements":{"iq":30},"pin":route}),
            )
            .await;
        let original = selected["decision_id"].as_str().unwrap().to_owned();
        let mut binding = self
            .adapter("history/keyed selected decision", |store| {
                store.v1_decision(&original)
            })
            .expect("the actual selection has no v1 binding");
        let now = i64::try_from(self.now).unwrap();
        binding.decision_id = format!("native-binding-{nonce}-{}", self.sequence);
        binding.affinity = None;
        binding.created_at_ms = now;
        binding.expires_at_ms = now + 120_000;
        let history_id = binding.decision_id.clone();
        self.command("history/v1 binding", move |tx| {
            tx.v1_record_decision(&binding)
        });
        let record = V1Recorded {
            body_json: json!({"report_id":format!("native-report-{nonce}")}).to_string(),
            answer_json: json!({"recorded":true}).to_string(),
        };
        let returned = self.command("history/v1 report", move |tx| {
            tx.v1_record_report(
                &history_id,
                &format!("native-report-{nonce}"),
                &record,
                nonce % 2 == 0,
                now,
            )
        });
        assert_eq!(returned.answer_json, json!({"recorded":true}).to_string());
        let update = DecisionOutcomeUpdate {
            provider_outcome: None,
            outcome: RouteOutcome::Error,
            last_error: Some("temporary transport failure".into()),
            observed_at_ms: now,
            failure_backoff: Default::default(),
        };
        self.adapter("history/terminal model failure", |store| {
            store.set_decision_outcome(&original, &update)
        });
        self.adapter("history/operator failure clear", |store| {
            writer::clear_failure_backoff(store, &route)
        });
    }
}
