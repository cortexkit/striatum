use super::*;
use crate::account_writer as writer;
use striatum_core::route_state_store::RouteStateUpdate;

impl World {
    pub(super) async fn healed_wall_ordering_witness(&mut self, reload: bool) {
        let route = format!("{}/model-c", fixture::SUBSCRIPTION_PROVIDER);
        for provider_wide in [false, true] {
            let at = self.now as i64;
            let update = RouteStateUpdate {
                observed_at_ms: Some(at),
                unscoped_wall: true,
                cooldown_until: Some((at + 600_000) as f64),
                last_error: Some("HTTP 503 scheduled maintenance".into()),
                ..Default::default()
            };
            self.write_ordered_wall(provider_wide, &route, &update);
            let status = self
                .ok(
                    Caller::Operator,
                    "route.cooldown_status",
                    json!({"routeKey":route}),
                )
                .await;
            assert_eq!(
                status["cooled"], true,
                "the ordering witness must start with an enforcing wall: {status}"
            );
            self.advance(1);
            let admitted_at = self.now as i64;
            self.advance(1);
            let now = self.now as i64;
            self.command("ordered wall/real serving heal", move |tx| {
                tx.heal_serving_fences(
                    fixture::SUBSCRIPTION_PROVIDER,
                    "whole-writer-serving-account",
                    "whole-writer-serving-credential",
                    "model-c",
                    None,
                    admitted_at,
                    now,
                )
            });
            let healed = self
                .ok(
                    Caller::Operator,
                    "route.cooldown_status",
                    json!({"routeKey":route}),
                )
                .await;
            assert_eq!(
                healed["cooled"], false,
                "a real serving heal must remove the wall first: {healed}"
            );
            if reload {
                self.reload();
            }
            let delayed = RouteStateUpdate {
                observed_at_ms: Some(at - 1),
                cooldown_until: Some((now + 900_000) as f64),
                last_error: Some("HTTP 503 older delayed report".into()),
                ..update
            };
            self.write_ordered_wall(provider_wide, &route, &delayed);
            let after = self
                .ok(
                    Caller::Operator,
                    "route.cooldown_status",
                    json!({"routeKey":route}),
                )
                .await;
            assert_eq!(
                after["cooled"],
                false,
                "an older {} wall report cannot recreate healed evidence: {after}",
                if provider_wide { "provider" } else { "route" }
            );
        }
    }

    fn write_ordered_wall(&mut self, provider_wide: bool, route: &str, update: &RouteStateUpdate) {
        if provider_wide {
            self.adapter("ordered wall/provider report", |store| {
                writer::record_provider_cooldown(
                    store,
                    fixture::SUBSCRIPTION_PROVIDER,
                    route,
                    route,
                    update,
                )
            });
        } else {
            self.adapter("ordered wall/route report", |store| {
                writer::update_route_state(store, route, route, update)
            });
        }
    }

    pub(super) async fn late_completion_ordering_witness(&mut self, reload: bool) {
        let route = self
            .catalog
            .models
            .iter()
            .find(|row| row.route_key().ends_with(&format!("/{POOL_MODEL}")))
            .unwrap()
            .route_key();
        let mut selected = Vec::new();
        for id in ["before-failure-completion", "later-failure"] {
            selected.push(
                self.ok(
                    Caller::Owner,
                    "route.select",
                    json!({"consumer":self.consumer,
                "requirements":{"iq":30},"pin":route,"affinity":id}),
                )
                .await,
            );
        }
        self.advance(1);
        self.ok(
            Caller::Owner,
            "route.report",
            json!({"decision_id":selected[1]["decision_id"],
            "outcome":"failed","runner_code":"HTTP 503 upstream unavailable"}),
        )
        .await;
        if reload {
            self.reload();
        }
        self.advance(1);
        self.ok(
            Caller::Owner,
            "route.report",
            json!({"decision_id":selected[0]["decision_id"],
            "outcome":"completed"}),
        )
        .await;
        let prior = self
            .adapter("late completion/inspect and clear actual streak", |store| {
                writer::clear_failure_backoff(store, &route)
            })
            .expect("the failed report must create route ordering state");
        assert_eq!(
            prior.consecutive_terminal_failures, 1,
            "a completion launched before a later failure cannot reset its sub-threshold streak"
        );
        assert_eq!(
            prior.failure_backoff_until, None,
            "one terminal failure must stay below the backoff threshold"
        );
        assert_eq!(prior.failure_backoff_reason, None);
    }
}
