//! Ledger capacity is a subtractive filter on the quality-gated shortlist.

use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct AccountConcurrency {
    pub account_id: String,
    pub count: usize,
    pub cap: u64,
    pub reset_at_ms: i64,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
pub struct ConcurrencyAvailability {
    pub full_routes: BTreeMap<String, Vec<AccountConcurrency>>,
}

impl ConcurrencyAvailability {
    pub fn accounts(&self, route: &str) -> Option<&Vec<AccountConcurrency>> {
        self.full_routes.get(route).or_else(|| {
            let mut parts = route.split('/');
            let capacity = format!("{}/{}", parts.next()?, parts.next()?);
            self.full_routes.get(&capacity)
        })
    }

    pub fn refusal(&self, routes: &[String]) -> crate::model_router::NoRouteError {
        use crate::model_router::{NoRouteError, RejectedRoute, RouteFailureReason};
        let earliest = routes
            .iter()
            .filter_map(|route| {
                self.accounts(route)?
                    .iter()
                    .map(|account| account.reset_at_ms)
                    .min()
                    .map(|reset| (reset, route.clone()))
            })
            .min();
        NoRouteError {
            candidates: Vec::new(),
            reason: RouteFailureReason::ConcurrencyFull,
            message: "Every eligible account of the candidate routes is at its concurrency cap."
                .into(),
            rejected: routes
                .iter()
                .map(|route| RejectedRoute {
                    route_key: route.clone(),
                    reason: "concurrency_full".into(),
                    detail: self
                        .accounts(route)
                        .map(|counts| serde_json::to_string(counts).expect("counts serialize")),
                    credential_ids: Vec::new(),
                })
                .collect(),
            requirements: None,
            best_rejected: None,
            earliest_reset: earliest.as_ref().map(|(reset, _)| *reset as f64),
            earliest_reset_route: earliest.map(|(_, route)| route),
            adequate_blockers: Default::default(),
        }
    }
}
