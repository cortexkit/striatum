//! Per-call eligibility narrows candidates using the route that would be served,
//! including the variant chosen by policy, without changing that policy.

use std::{borrow::Cow, collections::HashMap};

use super::{ModelFact, PinUnavailableReason, Refusal, RouteRef, SelectedModelRoute, Selection};

pub(super) fn includes_model(eligible: Option<&[RouteRef]>, fact: &ModelFact) -> bool {
    eligible.is_none_or(|routes| {
        routes
            .iter()
            .any(|route| route.provider == fact.provider_id && route.model == fact.model_id)
    })
}

impl Selection<'_> {
    pub(super) fn eligible_rejections(
        &self,
        facts: &[ModelFact],
        variants: &HashMap<String, Option<String>>,
    ) -> Cow<'_, HashMap<String, String>> {
        let Some(eligible) = self.eligible_routes else {
            return Cow::Borrowed(self.rejections);
        };
        let mut rejections = self.rejections.clone();
        for fact in facts {
            let route = striatum_core::fact_to_route(fact);
            if !eligible.contains(&RouteRef {
                provider: fact.provider_id.clone(),
                model: fact.model_id.clone(),
                variant: variants.get(&route.route_key).cloned().flatten(),
            }) {
                // Keep facts in scoring so eligibility cannot change the scores
                // used to measure IQ. Reject by exact route key because capacity
                // availability would also admit other catalog variants.
                rejections.insert(route.route_key, "not_available".into());
            }
        }
        Cow::Owned(rejections)
    }

    pub(super) fn check_eligible_pin(
        &self,
        pin: &str,
        selected: &SelectedModelRoute,
        explicit: Option<&str>,
    ) -> Result<(), Refusal> {
        let Some(eligible) = self.eligible_routes else {
            return Ok(());
        };
        // Pin admission can synthesize a variant absent from catalog rows. Test
        // its actual served identity, not the variant-less capacity key.
        let fact = self
            .facts
            .iter()
            .find(|fact| {
                fact.provider_id == selected.model.provider_id
                    && fact.model_id == selected.model.model_id
            })
            .expect("pin admission selected a catalog fact");
        let choice = self
            .effort(
                fact,
                !self.panel_seat,
                if self.panel_seat { None } else { explicit },
            )
            .expect("pin or admitted panel seat has a catalog effort");
        if eligible.contains(&RouteRef {
            provider: selected.model.provider_id.clone(),
            model: selected.model.model_id.clone(),
            variant: choice.variant,
        }) {
            Ok(())
        } else {
            Err(Refusal::pin_unavailable(
                pin,
                PinUnavailableReason::NotAvailable,
                None,
            ))
        }
    }
}
