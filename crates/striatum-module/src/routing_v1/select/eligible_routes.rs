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
        explicit: Option<&str>,
    ) -> Cow<'_, HashMap<String, String>> {
        let Some(eligible) = self.eligible_routes else {
            return Cow::Borrowed(self.rejections);
        };
        let mut rejections = self.rejections.clone();
        let catalog =
            crate::config_overrides::reasoning_catalog_for(self.sources.operator_config_path);
        for fact in facts {
            let route = striatum_core::fact_to_route(fact);
            let choice = crate::variant_policy::resolve(
                self.sources.operator_config.variant_groups(),
                self.sources.catalog_targets,
                catalog.as_ref(),
                &crate::variant_policy::VariantRequest {
                    route_key: &route.route_key,
                    provider_id: &fact.provider_id,
                    model_id: &fact.model_id,
                    explicit,
                    curated: fact
                        .prompt_defaults
                        .as_ref()
                        .and_then(|params| params.variant.as_deref())
                        .or(fact.variant.as_deref()),
                    consumer: Some(self.consumer),
                },
            );
            if !eligible.contains(&RouteRef {
                provider: fact.provider_id.clone(),
                model: fact.model_id.clone(),
                variant: choice.variant,
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
        let mut served = selected.clone();
        crate::apply_variant_policy(
            &mut served,
            self.sources.operator_config,
            self.sources.operator_config_path,
            self.sources.catalog_targets,
            Some(self.consumer),
            explicit,
        );
        if eligible.contains(&RouteRef {
            provider: served.model.provider_id,
            model: served.model.model_id,
            variant: served.prompt_params.variant,
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
