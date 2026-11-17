//! Serving validation and the post-validation declaration identity rule.

use cortexkit_role_routing::declarations::{
    parse_consumer_declarations_serving, ConsumerDeclaration, ConsumerDeclarations,
    DeclarationError, DemandDefaults,
};
use serde_json::Value;

use cortexkit_role_routing::{
    errors::{NoRouteReason, Refusal},
    placement::Placement,
};

use super::super::Caller;

pub(crate) fn placement_module(
    placement: Option<&Placement>,
    caller: &Caller,
    runners: Option<&[String]>,
    is_catalog_runner: impl Fn(&str) -> bool,
) -> Result<Option<String>, Refusal> {
    match placement {
        None => Ok(None),
        Some(Placement::Module { module }) => Ok(Some(module.clone())),
        Some(Placement::CallerRunner(_)) => {
            let module = caller
                .attested_module()
                .filter(|module| is_catalog_runner(module))
                .or_else(|| match runners {
                    Some([module]) if is_catalog_runner(module) => Some(module.as_str()),
                    _ => None,
                });
            module.map(|module| Some(module.to_owned())).ok_or_else(|| {
                let mut refusal = Refusal::no_route(NoRouteReason::NoAvailableModel, None);
                refusal.detail["reason"] = "caller_runner_unresolved".into();
                refusal.message = "caller_runner requires an attested catalog runner or exactly one catalog runner in runners".into();
                refusal
            })
        }
        // A newer placement form must not silently lose its constraint.
        Some(_) => Err(Refusal::no_route(NoRouteReason::HarnessUnavailable, None)),
    }
}

pub(crate) fn parse(slot: &Value) -> Result<ConsumerDeclarations, DeclarationError> {
    #[cfg(test)]
    PARSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    parse_consumer_declarations_serving(slot, &super::super::describe::capabilities())
}

pub(super) fn honor(
    module: &str,
    declaration: &ConsumerDeclaration,
    issues: &mut Vec<String>,
) -> bool {
    let name = &declaration.name;
    if crate::config_policy::consumer_matches(module, name) {
        return true;
    }
    issues.push(format!(
        "ignored consumer {name:?} from module {module:?}: a consumer must equal its module id or start with {module}."
    ));
    false
}

pub(super) fn extended(defaults: &DemandDefaults) -> bool {
    defaults.iq.is_some()
        || defaults.ladder.is_some()
        || defaults.placement.is_some()
        || defaults.selectors.is_some()
        || defaults.kind.is_some()
}

#[cfg(test)]
static PARSES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn parse_count() -> usize {
    PARSES.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
#[path = "declared_tests.rs"]
mod tests;
