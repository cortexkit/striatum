//! Serving validation and the post-validation declaration identity rule.

use cortexkit_role_routing::declarations::{
    parse_consumer_declarations_serving, ConsumerDeclaration, ConsumerDeclarations,
    DeclarationError,
};
use serde_json::Value;

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
    if name == module
        || name
            .strip_prefix(module)
            .is_some_and(|tail| tail.starts_with('.'))
    {
        return true;
    }
    let defaults = &declaration.defaults;
    let extended = defaults.iq.is_some()
        || defaults.ladder.is_some()
        || defaults.placement.is_some()
        || defaults.selectors.is_some()
        || defaults.kind.is_some();
    if extended {
        issues.push(format!(
            "ignored consumer {name:?} from module {module:?}: a consumer using declared defaults must equal its module id or start with {module}."
        ));
        false
    } else {
        // Legacy names remain owned and keep their demand defaults until the
        // modules and settings move to module-qualified consumer names together.
        // Remove this exception in the same release that removes the legacy
        // `substrates` settings and the legacy `route.select` adapter.
        issues.push(format!(
            "transitional consumer {name:?} from module {module:?}: outside the module namespace; legacy demand defaults and admission retained"
        ));
        true
    }
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
