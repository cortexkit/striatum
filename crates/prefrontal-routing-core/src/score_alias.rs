//! Routing providers that borrow another provider's catalog rows.
//!
//! A score alias such as `{"anthropic-platform": "anthropic"}` names a routing
//! provider with no catalog rows of its own. Its routes are the target
//! provider's routes under the alias's name: the same model ids, quality,
//! capability, context and speed. What differs is billing. The alias bills its
//! own credential, so the read-time subscription view of the target row (its
//! subsidy multiplier and billing plane) is never copied onto the alias row.

use std::collections::BTreeMap;

use crate::model_facts::ModelFact;

/// The provider an alias borrows rows from, when `provider` is an alias. An
/// entry whose target is itself an alias, or that names itself, is ignored: a
/// chain would make the borrowed row depend on map order.
pub fn alias_target<'a>(aliases: &'a BTreeMap<String, String>, provider: &str) -> Option<&'a str> {
    let target = aliases.get(provider)?;
    (target != provider && !aliases.contains_key(target.as_str())).then_some(target.as_str())
}

/// The catalog key (`provider/model`) whose rates price `route_key`: the target
/// provider's key for an aliased provider, the route key itself otherwise.
pub fn catalog_key(aliases: &BTreeMap<String, String>, route_key: &str) -> String {
    match route_key.split_once('/') {
        Some((provider, rest)) => match alias_target(aliases, provider) {
            Some(target) => format!("{target}/{rest}"),
            None => route_key.to_string(),
        },
        None => route_key.to_string(),
    }
}

/// Append one row per aliased provider for every row of its target provider.
///
/// Run this after read-time overlays have been applied to the stored rows, so
/// the alias inherits the target's live quality data (Elo overrides, observed
/// speed), and before pricing, which attaches the alias's own billing plane.
/// A stored row the alias already owns wins over a borrowed one.
pub fn append_alias_rows(facts: &mut Vec<ModelFact>, aliases: &BTreeMap<String, String>) {
    let mut borrowed = Vec::new();
    for alias in aliases.keys() {
        let Some(target) = alias_target(aliases, alias) else {
            continue;
        };
        for fact in facts.iter().filter(|fact| fact.provider_id == target) {
            let owned = facts.iter().any(|existing| {
                existing.provider_id == *alias
                    && existing.model_id == fact.model_id
                    && existing.variant == fact.variant
            });
            if owned {
                continue;
            }
            let mut row = fact.clone();
            row.provider_id = alias.clone();
            // The target's subsidy multiplier and billing plane describe the
            // target's logins. The alias bills a metered credential of its own.
            row.subscription_multiplier = None;
            row.billing_plane = None;
            borrowed.push(row);
        }
    }
    facts.extend(borrowed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_facts_seed::model_facts_seed;

    fn aliases() -> BTreeMap<String, String> {
        BTreeMap::from([("anthropic-platform".to_string(), "anthropic".to_string())])
    }

    #[test]
    fn alias_rows_copy_quality_and_drop_the_subscription_view() {
        let mut facts: Vec<ModelFact> = model_facts_seed()
            .iter()
            .filter(|fact| fact.provider_id == "anthropic")
            .cloned()
            .collect();
        assert!(!facts.is_empty(), "seed has anthropic rows");
        facts[0].subscription_multiplier = Some(12.0);
        let source = facts[0].clone();
        let stored = facts.len();
        append_alias_rows(&mut facts, &aliases());
        assert_eq!(facts.len(), stored * 2);
        let alias = facts
            .iter()
            .find(|fact| {
                fact.provider_id == "anthropic-platform"
                    && fact.model_id == source.model_id
                    && fact.variant == source.variant
            })
            .expect("alias row for the first anthropic row");
        assert_eq!(alias.scores, source.scores);
        assert_eq!(alias.context_window, source.context_window);
        assert_eq!(alias.supports_tool_use, source.supports_tool_use);
        assert_eq!(alias.family, source.family);
        assert_eq!(alias.subscription_multiplier, None);
        assert_eq!(alias.billing_plane, None);

        // Appending again adds nothing: rows the alias owns win.
        append_alias_rows(&mut facts, &aliases());
        assert_eq!(facts.len(), stored * 2);
    }

    #[test]
    fn chains_and_self_aliases_are_ignored() {
        let chained = BTreeMap::from([
            ("a".to_string(), "b".to_string()),
            ("b".to_string(), "c".to_string()),
            ("d".to_string(), "d".to_string()),
        ]);
        assert_eq!(alias_target(&chained, "a"), None);
        assert_eq!(alias_target(&chained, "b"), Some("c"));
        assert_eq!(alias_target(&chained, "d"), None);
        assert_eq!(
            catalog_key(&aliases(), "anthropic-platform/claude-opus-4-1"),
            "anthropic/claude-opus-4-1"
        );
        assert_eq!(
            catalog_key(&aliases(), "anthropic/claude-opus-4-1"),
            "anthropic/claude-opus-4-1"
        );
    }
}
