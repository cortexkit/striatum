//! Routing providers with no catalog rows of their own.
//!
//! A provider such as `anthropic-platform` (the paid Anthropic API keys) is a
//! name the operator maps vault credentials to, but Fusiform publishes no rows
//! for it. Such a provider resolves to one catalog provider through the vault:
//! the Fusiform provider whose id equals a vendor in the `serves` lists of the
//! active credentials mapped to it (see [`resolve_catalog_targets`]). The
//! result is a map from each resolved provider to its catalog provider, which
//! every other function here reads.
//!
//! A resolved provider's routes are the catalog provider's routes under the
//! resolved provider's name: the same model ids, quality, capability, context
//! and speed. What differs is billing. The resolved provider bills its own
//! credential, so the read-time subscription view of the catalog row (its
//! subsidy multiplier and billing plane) is never copied onto the borrowed row.
//!
//! The module keeps its historical name. The config loader still parses the
//! operator's old `score_alias` map, but nothing here reads it: the vault is
//! the only source of the mapping.

use std::collections::{BTreeMap, BTreeSet};

use crate::model_facts::ModelFact;

/// What one active `llm-provider` vault credential says about providers: the
/// routing provider ids the operator mapped it to (`provider_ids`) and the
/// model vendors it serves (`serves`).
#[derive(Debug, Clone, Copy)]
pub struct CredentialVendors<'a> {
    pub provider_ids: &'a [String],
    pub serves: &'a [String],
}

/// The outcome of resolving providers with no catalog rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogResolution {
    /// Each resolved provider and the one catalog provider it borrows rows
    /// from. Every target is a provider with catalog rows, so no target is
    /// itself a key here.
    pub targets: BTreeMap<String, String>,
    /// Each provider with no catalog rows that did not resolve, with the
    /// catalog vendors its credentials serve: empty when none does, two or
    /// more when the choice is ambiguous. Such a provider gets no scores and
    /// no prices, so it is never selected.
    pub unresolved: BTreeMap<String, Vec<String>>,
}

/// Resolve every provider that active credentials are mapped to but that has
/// no catalog rows.
///
/// `catalog_providers` holds the provider ids that have catalog rows. For each
/// other provider named in a credential's `provider_ids`, the candidates are
/// the vendors in the `serves` lists of all credentials mapped to it that are
/// themselves catalog providers. Exactly one candidate resolves the provider;
/// none or several leave it unresolved. Pass only active `llm-provider`
/// credentials: a revoked key must not decide where a provider's rows come
/// from.
pub fn resolve_catalog_targets<'a>(
    catalog_providers: &BTreeSet<String>,
    credentials: impl IntoIterator<Item = CredentialVendors<'a>>,
) -> CatalogResolution {
    let mut candidates: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for credential in credentials {
        for provider in credential.provider_ids {
            if provider.is_empty() || catalog_providers.contains(provider) {
                continue;
            }
            candidates.entry(provider.clone()).or_default().extend(
                credential
                    .serves
                    .iter()
                    .filter(|vendor| catalog_providers.contains(*vendor))
                    .cloned(),
            );
        }
    }
    let mut resolution = CatalogResolution::default();
    for (provider, vendors) in candidates {
        if vendors.len() == 1 {
            let target = vendors.into_iter().next().expect("one vendor");
            resolution.targets.insert(provider, target);
        } else {
            resolution
                .unresolved
                .insert(provider, vendors.into_iter().collect());
        }
    }
    resolution
}

/// The catalog provider `provider` borrows rows from, when it is a resolved
/// provider. An entry whose target is itself a key, or that names itself, is
/// ignored: a chain would make the borrowed row depend on map order.
/// [`resolve_catalog_targets`] never produces either.
pub fn alias_target<'a>(aliases: &'a BTreeMap<String, String>, provider: &str) -> Option<&'a str> {
    let target = aliases.get(provider)?;
    (target != provider && !aliases.contains_key(target.as_str())).then_some(target.as_str())
}

/// The catalog key (`provider/model`) whose rates price `route_key`: the target
/// provider's key for a resolved provider, the route key itself otherwise.
pub fn catalog_key(aliases: &BTreeMap<String, String>, route_key: &str) -> String {
    match route_key.split_once('/') {
        Some((provider, rest)) => match alias_target(aliases, provider) {
            Some(target) => format!("{target}/{rest}"),
            None => route_key.to_string(),
        },
        None => route_key.to_string(),
    }
}

/// Append one row per resolved provider for every row of its target provider.
///
/// Run this after read-time overlays have been applied to the stored rows, so
/// the borrowed row carries the target's live quality data (Elo overrides,
/// observed speed), and before pricing, which attaches the resolved provider's
/// own billing plane. A stored row the resolved provider already owns wins over
/// a borrowed one for scores and policy, but catalog classifications still come
/// from the model it serves: an alias cannot turn an engine into a chat model.
pub fn append_alias_rows(facts: &mut Vec<ModelFact>, aliases: &BTreeMap<String, String>) {
    let mut borrowed = Vec::new();
    let mut classifications = Vec::new();
    for alias in aliases.keys() {
        let Some(target) = alias_target(aliases, alias) else {
            continue;
        };
        for fact in facts.iter().filter(|fact| fact.provider_id == target) {
            for (index, _) in facts.iter().enumerate().filter(|(_, existing)| {
                existing.provider_id == *alias && existing.model_id == fact.model_id
            }) {
                classifications.push((index, fact.model_type.clone(), fact.model_kind.clone()));
            }
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
            // target's logins. The resolved provider bills a credential of its own.
            row.subscription_multiplier = None;
            row.billing_plane = None;
            borrowed.push(row);
        }
    }
    for (index, model_type, model_kind) in classifications {
        facts[index].model_type = model_type;
        facts[index].model_kind = model_kind;
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
        facts[0].model_type = Some("transcription".into());
        facts[0].model_kind = Some("decision".into());
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
        assert_eq!(alias.model_type, source.model_type);
        assert_eq!(alias.model_kind, source.model_kind);
        assert_eq!(alias.subscription_multiplier, None);
        assert_eq!(alias.billing_plane, None);

        // Appending again adds nothing: rows the alias owns win.
        append_alias_rows(&mut facts, &aliases());
        assert_eq!(facts.len(), stored * 2);
    }

    #[test]
    fn an_owned_alias_row_cannot_erase_the_catalog_classification() {
        let mut source = model_facts_seed().iter()
            .find(|fact| fact.provider_id == "anthropic").unwrap().clone();
        source.model_type = Some("image".into());
        source.model_kind = Some("extraction".into());
        let mut owned = source.clone();
        owned.provider_id = "anthropic-platform".into();
        owned.variant = Some("high".into());
        owned.model_type = None;
        owned.model_kind = None;
        owned.scores.elo = 1234.0;
        let mut facts = vec![source, owned];
        append_alias_rows(&mut facts, &aliases());
        assert_eq!(facts.len(), 3);
        assert_eq!(facts[1].model_type.as_deref(), Some("image"));
        assert_eq!(facts[1].model_kind.as_deref(), Some("extraction"));
        assert_eq!(facts[1].scores.elo, 1234.0);
        facts[0].model_type = None;
        facts[0].model_kind = None;
        append_alias_rows(&mut facts, &aliases());
        assert_eq!(facts[1].model_type, None);
        assert_eq!(facts[1].model_kind, None);
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

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn catalog(providers: &[&str]) -> BTreeSet<String> {
        providers
            .iter()
            .map(|provider| (*provider).to_string())
            .collect()
    }

    #[test]
    fn a_provider_without_rows_resolves_through_its_credentials_serves_vendor() {
        let catalog = catalog(&["anthropic", "openai", "meta"]);
        let (work, team) = (ids(&["anthropic-platform"]), ids(&["anthropic"]));
        // A login mapped to a catalog provider is not a resolution input.
        let (login, login_serves) = (ids(&["openai"]), ids(&["openai"]));
        let resolution = resolve_catalog_targets(
            &catalog,
            [
                CredentialVendors {
                    provider_ids: &work,
                    serves: &team,
                },
                CredentialVendors {
                    provider_ids: &work,
                    serves: &team,
                },
                CredentialVendors {
                    provider_ids: &login,
                    serves: &login_serves,
                },
            ],
        );
        assert_eq!(resolution.targets, aliases());
        assert!(resolution.unresolved.is_empty(), "{resolution:?}");
    }

    #[test]
    fn two_candidate_vendors_or_none_leave_the_provider_unresolved() {
        let catalog = catalog(&["anthropic", "openai"]);
        let platform = ids(&["platform"]);
        let (anthropic, openai) = (ids(&["anthropic"]), ids(&["openai"]));
        // Two keys of one provider serving different catalog vendors.
        let ambiguous = resolve_catalog_targets(
            &catalog,
            [
                CredentialVendors {
                    provider_ids: &platform,
                    serves: &anthropic,
                },
                CredentialVendors {
                    provider_ids: &platform,
                    serves: &openai,
                },
            ],
        );
        assert!(ambiguous.targets.is_empty(), "{ambiguous:?}");
        assert_eq!(
            ambiguous.unresolved["platform"],
            ids(&["anthropic", "openai"])
        );
        // One key serving two catalog vendors is just as ambiguous.
        let both = ids(&["anthropic", "openai"]);
        let one_key = resolve_catalog_targets(
            &catalog,
            [CredentialVendors {
                provider_ids: &platform,
                serves: &both,
            }],
        );
        assert!(one_key.targets.is_empty(), "{one_key:?}");
        // A vendor with no catalog rows is no candidate.
        let unknown = ids(&["vendor-without-rows"]);
        let none = resolve_catalog_targets(
            &catalog,
            [CredentialVendors {
                provider_ids: &platform,
                serves: &unknown,
            }],
        );
        assert!(none.targets.is_empty(), "{none:?}");
        assert_eq!(none.unresolved["platform"], Vec::<String>::new());
    }
}
