//! Operator reasoning-variant overrides (`model_routing.variants`).
//!
//! A selection runs at the first of: the variant the caller named (a pin's
//! `:variant` or a synthesizer pin's variant), the consumer's group, the `all`
//! group, then the curated default on the route's fact row. Keys match routes
//! with the exclusion grammar (`provider`, `provider/model`,
//! `provider/model/variant`); within one group the most specific key wins.
//!
//! The catalog decides which variants exist: Fusiform publishes them as the
//! effort values of `capability.reasoning_options`, and Broca refuses any other.
//! Config load refuses an override the catalog does not publish. When the
//! catalog later drops one, selection skips that override, records why, and
//! uses the next rule instead of failing.

use std::collections::BTreeMap;

use serde::Serialize;
use striatum_core::provider_model_spec::{parse_spec, spec_names_key, ParsedSpec};
use striatum_store::{PublishedEfforts, ReasoningCatalog};

use crate::config_policy::VariantGroups;
use crate::select_policy::Consumer;

/// Which rule chose a selection's variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum VariantSource {
    Pin,
    Consumer,
    All,
    Curated,
}

/// The variant a selection runs at, the rule that chose it, and every
/// configured override that was skipped because the catalog no longer
/// publishes its variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct VariantChoice {
    pub variant: Option<String>,
    pub source: VariantSource,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fallbacks: Vec<String>,
}

impl VariantChoice {
    /// Decision-log lines: the chosen variant and its rule, then each fallback.
    pub fn explanation_lines(&self, route_key: &str) -> Vec<String> {
        let source = match self.source {
            VariantSource::Pin => "the caller's pin",
            VariantSource::Consumer => "model_routing.variants for the consumer",
            VariantSource::All => "model_routing.variants.all",
            VariantSource::Curated => "the curated default",
        };
        let mut lines = vec![format!(
            "variant {} for {route_key} from {source}",
            self.variant.as_deref().unwrap_or("(none)")
        )];
        lines.extend(
            self.fallbacks
                .iter()
                .map(|fallback| format!("variant fallback: {fallback}")),
        );
        lines
    }
}

/// The config group a consumer reads, or `None` for a request that named no consumer.
pub(crate) fn consumer_group(consumer: Option<Consumer>) -> Option<&'static str> {
    consumer.map(|consumer| match consumer {
        Consumer::Mason => "mason",
        Consumer::Sidekick => "sidekick",
        Consumer::Athena => "athena",
        Consumer::Lab => "lab",
        Consumer::Core => "core",
        Consumer::Codemode => "codemode",
        Consumer::Flow => "flow",
    })
}

/// How specifically a spec names routes: a model (longer = narrower) beats a provider.
fn specificity(spec: &ParsedSpec) -> (bool, usize) {
    (
        spec.model.is_some(),
        spec.model.as_ref().map_or(0, String::len),
    )
}

/// The most specific entry naming `route_key`. Ties keep the first key in
/// sorted order so the choice never depends on file order.
fn matching_entry<'a>(
    entries: &'a BTreeMap<String, String>,
    route_key: &str,
) -> Option<(&'a str, &'a str)> {
    let mut best: Option<((bool, usize), &'a str, &'a str)> = None;
    for (key, variant) in entries {
        let Some(spec) = parse_spec(key).filter(|spec| !spec.negated) else {
            continue;
        };
        if !spec_names_key(&spec, route_key) {
            continue;
        }
        let rank = specificity(&spec);
        if best.is_none_or(|(best_rank, _, _)| rank > best_rank) {
            best = Some((rank, key, variant));
        }
    }
    best.map(|(_, key, variant)| (key, variant))
}

/// The catalog entry for a model, following a score alias to the provider
/// whose rows it borrows (an alias has no catalog rows of its own).
pub(crate) fn published<'a>(
    catalog: &'a ReasoningCatalog,
    aliases: &BTreeMap<String, String>,
    provider_id: &str,
    model_id: &str,
) -> Option<&'a PublishedEfforts> {
    catalog.get(provider_id, model_id).or_else(|| {
        aliases
            .get(provider_id)
            .and_then(|target| catalog.get(target, model_id))
    })
}

pub(crate) fn render(values: &[String]) -> String {
    if values.is_empty() {
        "none".into()
    } else {
        values.join(", ")
    }
}

/// One selected route, as the variant rules see it.
pub(crate) struct VariantRequest<'a> {
    pub route_key: &'a str,
    pub provider_id: &'a str,
    pub model_id: &'a str,
    /// A variant the caller named for this selection.
    pub explicit: Option<&'a str>,
    /// The variant the route would run at with no override.
    pub curated: Option<&'a str>,
    pub consumer: Option<&'a str>,
}

/// Choose the variant for one selected route. `catalog` is the live published
/// set; `None` (no store) admits every configured variant, since config load
/// already checked them.
pub(crate) fn resolve(
    groups: &VariantGroups,
    aliases: &BTreeMap<String, String>,
    catalog: Option<&ReasoningCatalog>,
    request: &VariantRequest<'_>,
) -> VariantChoice {
    if let Some(variant) = request.explicit {
        return VariantChoice {
            variant: Some(variant.to_string()),
            source: VariantSource::Pin,
            fallbacks: Vec::new(),
        };
    }
    let mut fallbacks = Vec::new();
    let levels = request
        .consumer
        .and_then(|consumer| {
            groups
                .consumer_groups()
                .into_iter()
                .filter(|(name, entries)| {
                    !entries.is_empty() && crate::config_policy::consumer_matches(name, consumer)
                })
                .max_by_key(|(name, _)| crate::config_policy::consumer_specificity(name))
        })
        .map(|(name, entries)| (name, entries, VariantSource::Consumer))
        .into_iter()
        .chain([("all", &groups.all, VariantSource::All)]);
    for (name, entries, source) in levels {
        let Some((key, variant)) = matching_entry(entries, request.route_key) else {
            continue;
        };
        match catalog
            .and_then(|catalog| published(catalog, aliases, request.provider_id, request.model_id))
        {
            Some(PublishedEfforts::Published(values))
                if !values.iter().any(|value| value == variant) =>
            {
                fallbacks.push(format!(
                    "model_routing.variants.{name}.{key} = {variant} skipped for {}: the catalog no longer publishes it (published: {})",
                    request.route_key,
                    render(values)
                ));
            }
            _ => {
                return VariantChoice {
                    variant: Some(variant.to_string()),
                    source,
                    fallbacks,
                };
            }
        }
    }
    VariantChoice {
        variant: request.curated.map(str::to_string),
        source: VariantSource::Curated,
        fallbacks,
    }
}

/// Check every override against the catalog. Refuses an override whose
/// variant a matched model does not publish, whose model publishes no
/// reasoning efforts, or whose key matches no catalog model. Models the
/// catalog has not been read for yet cannot be judged; they are returned as
/// warnings and checked on the next load after the catalog is read.
pub(crate) fn check_against_catalog(
    groups: &VariantGroups,
    aliases: &BTreeMap<String, String>,
    catalog: &ReasoningCatalog,
) -> Result<Vec<String>, String> {
    let mut warnings = Vec::new();
    for (name, entries) in groups.named() {
        for (key, variant) in entries {
            let Some(spec) = parse_spec(key) else {
                continue;
            };
            let catalog_provider = if catalog.models_of(&spec.provider).next().is_none() {
                aliases
                    .get(&spec.provider)
                    .map_or(spec.provider.as_str(), String::as_str)
            } else {
                spec.provider.as_str()
            };
            let matched: Vec<(&str, &PublishedEfforts)> = catalog
                .models_of(catalog_provider)
                .filter(|(model, _)| match &spec.model {
                    None => true,
                    Some(named) => named == model || named.starts_with(&format!("{model}/")),
                })
                .collect();
            if matched.is_empty() {
                return Err(format!(
                    "model_routing.variants.{name}.{key}: no catalog model matches {key}"
                ));
            }
            for (model, efforts) in matched {
                let route = format!("{}/{model}", spec.provider);
                match efforts {
                    PublishedEfforts::Unread => warnings.push(format!(
                        "model_routing.variants.{name}.{key}: {route} has not been read from the catalog yet; variant {variant} is checked once it is"
                    )),
                    PublishedEfforts::Published(values) if values.is_empty() => {
                        return Err(format!(
                            "model_routing.variants.{name}.{key}: route {route} publishes no reasoning options, so variant {variant} cannot be admitted"
                        ));
                    }
                    PublishedEfforts::Published(values)
                        if !values.iter().any(|value| value == variant) =>
                    {
                        return Err(format!(
                            "model_routing.variants.{name}.{key}: route {route} does not publish variant {variant}; published: {}",
                            render(values)
                        ));
                    }
                    PublishedEfforts::Published(_) => {}
                }
            }
        }
    }
    Ok(warnings)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn groups(entries: &[(&str, &str, &str)]) -> VariantGroups {
        let mut groups = VariantGroups::default();
        for (group, key, variant) in entries {
            let map = match *group {
                "all" => &mut groups.all,
                "mason" => &mut groups.mason,
                "sidekick" => &mut groups.sidekick,
                "athena" => &mut groups.athena,
                "lab" => &mut groups.lab,
                "core" => &mut groups.core,
                other => groups.consumers.entry(other.into()).or_default(),
            };
            map.insert((*key).into(), (*variant).into());
        }
        groups
    }

    fn catalog(entries: &[(&str, Option<&[&str]>)]) -> ReasoningCatalog {
        ReasoningCatalog {
            models: entries
                .iter()
                .map(|(key, values)| {
                    (
                        (*key).to_string(),
                        match values {
                            None => PublishedEfforts::Unread,
                            Some(values) => PublishedEfforts::Published(
                                values.iter().map(|value| (*value).to_string()).collect(),
                            ),
                        },
                    )
                })
                .collect(),
        }
    }

    fn request<'a>(explicit: Option<&'a str>, consumer: Option<Consumer>) -> VariantRequest<'a> {
        VariantRequest {
            route_key: "openai/gpt-6.1-sol",
            provider_id: "openai",
            model_id: "gpt-6.1-sol",
            explicit,
            curated: Some("medium"),
            consumer: consumer_group(consumer),
        }
    }

    #[test]
    fn precedence_runs_pin_then_consumer_then_all_then_curated() {
        let full = groups(&[
            ("all", "openai/gpt-6.1-sol", "high"),
            ("athena", "openai/gpt-6.1-sol", "xhigh"),
        ]);
        let published = catalog(&[(
            "openai/gpt-6.1-sol",
            Some(&["low", "medium", "high", "xhigh"]),
        )]);
        let aliases = BTreeMap::new();
        let choose = |groups: &VariantGroups, explicit, consumer| {
            let choice = resolve(
                groups,
                &aliases,
                Some(&published),
                &request(explicit, consumer),
            );
            (choice.variant.unwrap(), choice.source)
        };
        assert_eq!(
            choose(&full, Some("low"), Some(Consumer::Athena)),
            ("low".into(), VariantSource::Pin)
        );
        assert_eq!(
            choose(&full, None, Some(Consumer::Athena)),
            ("xhigh".into(), VariantSource::Consumer)
        );
        assert_eq!(
            choose(&full, None, Some(Consumer::Mason)),
            ("high".into(), VariantSource::All)
        );
        assert_eq!(
            choose(&full, None, None),
            ("high".into(), VariantSource::All)
        );
        assert_eq!(
            choose(&VariantGroups::default(), None, Some(Consumer::Athena)),
            ("medium".into(), VariantSource::Curated)
        );
    }

    #[test]
    fn the_same_route_runs_xhigh_for_athena_and_high_for_mason() {
        let config = groups(&[
            ("athena", "anthropic/claude-opus-5-5", "xhigh"),
            ("mason", "anthropic/claude-opus-5-5", "high"),
        ]);
        let published = catalog(&[(
            "anthropic/claude-opus-5-5",
            Some(&["low", "medium", "high", "xhigh", "max"]),
        )]);
        let mut request = VariantRequest {
            route_key: "anthropic/claude-opus-5-5",
            provider_id: "anthropic",
            model_id: "claude-opus-5-5",
            explicit: None,
            curated: Some("max"),
            consumer: Some("athena"),
        };
        let aliases = BTreeMap::new();
        let athena = resolve(&config, &aliases, Some(&published), &request);
        request.consumer = Some("mason");
        let mason = resolve(&config, &aliases, Some(&published), &request);
        assert_eq!(athena.variant.as_deref(), Some("xhigh"));
        assert_eq!(mason.variant.as_deref(), Some("high"));
        assert_eq!(
            (athena.source, mason.source),
            (VariantSource::Consumer, VariantSource::Consumer)
        );
    }

    #[test]
    fn nonlegacy_variants_obey_precedence_and_catalog_validation() {
        let config = groups(&[
            ("all", "openai/gpt-6.1-sol", "high"),
            ("mc_summary", "openai/gpt-6.1-sol", "xhigh"),
        ]);
        let published = catalog(&[(
            "openai/gpt-6.1-sol",
            Some(&["low", "medium", "high", "xhigh"]),
        )]);
        let aliases = BTreeMap::new();
        let mut request = VariantRequest {
            consumer: Some("mc_summary.child"),
            ..request(None, None)
        };
        assert!(check_against_catalog(&config, &aliases, &published).is_ok());
        let choice = resolve(&config, &aliases, Some(&published), &request);
        assert_eq!(choice.variant.as_deref(), Some("xhigh"));
        assert_eq!(choice.source, VariantSource::Consumer);
        request.explicit = Some("low");
        assert_eq!(
            resolve(&config, &aliases, Some(&published), &request).source,
            VariantSource::Pin
        );
        request.explicit = None;
        let changed = catalog(&[("openai/gpt-6.1-sol", Some(&["low", "medium", "high"]))]);
        assert!(check_against_catalog(&config, &aliases, &changed).is_err());
        let fallback = resolve(&config, &aliases, Some(&changed), &request);
        assert_eq!(fallback.variant.as_deref(), Some("high"));
        assert_eq!(fallback.source, VariantSource::All);
        assert_eq!(fallback.fallbacks.len(), 1);
        assert_eq!(
            resolve(&VariantGroups::default(), &aliases, None, &request).source,
            VariantSource::Curated
        );
    }

    #[test]
    fn the_most_specific_key_in_a_group_wins() {
        let config = groups(&[
            ("all", "openai", "low"),
            ("all", "openai/gpt-6.1-sol", "high"),
        ]);
        let aliases = BTreeMap::new();
        let choice = resolve(&config, &aliases, None, &request(None, None));
        assert_eq!(choice.variant.as_deref(), Some("high"));
        let mut other = request(None, None);
        other.route_key = "openai/gpt-6.1-luna";
        other.model_id = "gpt-6.1-luna";
        assert_eq!(
            resolve(&config, &aliases, None, &other).variant.as_deref(),
            Some("low")
        );
    }

    #[test]
    fn a_variant_the_catalog_dropped_falls_through_and_is_recorded() {
        let config = groups(&[
            ("athena", "openai/gpt-6.1-sol", "xhigh"),
            ("all", "openai/gpt-6.1-sol", "high"),
        ]);
        let published = catalog(&[("openai/gpt-6.1-sol", Some(&["low", "medium", "high"]))]);
        let aliases = BTreeMap::new();
        let choice = resolve(
            &config,
            &aliases,
            Some(&published),
            &request(None, Some(Consumer::Athena)),
        );
        assert_eq!(choice.variant.as_deref(), Some("high"));
        assert_eq!(choice.source, VariantSource::All);
        assert_eq!(
            choice.fallbacks,
            ["model_routing.variants.athena.openai/gpt-6.1-sol = xhigh skipped for openai/gpt-6.1-sol: the catalog no longer publishes it (published: low, medium, high)"]
        );
        // With both levels dropped, the curated default runs and both are recorded.
        let published = catalog(&[("openai/gpt-6.1-sol", Some(&["medium"]))]);
        let choice = resolve(
            &config,
            &aliases,
            Some(&published),
            &request(None, Some(Consumer::Athena)),
        );
        assert_eq!(
            (
                choice.variant.as_deref(),
                choice.source,
                choice.fallbacks.len()
            ),
            (Some("medium"), VariantSource::Curated, 2)
        );
    }

    #[test]
    fn catalog_check_refuses_unpublished_variants_and_names_the_published_list() {
        let aliases = BTreeMap::from([("anthropic-platform".to_string(), "anthropic".to_string())]);
        let published = catalog(&[
            (
                "anthropic/claude-opus-5-5",
                Some(&["low", "medium", "high", "xhigh", "max"]),
            ),
            ("openai/gpt-6.1-sol", Some(&["low", "medium", "high"])),
            ("deepseek/deepseek-flash", Some(&[])),
            ("google/gemini-4-argon", None),
        ]);
        let accepted = groups(&[
            ("athena", "anthropic/claude-opus-5-5", "xhigh"),
            ("mason", "anthropic-platform/claude-opus-5-5", "high"),
            ("all", "google/gemini-4-argon", "high"),
        ]);
        let warnings = check_against_catalog(&accepted, &aliases, &published).unwrap();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("google/gemini-4-argon has not been read"));
        for (config, message) in [
            (
                groups(&[("athena", "openai/gpt-6.1-sol", "xhigh")]),
                "model_routing.variants.athena.openai/gpt-6.1-sol: route openai/gpt-6.1-sol does not publish variant xhigh; published: low, medium, high",
            ),
            (
                groups(&[("all", "deepseek/deepseek-flash", "high")]),
                "route deepseek/deepseek-flash publishes no reasoning options",
            ),
            (
                groups(&[("core", "openai/gpt-7", "high")]),
                "model_routing.variants.core.openai/gpt-7: no catalog model matches openai/gpt-7",
            ),
        ] {
            let error = check_against_catalog(&config, &aliases, &published).unwrap_err();
            assert!(error.contains(message), "{error}");
        }
    }
}
