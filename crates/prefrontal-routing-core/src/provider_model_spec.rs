//! Provider/model spec matcher for the routing config lists (exclude,
//! subscription, prefer_fast_when_relaxed). Ported VERBATIM from the TS
//! `provider-model-spec.ts` so config resolution matches the plugin exactly.
//!
//! Grammar: a list of entries, each one of:
//!   provider              e.g. "anthropic"            (provider-level)
//!   provider/model        e.g. "openai/gpt-5.4-mini"  (model-level)
//!   -provider             negation                    (provider-level)
//!   -provider/model       negation                    (model-level)
//!
//! Resolution against a base value: provider-level entries override base, then
//! model-level entries override provider (last match wins in each pass), so
//! model beats provider beats base, and config always beats base. A model-level
//! spec `provider/model` matches both the bare capacity key and any variant route
//! key under it.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSpec {
    pub negated: bool,
    pub provider: String,
    /// `None` → a provider-level entry (matches every model under the provider).
    pub model: Option<String>,
}

pub fn parse_spec(entry: &str) -> Option<ParsedSpec> {
    let trimmed = entry.trim();
    if trimmed.is_empty() {
        return None;
    }
    let negated = trimmed.starts_with('-');
    let body = if negated {
        trimmed[1..].trim()
    } else {
        trimmed
    };
    if body.is_empty() {
        return None;
    }
    match body.find('/') {
        None => Some(ParsedSpec {
            negated,
            provider: body.to_string(),
            model: None,
        }),
        Some(slash) => {
            let provider = &body[..slash];
            let model = &body[slash + 1..];
            if provider.is_empty() || model.is_empty() {
                return None;
            }
            Some(ParsedSpec {
                negated,
                provider: provider.to_string(),
                model: Some(model.to_string()),
            })
        }
    }
}

pub fn parse_spec_list(entries: &[String]) -> Vec<ParsedSpec> {
    entries.iter().filter_map(|e| parse_spec(e)).collect()
}

/// First path segment of a route/capacity key is the provider.
fn key_provider(key: &str) -> &str {
    match key.find('/') {
        None => key,
        Some(slash) => &key[..slash],
    }
}

fn spec_matches_key(spec: &ParsedSpec, key: &str) -> bool {
    if key_provider(key) != spec.provider {
        return false;
    }
    match &spec.model {
        None => true,
        Some(model) => {
            let prefix = format!("{}/{}", spec.provider, model);
            key == prefix || key.starts_with(&format!("{prefix}/"))
        }
    }
}

/// Resolve a boolean for a key against parsed specs, starting from `base`.
/// Provider-level matches apply first, then model-level (more specific) win.
pub fn resolve_spec(specs: &[ParsedSpec], key: &str, base: bool) -> bool {
    let mut result = base;
    for s in specs {
        if s.model.is_none() && spec_matches_key(s, key) {
            result = !s.negated;
        }
    }
    for s in specs {
        if s.model.is_some() && spec_matches_key(s, key) {
            result = !s.negated;
        }
    }
    result
}

/// Remove keys matching an exclude spec list from an available-model set. base =
/// false (nothing excluded); a positive match excludes, a negation keeps the key.
pub fn filter_excluded_keys(
    available: &std::collections::HashSet<String>,
    exclude_entries: &[String],
) -> std::collections::HashSet<String> {
    let specs = parse_spec_list(exclude_entries);
    if specs.is_empty() {
        return available.clone();
    }
    available
        .iter()
        .filter(|key| !resolve_spec(&specs, key, false))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs(list: &[&str]) -> Vec<ParsedSpec> {
        parse_spec_list(&list.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn parses_provider_model_and_negation() {
        assert_eq!(
            parse_spec("anthropic"),
            Some(ParsedSpec {
                negated: false,
                provider: "anthropic".into(),
                model: None
            })
        );
        assert_eq!(
            parse_spec("-openai/gpt-5.4"),
            Some(ParsedSpec {
                negated: true,
                provider: "openai".into(),
                model: Some("gpt-5.4".into())
            })
        );
        assert_eq!(parse_spec("  "), None);
        assert_eq!(parse_spec("-"), None);
        assert_eq!(parse_spec("openai/"), None);
    }

    #[test]
    fn model_level_beats_provider_level_beats_base() {
        // Provider excludes all openai, but a model-level negation keeps one model.
        let s = specs(&["openai", "-openai/gpt-5.5"]);
        assert!(resolve_spec(&s, "openai/gpt-5.4", false)); // excluded by provider
        assert!(!resolve_spec(&s, "openai/gpt-5.5", false)); // kept by model negation
    }

    #[test]
    fn model_spec_matches_bare_capacity_and_variant_routes() {
        let s = specs(&["openai/gpt-5.5"]);
        assert!(resolve_spec(&s, "openai/gpt-5.5", false)); // capacity key
        assert!(resolve_spec(&s, "openai/gpt-5.5/high", false)); // variant route
        assert!(!resolve_spec(&s, "openai/gpt-5.5-mini", false)); // different model, no prefix match
    }

    #[test]
    fn filter_excluded_keys_drops_matches_keeps_negations() {
        let available: std::collections::HashSet<String> =
            ["anthropic/opus", "openai/gpt-5.5", "openai/gpt-5.4"]
                .iter()
                .map(|s| s.to_string())
                .collect();
        let out = filter_excluded_keys(&available, &["openai".into(), "-openai/gpt-5.5".into()]);
        assert!(out.contains("anthropic/opus"));
        assert!(out.contains("openai/gpt-5.5")); // kept by negation
        assert!(!out.contains("openai/gpt-5.4")); // excluded
    }
}
