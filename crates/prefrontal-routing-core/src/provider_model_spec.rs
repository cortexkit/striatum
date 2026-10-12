//! Provider/model spec matcher for the routing config lists (exclude,
//! subscription). Ported VERBATIM from the TS
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

/// The two exclusion lists a selection applies, deliberately kept apart.
///
/// `operator` is the standing fence the machine's owner wrote in the user-global
/// config; `caller` is what a single call adds on top (the route that just failed,
/// a consult's per-attempt exclusions). They are resolved SEPARATELY and the
/// results OR-ed. Concatenating them into one list would let a caller negation
/// ("-anthropic") cancel an operator entry, because within one list the last
/// matching entry wins: a caller must only ever be able to ADD an exclusion.
///
/// `explicit_only` is a third, provider-level list: providers the operator keeps
/// out of every automatic choice but still admits when a caller pins them by
/// name. Automatic selection passes it; pinned selection leaves it empty. It is
/// not an exclusion entry because the operator's `exclude` refuses pins too.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExclusionPolicy<'a> {
    pub operator: &'a [String],
    pub caller: &'a [String],
    pub explicit_only: &'a [String],
}

impl<'a> ExclusionPolicy<'a> {
    /// Per-call exclusions with no operator fence attached. Used by selection
    /// paths that already meet the fence another way (they run against an
    /// available-model set the fence has filtered) and by pinned selections,
    /// which bypass exclusion by design.
    pub fn caller_only(caller: &'a [String]) -> Self {
        Self {
            operator: &[],
            caller,
            explicit_only: &[],
        }
    }

    /// Neither list. Explicit roster pins admit with no exclusion applied at all.
    pub fn none() -> ExclusionPolicy<'static> {
        ExclusionPolicy {
            operator: &[],
            caller: &[],
            explicit_only: &[],
        }
    }
}

/// Which of the two lists excluded a route. They stay distinct because they mean
/// different things to whoever reads the rejection: the operator fence is a
/// standing policy decision, a caller exclusion is one attempt's local choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionSource {
    Operator,
    Caller,
    /// The provider is on the operator's explicit-only list and this selection
    /// was automatic.
    ExplicitOnly,
}

/// Both exclusion lists parsed once for a selection pass.
#[derive(Debug, Clone, Default)]
pub struct ExclusionSpecs {
    operator: Vec<ParsedSpec>,
    caller: Vec<ParsedSpec>,
    explicit_only: Vec<String>,
}

impl ExclusionSpecs {
    pub fn parse(policy: ExclusionPolicy<'_>) -> Self {
        Self {
            operator: parse_spec_list(policy.operator),
            caller: parse_spec_list(policy.caller),
            explicit_only: policy
                .explicit_only
                .iter()
                .map(|provider| provider.trim().to_string())
                .filter(|provider| !provider.is_empty())
                .collect(),
        }
    }

    /// The list that excludes `key`, if either does. The operator fence is checked
    /// first so a route that both lists name is reported as the standing policy
    /// decision it is, not as one attempt's local exclusion.
    pub fn source_for(&self, key: &str) -> Option<ExclusionSource> {
        if resolve_spec(&self.operator, key, false) {
            return Some(ExclusionSource::Operator);
        }
        let provider = key_provider(key);
        if self.explicit_only.iter().any(|entry| entry == provider) {
            return Some(ExclusionSource::ExplicitOnly);
        }
        if resolve_spec(&self.caller, key, false) {
            return Some(ExclusionSource::Caller);
        }
        None
    }

    pub fn excludes(&self, key: &str) -> bool {
        self.source_for(key).is_some()
    }
}

/// Remove keys matching an exclude spec list from an available-model set. base =
/// false (nothing excluded); a positive match excludes, a negation keeps the key.
pub fn filter_excluded_keys(
    available: &std::collections::HashSet<String>,
    exclude_entries: &[String],
) -> std::collections::HashSet<String> {
    filter_excluded_keys_with_policy(available, ExclusionPolicy::caller_only(exclude_entries))
}

/// Remove every key either exclusion list rejects. Union semantics: see
/// [`ExclusionPolicy`] for why the two lists are never concatenated.
pub fn filter_excluded_keys_with_policy(
    available: &std::collections::HashSet<String>,
    policy: ExclusionPolicy<'_>,
) -> std::collections::HashSet<String> {
    if policy.operator.is_empty() && policy.caller.is_empty() && policy.explicit_only.is_empty() {
        return available.clone();
    }
    let specs = ExclusionSpecs::parse(policy);
    available
        .iter()
        .filter(|key| !specs.excludes(key))
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
    fn caller_negation_cannot_lift_an_operator_exclusion() {
        // The operator fences the whole provider; the caller tries to negate it.
        // Resolved as one concatenated list the caller would win (last match wins),
        // which is exactly what union semantics must prevent.
        let operator = vec!["anthropic".to_string()];
        let caller = vec!["-anthropic".to_string()];
        let specs = ExclusionSpecs::parse(ExclusionPolicy {
            operator: &operator,
            caller: &caller,
            explicit_only: &[],
        });
        assert_eq!(
            specs.source_for("anthropic/claude-opus-4-8"),
            Some(ExclusionSource::Operator)
        );

        // A caller-only exclusion still reports as the caller's own.
        let caller_only = vec!["openai/gpt-5.4".to_string()];
        let specs = ExclusionSpecs::parse(ExclusionPolicy::caller_only(&caller_only));
        assert_eq!(
            specs.source_for("openai/gpt-5.4"),
            Some(ExclusionSource::Caller)
        );
        assert_eq!(specs.source_for("openai/gpt-5.5"), None);

        // An operator negation inside the operator's OWN list still works: the
        // union is between lists, not within one.
        let operator = vec!["openai".to_string(), "-openai/gpt-5.5".to_string()];
        let specs = ExclusionSpecs::parse(ExclusionPolicy {
            operator: &operator,
            caller: &[],
            explicit_only: &[],
        });
        assert_eq!(specs.source_for("openai/gpt-5.5"), None);
        assert_eq!(
            specs.source_for("openai/gpt-5.4"),
            Some(ExclusionSource::Operator)
        );
    }

    #[test]
    fn explicit_only_providers_are_named_by_provider_and_never_by_prefix() {
        let explicit_only = vec!["anthropic-platform".to_string()];
        let specs = ExclusionSpecs::parse(ExclusionPolicy {
            explicit_only: &explicit_only,
            ..ExclusionPolicy::default()
        });
        assert_eq!(
            specs.source_for("anthropic-platform/claude-opus-4-1"),
            Some(ExclusionSource::ExplicitOnly)
        );
        // The subscription provider whose name the platform provider extends
        // stays an ordinary automatic candidate.
        assert_eq!(specs.source_for("anthropic/claude-opus-4-1"), None);
        // A pinned selection passes no explicit-only list and admits the route.
        assert_eq!(
            ExclusionSpecs::parse(ExclusionPolicy::none())
                .source_for("anthropic-platform/claude-opus-4-1"),
            None
        );
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
