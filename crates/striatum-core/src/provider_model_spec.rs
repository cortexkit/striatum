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

/// Whether one parsed spec names `key` (ignoring its negation). The variant
/// overrides use this to match their keys exactly as exclusion entries match.
pub fn spec_names_key(spec: &ParsedSpec, key: &str) -> bool {
    spec_matches_key(spec, key)
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

/// Provider rules resolved without reparsing or scanning unrelated rules.
#[derive(Debug, Clone, Default)]
pub struct CompiledSpecs {
    by_provider: std::collections::HashMap<Box<str>, ProviderSpecs>,
}

/// Lets callers using parsed lists share an interface with precompiled rules.
pub trait SpecResolver {
    fn resolve(&self, key: &str, base: bool) -> bool;
}

impl SpecResolver for CompiledSpecs {
    fn resolve(&self, key: &str, base: bool) -> bool {
        CompiledSpecs::resolve(self, key, base)
    }
}

impl SpecResolver for [ParsedSpec] {
    fn resolve(&self, key: &str, base: bool) -> bool {
        resolve_spec(self, key, base)
    }
}

impl<const N: usize> SpecResolver for [ParsedSpec; N] {
    fn resolve(&self, key: &str, base: bool) -> bool {
        resolve_spec(self, key, base)
    }
}

impl SpecResolver for Vec<ParsedSpec> {
    fn resolve(&self, key: &str, base: bool) -> bool {
        resolve_spec(self, key, base)
    }
}

#[derive(Debug, Clone, Default)]
struct ProviderSpecs {
    provider_level: Option<bool>,
    models: std::collections::HashMap<Box<str>, (usize, bool)>,
}

impl CompiledSpecs {
    pub fn compile(entries: &[String]) -> Self {
        let mut compiled = Self::default();
        for (index, spec) in entries
            .iter()
            .filter_map(|entry| parse_spec(entry))
            .enumerate()
        {
            let provider = compiled
                .by_provider
                .entry(spec.provider.into())
                .or_default();
            match spec.model {
                Some(model) => {
                    provider.models.insert(model.into(), (index, !spec.negated));
                }
                None => provider.provider_level = Some(!spec.negated),
            }
        }
        compiled
    }

    /// Model rules outrank provider rules, even a later provider rule. Among
    /// matching model prefixes, list order wins rather than prefix length.
    pub fn override_for(&self, key: &str) -> Option<bool> {
        let (provider, model) = key
            .split_once('/')
            .map_or((key, None), |(p, m)| (p, Some(m)));
        let rules = self.by_provider.get(provider)?;
        let mut best = None;
        if let Some(model) = model {
            for end in model
                .match_indices('/')
                .map(|(index, _)| index)
                .chain(std::iter::once(model.len()))
            {
                if let Some(&(index, value)) = rules.models.get(&model[..end]) {
                    if best.is_none_or(|(previous, _)| index > previous) {
                        best = Some((index, value));
                    }
                }
            }
        }
        best.map(|(_, value)| value).or(rules.provider_level)
    }

    pub fn resolve(&self, key: &str, base: bool) -> bool {
        self.override_for(key).unwrap_or(base)
    }
}

#[cfg(test)]
mod compiled_tests {
    use super::*;

    #[test]
    fn compiled_specs_match_reference_for_subscription_exclusion_and_runner_rules() {
        // Every generated case includes the troublesome shapes; random extras
        // vary their order and signs without relying on chance to cover them.
        let mut random = 0x5eed_u64;
        let mut next = || {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        };
        for case in 0..1024 {
            for rule_kind in ["subscription", "exclusion", "runner"] {
                let mut entries = vec![
                    "p/m".into(),
                    "-p/m".into(),
                    "p/m/child".into(),
                    "-p/m/child/deep".into(),
                    "p".into(),
                    "-p".into(),
                    "".into(),
                    "   ".into(),
                    "/m".into(),
                    "p/".into(),
                    "- /m".into(),
                    "- p/".into(),
                ];
                let choices = [
                    "p",
                    "p/m",
                    "p/m/child",
                    "p/m/v/high",
                    "q",
                    "q/a",
                    "q/a//b",
                    "-",
                    "p//",
                    " p/m ",
                ];
                for _ in 0..next() % 40 {
                    let body = choices[next() as usize % choices.len()];
                    entries.push(format!("{}{body}", if next() & 1 == 0 { "-" } else { "" }));
                }
                let compiled = CompiledSpecs::compile(&entries);
                let reference = parse_spec_list(&entries);
                for key in [
                    "p",
                    "p/",
                    "p/m",
                    "p/m/child",
                    "p/m/child/deep",
                    "p/m/child/deep/v/extra",
                    "p/m/v/high",
                    "p/m2",
                    "q/a//b/v",
                    "q/a/z",
                    "absent/m/v/extra",
                    "",
                    "/m",
                ] {
                    for base in [false, true] {
                        assert_eq!(
                            compiled.resolve(key, base),
                            resolve_spec(&reference, key, base),
                            "{rule_kind} case {case}: {key:?}, base {base}: {entries:?}"
                        );
                    }
                }
            }
        }
    }
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
/// The operator fence keeps a route out of AUTOMATIC choice only. A selection
/// that names its model (a work run's `model=`, a synthesizer or planner pin)
/// passes an empty `operator` list, so naming an excluded route always admits it
/// when it is otherwise servable.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExclusionPolicy<'a> {
    pub operator: &'a [String],
    pub caller: &'a [String],
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
        }
    }

    /// Neither list. Explicit roster pins admit with no exclusion applied at all.
    pub fn none() -> ExclusionPolicy<'static> {
        ExclusionPolicy {
            operator: &[],
            caller: &[],
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
}

/// Both exclusion lists parsed once for a selection pass.
#[derive(Debug, Clone, Default)]
pub struct ExclusionSpecs {
    operator: CompiledSpecs,
    operator_group: CompiledSpecs,
    caller: CompiledSpecs,
}

impl ExclusionSpecs {
    pub fn parse(policy: ExclusionPolicy<'_>) -> Self {
        Self {
            operator: CompiledSpecs::compile(policy.operator),
            operator_group: CompiledSpecs::default(),
            caller: CompiledSpecs::compile(policy.caller),
        }
    }

    /// Resolve the consumer's exclusion list separately from the machine-wide
    /// list so negations in one cannot cancel exclusions in the other.
    pub fn with_operator_group(mut self, group: &[String]) -> Self {
        self.operator_group = CompiledSpecs::compile(group);
        self
    }

    /// The list that excludes `key`, if either does. The operator fence is checked
    /// first so a route that both lists name is reported as the standing policy
    /// decision it is, not as one attempt's local exclusion.
    pub fn source_for(&self, key: &str) -> Option<ExclusionSource> {
        if self.operator.resolve(key, false) || self.operator_group.resolve(key, false) {
            return Some(ExclusionSource::Operator);
        }
        if self.caller.resolve(key, false) {
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
    if policy.operator.is_empty() && policy.caller.is_empty() {
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

    #[test]
    fn consumer_and_all_lists_are_independent_including_variant_exceptions() {
        let all = vec![
            "anthropic".into(),
            "openai/pin".into(),
            "-openai/pin/high".into(),
        ];
        let group = vec![
            "-anthropic".into(),
            "-openai/pin".into(),
            "local".into(),
            "-local/safe".into(),
        ];
        let policy = ExclusionSpecs::parse(ExclusionPolicy {
            operator: &all,
            caller: &[],
        })
        .with_operator_group(&group);
        for key in [
            "anthropic/one",
            "anthropic/two/high",
            "openai/pin",
            "openai/pin/low",
            "local/unsafe",
        ] {
            assert_eq!(
                policy.source_for(key),
                Some(ExclusionSource::Operator),
                "{key}"
            );
        }
        for key in ["openai/pin/high", "local/safe", "other/model"] {
            assert_eq!(policy.source_for(key), None, "{key}");
        }
        let group = vec!["openai/pin/high".into()];
        let policy = ExclusionSpecs::parse(ExclusionPolicy {
            operator: &all,
            caller: &[],
        })
        .with_operator_group(&group);
        assert!(policy.excludes("openai/pin/high"));
    }

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
        });
        assert_eq!(specs.source_for("openai/gpt-5.5"), None);
        assert_eq!(
            specs.source_for("openai/gpt-5.4"),
            Some(ExclusionSource::Operator)
        );
    }

    #[test]
    fn a_provider_exclusion_is_named_by_provider_and_never_by_prefix() {
        let operator = vec!["anthropic-platform".to_string()];
        let specs = ExclusionSpecs::parse(ExclusionPolicy {
            operator: &operator,
            ..ExclusionPolicy::default()
        });
        assert_eq!(
            specs.source_for("anthropic-platform/claude-opus-4-1"),
            Some(ExclusionSource::Operator)
        );
        // The subscription provider whose name the platform provider extends
        // stays an ordinary automatic candidate.
        assert_eq!(specs.source_for("anthropic/claude-opus-4-1"), None);
        // A pinned selection passes no operator list and admits the route.
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
