//! Read-time identity aliases for prefrontal's module rename window. Keep the
//! spelling on the wire and in history; only comparisons use the canonical form.
use std::borrow::Cow;

pub const OLD: &str = "prefrontal-core";
pub const NEW: &str = "prefrontal";

fn rewrite<'a>(value: &'a str, from: &str, to: &str) -> Cow<'a, str> {
    let (principal, name) = value
        .strip_prefix("reserved:")
        .map_or(("", value), |name| ("reserved:", name));
    match name.strip_prefix(from) {
        Some(tail) if tail.is_empty() || (principal.is_empty() && tail.starts_with('.')) => {
            Cow::Owned(format!("{principal}{to}{tail}"))
        }
        _ => Cow::Borrowed(value),
    }
}

pub fn canonical(value: &str) -> Cow<'_, str> {
    rewrite(value, NEW, OLD)
}

pub fn equal(left: &str, right: &str) -> bool {
    canonical(left) == canonical(right)
}

/// Bind both values in indexed SQL `IN` predicates. Ordinary identities repeat
/// the input, so no other module acquires an alias.
pub fn spellings(value: &str) -> [Cow<'_, str>; 2] {
    [Cow::Borrowed(value), alternate(value)]
}

pub fn alternate(value: &str) -> Cow<'_, str> {
    if canonical(value) == value {
        rewrite(value, OLD, NEW)
    } else {
        canonical(value)
    }
}

/// Canonicalize only an aggregate's grouping expression, never an indexed WHERE
/// column. Constants are shared with the Rust identity rule.
pub fn workload_sql(expression: &str) -> String {
    format!("CASE WHEN {expression}='{NEW}' THEN '{OLD}' WHEN substr({expression},1,{})='{NEW}.' THEN '{OLD}' || substr({expression},{}) ELSE {expression} END", NEW.len() + 1, NEW.len() + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_prefrontal_identity_and_whole_consumer_root_have_aliases() {
        for (old, new) in [
            (OLD, NEW),
            ("reserved:prefrontal-core", "reserved:prefrontal"),
            (
                "prefrontal-core.athena.synthesizer",
                "prefrontal.athena.synthesizer",
            ),
        ] {
            assert!(equal(old, new));
            assert_eq!(spellings(new)[1], old);
            assert_eq!(spellings(old)[1], new);
        }
        for name in [
            "prefrontal-corex",
            "prefrontal-core/child",
            "other.prefrontal-core",
            "reserved:prefrontal-core.child",
            "reserved:other",
        ] {
            assert_eq!(canonical(name), name);
            assert_eq!(spellings(name), [name, name]);
        }
    }
}
