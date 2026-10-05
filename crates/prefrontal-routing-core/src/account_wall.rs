//! Account exhaustion is evidence about one credential, not a model's pool.
use crate::RouteWallClass;
use serde_json::Value;

/// Recognize typed account exhaustion and the exact legacy Broca summary core
/// persisted before account scope was retained through terminalization. Do not
/// infer scope from a provider's free-form message or from a generic HTTP 429.
pub fn account_scoped_error(error: &str) -> bool {
    if let Ok(value) = serde_json::from_str::<Value>(error) {
        let provider = value.get("provider").unwrap_or(&value);
        let code = provider.get("provider_code").and_then(Value::as_str);
        if matches!(code, Some("account_limit_exhausted" | "quota_exhausted")) {
            let named_account = provider
                .get("account_ref")
                .and_then(Value::as_str)
                .is_some_and(|account| !account.trim().is_empty());
            let named_window = provider
                .get("limit_window")
                .and_then(Value::as_str)
                .is_some_and(|window| !window.trim().is_empty())
                && provider
                    .get("resets_at_ms")
                    .and_then(Value::as_i64)
                    .is_some();
            return named_account || named_window;
        }
        return false;
    }
    if !error.starts_with("provider quota exhausted on ") || !error.contains(" in Broca run ") {
        return false;
    }
    let Some((_, description)) = error.split_once(" (Broca error class ") else {
        return false;
    };
    let Some((metadata, _)) = description.split_once("; the provider said: ") else {
        return false;
    };
    if !metadata.contains("; provider code account_limit_exhausted;") {
        return false;
    }
    let Some((_, account)) = metadata.split_once("; account ") else {
        return false;
    };
    // The old producer's account field is a vault UUID. A mention of an account
    // in ordinary error prose, or an unknown account, must not erase a real wall.
    let parts = account.split('-').collect::<Vec<_>>();
    parts.len() == 5
        && parts.iter().zip([8, 4, 4, 4, 12]).all(|(part, len)| {
            part.len() == len && part.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

/// A billing wall remains provider/model-wide even if its narrative mentions an
/// account. Only explicitly scoped quota evidence is quarantined.
pub fn account_scoped_wall(error: Option<&str>, class: Option<RouteWallClass>) -> bool {
    class != Some(RouteWallClass::Billing) && error.is_some_and(account_scoped_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_wall_scope_vectors_preserve_unknown_and_provider_walls() {
        let vectors: Vec<Value> =
            serde_json::from_str(include_str!("../tests/fixtures/account-wall-scope.json"))
                .unwrap();
        for vector in vectors {
            let class = vector["class"]
                .as_str()
                .map(|class| RouteWallClass::parse(class).unwrap());
            assert_eq!(
                account_scoped_wall(vector["error"].as_str(), class),
                vector["accountScoped"].as_bool().unwrap(),
                "{}",
                vector["name"]
            );
        }
    }
}
