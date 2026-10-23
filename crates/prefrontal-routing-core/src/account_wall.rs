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

/// Provider codes of a permanent refusal of one credential: the account has no
/// plan for the provider's models (`ENTITLEMENT_ERROR`), or no paid balance
/// (`billing_exhausted`, its legacy spelling `insufficient_quota`, and
/// `credit_insufficient` for a balance too small for the request). Routing fences
/// the credential for these (see the routing module's credential fences).
pub const CREDENTIAL_REFUSAL_CODES: [&str; 4] = [
    "ENTITLEMENT_ERROR",
    "billing_exhausted",
    "insufficient_quota",
    "credit_insufficient",
];

/// The credential a recorded error says the provider refused permanently, with
/// the provider code: Broca's typed run error (top level, or under `provider`)
/// with class `permanent`, one of [`CREDENTIAL_REFUSAL_CODES`], and a non-empty
/// `credential_id`. Such an error is evidence about that credential only; other
/// credentials of the provider still serve its routes. Prose that mentions a
/// credential is not this evidence.
pub fn credential_refusal_in(error: &str) -> Option<(String, String)> {
    let value = serde_json::from_str::<Value>(error).ok()?;
    let provider = value.get("provider").unwrap_or(&value);
    if provider.get("class").and_then(Value::as_str) != Some("permanent") {
        return None;
    }
    let code = provider.get("provider_code").and_then(Value::as_str)?;
    if !CREDENTIAL_REFUSAL_CODES.contains(&code) {
        return None;
    }
    let credential = provider
        .get("credential_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|credential| !credential.is_empty())?;
    Some((credential.to_string(), code.to_string()))
}

/// Whether a wall's evidence is about one account or credential rather than the
/// route, so it must not cool the route, its capacity or its provider.
///
/// A billing wall stays provider/model-wide even if its narrative mentions an
/// account: only explicitly scoped quota evidence is quarantined for quota. A
/// permanent refusal that names its credential (see [`credential_refusal_in`]) is
/// scoped whatever its class, including billing: routing fences that credential
/// instead, and rejects the route only once every credential serving it is fenced.
pub fn account_scoped_wall(error: Option<&str>, class: Option<RouteWallClass>) -> bool {
    let Some(error) = error else {
        return false;
    };
    (class != Some(RouteWallClass::Billing) && account_scoped_error(error))
        || credential_refusal_in(error).is_some()
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
