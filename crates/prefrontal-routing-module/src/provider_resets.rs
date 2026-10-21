//! Request-class provider reset fences, independent of polled quota windows.

use prefrontal_routing_core::route_state_store::ProviderResetFence;

/// Return a reset only when no known serving identity escapes the fence. If
/// Broca has not exposed a roster, use the latest stored reset fence instead.
pub(crate) fn blocked_until(
    fences: &[ProviderResetFence],
    identities: &[(String, Option<String>)],
) -> Option<i64> {
    if identities.is_empty() {
        return fences.iter().map(|f| f.resets_at_ms).max();
    }
    identities
        .iter()
        .map(|(credential, account)| {
            fences
                .iter()
                .filter(|f| matches_identity(f, credential, account.as_deref(), identities.len()))
                .map(|f| f.resets_at_ms)
                .max()
        })
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .min()
}

pub(crate) fn healthy_credential(
    fences: &[ProviderResetFence],
    identities: &[(String, Option<String>)],
) -> Option<String> {
    if fences.is_empty() {
        return None;
    }
    identities
        .iter()
        .find(|(credential, account)| {
            !fences
                .iter()
                .any(|f| matches_identity(f, credential, account.as_deref(), identities.len()))
        })
        .map(|(credential, _)| credential.clone())
}

fn matches_identity(
    f: &ProviderResetFence,
    credential: &str,
    account: Option<&str>,
    count: usize,
) -> bool {
    if let Some(id) = &f.credential_id {
        return id == credential;
    }
    f.account_ref == credential || account == Some(f.account_ref.as_str())
        // Provider account references and vault account ids can use different
        // namespaces. Treat a sole serving login as matching the fence even when
        // those account names cannot be compared.
        || count == 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_refused_identity_is_blocked_and_an_alternative_is_pinned() {
        let fences = vec![ProviderResetFence {
            route_key: "google/model/high".into(),
            account_ref: "a".into(),
            credential_id: Some("antigravity:google:a".into()),
            resets_at_ms: 100,
        }];
        let identities = vec![
            ("antigravity:google:a".into(), Some("a".into())),
            ("antigravity:google:b".into(), Some("b".into())),
        ];
        assert_eq!(blocked_until(&fences, &identities), None);
        assert_eq!(
            healthy_credential(&fences, &identities).as_deref(),
            Some("antigravity:google:b")
        );
        assert_eq!(blocked_until(&fences, &identities[..1]), Some(100));
        assert_eq!(blocked_until(&fences, &identities[1..]), None);
        assert_eq!(blocked_until(&fences, &[]), Some(100));
    }
}
