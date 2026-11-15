use super::*;

const NOW: u64 = 1_800_000_000_000;
const NEAR: u64 = NOW + 60_000;
const FAR: u64 = NOW + 86_400_000;

#[test]
fn account_pool_quota_retry_by_recheck_and_keep_near_reset() {
    let _clock = crate::InjectedClock::set(NOW);
    for reason in [
        "confirmed_fence",
        "below_reserve",
        "below_broca_floor",
        "scope_exhausted",
    ] {
        for (reset, expected) in [(NEAR, NEAR), (FAR, NOW + 900_000)] {
            let detail = json!({"reset_at_ms":reset, "retry_after_s":(reset-NOW)/1000,
                "exclusions":[{"account_id":"quota-account", "reason":reason, "window":"seven_day"}]});
            let refusal = account_refusal(
                "account_pool_exhausted",
                "pool exhausted".into(),
                Some(&detail),
            );
            assert_eq!(refusal.wait_until_ms(), Some(expected), "{refusal:?}");
            assert_eq!(refusal.detail["retry_after_s"], (expected - NOW) / 1000);
            assert_eq!(refusal.detail["exclusions"], detail["exclusions"]);
        }
    }
}

#[test]
fn account_reserve_retry_by_recheck_and_keep_near_reset() {
    let _clock = crate::InjectedClock::set(NOW);
    for (reset, expected) in [(NEAR, NEAR), (FAR, NOW + 900_000)] {
        let detail = json!({"reset_at_ms":reset,
            "exclusions":[{"account_id":"quota-account", "reason":"below_reserve", "window":"five_hour"}]});
        let refusal = account_refusal(
            "account_pool_reserve_limited",
            "reserve limited".into(),
            Some(&detail),
        );
        assert_eq!(refusal.detail["reset_at_ms"], expected, "{refusal:?}");
    }
}

#[test]
fn account_non_quota_or_untyped_fences_keep_their_deadlines() {
    let _clock = crate::InjectedClock::set(NOW);
    for reason in ["entitlement", "billing_exhausted", "route_fenced"] {
        let detail = json!({"reset_at_ms":FAR, "retry_after_s":86_400,
            "exclusions":[{"account_id":"fenced", "reason":reason, "reset_at_ms":FAR}]});
        let refusal = account_refusal(
            "account_pool_exhausted",
            "quota text is not evidence".into(),
            Some(&detail),
        );
        assert_eq!(refusal.wait_until_ms(), Some(FAR));
        assert_eq!(refusal.detail["retry_after_s"], 86_400);
    }
    let detail = json!({"reset_at_ms":FAR, "exclusions":[]});
    assert_eq!(
        account_refusal(
            "account_pool_exhausted",
            "unknown cause".into(),
            Some(&detail)
        )
        .wait_until_ms(),
        Some(FAR)
    );
    assert_eq!(
        account_refusal("account_pool_exhausted", "untimed".into(), None).wait_until_ms(),
        None
    );
    assert_eq!(
        account_refusal("concurrency_full", "full".into(), Some(&detail)).wait_until_ms(),
        Some(FAR)
    );
}

#[test]
fn mixed_account_pool_retry_follows_the_named_recovery_cause() {
    let _clock = crate::InjectedClock::set(NOW);
    for (quota_reset, fence_reset, expected) in
        [(FAR, FAR + 60_000, NOW + 900_000), (FAR + 60_000, FAR, FAR)]
    {
        let detail = json!({"reset_at_ms":FAR, "exclusions":[
            {"account_id":"quota", "reason":"below_reserve", "reset_at_ms":quota_reset},
            {"account_id":"fenced", "reason":"billing_exhausted", "reset_at_ms":fence_reset}]});
        let refusal = account_refusal("account_pool_exhausted", "mixed".into(), Some(&detail));
        assert_eq!(refusal.wait_until_ms(), Some(expected), "{refusal:?}");
    }
}
