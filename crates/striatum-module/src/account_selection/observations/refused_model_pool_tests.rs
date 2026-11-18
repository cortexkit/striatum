//! Observation tests verify that reported exhaustion is attributed to the right quota pool.

use super::*;

const NOW: EpochMs = 1_794_828_400_000;

fn pooled(observed_at: EpochMs, pools: &[&str]) -> QuotaReading {
    QuotaReading {
        observed_at_ms: Some(observed_at),
        error_class: None,
        stale: None,
        five_hour: None,
        seven_day: None,
        general_windows: Some(Vec::new()),
        scoped: ScopedList::Missing,
        pool_windows: Some(
            pools
                .iter()
                .map(|pool| PoolWindowReading {
                    pool: Some((*pool).to_string()),
                    window_minutes: 10080,
                    used_percent: 100.0,
                })
                .collect(),
        ),
    }
}

fn argon_pool() -> Vec<striatum_core::quota_extract::ModelPool> {
    vec![striatum_core::quota_extract::ModelPool {
        model_prefix: "antigravity-gemini-4-argon".into(),
        pool: "gemini-argon".into(),
    }]
}

fn pool_of(
    reading: Option<&QuotaReading>,
    model: &str,
    model_pools: &[striatum_core::quota_extract::ModelPool],
) -> Option<String> {
    refused_model_pool(
        reading,
        "google",
        model,
        model_pools,
        NOW,
        &AllocatorPolicy::default(),
    )
    .map(|key| {
        assert_eq!(key.window_minutes, None);
        assert_eq!(key.scope, striatum_core::accounts::PoolScope::EveryModel);
        key.name.unwrap()
    })
}

#[test]
fn a_fresh_pooled_reading_names_the_pool_the_refused_model_draws_on() {
    let fresh = pooled(NOW - 30_000, &["gemini", "3p"]);
    assert_eq!(
        pool_of(Some(&fresh), "antigravity-gemini-4-carbon", &[]).as_deref(),
        Some("gemini")
    );
    assert_eq!(
        pool_of(Some(&fresh), "antigravity-claude-sonnet-5-5", &[]).as_deref(),
        Some("3p")
    );
    // A model routing has no pool for, or one the reading does not show.
    assert_eq!(pool_of(Some(&fresh), "antigravity-mystery-1", &[]), None);
    let third_party_only = pooled(NOW - 30_000, &["3p"]);
    assert_eq!(
        pool_of(Some(&third_party_only), "antigravity-gemini-4-carbon", &[]),
        None
    );
    // A model drawing on two pools cannot be keyed to one.
    let pro = pooled(NOW - 30_000, &["gemini", "gemini-argon", "3p"]);
    assert_eq!(
        pool_of(Some(&pro), "antigravity-gemini-4-argon", &argon_pool()),
        None
    );
    // On an account without the Argon pool, Argon draws on Gemini alone.
    assert_eq!(
        pool_of(Some(&fresh), "antigravity-gemini-4-argon", &argon_pool()).as_deref(),
        Some("gemini")
    );
}

#[test]
fn without_a_fresh_reading_the_refused_model_names_its_pool() {
    let stale = pooled(NOW - 30 * 60_000, &["3p"]);
    for reading in [None, Some(&stale)] {
        assert_eq!(
            pool_of(reading, "antigravity-gemini-4-carbon", &[]).as_deref(),
            Some("gemini")
        );
        assert_eq!(
            pool_of(reading, "antigravity-gpt-6-1", &[]).as_deref(),
            Some("3p")
        );
        assert_eq!(pool_of(reading, "antigravity-mystery-1", &[]), None);
        assert_eq!(pool_of(reading, "gemini-4-carbon", &[]), None);
        // Config gives Argon a second pool some accounts publish.
        assert_eq!(
            pool_of(reading, "antigravity-gemini-4-argon", &argon_pool()),
            None
        );
    }
}

#[test]
fn a_fresh_reading_without_pool_windows_keeps_the_block_whole_account() {
    let aggregate = pooled(NOW - 30_000, &[]);
    assert_eq!(
        pool_of(Some(&aggregate), "antigravity-gemini-4-carbon", &[]),
        None
    );
}
