//! Maximum demand-driven recheck intervals; neither interval expires evidence.
pub const QUOTA_FENCE_RECHECK_MS: i64 = 15 * 60_000;
pub const NON_QUOTA_FENCE_MAX_MS: i64 = 60 * 60_000;

#[cfg(test)]
mod tests {
    #[test]
    fn fence_policy_intervals_are_fifteen_minutes_and_one_hour() {
        assert_eq!(super::QUOTA_FENCE_RECHECK_MS, 900_000);
        assert_eq!(super::NON_QUOTA_FENCE_MAX_MS, 3_600_000);
    }
}
