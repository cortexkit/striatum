//! The slow-select limiter reports how many similar lines it suppressed.

use crate::stage_timing::{rate_limit_route_select_line, RouteSelectLogLimiter};
use std::time::{Duration, Instant};

#[test]
fn slow_select_log_rate_limit_counts_suppressed_requests() {
    let start = Instant::now();
    let mut limiter = RouteSelectLogLimiter::default();
    assert_eq!(
        rate_limit_route_select_line("first".into(), start, &mut limiter),
        Some("first".into())
    );
    assert_eq!(
        rate_limit_route_select_line(
            "suppressed".into(),
            start + Duration::from_secs(1),
            &mut limiter,
        ),
        None
    );
    assert_eq!(
        rate_limit_route_select_line("next".into(), start + Duration::from_secs(10), &mut limiter,),
        Some("next (suppressed 1 similar lines)".into())
    );
}
