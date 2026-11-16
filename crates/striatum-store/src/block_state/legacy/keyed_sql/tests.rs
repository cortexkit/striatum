//! Schema-93 read checks keep their historical SQL here instead of reusing the current implementation's queries.

pub(super) fn reads() -> Vec<String> {
    let mut reads = [
        "SELECT owner,kind,origin_key,evidence,evidence_at,claimed_at,claim_generation
                FROM fence_probe_authorization WHERE owner=?1 ORDER BY kind,origin_key",
        "SELECT evidence,evidence_at,claim_generation,credential_id,model,cleared_at,claim_owner
                    FROM fence_recheck WHERE kind=?1 AND origin_key=?2",
        "UPDATE fence_recheck SET claim_owner=claim_owner WHERE kind=?1 AND origin_key=?2
            AND cleared_at IS NULL AND evidence=?3 AND evidence_at=?4 AND claim_generation=?5",
        "UPDATE fence_recheck SET claim_owner=claim_owner WHERE kind=?1 AND origin_key=?2
            AND cleared_at IS NULL AND evidence=?3 AND evidence_at=?4 AND claim_generation=?5 AND quota=0 AND (model IS NULL OR model=?6)",
        "UPDATE account_admission SET state = state WHERE principal=?1 \
           AND admission_id=?2 AND state IN ('reserved','expired') \
           AND started_at IS NULL RETURNING admission_valid_until",
    ].map(|sql| format!("{} {sql}",super::STATE_CHECK)).to_vec();
    reads.extend([
        "SELECT wall_observed_at FROM provider_cooldown_state WHERE provider_key=?1",
        "SELECT route_key FROM model_route_state m WHERE provider_id=?1 AND
                    ((cooldown_until IS NULL AND quota_exhausted_until IS NULL) OR EXISTS
                     (SELECT 1 FROM provider_cooldown_state p WHERE p.provider_key=?1
                      AND COALESCE(m.wall_observed_at,m.last_error_at,m.updated_at)=COALESCE(p.wall_observed_at,p.last_error_at,p.updated_at)
                      AND m.last_error IS p.last_error
                      AND COALESCE(m.cooldown_until,0)<=COALESCE(p.cooldown_until,0)
                      AND COALESCE(m.quota_exhausted_until,0)<=COALESCE(p.quota_exhausted_until,0)))",
        "SELECT cooldown_until, quota_exhausted_until, quota_pressure,
                                last_error, last_error_at, wall_class
                         FROM provider_cooldown_state WHERE provider_key = ?1",
        "SELECT wall_observed_at FROM model_route_state WHERE route_key=?1",
        "SELECT cooldown_until, quota_exhausted_until, quota_pressure,
                                latency_ema_ms, success_ema, failure_count, quota_error_count,
                                last_error, last_error_at, wall_class
                         FROM model_route_state WHERE route_key = ?1",
        "SELECT cooldown_until, quota_exhausted_until, quota_pressure,
                                last_error, last_error_at, wall_class
                         FROM provider_cooldown_state WHERE provider_key = ?1",
        "SELECT consecutive_terminal_failures, failure_backoff_until,
                                failure_backoff_reason,COALESCE(failure_observed_at,last_error_at,updated_at)
                         FROM model_route_state WHERE route_key=?1",
        "SELECT consecutive_terminal_failures, failure_backoff_until,
                                failure_backoff_reason
                         FROM model_route_state WHERE route_key = ?1",
    ].map(|sql| format!("{}{sql}",crate::ROUTE_WRITE_READ)));
    reads
}
