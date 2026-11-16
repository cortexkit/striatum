/// Marks a route-state writer's read of the row it is about to write
/// (`record_provider_cooldown`, `update_route_state`, the route-outcome
/// recorder in `set_decision_outcome`, `clear_failure_backoff`). Each reads
/// by primary key or the indexed `provider_id`, inside the account command
/// that writes, and its result decides only that statement's own write: the
/// merge is decided in SQL, the single decider for these writes, so the
/// in-memory state cannot be stale for it. The block read trace counts these
/// apart, and a test pins each statement's text.
macro_rules! route_write_read {
    () => {
        "/* route state write read */ "
    };
}

/// The marker [`route_write_read`] puts on those reads, for the read trace.
pub(crate) const ROUTE_WRITE_READ: &str = route_write_read!();

// The marked reads, one constant per statement, so the reviewed list of
// exempt reads (`reviewed_exempt_reads`) names exactly these texts.
pub(crate) const ROUTE_READ_PROVIDER_PRIOR_OBSERVED: &str = concat!(
    route_write_read!(),
    "SELECT wall_observed_at FROM provider_cooldown_state WHERE provider_key=?1"
);
pub(crate) const ROUTE_READ_PROVIDER_INHERITED_ROUTES: &str = concat!(route_write_read!(), "SELECT m.route_key,m.model_id,m.wall_observed_at,m.last_error,m.last_error_at,a.wall_class,COALESCE(a.wall_unscoped,0) FROM model_route_state m
    LEFT JOIN block a ON a.kind='route' AND a.origin_key=json_array(m.route_key) AND a.is_origin=1
    LEFT JOIN block b ON b.kind='route' AND b.origin_key=json_array(m.route_key) AND b.is_origin=1 AND b.source_present=1
    LEFT JOIN block p ON p.kind='provider' AND p.origin_key=json_array(m.provider_id) AND p.is_origin=1 AND p.source_present=1
    WHERE m.provider_id=?1 AND ((b.cooldown_until IS NULL AND b.quota_exhausted_until IS NULL)
        OR (b.observed_at=p.observed_at AND b.scope_error IS p.scope_error
          AND COALESCE(b.cooldown_until,0)<=COALESCE(p.cooldown_until,0)
          AND COALESCE(b.quota_exhausted_until,0)<=COALESCE(p.quota_exhausted_until,0)))");
pub(crate) const ROUTE_READ_PROVIDER_EXISTING: &str = concat!(
    route_write_read!(),
    "SELECT b.cooldown_until,b.quota_exhausted_until,p.quota_pressure,p.last_error,p.last_error_at,b.wall_class,COALESCE(b.wall_unscoped,0),p.wall_observed_at
        FROM provider_cooldown_state p LEFT JOIN block b ON b.kind='provider' AND b.origin_key=json_array(p.provider_key)
            AND b.is_origin=1 AND b.source_present=1 WHERE p.provider_key=?1"
);
pub(crate) const ROUTE_READ_ROUTE_PRIOR_OBSERVED: &str = concat!(
    route_write_read!(),
    "SELECT wall_observed_at FROM model_route_state WHERE route_key=?1"
);
pub(crate) const ROUTE_READ_ROUTE_EXISTING: &str = concat!(
    route_write_read!(),
    "SELECT b.cooldown_until,b.quota_exhausted_until,m.quota_pressure,m.latency_ema_ms,m.success_ema,m.failure_count,m.quota_error_count,
        m.last_error,m.last_error_at,b.wall_class,COALESCE(b.wall_unscoped,0)
        FROM model_route_state m LEFT JOIN block b ON b.kind='route' AND b.origin_key=json_array(m.route_key)
            AND b.is_origin=1 AND b.source_present=1 WHERE m.route_key=?1"
);
pub(crate) const ROUTE_READ_ROUTE_PROVIDER_ROW: &str = concat!(
    route_write_read!(),
    "SELECT b.cooldown_until,b.quota_exhausted_until,p.quota_pressure,p.last_error,p.last_error_at,b.wall_class,COALESCE(b.wall_unscoped,0),p.wall_observed_at
        FROM provider_cooldown_state p LEFT JOIN block b ON b.kind='provider' AND b.origin_key=json_array(p.provider_key)
            AND b.is_origin=1 AND b.source_present=1 WHERE p.provider_key=?1"
);
pub(crate) const ROUTE_READ_OUTCOME_FAILURE_STATE: &str = concat!(route_write_read!(), "SELECT m.consecutive_terminal_failures,b.deadline,b.reason,
        COALESCE(m.failure_observed_at,m.last_error_at,m.updated_at),m.wall_observed_at FROM model_route_state m
        LEFT JOIN block b ON b.kind='failure' AND b.origin_key=json_array(m.route_key) AND b.is_origin=1 AND b.source_present=1 WHERE m.route_key=?1");
pub(crate) const ROUTE_READ_CLEAR_FAILURE_STATE: &str = concat!(
    route_write_read!(),
    "SELECT m.consecutive_terminal_failures,b.deadline,b.reason FROM model_route_state m
        LEFT JOIN block b ON b.kind='failure' AND b.origin_key=json_array(m.route_key) AND b.is_origin=1 AND b.source_present=1 WHERE m.route_key=?1"
);

// Writes that run a `SELECT` of their own, over a bound `json_each` list,
// and read their target table only for the rows they write. The block read
// trace counts every read of a write's target once the write runs a
// `SELECT`, except for these reviewed statements
// (`group_reads::REVIEWED_OWN_ROW_WRITES`).
pub(crate) const UPDATE_INHERITED_ROUTE_WALLS: &str = "UPDATE model_route_state SET
                       quota_pressure = COALESCE(?3, quota_pressure),
                       failure_count = failure_count + ?5,
                       quota_error_count = quota_error_count + ?6,
                       last_error = COALESCE(?7, last_error),
                       last_error_at = CASE WHEN ?7 IS NULL THEN last_error_at ELSE ?8 END,
                       updated_at = ?8
                      WHERE provider_id = ?9 AND route_key IN (SELECT value FROM json_each(?10))";
pub(crate) const UPDATE_INHERITED_ROUTE_WALL_SCOPE: &str = "UPDATE model_route_state SET wall_observed_at=?2 WHERE provider_id=?1 AND route_key IN (SELECT value FROM json_each(?4))";

pub(crate) const OPERATOR_CLEAR_ROUTE_BLOCKS: &str = "UPDATE block SET source_present=0,cleared_at=?2,cleared_by='removed',wall_class=NULL,claim_owner=NULL
                WHERE kind='route' AND is_origin=1 AND source_present=1 AND origin_key IN (SELECT json_array(value) FROM json_each(?1))";
pub(crate) const OPERATOR_CLEAR_PROVIDER_MODEL: &str = "UPDATE block SET cleared_models=CASE WHEN EXISTS(SELECT 1 FROM json_each(cleared_models) WHERE value=?2)
                    THEN cleared_models ELSE json_insert(cleared_models,'$[#]',?2) END
                WHERE kind='provider' AND is_origin=1 AND source_present=1 AND provider=?1";
