//! Which routing-store tables a backup may restore empty.
//!
//! A backup can skip a table: on restore that table comes back empty while
//! every other table keeps its rows. Each table here is classed either as
//! safe to skip or as one every backup must carry, with the reason. A table
//! is only safe to skip when nothing backed up points at it by foreign key
//! and routing behaves correctly when it is empty while the backed-up tables
//! are full. The store's integration tests check that every table is classed
//! and that no backed-up table holds a foreign key into a skippable one.

/// Tables a backup may restore empty, each with what routing does when it
/// finds it empty.
pub const BACKUP_EXCLUDABLE_TABLES: &[(&str, &str)] = &[
    (
        "model_route_decisions",
        "decision ledger (audit history). An outcome naming a decision the ledger does not \
         hold is logged and changes nothing; the head content-policy withholding reads \
         model_route_parent_failures; route.explain and the decision list only lose history",
    ),
    (
        "account_decision",
        "account-selection decision history. Pins, admissions and holds name a decision id \
         without a foreign key; only diagnostics and retention read this table",
    ),
    (
        "account_observation",
        "verbatim runner observation log. Fences and exclusions name an observation id \
         without a foreign key; a re-sent runner event is recognised through \
         account_observation_event",
    ),
    (
        "quota_reading_log",
        "deduplicated quota time series; nothing reads it in production",
    ),
    (
        "route_winner_baseline",
        "winner-ladder notice baseline; a missing baseline is saved silently, suppressing \
         at most one change notice",
    ),
    (
        "fusiform_catalog_cursor",
        "which catalog snapshot was applied; a missing cursor makes the next refresh fetch \
         the whole catalog",
    ),
    (
        "routing_operator_quality_state",
        "Fusiform grade-cache presence; empty suppresses grade-coverage findings until \
         the next catalog refresh recreates the singleton",
    ),
    (
        "routing_operator_quality",
        "Fusiform operator-grade cache; empty falls back to stored scores and suppresses \
         grade-coverage findings until the next catalog refresh repopulates the grades",
    ),
];

/// Tables every backup must carry, each with the reason.
pub const BACKUP_REQUIRED_TABLES: &[(&str, &str)] = &[
    (
        "routing_operator_decommissioned",
        "operator withdrawals must survive restore; an empty cache would route withdrawn models before the next catalog poll",
    ),
    (
        "model_route_decision_detail_cursor",
        "one row saying how far decision-detail compaction has walked the ledger; restored \
         empty, compaction walks the whole ledger again, re-reading every compacted \
         decision once before it reaches new ones",
    ),
    (
        "routing_v1_report_delivery",
        "first-delivery answers and per-run usage identities prevent counting retries twice",
    ),
    (
        "routing_call_window",
        "counter deployment time bounds the observation window used before deleting handlers",
    ),
    (
        "routing_call_count",
        "persisted calls by method, shape and principal prevent an unsafe handler deletion",
    ),
    (
        "model_route_speed_samples",
        "per-generation timing history supplies observed speeds by served tier; no smaller \
         summary is retained, so losing it resets those speeds to cold-start values",
    ),
    (
        "routing_engine_eval",
        "reference-set evaluations are the only retained engine accuracy evidence; empty, \
         every engine ranks as unmeasured until its declaring provider evaluates it again",
    ),
    (
        "routing_engine_speed",
        "bounded job timing windows supply measured realtime eligibility; empty, routing \
         falls back to declarations and may select an engine already measured too slow",
    ),
    (
        "routing_unscored_admissible_models",
        "routing-owned first-admission timestamp history; restoring it empty would make \
         previously observed ungraded models appear newly discovered",
    ),
    (
        "model_route_usage",
        "per-message usage is the scoring input itself: observed speed per route and the \
         workload-class token profiles are computed from its trailing seven days, and no \
         smaller summary is kept, so an empty table scores every route from cold-start \
         values for up to a week",
    ),
    (
        "quota_snapshot",
        "last provider quota poll (one row); empty, serving runs on an empty quota cache \
         until the first poll, which can widen routing during an upstream outage",
    ),
    (
        "account_quota_reading",
        "per-account quota readings (tens of rows); empty, admissions before the first \
         complete poll see no quota",
    ),
    (
        "account_quota_completeness",
        "completeness markers for account_quota_reading (tens of rows); only meaningful \
         together with it",
    ),
    (
        "account_vault_roster",
        "latest credential-vault roster (one row); empty, account serving has no roster \
         until the next vault listing",
    ),
    (
        "model_route_parent_failures",
        "per-head failed-decision copy the content-policy withholding reads; small, and \
         pruned with the decision window",
    ),
    (
        "account_observation_event",
        "recent runner-event keys with the answer routing gave, so a re-sent event is not \
         applied twice; pruned after six hours",
    ),
    (
        "model_facts",
        "catalog facts including operator overrides and placements",
    ),
    (
        "model_route_state",
        "route health telemetry and report ordering; block evidence is backed up separately",
    ),
    (
        "provider_cooldown_state",
        "provider telemetry and report ordering; block evidence is backed up separately",
    ),
    (
        "account_selection_salt",
        "salt for conversation keys; a new salt changes every affinity",
    ),
    ("account_affinity", "current account pin per conversation"),
    (
        "routing_v1_panel",
        "panel ownership, request and seat history; restored empty, a consult running across \
         the restore has every seat replacement refused as an unknown panel. Small, and \
         pruned with the decision window",
    ),
    ("account_pin", "pin generations; admissions reference them"),
    (
        "account_admission",
        "committed duplicate-request replies, charged demand and lease lifecycle",
    ),
    (
        "account_continue",
        "durable selection replies, decision lineage and continuation reservations",
    ),
    (
        "account_hold",
        "client-visible retry answers within their window",
    ),
    ("block", "refusal evidence, retry clocks, partial route recovery, classification and source tombstones; losing them repeats refused requests or admits duplicate probes"),
    ("probe_authorization", "links an admitted probe to the exact refusal evidence its success may heal"),
    ("retirement_tombstone", "all retired refusal generations; restoring them empty can re-arm delayed evidence"),
    ("success_proof", "scoped success proofs required to recover whole-account quota without erasing another pool"),
    (
        "account_eligibility",
        "model/account eligibility facts retained until the credential listing changes; \
         restoring them empty would repeat refusals already learned",
    ),
    ("pool_key_state", "key-pool usage and report ordering; walls are backed up in block"),
    ("pool_key_lease", "live key leases"),
    ("pool_session_affinity", "session-to-key binding"),
    (
        "routing_v1_decision",
        "which principal and consumer may report each v1 decision and which runner may \
         cite it; empty, every report and decision-citing admission is unknown_decision",
    ),
    (
        "routing_v1_affinity",
        "previous v1 decision per affinity key; empty, warm prompt caches move routes",
    ),
    (
        "routing_v1_report",
        "first v1 report per decision; empty, a re-sent report is recorded twice",
    ),
    (
        "routing_v1_admission",
        "first v1 admission answer per runner and admission id; empty, a re-sent \
         admission is decided again",
    ),
    ("cortexkit_schema_version", "migration ledger"),
];
