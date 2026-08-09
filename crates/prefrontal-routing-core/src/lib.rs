//! prefrontal-routing-core: the pure model-routing core for Alfonso.
//!
//! Wire-agnostic: this crate knows nothing about subc. It exposes the model
//! selection scorer (a saturating-adequacy quality gate + a cost-primary
//! objective), the route-state types, and (in a later slice) the SQLite
//! route-state store. The subc module (`prefrontal-routing-module`) wraps the
//! selection behind a `route.select` route op.
//!
//! Ported from the Alfonso TS plugin's `src/features/model-routing/` logic. The
//! TS unit tests are preserved as Rust test-vectors (same inputs/outputs) so the
//! port's behavioral parity is provable, not assumed.

pub mod break_glass;
pub mod fast_variant;
pub mod model_facts;
pub mod model_facts_seed;
pub mod model_router;
pub mod panel;
pub mod provider_model_spec;
pub mod quota;
pub mod quota_extract;
pub mod quota_pace;
pub mod route_scoring;
pub mod route_state_store;
pub mod routing_config;
pub mod seat_routing;

pub use break_glass::{relax_earliest_reset, BreakGlassCandidate, RelaxedSnapshot};
pub use fast_variant::{
    maybe_upgrade_to_fast_variant, FastVariantUpgradeInput, FAST_UPGRADE_MAX_PRESSURE,
};
pub use model_facts::{
    derive_provider_key, fact_to_route, is_valid_grand_family_id, make_capacity_key,
    make_route_key, Confidence, ModelFact, ModelMetric, ModelPromptDefaults, ModelRequirements,
    ModelRoute, ModelScores, ReasoningEffort, MODEL_METRICS,
};
pub use model_facts_seed::model_facts_seed;
pub use model_router::{
    admit_routes, select_model_route, select_model_route_with_config, AdmissionResult,
    AdmittedRoute, BelowFloorWarning, GoverningWindow, ModelRouteStateSnapshot, NoRouteError,
    ObjectiveBreakdown, PromptParams, RejectedCandidate, RejectedRoute, RequiredCapabilities,
    RouteFailureReason, RouteRuntimeState, SelectModelRouteInput, SelectedModel,
    SelectedModelRoute, SelectedRouteScore,
};
pub use panel::{
    select_panel, select_panel_with_config, validate_panel_spec, InvalidPanelRequest, PanelCause,
    PanelCauseReason, PanelClass, PanelDiversity, PanelMember, PanelPolicy, PanelRosterEntry,
    PanelRunnerUp, PanelSelection, PanelSelectionPolicy, PanelShortfall, PanelShortfallCode,
    PanelSpec, SeatedSiblingVariant, SeatedVariant, SelectPanelInput, SelectedPanel,
    SiblingCollapse,
};
pub use provider_model_spec::{
    filter_excluded_keys, parse_spec, parse_spec_list, resolve_spec, ParsedSpec,
};
pub use quota::{
    merge_quota_overlay, merge_quota_overlay_with, ProviderUsage, DEFAULT_HARD_BLOCK_THRESHOLD,
    NO_RESET_REPROBE_HORIZON_MS,
};
pub use quota_extract::{
    extract_windows, match_capacity_key_by_title, ExtractedWindow, WindowScopeKind,
};
pub use quota_pace::{
    clamp01, project_quota_pressure, smoothstep, PaceConfig, PaceInput, PaceReason, PaceResult,
    DEFAULT_PACE_CONFIG,
};
pub use route_scoring::{score_adequacy, AdequacyScore, MetricFit};
pub use route_state_store::{
    DecisionInput, DecisionRow, FactSource, ProviderCooldownState, RouteOutcome, RouteStateResult,
    RouteStateStore, RouteStateStoreError, RouteStateUpdate, UsageObservation,
    OBSERVED_SPEED_MIN_OUTPUT_TOKENS, OBSERVED_SPEED_MIN_SAMPLES, OBSERVED_SPEED_SAMPLE_LIMIT,
    OBSERVED_SPEED_WINDOW_MS, USAGE_MIN_DURATION_MS, USAGE_MIN_OUTPUT_TOKENS,
};
pub use routing_config::{
    demand_bar, fit_ratio, RoutingConfig, TopRoutableScores, ADEQUACY_FLOOR, ANCHOR_VERSION,
    COST_REFERENCE_PER_MTOK, DEFAULT_DEMAND_EXPONENT, DEFAULT_ROUTING_CONFIG,
    MIN_BEST_EFFORT_ADEQUACY, OBJECTIVE_WEIGHT_QUOTA, RELATIVE_SHORTLIST_DELTA,
    UNKNOWN_FACT_CONFIDENCE_PENALTY, UNKNOWN_MODEL_SCORES,
};
pub use seat_routing::{
    incumbent_grand_families, select_seat_route, select_seat_route_with_config,
    SeatRecommendationMatch, SeatRecommendationTier, SeatRecommendations, SeatRouteError,
    SeatRouteInput, SeatRouteSelection,
};
