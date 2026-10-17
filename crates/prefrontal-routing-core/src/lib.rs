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

pub mod aa_infer;
pub mod account_wall;
pub mod accounts;
pub mod billing;
pub mod break_glass;
pub mod failure_backoff;
pub mod key_pool;
pub mod launch_policy;
pub mod model_facts;
pub mod model_facts_seed;
pub mod model_resolution;
pub mod model_router;
pub mod package_ceiling;
pub mod panel;
pub mod provider_model_spec;
pub mod quota;
pub mod quota_extract;
pub mod quota_pace;
pub mod quota_poll;
pub mod route_scoring;
pub mod route_state_store;
pub mod routing_config;
pub mod runner;
pub mod score_alias;
pub mod seat_routing;
pub mod slot_saturation;
pub mod subscription;

pub use aa_infer::{
    aa_elo_curve, elo_floor, infer_elo_from_aa, infer_elo_from_aa_static, inferred_scores,
    inherit_eq_from_curated_neighbors, interpolate_elo, EqSource, AA_INFERRED_DEFAULT_SPEED,
    AA_INFERRED_PENALTY,
};
pub use billing::{BillingPlane, BillingRate};
pub use break_glass::{
    live_billing_wall_error, relax_earliest_reset, BreakGlassCandidate, RelaxedSnapshot,
};
pub use failure_backoff::{
    classify_failure_attribution, failure_backoff_transition, is_terminal_send_failure,
    FailureAttribution, FailureBackoffConfig, FailureBackoffTransition,
    DEFAULT_CONSECUTIVE_TERMINAL_FAILURE_THRESHOLD, FAILURE_BACKOFF_INITIAL_MS,
    FAILURE_BACKOFF_MAX_MS,
};
pub use launch_policy::{
    resolve_launch_requirements, resolve_routing_policy, LaunchRequirementsError,
    ModelRoutingPolicyInput, RequestedCapabilities, RequestedModelRequirements,
    ResolvedLaunchRequirements, ResolvedRoutingPolicy, CURATED_SUBSCRIPTION_PROVIDERS,
};
pub use model_facts::{
    derive_provider_key, fact_to_route, is_valid_grand_family_id, make_capacity_key,
    make_route_key, normalize_grand_family_id, BasketDimension, BasketPrice, ClassVector,
    Confidence, FamilySource, GrandFamily, ModelFact, ModelMetric, ModelPromptDefaults,
    ModelRequirements, ModelRoute, ModelScores, ReasoningEffort, ScoreProvenance,
    CLASS_VECTOR_SAMPLE_FLOOR, CLASS_VECTOR_TRAILING_WINDOW_DAYS, CLASS_VECTOR_TRAILING_WINDOW_MS,
    ENUMERATED_WORKLOAD_CLASSES, GRAND_FAMILY_NORMALIZATIONS, MODEL_METRICS,
};
pub use model_facts_seed::model_facts_seed;
pub use model_resolution::{
    build_model_speed_observation, find_most_specific_fallback_entry, fuzzy_match_model,
    map_agent_to_workload_class, parse_workload_class, resolve_compatible_model_settings,
    resolve_model_capabilities, resolve_model_id_alias, resolve_model_pipeline,
    resolve_user_model_pin, use_basket_permitted, BrocaUsageFacts, DelegatedModelConfig,
    DesiredModelSettings, FallbackEntry, ModelAliasResolution, ModelCapabilities,
    ModelCapabilitiesInput, ModelCapabilitiesSnapshot, ModelCapabilitiesSnapshotEntry,
    ModelResolutionConstraints, ModelResolutionIntent, ModelResolutionPolicy,
    ModelResolutionRequest, ModelResolutionResult, ModelSettingsCompatibilityChange,
    ModelSettingsCompatibilityResult, NativeUsageFacts, SnapshotLimit, SnapshotModalities,
    UserModelPinRequest, VariantFact, ACCOUNT_PRODUCER, CLASS_PRODUCER, COUNTERS_PRODUCER,
    VALID_WORKLOAD_CLASSES,
};
pub use model_router::{
    admit_routes, select_model_route, select_model_route_with_config,
    select_model_route_with_slots, selection_cost, AdmissionResult, AdmittedRoute,
    BasketRoutePricing, BasketVectorSource, BelowFloorWarning, ModelRouteStateSnapshot,
    NoRouteError, ObjectiveBreakdown, PricingMode, PromptParams, RejectedCandidate, RejectedRoute,
    RequiredCapabilities, RouteFailureReason, RouteRuntimeState, SelectModelRouteInput,
    SelectedModel, SelectedModelRoute, SelectedRouteScore, REASON_EXCLUDED,
    REASON_EXCLUDED_BY_OPERATOR, REASON_NOT_AVAILABLE,
};
pub use panel::{
    select_panel, select_panel_with_config, select_panel_with_policy,
    select_panel_with_policy_context, validate_panel_spec, AvailabilitySubstrate, ClassPanelPolicy,
    InvalidPanelRequest, PanelCause, PanelCauseReason, PanelClass, PanelDiversity, PanelExtraOrder,
    PanelMember, PanelOrder, PanelPhase, PanelPins, PanelPolicy, PanelRosterEntry, PanelRunnerUp,
    PanelSeat, PanelSelection, PanelSelectionContext, PanelShortfall, PanelShortfallCode,
    PanelSpec, PanelSynthesizerPin, SeatedSiblingVariant, SeatedVariant, SelectPanelInput,
    SelectedPanel, SiblingCollapse,
};
pub use provider_model_spec::{
    filter_excluded_keys, filter_excluded_keys_with_policy, parse_spec, parse_spec_list,
    resolve_spec, ExclusionPolicy, ExclusionSource, ExclusionSpecs, ParsedSpec,
};
pub use quota::{
    merge_quota_overlay, merge_quota_overlay_with, ProviderUsage, DEFAULT_HARD_BLOCK_THRESHOLD,
    NO_RESET_REPROBE_HORIZON_MS,
};
pub use quota_extract::{
    extract_windows, fetched_at_ms, match_capacity_key_by_title, ExtractedWindow, WindowScopeKind,
};
pub use quota_pace::{
    clamp01, project_quota_pressure, smoothstep, PaceConfig, PaceError, PaceInput, PaceReason,
    PaceResult, PaceWindowModel, ProviderWindowModel, DEFAULT_PACE_CONFIG,
};
pub use route_scoring::{score_adequacy, AdequacyScore, MetricFit};
pub use route_state_store::{
    CatalogFactField, CooldownState, DecisionInput, DecisionOutcomeUpdate, DecisionRow, FactSource,
    FailureBackoffState, ProviderCooldownState, RouteOutcome, RouteStateResult, RouteStateStore,
    RouteStateStoreError, RouteStateUpdate, RouteWallClass, UsageObservation,
    OBSERVED_SPEED_MIN_OUTPUT_TOKENS, OBSERVED_SPEED_MIN_SAMPLES, OBSERVED_SPEED_SAMPLE_LIMIT,
    OBSERVED_SPEED_WINDOW_MS, USAGE_MIN_DURATION_MS, USAGE_MIN_OUTPUT_TOKENS,
};
pub use routing_config::{
    demand_bar, demand_for_score, fit_ratio, OverqualificationConfig, RoutingConfig,
    TopRoutableScores, ADEQUACY_FLOOR, ANCHOR_VERSION, BASKET_REFERENCE_COST_PER_MESSAGE,
    COST_REFERENCE_PER_MTOK, DEFAULT_DEMAND_EXPONENT, DEFAULT_OVERQUALIFICATION,
    DEFAULT_ROUTING_CONFIG, MIN_BEST_EFFORT_ADEQUACY, OBJECTIVE_WEIGHT_QUOTA,
    RELATIVE_SHORTLIST_DELTA, UNKNOWN_FACT_CONFIDENCE_PENALTY, UNKNOWN_MODEL_SCORES,
};
pub use seat_routing::{
    incumbent_grand_families, select_seat_route, select_seat_route_with_config,
    SeatRecommendationMatch, SeatRecommendationTier, SeatRecommendations, SeatRouteError,
    SeatRouteInput, SeatRouteSelection,
};
pub use slot_saturation::{
    ConcurrencySlotState, ConcurrencySlots, SkippedSaturatedRoute, SlotSaturation,
    REASON_PROVIDER_SATURATED,
};
