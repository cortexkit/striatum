//! Pure block decisions. Source identity is deliberately independent of scope
//! and cause: two origins with the same scope can have different retry clocks.

pub const SOURCE_KINDS: [&str; 14] = [
    "account",
    "credential",
    "account_route",
    "non_quota_route",
    "transient_route",
    "pool_billing",
    "pool_rate",
    "provider",
    "route",
    "failure",
    "size",
    "non_quota_size",
    "principal",
    "observation",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LifetimeClass {
    Quota,
    NonQuota,
}

impl LifetimeClass {
    pub fn from_quota(quota: bool) -> Self {
        if quota {
            Self::Quota
        } else {
            Self::NonQuota
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    AccountLimit,
    CredentialRefusal,
    RouteRefusal,
    Transient,
    Billing,
    RateLimit,
    ProviderWall,
    RouteWall,
    Failure,
    SizeRefusal,
    PrincipalExclusion,
}

impl Cause {
    pub fn for_kind(kind: &str) -> Option<Self> {
        Some(match kind {
            "account" | "account_route" => Self::AccountLimit,
            "credential" => Self::CredentialRefusal,
            "non_quota_route" => Self::RouteRefusal,
            "transient_route" | "observation" => Self::Transient,
            "pool_billing" => Self::Billing,
            "pool_rate" => Self::RateLimit,
            "provider" => Self::ProviderWall,
            "route" => Self::RouteWall,
            "failure" => Self::Failure,
            "size" | "non_quota_size" => Self::SizeRefusal,
            "principal" => Self::PrincipalExclusion,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scope<S = String> {
    pub provider: S,
    pub account_id: Option<S>,
    pub credential_id: Option<S>,
    pub model: Option<S>,
    pub size_class: Option<i64>,
    pub principal: Option<S>,
    pub admission_id: Option<S>,
}

impl Scope {
    pub fn borrowed(&self) -> Scope<&str> {
        Scope {
            provider: &self.provider,
            account_id: self.account_id.as_deref(),
            credential_id: self.credential_id.as_deref(),
            model: self.model.as_deref(),
            size_class: self.size_class,
            principal: self.principal.as_deref(),
            admission_id: self.admission_id.as_deref(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    pub kind: String,
    pub origin_key: String,
    pub scope: Scope,
    pub cause: Cause,
    pub lifetime: LifetimeClass,
    /// For credential origins, this and lifetime are the stored classification,
    /// not a classification inferred from today's quota hold.
    pub classified: bool,
    pub evidence: String,
    pub evidence_at: i64,
    pub next_recheck_at: i64,
    pub reset_check_at: Option<i64>,
    pub last_claimed_at: Option<i64>,
    pub claim_owner: Option<String>,
    pub claim_generation: i64,
    pub cleared_models: Vec<String>,
}

/// Borrowed input lets SQL rows and loaded blocks use the same evaluator
/// without allocating a second scope for each candidate.
#[derive(Clone, Debug)]
pub struct Origin<'a> {
    pub kind: &'a str,
    pub scope: Scope<&'a str>,
    pub lifetime: LifetimeClass,
    pub cleared_models: &'a [String],
}

impl Block {
    pub fn origin(&self) -> Origin<'_> {
        Origin {
            kind: &self.kind,
            scope: self.scope.borrowed(),
            lifetime: self.lifetime,
            cleared_models: &self.cleared_models,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Target<'a> {
    pub provider: &'a str,
    pub account: &'a str,
    pub credential: &'a str,
    pub model: &'a str,
}

pub fn targets(origin: &Origin<'_>, target: Target<'_>, credential_account: Option<&str>) -> bool {
    let scope = &origin.scope;
    let named = scope
        .credential_id
        .filter(|_| {
            (origin.lifetime == LifetimeClass::Quota
                || matches!(
                    origin.kind,
                    "account_route" | "non_quota_route" | "transient_route"
                ))
                && credential_account == Some(target.account)
        })
        .unwrap_or(target.credential);
    (scope.provider.is_empty() || scope.provider == target.provider)
        && scope.account_id.is_none_or(|id| id == target.account)
        && scope.credential_id.is_none_or(|id| id == named)
        && scope.model.is_none_or(|id| id == target.model)
        && !origin
            .cleared_models
            .iter()
            .any(|model| model == target.model)
}

/// Account pool applicability comes from the request's allocator policy. The
/// callback stays lazy for non-account origins.
pub fn origin_applies(
    origin: &Origin<'_>,
    targets: impl FnOnce() -> bool,
    account_pool_applies: impl FnOnce() -> bool,
) -> bool {
    origin.scope.principal.is_none()
        && origin.scope.size_class.is_none()
        && targets()
        && (origin.kind != "account" || account_pool_applies())
}

/// Whether one origin permits entitlement eligibility. Account and route
/// pressure do not remove a credential from entitlement candidates: separate
/// allocation and route checks enforce that pressure. Admission must still
/// claim a due probe before sending.
pub fn eligible(
    origin: &Origin<'_>,
    applies: impl FnOnce() -> bool,
    due: impl FnOnce() -> bool,
) -> bool {
    matches!(
        origin.kind,
        "account"
            | "account_route"
            | "non_quota_route"
            | "transient_route"
            | "observation"
            | "route"
            | "provider"
            | "failure"
    ) || !applies()
        || due()
}

pub fn probe_eligible(applies: bool, due: impl FnOnce() -> bool) -> bool {
    applies && due()
}

pub fn due(
    lifetime: LifetimeClass,
    recovered: bool,
    recheck_at: i64,
    now: i64,
    owned: bool,
    binding_reading: impl FnOnce() -> bool,
) -> bool {
    recovered
        || ((recheck_at <= now || owned)
            && (lifetime == LifetimeClass::NonQuota || !binding_reading()))
}
