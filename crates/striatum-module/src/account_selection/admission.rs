//! Admission requests, preflight, and commit.
//! Keep advisory planning and durable replay/commit on the same request model.

use super::*;

/// Hash only admission identity, using a typed struct's fixed-order JSON encoding.
/// Each accepted request field is classified here:
/// - `provider`: in; selects the provider (normalized like selection).
/// - `model`: in; selects the model and its family.
/// - `affinity_model`: in; selects the affinity family, defaulting to `model`.
/// - `conversation_key`: in; selects the conversation's affinity.
/// - `caller.runner`: in as caller kind; Broca and plugins have different pin rules.
/// - `caller.harness`: out; descriptive runner metadata, not admission identity.
/// - `admission_id`: out; already the lookup key, namespaced by principal.
/// - `mode`: in; distinguishes admission, renewal and reassignment.
/// - `expected_pin`: in; identifies the pin/generation being renewed or reassigned.
/// - `initial_input_bytes`: out; advisory demand estimate may change on retry.
/// - `initial_input_estimated`: out; describes that advisory estimate.
/// - `model_decision_id`: out; provenance of model choice, not the chosen model.
/// - `candidates` (including `view_digest`, `accounts`, `account_id` and
///   `credential_id`): out; the runner's vault/roster view may change on retry.
/// - `requested_account` (`account_id`, `credential_id`): in as the effective
///   account reference; an explicit operator override changes the admission.
/// - `preferred_existing_account` (`account_id`, `credential_id`): in as the
///   effective reference; importing a local pin changes the requested affinity.
/// - `hold_wait_max_s`: out; advisory wait budget, not admission identity.
/// - `pool`: in when present; it decides which pool blocks apply. A body
///   without it hashes exactly as it did before the member existed.
/// - `service_tier`: in when present; changing the requested service tier is a
///   different admission. Omission preserves existing fingerprints.
/// - `warm_credential`: in when present; changing the first-admission hint
///   changes identity, while omission preserves existing fingerprints.
/// Unknown fields are ignored by parsing, so quota hints, digests and timestamps
/// outside this schema cannot change identity either.
///
/// Replay returns the stored decision even if its account vanished from the new
/// candidates. The runner's credential read handles it like any revoked credential.
/// This encoding was introduced before production enabled account serving
/// (`account_selection.enabled`). With that flag off, handlers refuse admissions;
/// the former shadow mode recorded hypothetical decisions but created no admission rows.
/// Thus production has no whole-request fingerprints to migrate. This relies on
/// serving never having been enabled, not on the 60 s reservation start window:
/// expired admission rows remain stored and would still use their old hash.
pub(super) fn fingerprint(params: &AccountForParams) -> String {
    #[derive(Serialize)]
    struct AdmissionIdentity<'a> {
        #[serde(skip_serializing_if = "Option::is_none")]
        fork_of: Option<&'a ForkParams>,
        provider: String,
        model: String,
        affinity_model: String,
        conversation_key: &'a str,
        caller: CallerKind,
        mode: RequestMode,
        expected_pin: &'a Option<PinRef>,
        requested_account: Option<AccountRef>,
        preferred_existing_account: Option<AccountRef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pool: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_conversation_key: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        service_tier: Option<cortexkit_role_routing::service_tier::ServiceTier>,
        #[serde(skip_serializing_if = "Option::is_none")]
        warm_credential: Option<&'a str>,
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        head_turn: bool,
    }
    let identity = AdmissionIdentity {
        fork_of: params.fork_of.as_ref(),
        provider: normalize(&params.provider),
        model: normalize(&params.model),
        affinity_model: normalize(params.affinity_model()),
        conversation_key: &params.conversation_key,
        caller: params.caller_kind(),
        mode: params.mode,
        expected_pin: &params.expected_pin,
        requested_account: params
            .requested_account
            .as_ref()
            .and_then(AccountRefParams::to_ref),
        preferred_existing_account: params
            .preferred_existing_account
            .as_ref()
            .and_then(AccountRefParams::to_ref),
        pool: params.pool.as_deref(),
        parent_conversation_key: params.parent_conversation_key.as_deref(),
        service_tier: params.service_tier,
        warm_credential: params.warm_credential.as_deref(),
        head_turn: params.head_turn,
    };
    let encoded = serde_json::to_vec(&identity).expect("admission identity is serializable");
    hex(&Sha256::digest(&encoded))
}

// ---------------------------------------------------------------------------
// Stored rows as allocator inputs
// ---------------------------------------------------------------------------

fn conversation_affinity(row: &AccountAffinityRow, now_ms: i64) -> ConversationAffinity {
    ConversationAffinity {
        pin_id: row.pin_id.clone(),
        account_id: row.account_id.clone(),
        credential_id: row.credential_id.clone(),
        affinity_model: row.affinity_model.clone(),
        pin_generation: row.pin_generation,
        expires_at_ms: row.expires_at,
        lease_active: row.lease_valid_until.is_some_and(|until| until > now_ms),
    }
}

pub(super) fn admission_probe_owner(principal: &str, admission: &str) -> String {
    json!(["account_admission", principal, admission]).to_string()
}

/// Use the caller's explicit quota pool, or the known pool for this model route if mapped.
pub(super) fn request_pool(named: Option<&str>, provider: &str, model: &str) -> Option<String> {
    named.map(str::to_string).or_else(|| {
        striatum_core::quota_extract::antigravity_pool_for_capacity(&format!(
            "{}/{}",
            normalize(provider),
            normalize(model)
        ))
        .map(str::to_string)
    })
}

/// A malformed pool name is refused with the member it came from named.
pub(super) fn invalid_pool(pool: Option<&str>) -> Option<OpReply> {
    let error = validate_pool_name(pool?).err()?;
    Some(OpReply::typed(
        "invalid_request",
        format!("`pool`: {error}"),
        json!({ "code": "invalid_request", "field": "pool" }),
    ))
}
/// The demand the allocator can charge to `roster`'s accounts. Each account's
/// admissions are read from its [`demand_floor`] on, so a decision never
/// loads history the allocator would ignore; admissions of accounts outside
/// the roster are never read, because the allocator only prices roster
/// accounts.
#[cfg(test)]
pub(super) fn load_demand(
    tx: &AccountTx<'_>,
    provider: &str,
    roster: Option<&Roster>,
    policy: &AllocatorPolicy,
    context: &striatum_core::accounts::AllocationContext,
) -> RouteStateResult<Vec<AdmissionDemand>> {
    demand_with(
        |provider, account, floor| tx.demand(provider, account, floor),
        provider,
        roster,
        policy,
        context,
    )
}

/// [`load_demand`] over any source of `ACCOUNT_DEMAND_SQL` rows: SQL, or the
/// account state, which keeps the same rows in memory.
fn demand_with(
    mut read: impl FnMut(
        &str,
        &str,
        Option<i64>,
    ) -> RouteStateResult<Vec<striatum_store::AccountDemandRow>>,
    provider: &str,
    roster: Option<&Roster>,
    policy: &AllocatorPolicy,
    context: &striatum_core::accounts::AllocationContext,
) -> RouteStateResult<Vec<AdmissionDemand>> {
    // One read per account; a repeated account keeps its lowest floor.
    let mut floors: BTreeMap<&str, Option<EpochMs>> = BTreeMap::new();
    for view in roster.map_or(&[][..], |roster| &roster.accounts) {
        let floor = demand_floor(view, policy, context);
        floors
            .entry(view.account_id.as_str())
            .and_modify(|kept| *kept = kept.zip(floor).map(|(a, b)| a.min(b)))
            .or_insert(floor);
    }
    let mut demand = Vec::new();
    for (account_id, floor) in floors {
        demand.extend(
            read(provider, account_id, floor)?
                .into_iter()
                .filter_map(|row| {
                    let state: AdmissionState = serde_json::from_value(json!(row.state)).ok()?;
                    Some(AdmissionDemand {
                        account_id: row.account_id,
                        state,
                        committed_at_ms: row.committed_at,
                        request_bytes: row.request_bytes,
                    })
                }),
        );
    }
    Ok(demand)
}

/// The commit time an admission charged to `view` must be after to still
/// count in the allocator, or `None` when one of any age counts.
///
/// The allocator charges an account's non-released admissions committed
/// after its quota reading less `reflection_lag_ms`: an older one is already
/// reflected in the reading. With no reading (or no reading time) it charges
/// every one. It also charges every one to an account it places on paid
/// credits, because it then sets the reading aside. Credit placement needs
/// `paid_credits` and a spendable credit reading for the account; when both
/// hold, the whole history is kept, without repeating the allocator's further
/// conditions for credit mode. The round-robin and hybrid strategies order
/// accounts by how many admissions each holds, whatever their age, so they
/// need the whole history too.
fn demand_floor(
    view: &AccountView,
    policy: &AllocatorPolicy,
    context: &striatum_core::accounts::AllocationContext,
) -> Option<EpochMs> {
    let may_use_credits = policy.paid_credits
        && context
            .credits
            .get(&view.account_id)
            .is_some_and(|credit| credit.spendable);
    if may_use_credits
        || matches!(
            policy.strategy,
            AccountStrategy::RoundRobin | AccountStrategy::Hybrid
        )
    {
        return None;
    }
    let observed_at = view.quota.as_ref()?.observed_at_ms?;
    Some(observed_at.saturating_sub(policy.reflection_lag_ms))
}

fn load_exclusions(
    tx: &(impl BlockReads + ?Sized),
    principal: &str,
    admission_id: &str,
) -> RouteStateResult<Vec<PrincipalExclusion>> {
    Ok(tx
        .admission_exclusions(principal, admission_id)?
        .into_iter()
        .map(|row| PrincipalExclusion {
            principal: row.principal,
            credential_id: row.credential_id,
            runner_view_digest: row.runner_view_digest,
            roster_view_digest: row.roster_view_digest,
            recorded_at_ms: row.recorded_at,
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Decision records
// ---------------------------------------------------------------------------

fn family_name(family: ModelFamily) -> String {
    wire_str(&family)
}

fn refusal_code(code: RefusalCode) -> &'static str {
    match code {
        RefusalCode::AccountSelectionUnavailable => "account_selection_unavailable",
        RefusalCode::AccountSelectionUnsupported => "account_selection_unsupported",
        RefusalCode::AccountNotAuthorized => "account_not_authorized",
        RefusalCode::AccountReauthRequired => "auth_required",
        RefusalCode::AccountQuotaUnknown => "account_quota_unknown",
        RefusalCode::AccountPoolReserveLimited => "account_pool_reserve_limited",
        RefusalCode::AccountPoolExhausted => "account_pool_exhausted",
    }
}

/// `(outcome, code, account_id, credential_id)` for the decision log.
fn outcome_columns(decision: &Decision) -> (&'static str, String, Option<String>, Option<String>) {
    match &decision.outcome {
        Outcome::Admit(admission) => (
            "admit",
            wire_str(&admission.reason),
            Some(admission.account_id.clone()),
            Some(admission.credential_id.clone()),
        ),
        Outcome::Hold(hold) => (
            "hold",
            "account_hold".to_string(),
            Some(hold.account_id.clone()),
            Some(hold.credential_id.clone()),
        ),
        Outcome::Refuse(refusal) => ("refuse", refusal_code(refusal.code).to_string(), None, None),
    }
}

pub(super) struct RecordInput<'a> {
    pub(super) decision_id: &'a str,
    pub(super) mode: &'a str,
    pub(super) trigger: &'a str,
    pub(super) principal: &'a str,
    pub(super) admission_id: Option<&'a str>,
    pub(super) request_mode: Option<&'a str>,
    pub(super) decision: &'a Decision,
    pub(super) context: Value,
    pub(super) now_ms: i64,
}

pub(super) fn decision_record(input: RecordInput<'_>) -> AccountDecisionRecord {
    let (outcome, code, account_id, credential_id) = outcome_columns(input.decision);
    let explanation = &input.decision.explanation;
    AccountDecisionRecord {
        decision_id: input.decision_id.to_string(),
        mode: input.mode.to_string(),
        trigger: input.trigger.to_string(),
        principal: input.principal.to_string(),
        admission_id: input.admission_id.map(str::to_owned),
        request_mode: input.request_mode.map(str::to_owned),
        provider: explanation.provider.clone(),
        model: explanation.model.clone(),
        family: family_name(explanation.family),
        outcome: outcome.to_string(),
        code,
        account_id,
        credential_id,
        policy_version: explanation.policy_version,
        decision_json: wire(input.decision).to_string(),
        context_json: Some(input.context.to_string()),
        created_at: input.now_ms,
    }
}

/// Per-account exclusions as a refusal reports them: one entry per account
/// and reason, `{"account_id", "reason", ...}`, with a reserve exclusion split
/// into one entry per window it names (`"window": "seven_day"`).
fn wire_exclusions(decision: &Decision) -> Vec<Value> {
    let mut out = Vec::new();
    for note in &decision.explanation.accounts {
        for reason in &note.exclusions {
            let Value::Object(mut fields) = wire(reason) else {
                continue;
            };
            if let Some(Value::Array(windows)) = fields.remove("windows") {
                for window in windows {
                    let mut entry = fields.clone();
                    entry.insert("account_id".into(), json!(note.account_id));
                    entry.insert("window".into(), window);
                    out.push(Value::Object(entry));
                }
                continue;
            }
            fields.insert("account_id".into(), json!(note.account_id));
            out.push(Value::Object(fields));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// route.account_for
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum RequestMode {
    #[default]
    Admit,
    Renew,
    Reassign,
}

impl RequestMode {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            RequestMode::Admit => "admit",
            RequestMode::Renew => "renew",
            RequestMode::Reassign => "reassign",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct CallerParams {
    pub(super) runner: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
struct PinRef {
    pub(super) id: String,
    generation: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct CandidateParams {
    pub(super) account_id: String,
    pub(super) credential_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct CandidatesParams {
    pub(super) view_digest: String,
    #[serde(default)]
    pub(super) accounts: Vec<CandidateParams>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct AccountRefParams {
    #[serde(default)]
    pub(super) account_id: Option<String>,
    #[serde(default)]
    pub(super) credential_id: Option<String>,
}

impl AccountRefParams {
    fn to_ref(&self) -> Option<AccountRef> {
        match (&self.account_id, &self.credential_id) {
            (Some(account_id), _) => Some(AccountRef::AccountId(account_id.clone())),
            (None, Some(credential_id)) => Some(AccountRef::CredentialId(credential_id.clone())),
            (None, None) => None,
        }
    }
}

/// The parameters `route.account_for` accepts. Field names are the wire
/// names, snake_case like the rest of the account-selection schema.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AccountForParams {
    #[serde(default)]
    pub(super) fork_of: Option<ForkParams>,
    #[serde(skip)]
    pub(super) continuation_input: Option<continuation::Inputs>,
    #[serde(default)]
    pub(super) head_turn: bool,
    #[serde(skip)]
    pub(crate) call: Option<call_log::AccountCall>,
    #[serde(default)]
    pub(super) service_tier: Option<cortexkit_role_routing::service_tier::ServiceTier>,
    #[serde(skip)]
    pub(super) service_tier_chosen: Option<cortexkit_role_routing::service_tier::ServiceTier>,
    #[serde(skip)]
    pub(super) service_tier_bound: bool,
    /// Routing generated `admission_id` itself (a continuation's reservation),
    /// so no earlier admission can hold it and the writer skips its keyed
    /// existence check.
    #[serde(skip)]
    pub(super) admission_id_generated: bool,
    #[serde(default)]
    pub(super) parent_conversation_key: Option<String>,
    #[serde(default)]
    pub(super) hold_ok: Option<bool>,
    #[serde(default)]
    pub(super) request_input_tokens: Option<u64>,
    pub(super) provider: String,
    pub(super) model: String,
    #[serde(default)]
    pub(super) affinity_model: Option<String>,
    pub(super) conversation_key: String,
    pub(super) caller: CallerParams,
    pub(super) admission_id: String,
    #[serde(default)]
    pub(super) mode: RequestMode,
    #[serde(default)]
    expected_pin: Option<PinRef>,
    #[serde(default)]
    pub(super) initial_input_bytes: u64,
    #[serde(default)]
    pub(super) initial_input_estimated: bool,
    /// Identifies the selection whose stored payment class constrains this
    /// admission, regardless of later edits to the consumer's pay_per_token flag.
    #[serde(default)]
    pub(super) model_decision_id: Option<String>,
    /// Broca's caller-bearing requests name the same binding `decision_id`;
    /// it fills `model_decision_id` when that is absent. Not a serde alias:
    /// the v1 wrapper sends `model_decision_id` beside a continuation's own
    /// `decision_id`, and an alias would refuse that body as a duplicate.
    #[serde(default)]
    pub(super) decision_id: Option<String>,
    #[serde(default)]
    pub(super) candidates: Option<CandidatesParams>,
    #[serde(default)]
    pub(super) requested_account: Option<AccountRefParams>,
    /// A plugin's one-time import of its existing local pin.
    #[serde(default)]
    pub(super) preferred_existing_account: Option<AccountRefParams>,
    #[serde(default)]
    warm_credential: Option<String>,
    #[serde(default)]
    pub(super) hold_wait_max_s: Option<u64>,
    /// The quota pool the requested model draws on, as the runner names it;
    /// absent means the account is one pool.
    #[serde(default)]
    pub(super) pool: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ForkParams {
    pub(super) admission_id: String,
}

impl AccountForParams {
    pub(crate) fn scope_fork_parent(&mut self, key: impl FnOnce(&str) -> String) {
        if let Some(parent) = &mut self.fork_of {
            parent.admission_id = key(&parent.admission_id);
        }
    }
    pub(crate) fn from_v1(
        request: &cortexkit_role_routing::account::AccountForRequest,
        admission_key: String,
        runner: String,
        hold_wait_max_s: u64,
        extras: &Value,
    ) -> Result<Self, OpReply> {
        // Only the legacy-only optional extensions need decoding. The public
        // request has already been parsed and validated by the v1 contract.
        let optional = |name: &str| extras.get(name).cloned().unwrap_or(Value::Null);
        let parsed = Self {
            fork_of: serde_json::from_value(optional("fork_of"))
                .map_err(|e| invalid(e.to_string()))?,
            continuation_input: None,
            head_turn: request.head_turn,
            call: None,
            service_tier: request.service_tier,
            service_tier_chosen: None,
            service_tier_bound: false,
            admission_id_generated: false,
            parent_conversation_key: serde_json::from_value(optional("parent_conversation_key"))
                .map_err(|e| invalid(e.to_string()))?,
            hold_ok: serde_json::from_value(optional("hold_ok"))
                .map_err(|e| invalid(e.to_string()))?,
            request_input_tokens: serde_json::from_value(optional("request_input_tokens"))
                .map_err(|e| invalid(e.to_string()))?,
            provider: request.provider.clone(),
            model: request.model.clone(),
            affinity_model: request.affinity_model.clone(),
            conversation_key: request.conversation_key.clone(),
            caller: CallerParams { runner },
            admission_id: admission_key,
            mode: match request.mode().as_str() {
                "renew" => RequestMode::Renew,
                "reassign" => RequestMode::Reassign,
                _ => RequestMode::Admit,
            },
            expected_pin: request.expected_pin.as_ref().map(|pin| PinRef {
                id: pin.id.clone(),
                generation: pin.generation,
            }),
            initial_input_bytes: request.initial_input_bytes.unwrap_or(0),
            initial_input_estimated: request.initial_input_estimated.unwrap_or(false),
            model_decision_id: request.decision_id.clone(),
            decision_id: None,
            candidates: request
                .candidates
                .as_ref()
                .map(|candidates| CandidatesParams {
                    view_digest: candidates.view_digest.clone(),
                    accounts: candidates
                        .accounts
                        .iter()
                        .map(|account| CandidateParams {
                            account_id: account.account_id.clone(),
                            credential_id: account.credential_id.clone(),
                        })
                        .collect(),
                }),
            requested_account: request
                .requested_account
                .as_ref()
                .map(|account| AccountRefParams {
                    account_id: account.account_id.clone(),
                    credential_id: account.credential_id.clone(),
                }),
            preferred_existing_account: None,
            warm_credential: request.warm_credential.clone(),
            hold_wait_max_s: Some(hold_wait_max_s),
            pool: serde_json::from_value(optional("pool")).map_err(|e| invalid(e.to_string()))?,
        };
        parsed.validate()?;
        Ok(parsed)
    }

    fn validate(&self) -> Result<(), OpReply> {
        if let Some(parent) = &self.fork_of {
            if self.mode != RequestMode::Admit
                || parent.admission_id.trim().is_empty()
                || self.requested_account.is_some()
                || self.warm_credential.is_some()
                || self.parent_conversation_key.is_some()
                || self.candidates.is_some()
            {
                return Err(fork_invalid());
            }
        }
        if self.admission_id.trim().is_empty() {
            return Err(because(
                "admission_id_required",
                invalid("`admission_id` is required and must not be empty"),
            ));
        }
        if self.conversation_key.trim().is_empty() {
            return Err(because(
                "conversation_key_required",
                invalid("`conversation_key` must not be empty"),
            ));
        }
        if self.provider.trim().is_empty() || self.model.trim().is_empty() {
            return Err(because(
                "provider_model_required",
                invalid("`provider` and `model` must not be empty"),
            ));
        }
        if let Some(reply) = invalid_pool(self.pool.as_deref()) {
            return Err(because("pool_name", reply));
        }
        if self.mode != RequestMode::Admit && self.expected_pin.is_none() {
            checking("expected_pin_required");
            return Err(invalid(format!(
                "mode `{}` requires `expected_pin` as {{id, generation}}",
                self.mode.as_str()
            )));
        }
        Ok(())
    }

    fn caller_kind(&self) -> CallerKind {
        if self
            .caller
            .runner
            .eq_ignore_ascii_case(crate::legacy_wire::BROCA)
        {
            CallerKind::Broca
        } else {
            CallerKind::Plugin
        }
    }

    pub(super) fn affinity_model(&self) -> &str {
        self.affinity_model.as_deref().unwrap_or(&self.model)
    }

    /// The allocator's request. `import_allowed` is true only when routing
    /// holds no affinity for this conversation, the one time an import hint
    /// may be considered.
    pub(super) fn request(&self, principal: &str, import_allowed: bool) -> AccountRequest {
        AccountRequest {
            principal: principal.to_string(),
            provider: self.provider.clone(),
            model: self.model.clone(),
            affinity_model: self.affinity_model().to_string(),
            conversation_key: self.conversation_key.clone(),
            // Broca falls back to `broca:<run_id>` when a direct caller sent no
            // conversation key; such a run has no affinity across sends.
            conversation_key_source: if self
                .conversation_key
                .starts_with(crate::legacy_wire::BROCA_RUN_KEY_PREFIX)
            {
                ConversationKeySource::BrocaRunFallback
            } else {
                ConversationKeySource::Caller
            },
            caller: self.caller_kind(),
            // A model other than the affinity model was reached through
            // internal safety recovery, which is not direct work.
            via_safety_recovery: normalize(&self.model) != normalize(self.affinity_model()),
            initial_input_bytes: self.initial_input_bytes,
            initial_input_estimated: self.initial_input_estimated,
            candidates: self.candidates.as_ref().map(|candidates| RunnerCandidates {
                view_digest: candidates.view_digest.clone(),
                accounts: candidates
                    .accounts
                    .iter()
                    .map(|candidate| CandidateRef {
                        account_id: candidate.account_id.clone(),
                        credential_id: candidate.credential_id.clone(),
                    })
                    .collect(),
            }),
            requested_account: self
                .requested_account
                .as_ref()
                .and_then(AccountRefParams::to_ref),
            preferred_existing_account: if import_allowed {
                self.preferred_existing_account
                    .as_ref()
                    .and_then(AccountRefParams::to_ref)
                    // An explicit pin remains routing policy, not a warming
                    // preference. Both one-time hints use the allocator's
                    // import checks rather than pretending to be a live pin.
                    .or_else(|| {
                        (self.mode == RequestMode::Admit && self.requested_account.is_none())
                            .then(|| self.warm_credential.clone().map(AccountRef::CredentialId))
                            .flatten()
                    })
            } else {
                None
            },
            hold_wait_max_s: self.hold_wait_max_s.unwrap_or(DEFAULT_HOLD_WAIT_S),
            pool: request_pool(self.pool.as_deref(), &self.provider, &self.model),
        }
    }
}

pub(super) fn parse_account_for(params: &Value) -> Result<AccountForParams, OpReply> {
    let mut parsed: AccountForParams = serde_json::from_value(params.clone())
        .map_err(|error| invalid(format!("invalid route.account_for params: {error}")))?;
    if parsed.model_decision_id.is_none() {
        parsed.model_decision_id = parsed.decision_id.take();
    }
    parsed.validate()?;
    Ok(parsed)
}

fn fork_invalid() -> OpReply {
    OpReply::typed(
        "invalid_request",
        "fork_of must name an earlier granted admission for this principal and provider",
        json!({"field":"fork_of"}),
    )
}

pub(super) fn account_for(
    params: &Value,
    principal: &str,
    ctx: &Ctx<'_>,
    call: Option<call_log::AccountCall>,
) -> OpReply {
    checking("account_for_parse");
    let mut parsed = match parse_account_for(params) {
        Ok(parsed) => parsed,
        Err(reply) => return reply,
    };
    parsed.call = call;
    account_for_parsed(parsed, principal, ctx, None)
}

pub(crate) fn account_for_parsed(
    parsed: AccountForParams,
    principal: &str,
    ctx: &Ctx<'_>,
    finish: Option<crate::routing_v1::accounts::AdmissionRecord>,
) -> OpReply {
    if !ctx.flags.enabled {
        return OpReply::typed(
            DISABLED_CODE,
            format!(
                "{ACCOUNT_FOR_OP} is not served: account selection is disabled in routing policy"
            ),
            json!({ "code": DISABLED_CODE }),
        );
    }
    let provider = normalize(&parsed.provider);
    let mut classes = ctx.classes.clone();
    if let Some(classes) = &mut classes {
        classes.probe_owner = parsed.model_decision_id.clone();
    }
    let ctx = &Ctx {
        store: ctx.store,
        accounts: ctx.accounts,
        flags: ctx.flags.clone(),
        policy: ctx.policy,
        now_ms: ctx.now_ms,
        classes,
    };
    let request_fingerprint = fingerprint(&parsed);
    checking("admission_replay");
    let admission_id = parsed.admission_id.clone();
    // An admission the published state holds open replays at once. Any other
    // id goes on: the writer command checks it against SQL by key before
    // allocating, and a refusal before the writer checks it here, so a closed
    // admission always replays its stored answer.
    match crate::account_state::open_preflight_admission(
        ctx.store,
        principal,
        &admission_id,
        ctx.now_ms,
    ) {
        Ok(Some(row)) => {
            return replay_command(ctx, principal, row, request_fingerprint, finish);
        }
        Ok(None) => {}
        Err(error) => return store_error(error),
    }
    let mut finish = finish;
    match prepare_and_admit(
        parsed,
        principal,
        ctx,
        provider,
        request_fingerprint.clone(),
        &mut finish,
    ) {
        Ok(reply) => reply,
        Err(refusal) => {
            match crate::account_state::closed_admission(ctx.store, principal, &admission_id) {
                Ok(Some(row)) => replay_command(ctx, principal, row, request_fingerprint, finish),
                Ok(None) => refusal,
                Err(error) => store_error(error),
            }
        }
    }
}

/// Replay `row` in a writer command. An open admission may have moved on
/// since it was read, so the command reads it again; a closed one never
/// changes, so its row is used as read.
fn replay_command(
    ctx: &Ctx<'_>,
    principal: &str,
    row: AccountAdmissionRow,
    request_fingerprint: String,
    finish: Option<crate::routing_v1::accounts::AdmissionRecord>,
) -> OpReply {
    let principal = principal.to_owned();
    let now = ctx.now_ms;
    crate::account_writer::execute_with_error(
        ctx.store,
        striatum_store::AccountCommandKind::Replay,
        move |tx| {
            let row = if matches!(row.state.as_str(), "released" | "finished") {
                row.clone()
            } else {
                tx.admission(&principal, &row.admission_id)?
                    .ok_or_else(|| RouteStateStoreError("admission disappeared; retry".into()))?
            };
            let reply = replay(tx, &principal, row, &request_fingerprint, now)?;
            finish_reply(tx, reply, finish.as_ref(), now)
        },
    )
    .unwrap_or_else(admission_error)
}

pub(super) fn admission_error(error: crate::routing_v1::accounts::AdmissionError) -> OpReply {
    match error {
        crate::routing_v1::accounts::AdmissionError::Store(error) => store_error(error),
        crate::routing_v1::accounts::AdmissionError::Refusal(refusal) => OpReply::typed(
            refusal.code.as_str(),
            refusal.message,
            json!(refusal.detail),
        ),
    }
}

/// Prepare the roster and run the admission command. `Err` is a refusal made
/// before the writer, which the caller must check against a closed admission
/// of the same id before answering.
pub(super) fn prepare_and_admit(
    mut parsed: AccountForParams,
    principal: &str,
    ctx: &Ctx<'_>,
    provider: String,
    request_fingerprint: String,
    finish: &mut Option<crate::routing_v1::accounts::AdmissionRecord>,
) -> Result<OpReply, OpReply> {
    if parsed.fork_of.is_some() {
        return fork::prepare_and_admit(
            parsed,
            principal,
            ctx,
            provider,
            request_fingerprint,
            finish,
        );
    }
    checking("roster_prepare");
    let bound_tier = match parsed.model_decision_id.as_deref() {
        Some(id) => match ctx.store.decision_service_tier(id) {
            Ok(tier) => tier.map(|(tier, _)| tier),
            Err(error) => return Err(store_error(error)),
        },
        None => None,
    };
    let tier = bound_tier
        .as_deref()
        .or_else(|| parsed.service_tier.map(|tier| tier.as_str()));
    let unobserved = tier.filter(|tier| *tier != "standard").and_then(|_| {
        ctx.classes.as_ref().and_then(|classes| {
            crate::service_tiers::unobserved_account_input(&classes.credentials).or_else(|| {
                crate::routing_v1::runners::tier_view_unknown(
                    &classes.credentials,
                    &parsed.caller.runner,
                    ctx.now_ms,
                )
                .then_some("routing just started and hasn't read the runner's service tiers yet")
            })
        })
    });
    if let Some(reason) = unobserved {
        return Err(because(
            "service_tier_input_unobserved",
            OpReply::typed(
                "account_selection_unavailable",
                "Routing has just started and hasn't yet loaded what it needs to judge this service tier. Retry in 3 seconds; a bound decision keeps its tier.",
                service_tier_refusal_detail(&parsed, reason, Some(3_000)),
            ),
        ));
    }
    let (mut roster, route_fenced, class, selection) = match prepare_roster(&parsed, ctx) {
        Ok(prepared) => prepared,
        Err(reply) => return Err(reply),
    };
    checking("service_tier");
    // Accounts a route fence or another selection's reserved probe keeps out
    // for now, and when the first of them returns: a bound tier those
    // accounts serve waits for them instead of being refused for good.
    let held_until = route_fenced
        .values()
        .copied()
        .chain(ctx.classes.iter().flat_map(|classes| {
            classes
                .reserved_probe_accounts(&provider, &normalize(&parsed.model), &parsed)
                .into_iter()
                .map(|(_, until)| until)
        }))
        .filter(|until| *until > ctx.now_ms)
        .min();
    parsed.service_tier_chosen =
        match prepare_service_tier(&mut parsed, ctx, &mut roster, class, held_until) {
            Ok(tier) => tier,
            Err(reply) => return Err(reply),
        };
    parsed.continuation_input = Some(continuation::Inputs::prepare(
        &parsed,
        principal,
        ctx.classes.as_ref(),
        class,
        selection.as_ref(),
    ));
    if let Some(reply) = route_fence_refusal(&roster, &route_fenced, &parsed, ctx.now_ms) {
        return Err(because("route_fence", reply));
    }
    #[cfg(test)]
    run_after_roster_hook(&parsed.admission_id);
    if !roster.roster.as_ref().is_some_and(|roster| {
        roster.accounts.iter().any(|account| {
            parsed.candidates.as_ref().is_none_or(|candidates| {
                candidates.accounts.iter().any(|candidate| {
                    candidate.account_id == account.account_id
                        && account
                            .credentials
                            .iter()
                            .any(|credential| credential.credential_id == candidate.credential_id)
                })
            })
        })
    }) {
        if let Some(classes) = ctx.classes.as_ref() {
            if let Some(reply) = empty_roster_refusal(
                classes,
                &provider,
                &normalize(&parsed.model),
                class == striatum_core::accounts::AllowedAccountClass::PricedPerTokenOnly,
                &route_fenced,
                ctx.now_ms,
                Some(&parsed),
            ) {
                return Err(reply);
            }
            if let Some(reply) = disjoint_candidates_refusal(&parsed, ctx, &roster, class) {
                return Err(reply);
            }
            if roster
                .roster
                .as_ref()
                .is_none_or(|roster| roster.accounts.is_empty())
            {
                // No eligible set is available to prove a stable disagreement.
                // Keep temporary exclusions on the existing retry path.
                let accounts: BTreeSet<_> = classes.facts.iter()
                    .filter(|fact| fact.provider_id == provider && fact.model_id == normalize(&parsed.model))
                    .flat_map(|fact| classes.class(fact, class == striatum_core::accounts::AllowedAccountClass::PricedPerTokenOnly).rows)
                    .map(|row| classes.account_id(row).to_owned()).collect();
                return Err(because("candidates_filter", OpReply::typed(
                    "account_selection_unavailable",
                    "The runner's candidate list leaves out every account that can serve this model, so routing admitted none. Each exclusion names an account the runner didn't offer. Offer those accounts as candidates, or send no candidate list to let routing choose.",
                    json!({"code":"account_selection_unavailable", "retry_after_s":null, "decision_id":null,
                        "exclusions":accounts.into_iter().map(|account| json!({"account_id":account,"reason":"not_in_runner_candidates"})).collect::<Vec<_>>()}),
                )));
            }
        } else if roster
            .roster
            .as_ref()
            .is_none_or(|roster| roster.accounts.is_empty())
        {
            // Without class context the vault/quota roster names nothing usable
            // for this provider/model: no filtering by eligibility, size or
            // candidates happens here, so an empty roster means no credential
            // of the right kind serves it. That is `auth_required`, untimed:
            // the runner needs a login it does not have.
            checking("missing_model_credentials");
            return Err(OpReply::typed(
                "auth_required",
                format!("routing selects no accounts for provider `{provider}`"),
                json!({
                    "code": "auth_required",
                    "retry_after_s": null,
                    "decision_id": null,
                    "exclusions": [],
                }),
            ));
        }
    }
    // The roster is read before the transaction: no lock or fetch happens
    // while the selection transaction is open.
    let command_ctx = ctx.command_context();
    let principal = principal.to_owned();
    let finish = finish.take();
    Ok(crate::account_writer::execute_with_error(
        ctx.store,
        striatum_store::AccountCommandKind::Admit,
        move |tx| {
            // Expired probes and offers the selection read as released are
            // released before the admission decides.
            tx.release_expired(command_ctx.now_ms)?;
            tx.release_expired_continuations(command_ctx.now_ms)?;
            let reply = serve(
                tx,
                &command_ctx,
                &principal,
                &parsed,
                &provider,
                &roster,
                &request_fingerprint,
            )?;
            finish_reply(tx, reply, finish.as_ref(), command_ctx.now_ms)
        },
    )
    .unwrap_or_else(admission_error))
}

pub(super) fn finish_reply(
    tx: &AccountTx<'_>,
    reply: OpReply,
    finish: Option<&crate::routing_v1::accounts::AdmissionRecord>,
    now: i64,
) -> Result<OpReply, crate::routing_v1::accounts::AdmissionError> {
    match finish {
        Some(finish) => finish.finish(tx, reply, now),
        None => Ok(reply),
    }
}

#[cfg(test)]
type RosterHook = Box<dyn FnOnce() + Send>;

/// Test hooks that `account_for` runs once, for the named admission, after its
/// roster is prepared and before an empty roster is explained. A test uses one
/// to run a competing admission at exactly that point instead of hoping two
/// concurrent requests interleave there.
#[cfg(test)]
static AFTER_ROSTER_HOOKS: std::sync::Mutex<Vec<(String, RosterHook)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) fn after_roster_prepared(admission_id: &str, hook: impl FnOnce() + Send + 'static) {
    AFTER_ROSTER_HOOKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push((admission_id.to_owned(), Box::new(hook)));
}

/// Run and remove the test hook registered under `key` (an admission id, or
/// another stage's key such as `v1-replay-checked:<admission id>`).
#[cfg(test)]
pub(crate) fn run_after_roster_hook(admission_id: &str) {
    // Take the hook out before running it: it may run another admission,
    // which reaches this point too.
    let hook = {
        let mut hooks = AFTER_ROSTER_HOOKS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        hooks
            .iter()
            .position(|(id, _)| id == admission_id)
            .map(|at| hooks.remove(at).1)
    };
    if let Some(hook) = hook {
        hook();
    }
}

pub(super) fn prepare_roster(
    parsed: &AccountForParams,
    ctx: &Ctx<'_>,
) -> Result<
    (
        RosterBuild,
        BTreeMap<String, i64>,
        striatum_core::accounts::AllowedAccountClass,
        Option<striatum_store::V1Decision>,
    ),
    OpReply,
> {
    let provider = normalize(&parsed.provider);
    let selection = parsed
        .model_decision_id
        .as_deref()
        .map(|id| ctx.store.v1_decision(id))
        .transpose()
        .map_err(store_error)?
        .flatten();
    let class = match (parsed.model_decision_id.as_deref(), selection.as_ref()) {
        (_, Some(decision)) => decision.allowed_account_class,
        (None, None) => striatum_core::accounts::AllowedAccountClass::NotPayPerToken,
        (Some(id), None) => match ctx.store.decision_account_class(id) {
            Ok(Some(class)) => class,
            Ok(None) => {
                checking("unknown_decision");
                return Err(OpReply::error(
                    "unknown_decision",
                    "selection decision does not exist",
                ));
            }
            Err(error) => return Err(store_error(error)),
        },
    };
    let mut route_fenced = BTreeMap::new();
    let mut roster = match ctx.classes.as_ref() {
        Some(classes) => {
            let (roster, fenced) = classes.roster_before_login_conditions(
                &provider,
                &normalize(&parsed.model),
                class == striatum_core::accounts::AllowedAccountClass::PricedPerTokenOnly,
            );
            route_fenced = fenced;
            roster
        }
        None => roster_for_model(ctx, &provider, Some(&normalize(&parsed.model))),
    };
    if let Some(id) = parsed.model_decision_id.as_deref() {
        match ctx.store.decision_service_tier(id) {
            Ok(Some((tier, allowed))) => {
                if cortexkit_role_routing::service_tier::ServiceTier::parse(&tier).is_none() {
                    return Err(unservable_refusal(
                        cortexkit_role_routing::errors::AccountUnservableReason::StoredStateInvalid,
                        vec![],
                        vec![],
                    ));
                }
                // A decision from the older model-selection API has no routing/v1
                // row naming its runner. The requesting runner will serve the run,
                // so check that runner's tier support instead.
                let runner = Some(selection.as_ref().map_or_else(
                    || normalize(&parsed.caller.runner),
                    |decision| decision.runner_module.clone(),
                ));
                let registry = ctx.classes.as_ref().map(|classes| {
                    crate::routing_v1::runners::Registry::build(
                        classes.config.as_ref(),
                        &classes.credentials,
                        &classes.facts,
                        classes.now_ms.max(0) as u64,
                    )
                });
                if let Some(roster) = &mut roster.roster {
                    for account in &mut roster.accounts {
                        // The standard tier needs no plan, model or runner
                        // support, so only the decision's allowed accounts apply.
                        account.credentials.retain(|credential| {
                            allowed.contains(&credential.credential_id)
                                && (tier == "standard"
                                    || ctx.classes.as_ref().is_some_and(|classes| {
                                        let fact = classes.facts.iter().find(|fact| {
                                            fact.provider_id == provider
                                                && fact.model_id == normalize(&parsed.model)
                                        });
                                        let row = classes
                                            .credentials
                                            .listed_credentials()
                                            .iter()
                                            .find(|row| row.id == credential.credential_id);
                                        match (fact, row, runner.as_deref()) {
                                        (Some(fact), Some(row), Some(runner)) => {
                                            crate::service_tiers::model_offers(
                                                &classes.credentials,
                                                fact,
                                                &tier,
                                            ) && crate::service_tiers::plan_rate(
                                                &classes.credentials.plan_prices,
                                                row,
                                                &provider,
                                                &tier,
                                                false,
                                            )
                                            .is_some()
                                                && (tier == "standard"
                                                    || registry
                                                        .as_ref()
                                                        .and_then(|registry| registry.get(runner))
                                                        .is_some_and(|runner| {
                                                            crate::service_tiers::runner_supports(
                                                                &classes.credentials,
                                                                runner,
                                                                fact,
                                                                row,
                                                                &tier,
                                                            )
                                                        }))
                                        }
                                        _ => false,
                                    }
                                    }))
                        });
                    }
                    roster
                        .accounts
                        .retain(|account| !account.credentials.is_empty());
                }
            }
            Ok(None) => {}
            Err(error) => return Err(store_error(error)),
        }
    }
    if let Some(classes) = &ctx.classes {
        classes.restrict_size(
            &mut roster,
            &provider,
            &normalize(&parsed.model),
            parsed
                .request_input_tokens
                .unwrap_or_else(|| parsed.initial_input_bytes.div_ceil(4)),
        );
        let detail = classes.filter_login_conditions(&mut roster, |account, credential| {
            parsed.candidates.as_ref().is_none_or(|candidates| {
                candidates.accounts.iter().any(|candidate| {
                    candidate.account_id == account && candidate.credential_id == credential
                })
            })
        });
        if let Some(detail) = detail.filter(|_| route_fenced.is_empty()) {
            return Err(because("login_condition", OpReply::typed(
                "auth_required",
                "The vault lists a provider condition that requires action on the offered logins.",
                detail.detail(),
            )));
        }
    }
    Ok((roster, route_fenced, class, selection))
}

/// Check a cold worker launch without reserving an account or creating affinity.
/// The runner still performs the usual atomic admission when it starts sending.
#[cfg(test)]
pub(crate) fn preflight_model(
    ctx: &Ctx<'_>,
    provider: &str,
    model: &str,
) -> Result<(), (String, String)> {
    preflight_model_for_class(ctx, provider, model, false)
}
#[cfg(test)]
pub(crate) fn preflight_model_for_class(
    ctx: &Ctx<'_>,
    provider: &str,
    model: &str,
    priced_per_token: bool,
) -> Result<(), (String, String)> {
    preflight_model_for_class_with_reset(ctx, provider, model, priced_per_token)
        .map_err(|refusal| (refusal.code, refusal.message))
}
pub(crate) struct PreflightRefusal {
    pub code: String,
    pub message: String,
    pub reset_at_ms: Option<i64>,
}

/// Keep dated account evidence separate from the legacy diagnostic message.
pub(crate) fn preflight_model_for_class_with_reset(
    ctx: &Ctx<'_>,
    provider: &str,
    model: &str,
    priced_per_token: bool,
) -> Result<(), PreflightRefusal> {
    if !ctx.flags.enabled {
        return Ok(());
    }
    let roster = ctx.classes.as_ref().map_or_else(
        || roster_for_model(ctx, provider, Some(model)),
        |classes| classes.roster_for(provider, model, priced_per_token),
    );
    // Providers outside the account allocator retain their normal credential admission.
    if roster
        .roster
        .as_ref()
        .is_some_and(|roster| roster.accounts.is_empty())
    {
        if ctx.classes.is_some() {
            return Err(PreflightRefusal {
                code: "no_eligible_account".into(),
                message: format!("{provider}/{model}: no eligible account"),
                reset_at_ms: ctx.classes.as_ref().and_then(|classes| {
                    classes
                        .blocked_reset(provider, model, priced_per_token)
                        .flatten()
                }),
            });
        }
        return Ok(());
    }
    let request = synthetic_request(provider, model, "worker-model-preflight".into());
    let policy = ctx.provider_policy(provider);
    let inputs = ctx
        .classes
        .as_ref()
        .map_or_else(Default::default, |classes| {
            classes.allocation_context(provider, model)
        });
    // Demand comes from the published account state; a private projection
    // has none and reads SQL.
    let state = if ctx.store.is_private_account_copy() {
        None
    } else {
        Some(
            crate::account_state::published_at(ctx.store, ctx.now_ms).map_err(|error| {
                PreflightRefusal {
                    code: "account_selection_unavailable".into(),
                    message: error.to_string(),
                    reset_at_ms: None,
                }
            })?,
        )
    };
    let unavailable = |error: RouteStateStoreError| PreflightRefusal {
        code: "account_selection_unavailable".into(),
        message: error.to_string(),
        reset_at_ms: None,
    };
    // Fences come from the same published state as the demand, so one
    // decision never mixes two generations.
    let fences = match &state {
        Some(state) if state.blocks.enabled() => load_fences(
            &crate::account_state::Published::State(std::sync::Arc::clone(state)),
            provider,
        ),
        _ => crate::account_state::published(ctx.store, ctx.now_ms)
            .and_then(|blocks| load_fences(&blocks, provider)),
    }
    .map_err(unavailable)?;
    let admissions = match &state {
        Some(state) => demand_with(
            |provider, account, floor| Ok(state.admissions.demand(provider, account, floor)),
            provider,
            roster.roster.as_ref(),
            &policy,
            &inputs,
        ),
        None => ctx.store.account_read(|tx| {
            demand_with(
                |provider, account, floor| tx.demand(provider, account, floor),
                provider,
                roster.roster.as_ref(),
                &policy,
                &inputs,
            )
        }),
    }
    .map_err(unavailable)?;
    let decision = striatum_core::accounts::decide_with_context(
        &request,
        &AllocatorState {
            roster: roster.roster.clone(),
            affinity: None,
            fences: fences.clone(),
            principal_exclusions: Vec::new(),
            admissions,
            operator_override: None,
            policy: policy.clone(),
        },
        ctx.now_ms,
        &inputs,
    );
    match decision.outcome {
        Outcome::Admit(_) => Ok(()),
        Outcome::Hold(hold) => Err(PreflightRefusal {
            code: "quota_exhausted".into(),
            message: format!(
                "{provider}/{model}: quota_exhausted until {}",
                rfc3339(hold.reset_at_ms)
            ),
            reset_at_ms: Some(hold.reset_at_ms),
        }),
        Outcome::Refuse(refusal) => {
            let code = refusal_code(refusal.code).to_string();
            // A retry includes jitter/margin and is not itself a provider reset.
            // Name the published reset only for quota refusals with dated evidence.
            let family = ctx.policy.family_for_model(model);
            let resets = roster
                .roster
                .iter()
                .flat_map(|roster| &roster.accounts)
                .filter_map(|account| account.quota.as_ref())
                .flat_map(|quota| {
                    let mut dates: Vec<i64> = quota
                        .required_windows()
                        .into_iter()
                        .filter_map(|window| quota.window(window))
                        .filter_map(|window| window.resets_at.at_ms())
                        .collect();
                    if let ScopedList::Listed(windows) = &quota.scoped {
                        dates.extend(
                            windows
                                .iter()
                                .filter(|window| {
                                    ctx.policy.family_for_scope(window) == Some(family)
                                })
                                .filter_map(|window| window.resets_at.at_ms()),
                        );
                    }
                    dates
                })
                .chain(
                    fences
                        .iter()
                        .filter(|fence| fence.applies_to(model, family, request.pool.as_deref()))
                        .filter_map(|fence| fence.reset.at_ms()),
                );
            let reset_at_ms = if matches!(
                refusal.code,
                RefusalCode::AccountPoolExhausted | RefusalCode::AccountPoolReserveLimited
            ) {
                resets.filter(|date| *date > ctx.now_ms).min()
            } else {
                None
            };
            let clearing = if matches!(
                refusal.code,
                RefusalCode::AccountPoolExhausted | RefusalCode::AccountPoolReserveLimited
            ) {
                reset_at_ms
                    .map(|date| format!(" until {}", rfc3339(date)))
                    .unwrap_or_else(|| "; clearing time unknown".into())
            } else {
                String::new()
            };
            Err(PreflightRefusal {
                message: format!("{provider}/{model}: {code}{clearing}"),
                code,
                reset_at_ms: ctx.classes.as_ref().map_or(reset_at_ms, |classes| {
                    classes
                        .blocked_reset(provider, model, priced_per_token)
                        .flatten()
                }),
            })
        }
    }
}

/// The whole admission inside one write transaction: idempotency, the
/// compare-and-set, the decision and its commit. The transaction commits
/// before the reply leaves, so a reply always describes a durable decision.
/// How long a request whose probe claim was refused while every clock
/// looked due waits before asking again.
const LOST_PROBE_RETRY_MS: i64 = 1_000;

/// When a request whose claim on `origins` was refused may try again: the
/// latest time any of them opens. A slot another request holds stays closed
/// until its claim's interval ends, even where a provider reset is already
/// due, so that interval is the retry time; quota origins share one periodic
/// check. A claim refused while every clock looks due (its evidence changed
/// under it) is retried shortly rather than answered without a time.
/// `owner` is the decision that may already own the probe.
pub(crate) fn lost_probe_retry_at(
    origins: &[striatum_store::FenceRecheck],
    owner: Option<&str>,
    now: i64,
) -> i64 {
    let quota_until = striatum_store::FenceRecheck::quota_group_at(origins, owner);
    origins
        .iter()
        .map(|origin| {
            let owned = owner.is_some() && origin.claim_owner.as_deref() == owner;
            if origin.quota {
                quota_until
            } else if origin.claim_owner.is_some() && !owned {
                origin.due_at().max(origin.next_recheck_at)
            } else {
                origin.due_at()
            }
        })
        .filter(|until| *until > now)
        .max()
        .unwrap_or(now.saturating_add(LOST_PROBE_RETRY_MS))
}

pub(super) fn serve(
    tx: &mut crate::account_state::AccountCommand<'_>,
    ctx: &CommandContext,
    principal: &str,
    params: &AccountForParams,
    provider: &str,
    roster: &RosterBuild,
    request_fingerprint: &str,
) -> RouteStateResult<OpReply> {
    if let Some(call) = &params.call {
        call.reset();
    }
    checking("admission_transaction");
    let now = ctx.now_ms;
    let policy = ctx.provider_policy(provider);
    // An admission not open in memory may be closed in SQL: check by key here,
    // before allocating, so a closed admission replays its stored answer.
    let existing = if params.admission_id_generated {
        tx.open_admission(principal, &params.admission_id)?
    } else {
        tx.admission(principal, &params.admission_id)?
    };
    if let Some(existing) = existing {
        return replay(tx, principal, existing, request_fingerprint, now);
    }
    let salt = tx.salt(now, fresh_salt)?;
    let hash = conversation_hash(&salt, principal, &params.conversation_key);
    let affinity_row = tx.affinity(principal, &hash, provider)?;
    let mut fork_parent = fork::resolve(tx, principal, params, provider)?;
    if params.fork_of.is_some() && fork_parent.is_none() {
        return Ok(fork_invalid());
    }
    // The tier-filtered roster must still contain the conversation's exact
    // credential. A healthy sibling cannot replace a bound session's pin.
    if params.service_tier_bound
        && affinity_row.as_ref().is_some_and(|affinity| {
            !roster.roster.as_ref().is_some_and(|roster| {
                roster.accounts.iter().any(|account| {
                    account.account_id == affinity.account_id
                        && account.credentials.iter().any(|credential| {
                            credential.credential_id == affinity.credential_id
                                && tier_candidate_matches(params, account, credential)
                        })
                })
            })
        })
    {
        return Ok(bound_service_tier_refusal(params));
    }
    let established = match affinity_row.as_ref() {
        Some(affinity) => tx.pin_has_started(affinity)?,
        None => false,
    };
    let mut roster = roster.clone();
    // Explicit candidates already bound the choice. Keep their blocked
    // credentials until the atomic probe check can preserve the refusal cause.
    if !established && params.candidates.is_none() && params.fork_of.is_none() {
        if let Some(classes) = &ctx.classes {
            classes.avoid_transient_accounts(&mut roster, provider, &params.model);
        }
    }
    let roster = &roster;
    if let Some(expected) = &params.expected_pin {
        let current = affinity_row
            .as_ref()
            .map(|row| (row.pin_id.as_str(), row.pin_generation));
        if params.mode != RequestMode::Admit
            && current != Some((expected.id.as_str(), expected.generation))
        {
            return Ok(pin_conflict(affinity_row.as_ref(), expected));
        }
    }
    refresh_blocks(
        tx,
        provider,
        roster
            .roster
            .as_ref()
            .map_or(&[][..], |roster| &roster.accounts),
        now,
        &policy,
    )?;
    // The allocation context comes first: whether an account may go on paid
    // credits decides how much of its demand history the allocator can use.
    let mut inputs = ctx
        .classes
        .as_ref()
        .map_or_else(Default::default, |classes| {
            classes.allocation_context(provider, &params.model)
        });
    inputs.hold_ok = params.hold_ok;
    if let (Some(classes), Some(roster)) = (&ctx.classes, &roster.roster) {
        inputs.credit_expiry_by_credential = classes.expiring_org_credits(roster);
    }
    if let Some(parent) = &params.parent_conversation_key {
        let hash = conversation_hash(&salt, principal, parent);
        inputs.parent_account_id = tx.parent_used_account(principal, &hash, provider, now)?;
    }
    let admissions = if params.fork_of.is_some() {
        Vec::new()
    } else {
        demand_with(
            |provider, account, floor| tx.demand(provider, account, floor),
            provider,
            roster.roster.as_ref(),
            &policy,
            &inputs,
        )?
    };
    let blocks = load_blocks(tx, provider)?;
    let bound_pool = request_pool(params.pool.as_deref(), provider, &params.model);
    let mut origins = tx.admission_fence_rechecks(provider)?;
    let fork_retry = fork_parent
        .as_mut()
        .and_then(|parent| fork::apply_blocking_origins(parent, &origins, provider, params, now));
    inputs.fork_parent = fork_parent;
    origins.retain(|origin| {
        origin.kind != "account"
            || blocks.iter().any(|(id, fence)| {
                origin.origin_key == json!([id]).to_string()
                    && fence.applies_to(
                        &params.model,
                        policy.family_for_model(&params.model),
                        bound_pool.as_deref(),
                    )
            })
    });
    // Which probe clocks (the fence_recheck rows that schedule each block's
    // recovery test) apply to this admission was decided from the working
    // state, and neither the admission nor its claim names the rows that
    // ruled a clock out. Check those rows against SQL first.
    tx.check_decision_inputs(&[provider, ""])?;
    // So does which credentials its principal's exclusions rule out.
    tx.check_admission_exclusions(principal, &params.admission_id)?;
    let targets = |origin: &striatum_store::FenceRecheck, account: &str, credential: &str| {
        ctx.classes.as_ref().map_or_else(
            || {
                let named = origin
                    .credential_id
                    .as_deref()
                    .filter(|id| {
                        origin.quota
                            && roster.roster.as_ref().is_some_and(|roster| {
                                roster.accounts.iter().any(|view| {
                                    view.account_id == account
                                        && view
                                            .credentials
                                            .iter()
                                            .any(|record| record.credential_id == *id)
                                })
                            })
                    })
                    .unwrap_or(credential);
                origin.applies(provider, account, named, &params.model)
            },
            |classes| classes.origin_targets(origin, provider, account, credential, &params.model),
        )
    };
    let size = striatum_core::accounts::input_size_class(
        params
            .request_input_tokens
            .unwrap_or_else(|| params.initial_input_bytes.div_ceil(4)),
    ) as i64;
    let quota_due = |account: &str, credential: &str| {
        striatum_store::FenceRecheck::quota_group_due(
            origins.iter().filter(|origin| {
                targets(origin, account, credential)
                    && origin.principal.is_none()
                    && origin.size_class.is_none_or(|class| class == size)
            }),
            now,
            params.model_decision_id.as_deref(),
        )
    };
    let block_fences: Vec<_> = blocks.iter().map(|(_, fence)| fence.clone()).collect();
    let binding_reading = |origin: &striatum_store::FenceRecheck, account: &str| {
        ctx.classes.as_ref().map_or_else(
            || {
                let reading = roster
                    .roster
                    .as_ref()
                    .and_then(|roster| {
                        roster
                            .accounts
                            .iter()
                            .find(|view| view.account_id == account)
                    })
                    .and_then(|view| view.quota.as_ref());
                quota_reading_binds_origin(origin, account, &block_fences, reading, now, &policy)
            },
            |classes| classes.origin_has_binding_reading(origin, account),
        )
    };
    let mut probe_roster = roster.roster.clone();
    if let Some(roster) = &mut probe_roster {
        for account in &mut roster.accounts {
            let fresh = origins
                .iter()
                .filter(|origin| {
                    origin.quota
                        && account.credentials.iter().any(|credential| {
                            targets(origin, &account.account_id, &credential.credential_id)
                        })
                })
                .any(|origin| binding_reading(origin, &account.account_id));
            if !fresh
                && origins.iter().any(|origin| {
                    origin.quota
                        && account.credentials.iter().any(|credential| {
                            targets(origin, &account.account_id, &credential.credential_id)
                                && quota_due(&account.account_id, &credential.credential_id)
                        })
                })
            {
                // A stale reading is not an independent wall against the very
                // probe that will recheck its retained quota evidence.
                account.quota = None;
            }
        }
    }
    let mut fences = load_fences(tx, provider)?;
    fences.retain(|fence| {
        params.fork_of.is_some() || {
            !origins.iter().any(|origin| {
                origin.kind == "account"
                    && origin.account_id.as_deref() == Some(&fence.account_id)
                    && origin.evidence_at == fence.observed_at_ms
                    && quota_due(&fence.account_id, "")
                    && !binding_reading(origin, &fence.account_id)
            })
        }
    });
    let mut state = AllocatorState {
        roster: probe_roster,
        affinity: affinity_row
            .as_ref()
            .map(|row| conversation_affinity(row, now)),
        fences,
        principal_exclusions: load_exclusions(&*tx, principal, &params.admission_id)?,
        admissions,
        operator_override: None,
        policy: policy.clone(),
    };
    let request = params.request(principal, affinity_row.is_none());
    let mut reserved_probe = false;
    let mut lost_probe_fences = BTreeMap::new();
    let mut gate = concurrency_gate::Gate::new(ctx, params);
    let decision = loop {
        let mut decision =
            striatum_core::accounts::decide_with_context(&request, &state, now, &inputs);
        let Outcome::Admit(admission) = &decision.outcome else {
            if let (Some(parent), Some(until), Outcome::Hold(hold)) =
                (&inputs.fork_parent, fork_retry, &decision.outcome)
            {
                if until > hold.reset_at_ms {
                    break fork::hold_after_lost_probe(decision, parent, until, now);
                }
            }
            if gate.exhausted(&mut state) {
                continue;
            }
            break decision;
        };
        let credential = admission.credential_id.clone();
        let applicable: Vec<_> = origins
            .iter()
            .filter(|origin| {
                origin.kind != "principal"
                    || state.principal_exclusions.iter().any(|exclusion| {
                        exclusion.credential_id == credential
                            && exclusion.recorded_at_ms == origin.evidence_at
                            && state.roster.as_ref().is_none_or(|roster| {
                                roster.view_digest == exclusion.roster_view_digest
                            })
                            && request.candidates.as_ref().is_none_or(|candidates| {
                                candidates.view_digest == exclusion.runner_view_digest
                            })
                    })
            })
            .filter(|origin| {
                ctx.classes
                    .as_ref()
                    .is_none_or(|classes| !classes.origin_recovered(origin, &admission.account_id))
            })
            .filter(|origin| {
                targets(origin, &admission.account_id, &credential)
                    && origin.principal.as_deref().is_none_or(|id| id == principal)
                    && origin
                        .admission_id
                        .as_deref()
                        .is_none_or(|id| id == params.admission_id)
                    && origin.size_class.is_none_or(|class| {
                        class
                            == striatum_core::accounts::input_size_class(
                                params
                                    .request_input_tokens
                                    .unwrap_or_else(|| params.initial_input_bytes.div_ceil(4)),
                            ) as i64
                    })
            })
            .cloned()
            .collect();
        if applicable.is_empty() {
            match gate.check(
                tx, ctx, params, principal, &hash, provider, admission, &mut state,
            ) {
                concurrency_gate::Check::Next => continue,
                concurrency_gate::Check::Room | concurrency_gate::Check::Full => break decision,
            }
        }
        let owns_probe = |origin: &striatum_store::FenceRecheck| {
            params.model_decision_id.is_some() && origin.claim_owner == params.model_decision_id
        };
        let non_quota_due = |origin: &striatum_store::FenceRecheck| {
            ctx.classes
                .as_ref()
                .map_or(origin.due_at() <= now || owns_probe(origin), |classes| {
                    classes.origin_due(origin, &admission.account_id, &params.model)
                })
        };
        let blocking_transient_until = applicable
            .iter()
            .filter(|origin| {
                origin.kind == "transient_route" && origin.due_at() > now && !owns_probe(origin)
            })
            .map(|origin| origin.due_at())
            .max();
        if params.fork_of.is_none()
            && established
            && state.affinity.as_ref().is_some_and(|affinity| {
                affinity.account_id == admission.account_id && affinity.credential_id == credential
            })
            && blocking_transient_until.is_some()
            && applicable
                .iter()
                .filter(|origin| origin.kind != "transient_route")
                .all(non_quota_due)
        {
            // Use the transaction-current origins: another request may have
            // claimed a transient probe since this roster was prepared. The
            // selecting decision's own claim must reach atomic consumption.
            let affinity = state.affinity.as_ref().unwrap();
            let until = blocking_transient_until.unwrap();
            let seconds = (until.saturating_sub(now).max(0) as u64).div_ceil(1_000);
            decision.outcome = Outcome::Hold(striatum_core::accounts::Hold {
                account_id: affinity.account_id.clone(),
                credential_id: affinity.credential_id.clone(),
                pin_id: affinity.pin_id.clone(),
                pin_generation: affinity.pin_generation,
                reset_at_ms: until,
                base_retry_after_s: seconds,
                retry_after_s: seconds,
                basis: striatum_core::accounts::HoldBasis::ConfirmedFence,
            });
            decision.explanation.affinity = striatum_core::accounts::AffinityNote::Held {
                account_id: affinity.account_id.clone(),
            };
            break decision;
        }
        // Before the probe claim: a full account consumes no probe. After the
        // short hold above, which is the allocator's own answer and comes first.
        match gate.check(
            tx, ctx, params, principal, &hash, provider, admission, &mut state,
        ) {
            concurrency_gate::Check::Room => {}
            concurrency_gate::Check::Next => continue,
            concurrency_gate::Check::Full => break decision,
        }
        let claim = || {
            let ready = applicable.iter().all(|origin| {
                if origin.quota {
                    let fresh = binding_reading(origin, &admission.account_id);
                    return !fresh
                        && striatum_store::FenceRecheck::quota_group_due(
                            &applicable,
                            now,
                            params.model_decision_id.as_deref(),
                        );
                }
                non_quota_due(origin)
            });
            Ok(
                if ready
                    && tx.claim_fence_rechecks_for_authorized_admission(
                        &applicable,
                        now,
                        params.model_decision_id.as_deref(),
                        &admission_probe_owner(principal, &params.admission_id),
                    )?
                {
                    striatum_store::CredentialProbeClaim::Claimed
                } else {
                    striatum_store::CredentialProbeClaim::Blocked
                },
            )
        };
        let claimed = if let Some(classes) = &ctx.classes {
            probe_reservations::claim(
                &classes.probe_scope,
                provider,
                &credential,
                classes.probe_owner.as_deref(),
                now,
                claim,
            )?
        } else {
            probe_reservations::ProbeClaim::Store(claim()?)
        };
        let claimed = match claimed {
            probe_reservations::ProbeClaim::Store(claimed) => claimed,
            probe_reservations::ProbeClaim::ReservedUntil(until) => {
                if let Some(parent) = &inputs.fork_parent {
                    break fork::hold_after_lost_probe(decision, parent, until, now);
                }
                reserved_probe = true;
                // Use the hold that refused this claim, not a second look:
                // its holder may claim the probe and drop the hold before an
                // empty roster is explained below.
                if let Some(classes) = &ctx.classes {
                    classes.note_probe_reserved(provider, &credential, until);
                }
                lost_probe_fences.insert(admission.account_id.clone(), until);
                inputs.credential_probe_notes.remove(&credential);
                if let Some(roster) = &mut state.roster {
                    for account in &mut roster.accounts {
                        account
                            .credentials
                            .retain(|record| record.credential_id != credential);
                    }
                }
                continue;
            }
        };
        match claimed {
            striatum_store::CredentialProbeClaim::Claimed => break decision,
            striatum_store::CredentialProbeClaim::Unfenced => {
                inputs.credential_probe_notes.remove(&credential);
            }
            striatum_store::CredentialProbeClaim::Blocked => {
                // Another request can claim the probe after scoring prepared
                // this roster. Preserve the actual fence and its retry time
                // (see `lost_probe_retry_at`); removing only the credential
                // misreports missing candidates.
                let until =
                    lost_probe_retry_at(&applicable, params.model_decision_id.as_deref(), now);
                if let Some(parent) = &inputs.fork_parent {
                    break fork::hold_after_lost_probe(decision, parent, until, now);
                }
                lost_probe_fences.insert(admission.account_id.clone(), until);
                inputs.credential_probe_notes.remove(&credential);
                if let Some(roster) = &mut state.roster {
                    for account in &mut roster.accounts {
                        account
                            .credentials
                            .retain(|record| record.credential_id != credential);
                    }
                }
            }
        }
    };
    if gate.refused() {
        return gate.refuse(
            tx, ctx, params, principal, &hash, provider, &decision, roster,
        );
    }
    // An account the class context left out because another selection holds
    // its probe reserved is a lost race as well.
    if params.fork_of.is_none() && !matches!(decision.outcome, Outcome::Admit(_)) {
        if let Some(classes) = &ctx.classes {
            for (account, until) in
                classes.reserved_probe_accounts(provider, &normalize(&params.model), params)
            {
                lost_probe_fences.entry(account).or_insert(until);
            }
        }
    }
    if !matches!(decision.outcome, Outcome::Admit(_)) && !lost_probe_fences.is_empty() {
        let mut remaining = state.roster.clone();
        if let Some(roster) = &mut remaining {
            roster
                .accounts
                .retain(|account| !account.credentials.is_empty());
        }
        let remaining = RosterBuild {
            roster: remaining,
            context: roster.context.clone(),
        };
        if let Some(reply) = route_fence_refusal(&remaining, &lost_probe_fences, params, now) {
            return Ok(because("route_fence", reply));
        }
    }
    if reserved_probe && !matches!(decision.outcome, Outcome::Admit(_)) {
        if let Some(classes) = &ctx.classes {
            if let Some(reply) = empty_roster_refusal(
                classes,
                provider,
                &normalize(&params.model),
                roster.context["priced_per_token"]
                    .as_bool()
                    .unwrap_or(false),
                &BTreeMap::new(),
                now,
                Some(params),
            ) {
                // A reserved probe is a lost race too: the timed rule below
                // applies, whatever the emptied roster's own code says.
                return Ok(match lost_probe_fences.values().copied().min() {
                    Some(lost) => {
                        let (decision_id, exclusions, reset) = match &reply {
                            OpReply::Error {
                                detail: Some(detail),
                                ..
                            } => (
                                detail["decision_id"].clone(),
                                detail["exclusions"].clone(),
                                detail["reset_at_ms"].as_i64(),
                            ),
                            _ => (Value::Null, json!([]), None),
                        };
                        let exclusions = if exclusions.is_null() {
                            json!([])
                        } else {
                            exclusions
                        };
                        lost_probe_refusal(
                            reset.map_or(lost, |reset| reset.min(lost)),
                            now,
                            decision_id,
                            exclusions,
                        )
                    }
                    None => reply,
                });
            }
        }
    }
    if let Some(call) = params.call.as_ref().filter(|_| !gate.enforced()) {
        call.evaluate(
            tx, ctx, params, principal, &hash, &decision, &state, &inputs,
        );
    }
    let decision_id = new_id("acd");
    let mut context = json!({ "roster": roster.context });
    if let (Some(input), Outcome::Admit(admitted)) = (&params.continuation_input, &decision.outcome)
    {
        let mut input = input.clone();
        input.auth_method = ctx
            .classes
            .as_ref()
            .map_or("oauth", |classes| classes.method(&admitted.credential_id))
            .into();
        context["continuation_input"] = json!(input);
    }
    if let Some(credential) = &params.warm_credential {
        let result = if affinity_row.is_some() {
            json!({"kind":"ignored", "reason":"existing_affinity"})
        } else if params.requested_account.is_some() {
            json!({"kind":"ignored", "reason":"explicit_pin"})
        } else {
            wire(&decision.explanation.import)
        };
        context["warm_credential"] = json!({"credential_id":credential, "result":result});
    }
    if ctx
        .classes
        .as_ref()
        .is_some_and(|classes| classes.media_route)
    {
        // Persist the admission's media origin; the same provider can also
        // serve LLM runs, whose success is reported through route.report.
        context["media_admission"] = json!(true);
    }
    tx.insert_decision(&decision_record(RecordInput {
        decision_id: &decision_id,
        mode: DECISION_MODE_SERVE,
        trigger: "request",
        principal,
        admission_id: Some(&params.admission_id),
        request_mode: Some(params.mode.as_str()),
        decision: &decision,
        context,
        now_ms: now,
    }))?;
    if let Some(tier) = params.service_tier_chosen {
        tx.record_account_service_tier(
            &decision_id,
            params.service_tier.map(|tier| tier.as_str()),
            tier.as_str(),
        )?;
    }
    match &decision.outcome {
        Outcome::Admit(admission) => {
            // Read before the insert: afterwards every request of the
            // conversation would look like a renewal of a holder.
            let taken = gate.slot_taken(
                tx,
                ctx,
                params,
                principal,
                &hash,
                provider,
                &admission.account_id,
            );
            let reply = commit_admission(
                tx,
                ctx,
                principal,
                params,
                provider,
                &hash,
                affinity_row.as_ref(),
                &decision_id,
                &decision,
                admission,
                request_fingerprint,
                roster,
            )?;
            // Taking a slot on a count it waits on ends the conversation's
            // wait; a slot elsewhere leaves it waiting.
            if let (Some(count), OpReply::Result(_)) = (&taken, &reply) {
                tx.took_slot(&(principal.to_owned(), hash.clone()), count);
            }
            Ok(reply)
        }
        Outcome::Hold(hold) => {
            tx.insert_hold(&AccountHoldRow {
                decision_id: decision_id.clone(),
                principal: principal.to_string(),
                conversation_hash: hash.clone(),
                provider: provider.to_string(),
                pin_id: hold.pin_id.clone(),
                pin_generation: hold.pin_generation,
                account_id: hold.account_id.clone(),
                credential_id: hold.credential_id.clone(),
                basis: wire_str(&hold.basis),
                reset_at: hold.reset_at_ms,
                retry_after_s: hold.retry_after_s,
                created_at: now,
            })?;
            let refresh_adapter = refresh_adapter_for(roster, &hold.account_id);
            Ok(OpReply::typed(
                "account_hold",
                format!(
                    "account {} is in a short hold; retry after {} s",
                    hold.account_id, hold.retry_after_s
                ),
                fork::hold_detail(
                    params,
                    json!({
                        "code": "account_hold",
                        "retry_after_s": hold.retry_after_s,
                        "decision_id": decision_id,
                        "account": {
                            "provider": provider,
                            "account_id": hold.account_id,
                            "credential_id": hold.credential_id,
                            "refresh_adapter": refresh_adapter,
                            "method": ctx.classes.as_ref().map_or("oauth", |classes| classes.method(&hold.credential_id)),
                        },
                        "pin": { "id": hold.pin_id, "generation": hold.pin_generation },
                        "reset_at": rfc3339(hold.reset_at_ms),
                        "basis": wire(&hold.basis),
                        "exclusions": wire_exclusions(&decision),
                    }),
                ),
            ))
        }
        Outcome::Refuse(refusal) => {
            checking("allocator_filters");
            // The caller sees the allocator's own contract code. The store
            // keeps that code too (`outcome_columns`), so this mapping must
            // not change the recorded decision, only the reply. The contract
            // has no `account_selection_unsupported`, so map it by cause.
            // The allocator emits it in three places: an empty provider
            // slice of a present roster, a cold roster whose accounts all
            // fail model reach, and a hard-filtered operator override that
            // fails reach (the override never runs here). The first two
            // both mean no credential of the right kind serves this
            // provider/model, which is `auth_required`: the runner needs a
            // login it does not have. It is untimed and must never read as
            // a transient retry. The one temporary case is probe
            // contention: the probe loop below removed every credential
            // after another admission claimed the probe, and a re-read may
            // admit, so that stays `account_selection_unavailable`.
            let mut code = refusal_code(refusal.code);
            if code == "account_selection_unsupported" {
                // The probe loop removes credentials, not accounts: an
                // account left with none counts as removed.
                let probe_emptied = roster
                    .roster
                    .as_ref()
                    .is_some_and(|roster| !roster.accounts.is_empty())
                    && state.roster.as_ref().is_none_or(|roster| {
                        roster
                            .accounts
                            .iter()
                            .all(|account| account.credentials.is_empty())
                    });
                code = if probe_emptied {
                    "account_selection_unavailable"
                } else {
                    "auth_required"
                };
            }
            let reset = pool_recovery_reset(
                &decision,
                state.roster.as_ref(),
                &state.fences,
                &ctx.policy,
                &params.model,
                now,
            );
            let timed = matches!(
                code,
                "account_pool_exhausted" | "account_pool_reserve_limited"
            );
            // A request that lost a probe race to another request is never
            // refused untimed, whatever refused the accounts it had left: the
            // probe it lost is retried at a known time, and an untimed code
            // such as `auth_required` would make the runner stop for a login.
            if let Some(lost) = lost_probe_fences.values().copied().min() {
                return Ok(lost_probe_refusal(
                    reset.map_or(lost, |reset| reset.min(lost)),
                    now,
                    json!(decision_id),
                    json!(wire_exclusions(&decision)),
                ));
            }
            Ok(OpReply::typed(
                code,
                format!("no account admitted: {code}"),
                json!({
                    "code": code,
                    "retry_after_s": timed.then_some(refusal.retry_after_s).flatten(),
                    "decision_id": decision_id,
                    "exclusions": wire_exclusions(&decision),
                    "reset_at_ms": timed.then_some(reset).flatten(),
                }),
            ))
        }
    }
}

/// A duplicate admission id: the same identity gets the committed decision
/// back (marked `expired` once its start window has passed) and charges
/// nothing; different identity fields are a conflict.
pub(super) fn replay(
    tx: &mut crate::account_state::AccountCommand<'_>,
    principal: &str,
    existing: AccountAdmissionRow,
    request_fingerprint: &str,
    now: i64,
) -> RouteStateResult<OpReply> {
    if existing.request_fingerprint != request_fingerprint {
        checking("admission_fingerprint");
        return Ok(OpReply::typed(
            "admission_conflict",
            format!(
                "admission {} was already decided with different parameters",
                existing.admission_id
            ),
            json!({
                "code": "admission_conflict",
                "retry_after_s": null,
                "decision_id": existing.decision_id,
                "exclusions": [],
            }),
        ));
    }
    let mut state = existing.state.clone();
    if state == "reserved" && now >= existing.admission_valid_until {
        tx.mark_admission_expired(principal, &existing.admission_id)?;
        state = "expired".into();
    }
    let mut reply: Value = serde_json::from_str(&existing.reply_json).map_err(|error| {
        RouteStateStoreError(format!("stored admission reply undecodable: {error}"))
    })?;
    if existing.request_mode == "admit" && state == "started" {
        if let Some(current) = tx.latest_used_continuation(principal, &existing.admission_id)? {
            reply = serde_json::from_str(&current.reply_json).map_err(|error| {
                RouteStateStoreError(format!("stored continuation reply undecodable: {error}"))
            })?;
            reply["admission"]["id"] = json!(existing.admission_id);
        }
    }
    reply["admission"]["state"] = json!(state);
    Ok(OpReply::Result(reply))
}

fn pin_conflict(current: Option<&AccountAffinityRow>, expected: &PinRef) -> OpReply {
    checking("pin_mismatch");
    OpReply::typed(
        "pin_generation_conflict",
        format!(
            "expected pin {} generation {} is not the conversation's current pin",
            expected.id, expected.generation
        ),
        json!({
            "code": "pin_generation_conflict",
            "retry_after_s": null,
            "decision_id": null,
            "exclusions": [],
            "current_pin": current.map(|row| json!({
                "id": row.pin_id,
                "generation": row.pin_generation,
                "account_id": row.account_id,
                "credential_id": row.credential_id,
            })),
        }),
    )
}

#[allow(clippy::too_many_arguments)]
fn commit_admission(
    tx: &mut crate::account_state::AccountCommand<'_>,
    ctx: &CommandContext,
    principal: &str,
    params: &AccountForParams,
    provider: &str,
    hash: &str,
    affinity_row: Option<&AccountAffinityRow>,
    decision_id: &str,
    decision: &Decision,
    admission: &striatum_core::accounts::Admission,
    request_fingerprint: &str,
    roster: &RosterBuild,
) -> RouteStateResult<OpReply> {
    let now = ctx.now_ms;
    let pin_mode = match params.caller_kind() {
        CallerKind::Broca => "run_frozen",
        CallerKind::Plugin => "conversation_lease",
    };
    let lease_valid_until = now + LEASE_MS;
    let admission_valid_until = now + ADMISSION_VALID_MS;
    let expected = affinity_row.map(|row| (row.pin_id.as_str(), row.pin_generation));
    let (pin_id, pin_generation, affinity_expires_at, moved) = match &admission.affinity {
        AffinityOutcome::Retained {
            pin_id,
            pin_generation,
        } => {
            let row = affinity_row.ok_or_else(|| {
                RouteStateStoreError("allocator retained an affinity routing does not hold".into())
            })?;
            // Accepted use renews affinity at most once a minute; the lease
            // moves on every admission.
            let (last_used, expires_at) = if now - row.last_used_at >= AFFINITY_TOUCH_MS {
                (now, now + AFFINITY_IDLE_MS)
            } else {
                (row.last_used_at, row.expires_at)
            };
            if !tx.touch_affinity(
                principal,
                hash,
                provider,
                (pin_id, *pin_generation),
                last_used,
                expires_at,
                Some(lease_valid_until),
            )? {
                return Err(RouteStateStoreError(
                    "conversation pin changed inside the admission transaction".into(),
                ));
            }
            (pin_id.clone(), *pin_generation, expires_at, false)
        }
        AffinityOutcome::New {
            previous,
            pin_generation,
        } => {
            let pin_id = new_id("pin");
            tx.insert_pin(&AccountPinRow {
                pin_id: pin_id.clone(),
                principal: principal.to_string(),
                conversation_hash: hash.to_string(),
                provider: provider.to_string(),
                pin_generation: *pin_generation,
                affinity_model: normalize(params.affinity_model()),
                account_id: admission.account_id.clone(),
                credential_id: admission.credential_id.clone(),
                pin_mode: pin_mode.to_string(),
                previous_pin_id: previous.as_ref().map(|pin| pin.pin_id.clone()),
                decision_id: decision_id.to_string(),
                created_at: now,
            })?;
            let binding_revision = match affinity_row {
                Some(row) if row.credential_id == admission.credential_id => {
                    row.credential_binding_revision
                }
                Some(row) => row.credential_binding_revision + 1,
                None => 1,
            };
            let expires_at = now + AFFINITY_IDLE_MS;
            let applied = tx.put_affinity(
                &AccountAffinityRow {
                    principal: principal.to_string(),
                    conversation_hash: hash.to_string(),
                    provider: provider.to_string(),
                    affinity_model: normalize(params.affinity_model()),
                    pin_id: pin_id.clone(),
                    pin_generation: *pin_generation,
                    account_id: admission.account_id.clone(),
                    credential_id: admission.credential_id.clone(),
                    credential_binding_revision: binding_revision,
                    policy_version: decision.explanation.policy_version,
                    initial_demand_bytes: admission.request_bytes,
                    created_at: now,
                    last_used_at: now,
                    expires_at,
                    lease_valid_until: Some(lease_valid_until),
                },
                expected,
            )?;
            if !applied {
                return Err(RouteStateStoreError(
                    "conversation pin changed inside the admission transaction".into(),
                ));
            }
            (pin_id, *pin_generation, expires_at, true)
        }
    };
    // Admission charges its prompt once; a renewal charges nothing; a
    // reassignment charges only when it actually moved the conversation.
    let charged = match params.mode {
        RequestMode::Admit => true,
        RequestMode::Renew => false,
        RequestMode::Reassign => moved,
    };
    let refresh_adapter = refresh_adapter_for(roster, &admission.account_id);
    let mut reply = json!({
        "admission": { "id": params.admission_id, "state": "reserved" },
        "account": {
            "provider": provider,
            "account_id": admission.account_id,
            "credential_id": admission.credential_id,
            "refresh_adapter": refresh_adapter,
            "method": ctx.classes.as_ref().map_or("oauth", |classes| classes.method(&admission.credential_id)),
        },
        "pin": {
            "id": pin_id,
            "generation": pin_generation,
            "affinity_expires_at": rfc3339(affinity_expires_at),
            "admission_valid_until": rfc3339(admission_valid_until),
            "lease_valid_until": rfc3339(lease_valid_until),
            "mode": pin_mode,
        },
        "reason": {
            "code": wire(&admission.reason),
            "decision_id": decision_id,
            "policy_version": decision.explanation.policy_version,
            "quota_fetched_at": admission.quota_observed_at_ms.map(rfc3339),
            "quota_age_band": admission.quota_age_band.map(|band| wire(&band)),
        },
    });
    if let Some(tier) = params.service_tier_chosen {
        reply["service_tier_chosen"] = json!(tier);
    }
    if let Some(note) = &decision.explanation.credential_fence_probe {
        reply["reason"]["fence_probe"] = json!(note);
    }
    if let Some(model_decision) = &params.model_decision_id {
        tx.bind_model_account(
            model_decision,
            provider,
            &normalize(&params.model),
            &admission.account_id,
            &admission.credential_id,
        )?;
    }
    tx.insert_admission(&AccountAdmissionRow {
        principal: principal.to_string(),
        admission_id: params.admission_id.clone(),
        request_fingerprint: request_fingerprint.to_string(),
        request_mode: params.mode.as_str().to_string(),
        decision_id: decision_id.to_string(),
        conversation_hash: hash.to_string(),
        provider: provider.to_string(),
        model: normalize(&params.model),
        account_id: admission.account_id.clone(),
        credential_id: admission.credential_id.clone(),
        pin_id,
        pin_generation,
        state: "reserved".into(),
        charged,
        request_bytes: if charged { admission.request_bytes } else { 0 },
        request_bytes_estimated: admission.request_bytes_estimated,
        committed_at: now,
        admission_valid_until,
        lease_valid_until,
        reply_json: reply.to_string(),
        run_id: None,
        started_at: None,
        started_late: false,
        released_at: None,
        finished_at: None,
        last_observed_at: None,
    })?;
    tx.record_admission_probe_scope(
        principal,
        &params.admission_id,
        striatum_core::accounts::input_size_class(
            params
                .request_input_tokens
                .unwrap_or_else(|| params.initial_input_bytes.div_ceil(4)),
        ),
        params.model_decision_id.as_deref(),
    )?;
    Ok(OpReply::Result(reply))
}

// ---------------------------------------------------------------------------
// Preflight request
// ---------------------------------------------------------------------------

/// A synthetic new cold session for `model`, decided by the worker-launch
/// preflight (`preflight_model`) and never recorded.
pub(super) fn synthetic_request(provider: &str, model: &str, key: String) -> AccountRequest {
    AccountRequest {
        principal: PREFLIGHT_PRINCIPAL.to_string(),
        provider: provider.to_string(),
        model: model.to_string(),
        affinity_model: model.to_string(),
        conversation_key: key,
        conversation_key_source: ConversationKeySource::Caller,
        // Broca is the first caller account selection will serve, and its
        // frozen runs need the stricter spendable floor.
        caller: CallerKind::Broca,
        via_safety_recovery: false,
        initial_input_bytes: 0,
        initial_input_estimated: true,
        candidates: None,
        requested_account: None,
        preferred_existing_account: None,
        hold_wait_max_s: DEFAULT_HOLD_WAIT_S,
        pool: request_pool(None, provider, model),
    }
}
