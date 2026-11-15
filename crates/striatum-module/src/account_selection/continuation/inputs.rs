use super::*;
use striatum_core::accounts::AllowedAccountClass;

/// Versioned allocator inputs frozen with an admission, independent of its wire
/// surface. `version` selects the stored schema; `provider`, `model` and
/// `affinity_model` keep the route and affinity family; `auth_method`,
/// `allowed_account_class` and `subscription` keep authentication and payment
/// boundaries. `service_tier` is the chosen level, not a fresh step-down request.
/// `requested_account` preserves an explicit pin and `candidates` the runner's
/// offered set. `model_decision_id` retains selection provenance. `harness` names
/// the runner from that decision, or the daemon-authenticated route principal for
/// a select-less run, never the request body's caller. `owner_scope` records who
/// owns the run and its project, using the already-loaded selection. The selected
/// harness and workdir remain on the model decision and are preserved by the
/// continuation's decision link, rather than duplicated here. Daemon session and
/// run provenance are not persisted on selections today, so continuation does not
/// check them; its run binding comes from the admission's own `run_id`.
/// `parent_conversation_key` retains placement preference.
/// `initial_input_bytes`, `initial_input_estimated` and `request_input_tokens`
/// retain the demand/size estimate. No token, secret or raw conversation key is
/// stored: routing hashes the supplied key with its private salt and the attested
/// principal, then compares it with the conversation hash stored on the admission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Inputs {
    version: u32,
    provider: String,
    model: String,
    affinity_model: Option<String>,
    pub(crate) auth_method: String,
    allowed_account_class: AllowedAccountClass,
    subscription: Option<bool>,
    service_tier: Option<cortexkit_role_routing::service_tier::ServiceTier>,
    requested_account: Option<AccountRefParams>,
    candidates: Option<CandidatesParams>,
    model_decision_id: Option<String>,
    harness: String,
    owner_scope: Value,
    parent_conversation_key: Option<String>,
    initial_input_bytes: u64,
    initial_input_estimated: bool,
    request_input_tokens: Option<u64>,
}

impl Inputs {
    pub(crate) fn prepare(
        params: &AccountForParams,
        principal: &str,
        classes: Option<&ClassContext>,
        class: AllowedAccountClass,
        selection: Option<&V1Decision>,
    ) -> Self {
        let subscription = classes.and_then(|classes| {
            classes
                .facts
                .iter()
                .find(|fact| {
                    fact.provider_id == normalize(&params.provider)
                        && fact.model_id == normalize(&params.model)
                })
                .map(|fact| {
                    classes
                        .class(fact, class == AllowedAccountClass::PricedPerTokenOnly)
                        .subscription
                })
        });
        Self {
            version: 1,
            provider: params.provider.clone(),
            model: params.model.clone(),
            affinity_model: params.affinity_model.clone(),
            auth_method: String::new(),
            allowed_account_class: class,
            subscription,
            service_tier: params.service_tier_chosen,
            requested_account: params.requested_account.clone(),
            candidates: params.candidates.clone(),
            model_decision_id: params.model_decision_id.clone(),
            harness: selection.map_or_else(
                || {
                    principal
                        .strip_prefix("reserved:")
                        .unwrap_or(principal)
                        .to_owned()
                },
                |s| s.runner_module.clone(),
            ),
            owner_scope: json!({
                "owner":selection.map_or(principal, |s| s.principal.as_str()),
                "project_id":selection.and_then(|s|s.project_id.as_deref()),
            }),
            parent_conversation_key: params.parent_conversation_key.clone(),
            initial_input_bytes: params.initial_input_bytes,
            initial_input_estimated: params.initial_input_estimated,
            request_input_tokens: params.request_input_tokens,
        }
    }

    pub(super) fn body(&self, request: &ContinueRequest, runner: &str) -> Option<Value> {
        if self.version != 1 || self.harness != runner || self.auth_method.is_empty() {
            return None;
        }
        Some(json!({
            "provider":self.provider,"model":self.model,"affinity_model":self.affinity_model,
            "service_tier":self.service_tier,"requested_account":self.requested_account,
            "candidates":self.candidates,"decision_id":self.model_decision_id,
            "parent_conversation_key":self.parent_conversation_key,
            "initial_input_bytes":self.initial_input_bytes,"initial_input_estimated":self.initial_input_estimated,
            "request_input_tokens":self.request_input_tokens,"conversation_key":request.conversation_key,
        }))
    }

    pub(super) fn compatible(&self, classes: &ClassContext, credential: &str) -> bool {
        if classes.method(credential) != self.auth_method {
            return false;
        }
        let Some(fact) = classes.facts.iter().find(|fact| {
            fact.provider_id == normalize(&self.provider) && fact.model_id == normalize(&self.model)
        }) else {
            return false;
        };
        let entry = classes.serving_index.fact(
            &classes.config,
            &classes.credentials,
            fact,
            &classes.catalog,
            &classes.targets,
        );
        entry.selection.iter().any(|row| {
            classes.credentials.listed_credentials()[row.index].id == credential
                && self
                    .subscription
                    .is_some_and(|subscription| row.covered == subscription)
                && (self.allowed_account_class != AllowedAccountClass::PricedPerTokenOnly
                    || row.metered)
        })
    }
}

pub(super) fn original(
    request: &ContinueRequest,
    principal: &str,
    runner: &str,
    ctx: &Ctx<'_>,
) -> Result<(Option<Value>, Option<Inputs>), Refusal> {
    let admitted = crate::account_state::open_preflight_admission(
        ctx.store,
        principal,
        &request.admission_id,
        ctx.now_ms,
    )
    .map_err(unavailable)?;
    let decision = admitted
        .as_ref()
        .map(|row| ctx.store.account_decision(&row.decision_id))
        .transpose()
        .map_err(unavailable)?
        .flatten();
    let context = decision
        .and_then(|decision| decision.context_json)
        .map(|context| serde_json::from_str::<Value>(&context))
        .transpose();
    let Ok(context) = context else {
        return Ok((Some(Value::Null), None));
    };
    let snapshot = context.and_then(|context| context.get("continuation_input").cloned());
    if let Some(snapshot) = snapshot {
        let input = serde_json::from_value::<Inputs>(snapshot).ok();
        let body = input.as_ref().and_then(|input| input.body(request, runner));
        return Ok((
            if input.is_none() {
                Some(Value::Null)
            } else {
                body
            },
            input,
        ));
    }
    let body = ctx
        .store
        .v1_admission(principal, &request.admission_id)
        .map_err(unavailable)?
        .map(|record| serde_json::from_str::<Value>(&record.body_json).unwrap_or(Value::Null));
    Ok((body, None))
}
