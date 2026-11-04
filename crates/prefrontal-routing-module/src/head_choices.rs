use super::*;
use cortexkit_role_routing::spellings::{AUTH_METHODS, WIRE_APIS};
use prefrontal_routing_store::PublishedEfforts;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    runner_module: String,
}

#[derive(Serialize)]
struct ModelChoice {
    provider: String,
    model: String,
    pin_only: bool,
    billing: Billing,
    variants: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_variant: Option<String>,
    service_tiers: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Billing {
    Subscription,
    PayPerToken,
}

fn credential_reaches_runner(
    credentials: &BrocaCredentials,
    runner: &routing_v1::runners::EffectiveRunner,
    fact: &ModelFact,
    row: &cortexkit_claustrum_client::ListedCredential,
) -> bool {
    // Registry::runs establishes model reach through any account. Billing
    // must use the accounts whose wire/auth pair this runner can actually use.
    runner.pairs.as_ref().is_none_or(|pairs| {
        runner
            .standing_limits
            .as_ref()
            .and_then(|limits| limits.explicit_providers.as_ref())
            .is_none_or(|providers| providers.contains(&fact.provider_id))
            && row.auth_method.as_deref().is_some_and(|method| {
                fact.billing_plane
                    .as_ref()
                    .is_none_or(|plane| plane.auth_method == method)
                    && credentials.wire_api(fact, method).is_some_and(|wire| {
                        pairs.iter().any(|pair| {
                            WIRE_APIS.contains(&pair.wire_api.as_str())
                                && AUTH_METHODS.contains(&pair.auth_method.as_str())
                                && pair.wire_api == wire
                                && pair.auth_method == method
                        })
                    })
            })
    })
}

static OPERATOR_BINDS: OnceLock<Mutex<HashMap<u16, (u32, bool)>>> = OnceLock::new();

fn operator_binds() -> &'static Mutex<HashMap<u16, (u32, bool)>> {
    OPERATOR_BINDS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn remember_bind(
    channel: u16,
    epoch: u32,
    principal: Option<&subc_protocol::Principal>,
    scoped: bool,
    harness: &str,
    session: &str,
) {
    // Mirror core's operator_route: Callosum attests federated operators;
    // local direct callers must not be scoped agents or the shared plugin.
    let operator = match principal {
        Some(subc_protocol::Principal::Reserved { module_id }) => module_id == "callosum",
        Some(subc_protocol::Principal::Direct) => {
            !scoped
                && !harness.starts_with("fed-")
                && !(harness == "alfonso" && session == "prefrontal-core")
        }
        _ => false,
    };
    if let Ok(mut binds) = operator_binds().lock() {
        binds.insert(channel, (epoch, operator));
    }
}

pub(super) fn forget_bind(channel: u16, epoch: u32) {
    if let Ok(mut binds) = operator_binds().lock() {
        if binds
            .get(&channel)
            .is_some_and(|(bound, _)| *bound == epoch)
        {
            binds.remove(&channel);
        }
    }
}

fn admitted(channel: u16, epoch: u32) -> bool {
    route_principal(channel, epoch) == "reserved:occipital"
        || operator_binds().lock().is_ok_and(|binds| {
            binds
                .get(&channel)
                .is_some_and(|(bound, operator)| *bound == epoch && *operator)
        })
}

fn not_ready() -> ErrorBody {
    ErrorBody {
        code: "routing_not_ready".into(),
        message:
            "Routing is still loading the model catalog and accounts, so it can't list a head's choices yet. Try again in a second."
                .into(),
        detail: Some(json!({"retryable": true, "retry_after_ms": 1000})),
    }
}

fn store_error(_: impl fmt::Display) -> ErrorBody {
    ErrorBody::new(
        "store_error",
        "Routing couldn't read its stored model facts, so it can't list a head's choices right now. Try again.",
    )
}

fn choices(
    store: &SqliteRouteStateStore,
    config: &config_overrides::LiveConfig,
    credentials: &BrocaCredentials,
    module: &str,
) -> Result<serde_json::Value, ErrorBody> {
    if credentials.listing_version().is_none() {
        return Err(not_ready());
    }
    let snapshot = credentials
        .catalog_snapshot
        .as_ref()
        .ok_or_else(not_ready)?;
    let (catalog_version, _) = snapshot.provider_index();
    let mut facts = store.get_facts().map_err(store_error)?;
    let aliases = credentials.catalog_targets(&facts);
    prefrontal_routing_core::score_alias::append_alias_rows(&mut facts, &aliases);
    config.apply(&mut facts);
    let registry =
        routing_v1::runners::Registry::build(config.v1_runners(), credentials, &facts, now_ms());
    let runner = registry.get(module).ok_or_else(|| {
        ErrorBody::new(
            "unknown_runner",
            "runner_module doesn't name a runner routing knows, so it has no choices to list for it.",
        )
        .with_detail(json!({"runner_module": module}))
    })?;
    let reasoning = store.reasoning_catalog().map_err(store_error)?;
    let providers = facts.iter().map(|fact| fact.provider_id.clone()).collect();
    let subscription_specs = prefrontal_routing_core::provider_model_spec::parse_spec_list(
        &config.operator_policy().subscription,
    );
    let mut grouped: BTreeMap<(&str, &str), Vec<&ModelFact>> = BTreeMap::new();
    for fact in &facts {
        grouped
            .entry((&fact.provider_id, &fact.model_id))
            .or_default()
            .push(fact);
    }
    let mut models = Vec::new();
    for ((provider, model), mut rows) in grouped {
        // Match pin resolution's preference for the unqualified fact row.
        rows.sort_by(|a, b| a.variant.cmp(&b.variant));
        let fact = rows[0];
        if fact.is_deprecated_upstream()
            || fact.score_provenance == prefrontal_routing_core::ScoreProvenance::Unscored
            || fact.supports_tool_use != Some(true)
            || !credentials
                .listed_credentials()
                .iter()
                .any(|row| broca_credentials::credential_serves_fact(row, fact, &providers))
        {
            continue;
        }
        let capacity = format!("{provider}/{model}");
        let allowed = |variant: Option<&str>, pinned: bool| {
            let route = prefrontal_routing_core::make_route_key(provider, model, variant);
            config.route_allowed("head", &route, pinned) && registry.runs(module, &route, &capacity)
        };
        let published = variant_policy::published(&reasoning, &aliases, provider, model);
        let mut variants = match published {
            Some(PublishedEfforts::Published(values)) => values.clone(),
            _ => rows.iter().filter_map(|row| row.variant.clone()).collect(),
        };
        let curated = fact
            .prompt_defaults
            .as_ref()
            .and_then(|defaults| defaults.variant.as_deref())
            .or(fact.variant.as_deref());
        let default_variant = variant_policy::resolve(
            config.variant_groups(),
            &aliases,
            Some(&reasoning),
            &variant_policy::VariantRequest {
                route_key: &prefrontal_routing_core::fact_to_route(fact).route_key,
                provider_id: provider,
                model_id: model,
                explicit: None,
                curated,
                consumer: Some("head"),
            },
        )
        .variant
        .filter(|variant| allowed(Some(variant), true));
        if !matches!(published, Some(PublishedEfforts::Published(_))) {
            variants.extend(default_variant.iter().cloned());
        }
        variants.retain(|variant| allowed(Some(variant), true));
        let mut seen = std::collections::BTreeSet::new();
        variants.retain(|variant| seen.insert(variant.clone()));
        let default_variant = default_variant.filter(|variant| variants.contains(variant));
        if !allowed(None, true) && variants.is_empty() {
            continue;
        }
        // A model entry can expose several routes; one automatic route is
        // enough for the entry not to be pin-only on this runner.
        let pin_only =
            !allowed(None, false) && !variants.iter().any(|variant| allowed(Some(variant), false));
        let reachable_credentials: Vec<_> = credentials
            .listed_credentials()
            .iter()
            .filter(|row| credential_reaches_runner(credentials, runner, fact, row))
            .cloned()
            .collect();
        // A picker pin can use any covered account, regardless of temporary
        // quota or refusal fences; use account selection's coverage rule.
        let class = account_selection::resolve_class(
            &reachable_credentials,
            fact,
            &providers,
            &subscription_specs,
            aliases.contains_key(provider),
            false,
            false,
            |_| true,
        );
        let billing = if class.subscription {
            Billing::Subscription
        } else {
            Billing::PayPerToken
        };
        let mut service_tiers = vec!["standard".into()];
        for tier in ["fast", "ultrafast"] {
            if service_tiers::model_offers(credentials, fact, tier)
                && credentials.listed_credentials().iter().any(|row| {
                    broca_credentials::credential_serves_fact(row, fact, &providers)
                        && fact.billing_plane.as_ref().is_none_or(|plane| {
                            row.auth_method.as_deref() == Some(&plane.auth_method)
                        })
                        && service_tiers::runner_supports(credentials, runner, fact, row, tier)
                        && service_tiers::plan_rate(
                            &credentials.plan_prices,
                            row,
                            provider,
                            tier,
                            false,
                        )
                        .is_some()
                })
            {
                service_tiers.push(tier.into());
            }
        }
        models.push(ModelChoice {
            provider: provider.into(),
            model: model.into(),
            pin_only,
            billing,
            variants,
            default_variant,
            service_tiers,
        });
    }
    Ok(json!({"catalog_version": catalog_version, "runner_module": module, "models": models}))
}

pub(super) async fn serve(
    frame: Frame,
    writer: &mpsc::Sender<Frame>,
    store: &SharedStore,
    operator_config_path: &Path,
    credentials: &SharedBrocaCredentials,
) -> Result<(), ModuleError> {
    let (ver, channel, epoch, corr) = (
        frame.header.ver,
        frame.header.channel,
        frame.header.epoch,
        frame.header.corr,
    );
    let result = (|| {
        if !admitted(channel, epoch) {
            return Err(ErrorBody::new(
                "not_permitted",
                "Only the Alfonso apps' operator routes and Occipital can list a head's model choices.",
            ));
        }
        let params = serde_json::from_slice::<MethodParamsBody<Params>>(&frame.body)
            .ok()
            .filter(|body| !body.params.runner_module.is_empty()
                && !body.params.runner_module.chars().any(char::is_whitespace))
            .ok_or_else(|| ErrorBody::new("invalid_request", "Send exactly one field, runner_module, naming the head's runner, for example thalamus-harness."))?;
        let guard = credentials.lock().map_err(|_| not_ready())?;
        choices(
            store,
            &config_overrides::live_config(operator_config_path),
            &guard,
            &params.params.runner_module,
        )
    })();
    let result = match result {
        Ok(result) => result,
        Err(error) => return send_route_error_body(writer, ver, channel, epoch, corr, error).await,
    };
    let response = Frame::build_with_version(
        ver,
        FrameType::Response,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        corr,
        serde_json::to_vec(&json!({"result": result})).map_err(ModuleError::Json)?,
    )
    .map_err(|error| ModuleError::Message(error.to_string()))?;
    send(writer, response).await
}
