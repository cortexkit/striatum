//! Explicit media identities bypass ranking, never admission or reachability.

use std::collections::BTreeSet;

use super::{wire, EngineCandidate, EvalScore, FactNumber};

pub(super) fn entry<'a>(
    request: &wire::SelectEngineRequest,
    candidates: &'a [EngineCandidate<EvalScore, FactNumber>],
    declared_routes: &BTreeSet<String>,
    entry: &str,
) -> Option<&'a EngineCandidate<EvalScore, FactNumber>> {
    // Engines have no variants or slots. Request pins are also checked by the
    // contract parser, but settings pins and ladder entries need the same shape.
    if !cortexkit_role_routing::select::is_plain_route_key(entry) {
        return None;
    }
    candidates.iter().find(|candidate| {
        format!("{}/{}", candidate.engine.provider, candidate.engine.model) == entry
            && candidate.selected.caller_local != declared_routes.contains(entry)
            && wire::select_engine(request, std::slice::from_ref(*candidate)).is_ok()
    })
}
