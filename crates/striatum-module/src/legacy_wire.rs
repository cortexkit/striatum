//! Runner names fixed by the legacy route.select_panel and account wire shapes.
//!
//! Routing never names a runner in code: runners declare themselves, and the
//! routing/v1 shapes carry the runner a caller registered. The legacy shapes
//! predate that. Today's core sends and reads these fixed names on them, and
//! decision rows and the stored winner baseline already hold them, so they
//! live here, in one place, rather than spread through selection code.
//!
//! The literal census (`literal_census_tests.rs`) exempts this compatibility
//! vocabulary by module name.

use striatum_core::AvailabilitySubstrate;

/// The runner the legacy shapes call Broca. It is the `substrate` request
/// value that asks for selection over Broca's reach, the `requiredSubstrate`
/// and `harness` values on answers, the selection label recorded on decisions
/// and in the winner baseline, and the legacy account caller's `runner`.
pub(crate) const BROCA: &str = "broca";

/// The prefix of the conversation key Broca falls back to, `broca:<run_id>`,
/// when a legacy account request's direct caller sent none.
pub(crate) const BROCA_RUN_KEY_PREFIX: &str = "broca:";

/// The text a panel decision records for the availability list it consulted,
/// and that every availability rejection on that panel carries.
pub(crate) fn availability_label(substrate: AvailabilitySubstrate) -> &'static str {
    match substrate {
        AvailabilitySubstrate::Host => "host",
        AvailabilitySubstrate::Broca => BROCA,
        AvailabilitySubstrate::BrocaServedCatalog => {
            "broca reachability unknown: judged against served catalog"
        }
    }
}
