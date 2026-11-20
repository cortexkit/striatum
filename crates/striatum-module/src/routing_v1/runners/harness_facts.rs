//! Validated harness facts travel with the declaration's immutable snapshot.

use std::collections::BTreeMap;
use std::ops::Deref;

use cortexkit_role_routing::declarations::{
    HarnessLimits, HarnessLimitsError, RunnerDeclaration as ContractDeclaration,
};
use cortexkit_role_routing::placement::HarnessCapability;
use serde::{Deserialize, Deserializer, Serialize};

/// Keep the serving validation result, including a refused map, at the read
/// boundary. Request-time registry construction must not decode the raw JSON.
/// Equality includes the result so credential revisions also distinguish facts
/// read with different serving capabilities.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub(crate) struct RunnerDeclaration {
    declaration: ContractDeclaration,
    #[serde(skip)]
    facts: Option<Result<BTreeMap<String, HarnessLimits>, HarnessLimitsError>>,
}

impl<'de> Deserialize<'de> for RunnerDeclaration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        ContractDeclaration::deserialize(deserializer).map(Self::read)
    }
}

impl Deref for RunnerDeclaration {
    type Target = ContractDeclaration;

    fn deref(&self) -> &Self::Target {
        &self.declaration
    }
}

// Test fixtures change runner-wide offers directly. Production replaces the
// whole declaration, so its cached harness facts cannot outlive their input.
#[cfg(test)]
impl std::ops::DerefMut for RunnerDeclaration {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.declaration
    }
}

impl RunnerDeclaration {
    pub(super) fn read(declaration: ContractDeclaration) -> Self {
        #[cfg(test)]
        PARSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let facts =
            declaration.typed_harness_limits_serving(&super::super::describe::capabilities());
        Self { declaration, facts }
    }

    pub(super) fn as_contract(&self) -> &ContractDeclaration {
        &self.declaration
    }

    pub(super) fn harness_limits_error(&self) -> Option<&HarnessLimitsError> {
        self.facts.as_ref().and_then(|facts| facts.as_ref().err())
    }

    pub(crate) fn declares_harness_facts(&self) -> bool {
        self.facts
            .as_ref()
            .and_then(|facts| facts.as_ref().ok())
            .is_some_and(|facts| {
                facts.values().any(|limits| {
                    limits.starts_sessions.is_some()
                        || limits.capabilities.is_some()
                        || limits.exclude.is_some()
                })
            })
    }

    pub(crate) fn limits_for_harness(
        &self,
        harness: Option<&str>,
    ) -> Result<HarnessLimits, HarnessLimitsError> {
        let facts = self
            .facts
            .as_ref()
            .map(|facts| facts.as_ref())
            .transpose()
            .map_err(Clone::clone)?;
        let entry = harness.and_then(|harness| facts.and_then(|facts| facts.get(harness)));
        let mut limits = entry.cloned().unwrap_or_default();
        limits.starts_sessions =
            Some(self.starts_sessions && limits.starts_sessions.unwrap_or(true));
        limits.pairs = Some(limits.pairs.unwrap_or_else(|| self.pairs.clone()));
        limits.explicit_providers = limits.explicit_providers.or_else(|| {
            self.standing_limits
                .as_ref()
                .and_then(|limits| limits.explicit_providers.clone())
        });
        Ok(limits)
    }

    /// Legacy runners retain today's task support until that runner declares
    /// facts. Unknown capability tokens remain in the parsed harness facts,
    /// but do not grant a known capability.
    pub(crate) fn harness_supports(&self, harness: &str, capability: &HarnessCapability) -> bool {
        if !self.declares_harness_facts() {
            return true;
        }
        capability.is_known()
            && self
                .limits_for_harness(Some(harness))
                .ok()
                .and_then(|limits| limits.capabilities)
                .is_some_and(|capabilities| capabilities.contains(capability))
    }

    pub(crate) fn harness_excludes(&self, harness: &str, route: &str) -> bool {
        self.limits_for_harness(Some(harness))
            .ok()
            .and_then(|limits| limits.exclude)
            .is_some_and(|specs| {
                cortexkit_role_routing::route_spec::resolve(
                    specs.iter().map(String::as_str),
                    route,
                    true,
                )
                .unwrap_or(false)
            })
    }
}

#[cfg(test)]
pub(super) static PARSES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
