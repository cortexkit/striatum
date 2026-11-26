use super::*;

// Keep required scenarios separate from the commands selected by the test:
// removing a command case must not also remove the scenario it should prove.
// A case records its name only after asserting the resulting state or an
// actual request reply, not merely after invoking a writer.
fn expected_witnesses() -> BTreeSet<String> {
    let mut expected: BTreeSet<String> = [
        "admission_replay",
        "pool_rate_reopen",
        "probe_expiry",
        "fresh_headroom",
        "tombstone",
        "provider_wall",
        "route_wall",
        "pool_lease_claim",
        "operator_clear",
        "retention_delete",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    for reload in [false, true] {
        for scope in ["size", "principal"] {
            expected.insert(format!("scope/{scope}/reload={reload}"));
        }
        for case in ["reading", "listing"] {
            expected.insert(format!("credential/{case}/reload={reload}"));
        }
        for charge in ["round_robin", "reading_floor"] {
            expected.insert(format!("charge/{charge}/reload={reload}"));
        }
        for family in ["account", "size"] {
            for reverse in [false, true] {
                expected.insert(format!("shared/{family}/reverse={reverse}/reload={reload}"));
            }
        }
        for case in [
            "expired_renew",
            "released_renew",
            "closed_observation",
            "older_route",
            "older_provider",
            "late_completion",
        ] {
            expected.insert(format!("outside/{case}/reload={reload}"));
        }
        for outcome in [
            "completed",
            "cancelled",
            "contract_error",
            "session_error",
            "interrupted",
            "launch_failed",
            "failed",
            "refused",
            "timed_out",
            "vanished",
        ] {
            expected.insert(format!("report/{outcome}/reload={reload}"));
        }
        expected.insert(format!("config/reload={reload}"));
    }
    expected
}

impl World {
    pub(super) fn witness(&mut self, name: impl Into<String>) {
        self.witnesses.insert(name.into());
    }

    pub(super) fn inherit_witnesses(&mut self, child: &World) {
        self.witnesses.extend(child.witnesses.iter().cloned());
    }

    pub(super) fn assert_semantic_coverage(&self) {
        let expected = expected_witnesses();
        let missing: Vec<_> = expected.difference(&self.witnesses).collect();
        let undeclared: Vec<_> = self.witnesses.difference(&expected).collect();
        assert!(missing.is_empty() && undeclared.is_empty(),
            "semantic coverage is not producer-entry coverage: seed/program {}, missing {missing:?}, undeclared {undeclared:?}", self.sequence);
    }
}
