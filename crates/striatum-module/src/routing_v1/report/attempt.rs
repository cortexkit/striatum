use cortexkit_role_routing::capabilities::REPORT_ATTEMPT_OUTCOMES;
use cortexkit_role_routing::errors::Refusal;
use cortexkit_role_routing::report::{parse_report_request_serving, ReportRequest};
use serde_json::Value;
use std::borrow::Cow;

pub(super) fn parse(params: &Value) -> Result<ReportRequest, Refusal> {
    let capabilities = super::super::describe::capabilities();
    // Keep the closed member list aligned with the serving parser, including
    // refusing a null attempt_outcome while its group is not advertised.
    let members: Cow<'_, [&str]> = if capabilities.contains(&REPORT_ATTEMPT_OUTCOMES) {
        Cow::Owned(
            super::MEMBERS
                .iter()
                .copied()
                .chain(["attempt_outcome"])
                .collect(),
        )
    } else {
        Cow::Borrowed(super::MEMBERS)
    };
    super::super::refuse_unknown_members(params, &members)?;
    super::super::shapes::closed(params, "eligibility", &["scope", "code"])?;
    parse_report_request_serving(params, &capabilities)
}
