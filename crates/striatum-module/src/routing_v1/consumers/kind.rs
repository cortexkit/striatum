//! Kind resolution shared by session and media selection.

use cortexkit_role_routing::errors::Refusal;

/// Catalog type spellings and routing kind spellings identify the same kind.
pub(crate) fn resolve<'a>(
    declared: Option<&'a str>,
    request: Option<&'a str>,
) -> Result<&'a str, Refusal> {
    let canonical = |kind| match kind {
        "transcription" => "transcribe",
        "speech" => "synthesize",
        kind => kind,
    };
    let declared = declared.map(canonical);
    let request = request.map(canonical);
    if let (Some(declared), Some(request)) = (declared, request) {
        if declared != request {
            return Err(Refusal::invalid_request(
                "kind",
                format!("consumer kind {declared} cannot be selected as {request}"),
            ));
        }
    }
    Ok(request.or(declared).unwrap_or("llm"))
}

#[cfg(test)]
mod tests {
    use super::resolve;

    #[test]
    fn omitted_kind_uses_declaration_or_llm_and_retains_unknown_kinds() {
        assert_eq!(resolve(None, None).unwrap(), "llm");
        assert_eq!(resolve(Some("search"), None).unwrap(), "search");
        assert_eq!(
            resolve(Some("future-engine"), None).unwrap(),
            "future-engine"
        );
        assert_eq!(resolve(None, Some("image")).unwrap(), "image");
        assert_eq!(resolve(Some("image"), Some("image")).unwrap(), "image");
    }

    #[test]
    fn a_request_cannot_cross_the_declared_kind() {
        let refusal = resolve(Some("transcribe"), Some("synthesize")).unwrap_err();
        assert_eq!(
            serde_json::to_value(&refusal).unwrap()["code"],
            "invalid_request"
        );
        assert_eq!(refusal.detail["field"], "kind");
    }

    #[test]
    fn catalog_model_types_map_to_routing_kinds() {
        for (model_type, kind) in [("transcription", "transcribe"), ("speech", "synthesize")] {
            assert_eq!(resolve(Some(model_type), None).unwrap(), kind);
            assert_eq!(resolve(Some(kind), Some(model_type)).unwrap(), kind);
            assert_eq!(resolve(Some(model_type), Some(kind)).unwrap(), kind);
        }
    }
}
