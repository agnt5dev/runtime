//! Canonical label values for metrics and traces.
//!
//! Both the coordinator and processor (and the gateway, indirectly through
//! request metadata) need to attach a `component_type` label to metrics.
//! Without a shared normalizer the label values diverge:
//!
//!   - Coordinator side: `req.component_type().as_str_name()` returns
//!     `"COMPONENT_TYPE_FUNCTION"` (uppercase, prefixed).
//!   - Processor side: `record.metadata.get("component_type")` returns
//!     whatever the gateway put there — historically `"functions"`
//!     (lowercase plural URL segment).
//!
//! Dashboards trying to join across metric sources fail when the label
//! values don't match. This module is the single source of truth for the
//! short-form `component_type` label: lowercase, singular, matching the
//! proto enum names without the `COMPONENT_TYPE_` prefix.

use crate::api::v1::ComponentType;

/// Canonical short-form component_type label. Returns a `&'static str` so
/// callers can put it directly in an OTel `KeyValue` without extra
/// allocation. Values: `function`, `flow`, `object`, `task`, `workflow`,
/// `agent`, `tool`, `mcp`, `entity`, `scorer`, or `unknown`.
pub fn component_type_label(ct: ComponentType) -> &'static str {
    match ct {
        ComponentType::Function => "function",
        ComponentType::Flow => "flow",
        ComponentType::Object => "object",
        ComponentType::Task => "task",
        ComponentType::Workflow => "workflow",
        ComponentType::Agent => "agent",
        ComponentType::Tool => "tool",
        ComponentType::Mcp => "mcp",
        ComponentType::Entity => "entity",
        ComponentType::Scorer => "scorer",
        ComponentType::Unspecified => "unknown",
    }
}

/// Normalize a free-form string (URL segment, request metadata, …) into the
/// same short-form label. Handles the URL-plural shapes the gateway uses
/// (`functions`, `agents`, etc.) plus already-canonical inputs. Anything
/// else returns `"unknown"`.
pub fn component_type_label_from_str(s: &str) -> &'static str {
    match s {
        "function" | "functions" | "COMPONENT_TYPE_FUNCTION" => "function",
        "flow" | "flows" | "COMPONENT_TYPE_FLOW" => "flow",
        "object" | "objects" | "COMPONENT_TYPE_OBJECT" => "object",
        "task" | "tasks" | "COMPONENT_TYPE_TASK" => "task",
        "workflow" | "workflows" | "COMPONENT_TYPE_WORKFLOW" => "workflow",
        "agent" | "agents" | "COMPONENT_TYPE_AGENT" => "agent",
        "tool" | "tools" | "COMPONENT_TYPE_TOOL" => "tool",
        "mcp" | "COMPONENT_TYPE_MCP" => "mcp",
        "entity" | "entities" | "COMPONENT_TYPE_ENTITY" => "entity",
        "scorer" | "scorers" | "COMPONENT_TYPE_SCORER" => "scorer",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proto_enum_round_trip() {
        for (ct, expected) in [
            (ComponentType::Function, "function"),
            (ComponentType::Workflow, "workflow"),
            (ComponentType::Agent, "agent"),
            (ComponentType::Tool, "tool"),
            (ComponentType::Scorer, "scorer"),
            (ComponentType::Entity, "entity"),
            (ComponentType::Unspecified, "unknown"),
        ] {
            assert_eq!(component_type_label(ct), expected);
        }
    }

    #[test]
    fn from_str_handles_url_plurals() {
        assert_eq!(component_type_label_from_str("functions"), "function");
        assert_eq!(component_type_label_from_str("entities"), "entity");
        assert_eq!(component_type_label_from_str("workflows"), "workflow");
    }

    #[test]
    fn from_str_handles_proto_uppercase() {
        assert_eq!(
            component_type_label_from_str("COMPONENT_TYPE_FUNCTION"),
            "function"
        );
        assert_eq!(
            component_type_label_from_str("COMPONENT_TYPE_AGENT"),
            "agent"
        );
    }

    #[test]
    fn from_str_passes_through_canonical_form() {
        assert_eq!(component_type_label_from_str("function"), "function");
        assert_eq!(component_type_label_from_str("agent"), "agent");
    }

    #[test]
    fn from_str_unknown_fallback() {
        assert_eq!(component_type_label_from_str(""), "unknown");
        assert_eq!(component_type_label_from_str("bogus"), "unknown");
    }
}
