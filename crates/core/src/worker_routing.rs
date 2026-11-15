//! Worker capability normalization shared by runtime profiles.

use std::collections::{HashMap, HashSet};

/// Normalize worker route advertisements from either the capability envelope
/// used by current SDKs or the legacy component list.
pub fn route_components_from_capabilities(
    capabilities: &[engine_proto::WorkerCapability],
    component_infos: &[engine_proto::ComponentInfo],
) -> Vec<engine_proto::ComponentRouteSpec> {
    let mut seen = HashSet::new();
    let component_info_by_route: HashMap<(i32, &str), &engine_proto::ComponentInfo> =
        component_infos
            .iter()
            .map(|component| {
                (
                    (component.component_type, component.name.as_str()),
                    component,
                )
            })
            .collect();
    let mut route_components: Vec<engine_proto::ComponentRouteSpec> = capabilities
        .iter()
        .filter(|capability| {
            !capability.component_name.is_empty()
                && capability.component_type != engine_proto::ComponentType::Unspecified as i32
                && seen.insert((capability.component_type, capability.component_name.clone()))
        })
        .map(|capability| {
            let component_info = component_info_by_route.get(&(
                capability.component_type,
                capability.component_name.as_str(),
            ));
            engine_proto::ComponentRouteSpec {
                name: capability.component_name.clone(),
                component_type: capability.component_type,
                config: component_info
                    .map(|component| component_route_config(component))
                    .unwrap_or_default(),
                metadata: component_info
                    .map(|component| component.metadata.clone())
                    .unwrap_or_default(),
            }
        })
        .collect();

    if route_components.is_empty() {
        route_components = component_infos
            .iter()
            .filter(|component| {
                !component.name.is_empty()
                    && component.component_type != engine_proto::ComponentType::Unspecified as i32
                    && seen.insert((component.component_type, component.name.clone()))
            })
            .map(|component| engine_proto::ComponentRouteSpec {
                name: component.name.clone(),
                component_type: component.component_type,
                config: component_route_config(component),
                metadata: component.metadata.clone(),
            })
            .collect();
    }

    route_components
}

fn component_route_config(component: &engine_proto::ComponentInfo) -> HashMap<String, String> {
    let mut config = component.config.clone();
    if let Some(max_attempts) = component.max_attempts {
        config.insert("max_attempts".into(), max_attempts.max(0).to_string());
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_are_preferred_and_deduplicated() {
        let routes = route_components_from_capabilities(
            &[
                engine_proto::WorkerCapability {
                    component_name: "json_valid".to_string(),
                    component_type: engine_proto::ComponentType::Scorer as i32,
                },
                engine_proto::WorkerCapability {
                    component_name: "json_valid".to_string(),
                    component_type: engine_proto::ComponentType::Scorer as i32,
                },
            ],
            &[engine_proto::ComponentInfo {
                name: "declared_only".to_string(),
                component_type: engine_proto::ComponentType::Function as i32,
                ..Default::default()
            }],
        );

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].name, "json_valid");
        assert_eq!(
            routes[0].component_type,
            engine_proto::ComponentType::Scorer as i32
        );
    }

    #[test]
    fn typed_retry_limit_overrides_config_on_capability_and_legacy_routes() {
        let component = engine_proto::ComponentInfo {
            name: "oom_once".into(),
            component_type: engine_proto::ComponentType::Function as i32,
            max_attempts: Some(1),
            config: HashMap::from([("max_attempts".into(), "3".into())]),
            ..Default::default()
        };
        for capabilities in [
            vec![],
            vec![engine_proto::WorkerCapability {
                component_name: "oom_once".into(),
                component_type: engine_proto::ComponentType::Function as i32,
            }],
        ] {
            let routes =
                route_components_from_capabilities(&capabilities, std::slice::from_ref(&component));
            assert_eq!(
                routes[0].config.get("max_attempts").map(String::as_str),
                Some("1")
            );
        }
    }
}
