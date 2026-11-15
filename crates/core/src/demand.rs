//! Canonical routing identity for pull-dispatch demand and supply.
//!
//! A demand key names one deployment component bucket. Worker route details
//! such as retry configuration and metadata deliberately do not participate:
//! they are rechecked when a concrete offer is granted.

use std::collections::BTreeSet;

use engine_proto::{ComponentInfo, ComponentRouteSpec, ComponentType, Run, WorkerCapability};
use thiserror::Error;

use crate::worker_routing::route_components_from_capabilities;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DemandKey {
    project_id: String,
    deployment_id: String,
    component_type: i32,
    component_name: String,
}

impl DemandKey {
    pub fn new(
        project_id: impl Into<String>,
        deployment_id: impl Into<String>,
        component_type: ComponentType,
        component_name: impl Into<String>,
    ) -> Result<Self, DemandKeyError> {
        let project_id = project_id.into();
        let deployment_id = deployment_id.into();
        let component_name = component_name.into();
        for (value, field) in [
            (project_id.as_str(), "project_id"),
            (deployment_id.as_str(), "deployment_id"),
            (component_name.as_str(), "component_name"),
        ] {
            if value.is_empty() {
                return Err(DemandKeyError::EmptyField(field));
            }
            if value.as_bytes().contains(&0) {
                return Err(DemandKeyError::ReservedSeparator(field));
            }
        }
        if component_type == ComponentType::Unspecified {
            return Err(DemandKeyError::UnspecifiedComponentType);
        }
        Ok(Self {
            project_id,
            deployment_id,
            component_type: component_type as i32,
            component_name,
        })
    }

    pub fn from_run(run: &Run) -> Result<Self, DemandKeyError> {
        Self::new(
            run.project_id.clone(),
            run.deployment_id.clone(),
            component_type_from_run_label(&run.component_type)?,
            run.component_name.clone(),
        )
    }

    pub fn from_worker_route(
        project_id: &str,
        deployment_id: &str,
        route: &ComponentRouteSpec,
    ) -> Result<Self, DemandKeyError> {
        let component_type = ComponentType::try_from(route.component_type)
            .map_err(|_| DemandKeyError::UnknownComponentTypeValue(route.component_type))?;
        Self::new(
            project_id,
            deployment_id,
            component_type,
            route.name.clone(),
        )
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn deployment_id(&self) -> &str {
        &self.deployment_id
    }

    pub fn component_type(&self) -> ComponentType {
        ComponentType::try_from(self.component_type)
            .expect("DemandKey construction validates component_type")
    }

    pub const fn component_type_value(&self) -> i32 {
        self.component_type
    }

    pub fn component_name(&self) -> &str {
        &self.component_name
    }
}

/// Convert a worker's advertised capability envelope into stable deployment
/// component buckets. Config and metadata differences collapse to one key.
pub fn demand_keys_from_worker_capabilities(
    project_id: &str,
    deployment_id: &str,
    capabilities: &[WorkerCapability],
    component_infos: &[ComponentInfo],
) -> Result<Vec<DemandKey>, DemandKeyError> {
    let keys = route_components_from_capabilities(capabilities, component_infos)
        .iter()
        .map(|route| DemandKey::from_worker_route(project_id, deployment_id, route))
        .collect::<Result<BTreeSet<_>, _>>()?;
    Ok(keys.into_iter().collect())
}

/// Parse the historical singular/plural Run label into the protocol enum.
pub fn component_type_from_run_label(value: &str) -> Result<ComponentType, DemandKeyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "function" | "functions" | "component_type_function" => Ok(ComponentType::Function),
        "flow" | "flows" | "component_type_flow" => Ok(ComponentType::Flow),
        "object" | "objects" | "component_type_object" => Ok(ComponentType::Object),
        "task" | "tasks" | "component_type_task" => Ok(ComponentType::Task),
        "workflow" | "workflows" | "component_type_workflow" => Ok(ComponentType::Workflow),
        "agent" | "agents" | "component_type_agent" => Ok(ComponentType::Agent),
        "tool" | "tools" | "component_type_tool" => Ok(ComponentType::Tool),
        "mcp" | "component_type_mcp" => Ok(ComponentType::Mcp),
        "entity" | "entities" | "component_type_entity" => Ok(ComponentType::Entity),
        "scorer" | "scorers" | "component_type_scorer" => Ok(ComponentType::Scorer),
        _ => Err(DemandKeyError::UnknownComponentTypeLabel(value.to_string())),
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DemandKeyError {
    #[error("demand key field {0} must not be empty")]
    EmptyField(&'static str),
    #[error("demand key field {0} contains a reserved NUL separator")]
    ReservedSeparator(&'static str),
    #[error("demand key component type must not be unspecified")]
    UnspecifiedComponentType,
    #[error("unknown demand key component type value {0}")]
    UnknownComponentTypeValue(i32),
    #[error("unknown run component type label {0:?}")]
    UnknownComponentTypeLabel(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_labels_map_to_one_canonical_key() {
        for label in ["function", "functions", "COMPONENT_TYPE_FUNCTION"] {
            let run = Run {
                project_id: "project-1".into(),
                deployment_id: "deployment-1".into(),
                component_name: "summarize".into(),
                component_type: label.into(),
                ..Default::default()
            };
            assert_eq!(
                DemandKey::from_run(&run).unwrap(),
                DemandKey::new(
                    "project-1",
                    "deployment-1",
                    ComponentType::Function,
                    "summarize"
                )
                .unwrap()
            );
        }
    }

    #[test]
    fn invalid_or_incomplete_keys_are_rejected() {
        assert_eq!(
            DemandKey::new("", "deployment-1", ComponentType::Function, "summarize"),
            Err(DemandKeyError::EmptyField("project_id"))
        );
        assert_eq!(
            DemandKey::new(
                "project-1",
                "deployment-1",
                ComponentType::Function,
                "bad\0name"
            ),
            Err(DemandKeyError::ReservedSeparator("component_name"))
        );
        assert_eq!(
            DemandKey::new(
                "project-1",
                "deployment-1",
                ComponentType::Unspecified,
                "summarize"
            ),
            Err(DemandKeyError::UnspecifiedComponentType)
        );
        assert!(matches!(
            component_type_from_run_label("unknown"),
            Err(DemandKeyError::UnknownComponentTypeLabel(_))
        ));
    }

    #[test]
    fn worker_routes_collapse_config_and_duplicate_capabilities() {
        let capabilities = vec![
            WorkerCapability {
                component_name: "summarize".into(),
                component_type: ComponentType::Function as i32,
            },
            WorkerCapability {
                component_name: "summarize".into(),
                component_type: ComponentType::Function as i32,
            },
        ];
        let component_infos = vec![ComponentInfo {
            name: "summarize".into(),
            component_type: ComponentType::Function as i32,
            config: [("max_attempts".into(), "3".into())].into(),
            ..Default::default()
        }];

        let keys = demand_keys_from_worker_capabilities(
            "project-1",
            "deployment-1",
            &capabilities,
            &component_infos,
        )
        .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].component_name(), "summarize");
        assert_eq!(keys[0].component_type(), ComponentType::Function);
    }

    #[test]
    fn legacy_component_infos_are_supported_when_capabilities_are_absent() {
        let keys = demand_keys_from_worker_capabilities(
            "project-1",
            "deployment-1",
            &[],
            &[ComponentInfo {
                name: "score".into(),
                component_type: ComponentType::Scorer as i32,
                ..Default::default()
            }],
        )
        .unwrap();

        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].component_type(), ComponentType::Scorer);
    }
}
