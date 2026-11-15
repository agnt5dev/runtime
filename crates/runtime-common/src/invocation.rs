use serde_json::Value;
use std::{collections::HashMap, time::Duration};

/// Frozen invocation facts chosen by a built-in controller; the adapter must preserve the supplied
/// authority and apply normal route resolution, admission and execution fences.
#[derive(Clone, Copy)]
pub struct ExecuteRequest<'a> {
    pub run_id: &'a str,
    pub component_type: &'a str,
    pub component_name: &'a str,
    pub input: &'a Value,
    pub session_id: Option<&'a str>,
    pub custom_metadata: &'a HashMap<String, String>,
    pub deployment_id: Option<&'a str>,
    pub sub_tenant: &'a str,
    pub timeout: Duration,
}
