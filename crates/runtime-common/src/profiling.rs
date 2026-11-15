//! Optional continuous heap profiling for dataplane binaries.
//!
//! Set `AGNT5_PYROSCOPE_URL` (or `PYROSCOPE_URL`) to enable Pyroscope upload.
//! The runtime binary must use jemalloc as its global allocator, and profiling
//! must be activated before process start via `_RJEM_MALLOC_CONF`, for example
//! `prof:true,prof_active:true,lg_prof_sample:19`. The guard returned by
//! [`init_pyroscope`] must be held for the process lifetime so the agent can
//! flush a final profile on shutdown.

use std::collections::HashMap;

use pyroscope::backend::jemalloc::jemalloc_backend;
use pyroscope::pyroscope::{PyroscopeAgent, PyroscopeAgentBuilder, PyroscopeAgentRunning};
use tracing::{info, warn};

type RunningAgent = PyroscopeAgent<PyroscopeAgentRunning>;

pub struct PyroscopeGuard {
    agent: Option<RunningAgent>,
}

impl Drop for PyroscopeGuard {
    fn drop(&mut self) {
        let Some(agent) = self.agent.take() else {
            return;
        };

        match agent.stop() {
            Ok(ready) => {
                ready.shutdown();
                info!("Pyroscope profiling stopped");
            }
            Err(e) => warn!(error = %e, "Failed to stop Pyroscope profiling cleanly"),
        }
    }
}

/// Start the Pyroscope jemalloc heap profiler when configured.
pub fn init_pyroscope(
    service_name: &str,
    service_version: &str,
    node_id: &str,
    target: &str,
) -> Option<PyroscopeGuard> {
    let url = env_first(&["AGNT5_PYROSCOPE_URL", "PYROSCOPE_URL"])?;
    let application_name = env_first(&[
        "AGNT5_PYROSCOPE_APPLICATION_NAME",
        "PYROSCOPE_APPLICATION_NAME",
    ])
    .unwrap_or_else(|| service_name.to_string());
    let upload_sample_rate =
        env_first(&["AGNT5_PYROSCOPE_SAMPLE_RATE_HZ", "PYROSCOPE_SAMPLE_RATE_HZ"])
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(100);
    if env_first(&["_RJEM_MALLOC_CONF", "MALLOC_CONF"]).is_none() {
        warn!(
            "Pyroscope URL is configured but jemalloc profiling env is not set; \
             set _RJEM_MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:19"
        );
    }

    let mut tags = vec![
        ("service".to_string(), service_name.to_string()),
        ("target".to_string(), target.to_string()),
        ("node_id".to_string(), node_id.to_string()),
    ];
    if let Some(environment) = env_first(&["AGNT5_ENVIRONMENT", "SENTRY_ENVIRONMENT"]) {
        tags.push(("environment".to_string(), environment));
    }
    if let Some(namespace) = env_first(&["POD_NAMESPACE", "KUBERNETES_NAMESPACE"]) {
        tags.push(("namespace".to_string(), namespace));
    }
    if let Some(pod_name) = env_first(&["POD_NAME", "HOSTNAME"]) {
        tags.push(("pod".to_string(), pod_name));
    }
    tags.extend(parse_tags(&env_first(&[
        "AGNT5_PYROSCOPE_TAGS",
        "PYROSCOPE_TAGS",
    ])));

    let tag_refs: Vec<(&str, &str)> = tags
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();

    let mut builder = PyroscopeAgentBuilder::new(
        &url,
        &application_name,
        upload_sample_rate,
        "pyroscope-rs",
        service_version,
        jemalloc_backend(),
    )
    .tags(tag_refs);

    if let Some(tenant_id) = env_first(&["AGNT5_PYROSCOPE_TENANT_ID", "PYROSCOPE_TENANT_ID"]) {
        builder = builder.tenant_id(tenant_id);
    }

    if let Some(headers) = parse_http_headers(&env_first(&[
        "AGNT5_PYROSCOPE_HTTP_HEADERS",
        "PYROSCOPE_HTTP_HEADERS",
    ])) {
        builder = builder.http_headers(headers);
    }

    if let (Some(user), Some(password)) = (
        env_first(&[
            "AGNT5_PYROSCOPE_BASIC_AUTH_USER",
            "PYROSCOPE_BASIC_AUTH_USER",
        ]),
        env_first(&[
            "AGNT5_PYROSCOPE_BASIC_AUTH_PASSWORD",
            "PYROSCOPE_BASIC_AUTH_PASSWORD",
        ]),
    ) {
        builder = builder.basic_auth(user, password);
    }

    match builder.build().and_then(|agent| agent.start()) {
        Ok(agent) => {
            info!(
                url = %url,
                application = %application_name,
                sample_rate = upload_sample_rate,
                "Pyroscope jemalloc profiling started"
            );
            Some(PyroscopeGuard { agent: Some(agent) })
        }
        Err(e) => {
            warn!(error = %e, url = %url, "Pyroscope jemalloc profiling disabled after startup error");
            None
        }
    }
}

fn env_first(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn parse_tags(raw: &Option<String>) -> Vec<(String, String)> {
    raw.as_deref()
        .unwrap_or_default()
        .split(',')
        .filter_map(parse_pair)
        .collect()
}

fn parse_http_headers(raw: &Option<String>) -> Option<HashMap<String, String>> {
    let headers: HashMap<String, String> = raw
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .filter_map(parse_pair)
        .collect();
    if headers.is_empty() {
        None
    } else {
        Some(headers)
    }
}

fn parse_pair(raw: &str) -> Option<(String, String)> {
    let (key, value) = raw.split_once('=')?;
    let key = key.trim();
    let value = value.trim();
    if key.is_empty() || value.is_empty() {
        return None;
    }
    Some((key.to_string(), value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tags_skips_empty_and_malformed_entries() {
        let tags = parse_tags(&Some("env=prod,,bad,team=runtime,empty=".to_string()));
        assert_eq!(
            tags,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "runtime".to_string())
            ]
        );
    }

    #[test]
    fn parse_headers_returns_none_for_no_valid_pairs() {
        assert!(parse_http_headers(&Some("bad,nope=".to_string())).is_none());
    }
}
