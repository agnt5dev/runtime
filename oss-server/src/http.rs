use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use engine_core::lease_fence::META_RESUME_SOURCE_ID;
use engine_core::resume_target::check_resume_target;
use engine_core::types::{Offset, Record};
use engine_processor::run_state_machine::{can_transition, record_target_status, RunStatus};
use engine_proto::{ListRunsRequest, Priority, Run};
use prost::Message;
use runtime_common::backend::EngineBackend;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_stream::StreamExt;
use tower_http::trace::TraceLayer;

use crate::auth::{authorize, AuthConfig};
use crate::health::RuntimeHealth;

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<dyn EngineBackend>,
    pub health: Arc<RuntimeHealth>,
    pub auth: AuthConfig,
    pub project_id: Arc<str>,
    pub deployment_id: Arc<str>,
    pub workspace_id: Arc<str>,
    pub run_timeout: Duration,
}

#[derive(Debug, Deserialize, Default)]
struct ListRunsParams {
    status: Option<String>,
    component_name: Option<String>,
    component_type: Option<String>,
    deployment_id: Option<String>,
    since_ms: Option<i64>,
    until_ms: Option<i64>,
    limit: Option<i32>,
    offset: Option<i32>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ReasonBody {
    reason: Option<String>,
}

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/v1/{component_type}/{component}/run", post(run_component))
        .route(
            "/v1/{component_type}/{component}/submit",
            post(submit_component),
        )
        .route(
            "/v1/{component_type}/{component}/stream",
            post(stream_component),
        )
        .route("/v1/runs", get(list_runs))
        .route("/v1/runs/{run_id}", get(get_run))
        .route("/v1/runs/{run_id}/events", get(run_events))
        .route("/v1/runs/{run_id}/stream", get(run_events))
        .route("/v1/runs/{run_id}/cancel", post(cancel_run))
        .route("/v1/runs/{run_id}/suspend", post(suspend_run))
        .route("/v1/runs/{run_id}/resume", post(resume_run))
        .route("/v1/workflows/resume/{run_id}", post(resume_run))
        .route("/v1/status/{run_id}", get(get_status))
        .route("/v1/result/{run_id}", get(get_result))
        .layer(middleware::from_fn_with_state(
            state.auth.clone(),
            authorize,
        ));

    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/v1/health", get(readyz))
        .route("/v1/health/live", get(livez))
        .route("/v1/health/ready", get(readyz))
        .merge(protected)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn livez(State(state): State<AppState>) -> Response {
    Json(json!({
        "status": "ok",
        "processor_alive": state.health.processor_alive(),
        "uptime_seconds": state.health.uptime_seconds(),
    }))
    .into_response()
}

async fn readyz(State(state): State<AppState>) -> Response {
    if state.health.is_ready().await {
        Json(json!({"status": "ready"})).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "not_ready"})),
        )
            .into_response()
    }
}

async fn metrics(State(state): State<AppState>) -> Response {
    let ready = u8::from(state.health.is_ready().await);
    let alive = u8::from(state.health.processor_alive());
    let backend = state.health.backend();
    let connections = state.health.connection_count();
    let idle_connections = state.health.idle_connection_count();
    let mut body = format!(
        "# HELP agnt5_runtime_up Whether the runtime process is alive.\n\
         # TYPE agnt5_runtime_up gauge\n\
         agnt5_runtime_up 1\n\
         # HELP agnt5_runtime_ready Whether durable storage and the processor are ready.\n\
         # TYPE agnt5_runtime_ready gauge\n\
         agnt5_runtime_ready {ready}\n\
         # HELP agnt5_oss_processor_alive Whether the partition processor is running.\n\
         # TYPE agnt5_oss_processor_alive gauge\n\
         agnt5_oss_processor_alive {alive}\n\
         # HELP agnt5_oss_storage_connections Open storage connections.\n\
         # TYPE agnt5_oss_storage_connections gauge\n\
         agnt5_oss_storage_connections{{backend=\"{}\"}} {}\n\
         # HELP agnt5_oss_storage_idle_connections Idle pooled storage connections.\n\
         # TYPE agnt5_oss_storage_idle_connections gauge\n\
         agnt5_oss_storage_idle_connections{{backend=\"{}\"}} {}\n",
        backend, connections, backend, idle_connections,
    );
    // Preserve the PostgreSQL-specific metric names shipped by the initial OSS
    // profile so existing scrapers and dashboards do not break on upgrade.
    if backend == "postgres" {
        body.push_str(&format!(
            "# HELP agnt5_oss_postgres_pool_connections Open PostgreSQL pool connections.\n\
             # TYPE agnt5_oss_postgres_pool_connections gauge\n\
             agnt5_oss_postgres_pool_connections {connections}\n\
             # HELP agnt5_oss_postgres_pool_idle Idle PostgreSQL pool connections.\n\
             # TYPE agnt5_oss_postgres_pool_idle gauge\n\
             agnt5_oss_postgres_pool_idle {idle_connections}\n"
        ));
    }
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

async fn run_component(
    State(state): State<AppState>,
    Path((collection, component)): Path<(String, String)>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    let component_type = match normalize_component_type(&collection) {
        Some(component_type) => component_type,
        None => return component_not_found(&collection, &component),
    };
    let run_id = uuid::Uuid::now_v7().to_string();
    let mut tail = match state
        .engine
        .tail(&state.project_id, &run_id, Offset::ZERO)
        .await
    {
        Ok(tail) => tail,
        Err(error) => return internal_error("open run tail", error),
    };
    if let Err(error) = queue_run(
        &state,
        &headers,
        &run_id,
        component_type,
        &component,
        input,
        Priority::Interactive,
    )
    .await
    {
        return internal_error("enqueue run", error);
    }

    let started = std::time::Instant::now();
    let wait = async {
        while let Some(next) = tail.next().await {
            let record = next.map_err(|error| error.to_string())?;
            if is_terminal(&record) {
                return Ok::<_, String>(record);
            }
        }
        Err("run tail ended before a terminal event".to_string())
    };
    match tokio::time::timeout(state.run_timeout, wait).await {
        Ok(Ok(record)) => run_response(&run_id, &record, Some(started.elapsed())),
        Ok(Err(error)) => internal_error("wait for terminal event", error),
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            Json(json!({"error": "execution timeout", "run_id": run_id})),
        )
            .into_response(),
    }
}

async fn submit_component(
    State(state): State<AppState>,
    Path((collection, component)): Path<(String, String)>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    let component_type = match normalize_component_type(&collection) {
        Some(component_type) => component_type,
        None => return component_not_found(&collection, &component),
    };
    let run_id = uuid::Uuid::now_v7().to_string();
    match queue_run(
        &state,
        &headers,
        &run_id,
        component_type,
        &component,
        input,
        Priority::Batch,
    )
    .await
    {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "run_id": run_id,
                "status": "queued",
                "component": component,
                "links": {
                    "self": format!("/v1/status/{run_id}"),
                    "result": format!("/v1/result/{run_id}"),
                },
            })),
        )
            .into_response(),
        Err(error) => internal_error("enqueue run", error),
    }
}

async fn stream_component(
    State(state): State<AppState>,
    Path((collection, component)): Path<(String, String)>,
    headers: HeaderMap,
    Json(input): Json<Value>,
) -> Response {
    let component_type = match normalize_component_type(&collection) {
        Some(component_type) => component_type,
        None => return component_not_found(&collection, &component),
    };
    let run_id = uuid::Uuid::now_v7().to_string();
    let mut tail = match state
        .engine
        .tail(&state.project_id, &run_id, Offset::ZERO)
        .await
    {
        Ok(tail) => tail,
        Err(error) => return internal_error("open run tail", error),
    };
    if let Err(error) = queue_run(
        &state,
        &headers,
        &run_id,
        component_type,
        &component,
        input,
        Priority::Interactive,
    )
    .await
    {
        return internal_error("enqueue run", error);
    }

    let stream = async_stream::stream! {
        while let Some(next) = tail.next().await {
            match next {
                Ok(record) => {
                    let terminal = is_terminal(&record);
                    yield Ok::<Event, Infallible>(record_sse_event(&record));
                    if terminal {
                        break;
                    }
                }
                Err(error) => {
                    yield Ok(Event::default().event("error").data(error.to_string()));
                    break;
                }
            }
        }
    };
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}

async fn get_run(State(state): State<AppState>, Path(run_id): Path<String>) -> Response {
    match load_run(&state, &run_id).await {
        Ok(Some(run)) => Json(run_to_json(&run)).into_response(),
        Ok(None) => not_found(&run_id),
        Err(error) => internal_error("load run", error),
    }
}

async fn list_runs(
    State(state): State<AppState>,
    Query(params): Query<ListRunsParams>,
) -> Response {
    #[allow(deprecated)]
    let request = ListRunsRequest {
        workspace_id: state.workspace_id.to_string(),
        project_id: state.project_id.to_string(),
        deployment_id: params.deployment_id,
        status: params.status,
        component_name: params.component_name,
        component_type: params.component_type,
        since_ms: params.since_ms,
        until_ms: params.until_ms,
        limit: params.limit.unwrap_or(50),
        offset: params.offset.unwrap_or(0),
        cursor: params.cursor.unwrap_or_default(),
        ..Default::default()
    };
    match state.engine.list_runs(request).await {
        Ok(result) => {
            #[allow(deprecated)]
            let next_offset = result.next_offset;
            Json(json!({
                "items": result.items.iter().map(run_to_json).collect::<Vec<_>>(),
                "limit": result.limit,
                "offset": result.offset,
                "has_more": result.has_more,
                "next_cursor": result.next_cursor,
                "next_offset": next_offset,
                "total_count": -1,
            }))
            .into_response()
        }
        Err(error) => internal_error("list runs", error),
    }
}

async fn get_status(State(state): State<AppState>, Path(run_id): Path<String>) -> Response {
    let run = match load_run(&state, &run_id).await {
        Ok(run) => run,
        Err(error) => return internal_error("load run status", error),
    };
    let journal = journal_status(&state, &run_id).await;
    let Some(status) = reported_status(run.as_ref().map(|run| run.status.as_str()), journal) else {
        return not_found(&run_id);
    };
    match run {
        Some(run) => Json(json!({
            "run_id": run.run_id,
            "status": status,
            "component_name": run.component_name,
        }))
        .into_response(),
        None => Json(json!({"run_id": run_id, "status": status})).into_response(),
    }
}

async fn get_result(State(state): State<AppState>, Path(run_id): Path<String>) -> Response {
    let run = match load_run(&state, &run_id).await {
        Ok(run) => run,
        Err(error) => return internal_error("load run result", error),
    };
    let terminal = latest_terminal(&state, &run_id).await;
    let Some(run) = run else {
        return match terminal {
            Some(record) => result_from_record(&run_id, &record),
            None => not_found(&run_id),
        };
    };
    if let Some(record) = terminal {
        return result_from_record(&run_id, &record);
    }
    if !terminal_status(&run.status) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "run not complete",
                "run_id": run_id,
                "status": run.status,
            })),
        )
            .into_response();
    }
    Json(json!({
        "run_id": run.run_id,
        "status": run.status,
        "output": parse_json_or_null(&run.output_data),
        "error": nonempty(&run.error_message),
    }))
    .into_response()
}

async fn run_events(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let wants_sse = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("text/event-stream"));
    if !wants_sse {
        return match state
            .engine
            .read_by_run_id(&state.project_id, &run_id, Offset::ZERO, None)
            .await
        {
            Ok(records) => Json(json!({
                "count": records.len(),
                "items": records.iter().map(record_to_json).collect::<Vec<_>>(),
            }))
            .into_response(),
            Err(error) => internal_error("read run events", error),
        };
    }

    let from_offset = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(|offset| Offset(offset.saturating_add(1)))
        .unwrap_or(Offset::ZERO);
    let mut tail = match state
        .engine
        .tail(&state.project_id, &run_id, from_offset)
        .await
    {
        Ok(tail) => tail,
        Err(error) => return internal_error("open event tail", error),
    };
    let stream = async_stream::stream! {
        while let Some(next) = tail.next().await {
            match next {
                Ok(record) => {
                    let terminal = is_terminal(&record);
                    yield Ok::<Event, Infallible>(record_sse_event(&record));
                    if terminal {
                        break;
                    }
                }
                Err(error) => {
                    yield Ok(Event::default().event("error").data(error.to_string()));
                    break;
                }
            }
        }
    };
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}

async fn cancel_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    body: Option<Json<ReasonBody>>,
) -> Response {
    let run = match load_run(&state, &run_id).await {
        Ok(Some(run)) => run,
        Ok(None) => return not_found(&run_id),
        Err(error) => return internal_error("load run for cancellation", error),
    };
    if terminal_status(&run.status) {
        return conflict(&run_id, &run.status, "cancel");
    }
    let reason = body
        .and_then(|Json(body)| body.reason)
        .unwrap_or_else(|| "manual".to_string());
    let mut metadata = run.metadata.clone();
    metadata.insert("cancel_reason".to_string(), reason.clone());
    let record = Record {
        offset: Offset::ZERO,
        project_id: state.project_id.to_string(),
        run_id: run_id.clone(),
        event_type: "run.cancelled".to_string(),
        data: Bytes::from(
            serde_json::to_vec(&json!({"reason": reason})).unwrap_or_else(|_| b"{}".to_vec()),
        ),
        timestamp_ns: 0,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    };
    match state.engine.append(record).await {
        Ok((offset, _)) => Json(json!({
            "run_id": run_id,
            "status": "cancelled",
            "offset": offset.0,
        }))
        .into_response(),
        Err(error) => internal_error("cancel run", error),
    }
}

async fn suspend_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    body: Option<Json<ReasonBody>>,
) -> Response {
    let run = match load_run(&state, &run_id).await {
        Ok(Some(run)) => run,
        Ok(None) => return not_found(&run_id),
        Err(error) => return internal_error("load run for suspension", error),
    };
    if terminal_status(&run.status) {
        return conflict(&run_id, &run.status, "suspend");
    }
    if !run.lease_id.is_empty() {
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "an assigned pull run must reach a worker checkpoint before suspension",
                "run_id": run_id,
                "status": run.status,
            })),
        )
            .into_response();
    }
    let reason = body
        .and_then(|Json(body)| body.reason)
        .unwrap_or_else(|| "manual".to_string());
    let mut metadata = run.metadata.clone();
    metadata.insert("pause_reason".to_string(), reason.clone());
    metadata.insert(
        META_RESUME_SOURCE_ID.to_string(),
        format!("manual-suspend:{}", uuid::Uuid::now_v7()),
    );
    let record = Record {
        offset: Offset::ZERO,
        project_id: state.project_id.to_string(),
        run_id: run_id.clone(),
        event_type: "workflow.paused".to_string(),
        data: Bytes::from(
            serde_json::to_vec(&json!({"pause_reason": reason})).unwrap_or_else(|_| b"{}".to_vec()),
        ),
        timestamp_ns: 0,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    };
    match state.engine.append(record).await {
        Ok((offset, _)) => Json(json!({
            "run_id": run_id,
            "status": "paused",
            "offset": offset.0,
        }))
        .into_response(),
        Err(error) => internal_error("suspend run", error),
    }
}

async fn resume_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    body: Option<Json<Value>>,
) -> Response {
    let run = match load_run(&state, &run_id).await {
        Ok(Some(run)) => run,
        Ok(None) => return not_found(&run_id),
        Err(error) => return internal_error("load run for resume", error),
    };
    let journal = journal_status(&state, &run_id).await;
    let status = reported_status(Some(run.status.as_str()), journal).unwrap_or(run.status.clone());
    if status != "paused" {
        return conflict(&run_id, &status, "resume");
    }
    let records = match state
        .engine
        .read_by_run_id(
            &state.project_id,
            &run_id,
            Offset::ZERO,
            Some(vec![
                "run.queued".to_string(),
                "workflow.paused".to_string(),
            ]),
        )
        .await
    {
        Ok(records) => records,
        Err(error) => return internal_error("load run resume context", error),
    };
    let Some(queued) = records
        .iter()
        .find(|record| record.event_type == "run.queued")
    else {
        return conflict(&run_id, &run.status, "resume without a queued event");
    };
    let Some(paused) = records
        .iter()
        .rev()
        .find(|record| record.event_type == "workflow.paused")
    else {
        return conflict(&run_id, &run.status, "resume without a pause event");
    };
    let body = body.map(|Json(body)| body).unwrap_or_else(|| json!({}));
    // Only a pause that asked a question takes an answer.
    if let Err(rejection) = check_resume_target(paused, &body) {
        return (StatusCode::CONFLICT, Json(rejection.response_body(&run_id))).into_response();
    }
    let mut metadata = run.metadata.clone();
    metadata.extend(paused.metadata.clone());
    // The run's metadata keeps the previous resume's answer. It belongs to
    // that pause, not this one.
    metadata.remove("user_response");
    metadata
        .entry(META_RESUME_SOURCE_ID.to_string())
        .or_insert_with(|| format!("pause:{}", paused.offset.0));
    if let Some(response) = body.get("user_response").filter(|value| !value.is_null()) {
        metadata.insert(
            "user_response".to_string(),
            response
                .as_str()
                .map(ToString::to_string)
                .unwrap_or_else(|| response.to_string()),
        );
    }
    let record = Record {
        offset: Offset::ZERO,
        project_id: state.project_id.to_string(),
        run_id: run_id.clone(),
        event_type: "run.resumed".to_string(),
        data: queued.data.clone(),
        timestamp_ns: 0,
        step_key: None,
        correlation_id: None,
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    };
    match state.engine.append(record).await {
        Ok((offset, _)) => Json(json!({
            "run_id": run_id,
            "status": "resumed",
            "offset": offset.0,
        }))
        .into_response(),
        Err(error) => internal_error("resume run", error),
    }
}

async fn queue_run(
    state: &AppState,
    headers: &HeaderMap,
    run_id: &str,
    component_type: &str,
    component: &str,
    input: Value,
    priority: Priority,
) -> Result<(), String> {
    let mut metadata = HashMap::new();
    metadata.insert("component_name".to_string(), component.to_string());
    metadata.insert("component_type".to_string(), component_type.to_string());
    metadata.insert("project_id".to_string(), state.project_id.to_string());
    metadata.insert("deployment_id".to_string(), state.deployment_id.to_string());
    metadata.insert("workspace_id".to_string(), state.workspace_id.to_string());
    metadata.insert("tenant_id".to_string(), "default".to_string());
    metadata.insert("dispatch_mode".to_string(), "pull".to_string());
    metadata.insert("priority".to_string(), (priority as i32).to_string());
    metadata.insert("root_run_id".to_string(), run_id.to_string());
    for (header_name, metadata_key) in [
        ("x-session-id", "session_id"),
        ("x-user-id", "user_id"),
        ("x-parent-run-id", "parent_run_id"),
        ("x-experiment-id", "experiment_id"),
        ("traceparent", "traceparent"),
        ("baggage", "baggage"),
    ] {
        if let Some(value) = headers
            .get(header_name)
            .and_then(|value| value.to_str().ok())
        {
            metadata.insert(metadata_key.to_string(), value.to_string());
        }
    }
    metadata
        .entry("traceparent".to_string())
        .or_insert_with(new_traceparent);
    metadata.insert("gateway_received_at_ns".to_string(), now_ns().to_string());
    let record = Record {
        offset: Offset::ZERO,
        project_id: state.project_id.to_string(),
        run_id: run_id.to_string(),
        event_type: "run.queued".to_string(),
        data: Bytes::from(serde_json::to_vec(&input).map_err(|error| error.to_string())?),
        timestamp_ns: 0,
        step_key: None,
        correlation_id: Some(run_id.chars().take(8).collect()),
        parent_event_id: None,
        metadata,
        data_type: "json".to_string(),
        data_checksum: None,
        data_compressed: false,
    };
    state
        .engine
        .append(record)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

async fn load_run(state: &AppState, run_id: &str) -> Result<Option<Run>, String> {
    state
        .engine
        .get_run(&state.project_id, run_id)
        .await
        .map_err(|error| error.to_string())?
        .map(|bytes| Run::decode(bytes.as_slice()).map_err(|error| error.to_string()))
        .transpose()
}

async fn latest_terminal(state: &AppState, run_id: &str) -> Option<Record> {
    state
        .engine
        .read_by_run_id(
            &state.project_id,
            run_id,
            Offset::ZERO,
            Some(vec![
                "run.completed".to_string(),
                "run.failed".to_string(),
                "run.cancelled".to_string(),
                "workflow.paused".to_string(),
                "run.paused".to_string(),
            ]),
        )
        .await
        .ok()?
        .into_iter()
        .rev()
        .find(is_terminal)
}

/// Every record the run state machine reads, so a run's status can be
/// replayed from the journal exactly as the runs projection applies it.
const LIFECYCLE_EVENTS: [&str; 11] = [
    "run.queued",
    "run.assigned",
    "run.started",
    "run.paused",
    "workflow.paused",
    "run.resumed",
    "run.lease_expired",
    "run.terminalizing",
    "run.completed",
    "run.failed",
    "run.cancelled",
];

/// The run's status replayed from its lifecycle records in the journal.
/// `None` when the journal can't be read or holds no lifecycle record yet.
async fn journal_status(state: &AppState, run_id: &str) -> Option<RunStatus> {
    let records = state
        .engine
        .read_by_run_id(
            &state.project_id,
            run_id,
            Offset::ZERO,
            Some(
                LIFECYCLE_EVENTS
                    .iter()
                    .map(|event| event.to_string())
                    .collect(),
            ),
        )
        .await
        .ok()?;
    replay_status(&records)
}

/// Apply lifecycle records in journal order with the projection's transition
/// rules. A record that would make an illegal transition is skipped, as the
/// projection skips it: a `run.cancelled` that lands after `run.completed`
/// leaves the run completed, and a retryable `run.failed` is a retry, not a
/// failure.
fn replay_status(records: &[Record]) -> Option<RunStatus> {
    records.iter().fold(None, |current, record| {
        let Some(target) = record_target_status(&record.event_type, &record.metadata) else {
            return current;
        };
        match current {
            None => Some(target),
            Some(from) if from == target || can_transition(from, target) => Some(target),
            Some(from) => Some(from),
        }
    })
}

/// The status the API reports for a run.
///
/// A run's stored state is updated from the journal a moment after each
/// record is written, so it can trail what has already happened. The status
/// replayed from the journal never trails it and follows the same rules, so
/// it wins whenever there is one. Every endpoint uses this one view, so status
/// and resume can't disagree. `None` means the runtime has no record of the run.
fn reported_status(stored: Option<&str>, journal: Option<RunStatus>) -> Option<String> {
    journal
        .map(|status| status.as_str().to_string())
        .or_else(|| stored.map(str::to_string))
}

fn normalize_component_type(collection: &str) -> Option<&'static str> {
    match collection.to_ascii_lowercase().as_str() {
        "function" | "functions" => Some("function"),
        "workflow" | "workflows" => Some("workflow"),
        "agent" | "agents" => Some("agent"),
        "tool" | "tools" => Some("tool"),
        "scorer" | "scorers" => Some("scorer"),
        _ => None,
    }
}

fn is_terminal(record: &Record) -> bool {
    matches!(
        record.event_type.as_str(),
        "run.completed" | "run.failed" | "run.cancelled" | "run.paused" | "workflow.paused"
    )
}

fn terminal_status(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "cancelled" | "paused")
}

fn terminal_event_status(record: &Record) -> &'static str {
    match record.event_type.as_str() {
        "run.completed" => "completed",
        "run.failed" => "failed",
        "run.cancelled" => "cancelled",
        "run.paused" | "workflow.paused" => "paused",
        _ => "unknown",
    }
}

fn terminal_output(record: &Record) -> Option<Value> {
    if record.event_type != "run.completed" {
        return None;
    }
    let payload = serde_json::from_slice::<Value>(&record.data).ok()?;
    payload
        .get("output_data")
        .or_else(|| payload.get("output"))
        .cloned()
        .or(Some(payload))
}

fn terminal_error(record: &Record) -> Option<String> {
    if record.event_type != "run.failed" {
        return None;
    }
    let payload = serde_json::from_slice::<Value>(&record.data).ok()?;
    payload
        .get("error_message")
        .or_else(|| payload.get("error"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn run_response(run_id: &str, record: &Record, duration: Option<Duration>) -> Response {
    let status = terminal_event_status(record);
    Json(json!({
        "run_id": run_id,
        "status": status,
        "output": terminal_output(record),
        "error": terminal_error(record),
        "error_code": if record.event_type == "run.failed" {
            serde_json::from_slice::<Value>(&record.data).ok()
                .and_then(|payload| payload.get("error_code").and_then(Value::as_str).map(ToString::to_string))
        } else {
            None
        },
        "duration_ms": duration.map(|duration| duration.as_millis() as u64),
        "trace_id": record.metadata.get("trace_id"),
    }))
    .into_response()
}

fn result_from_record(run_id: &str, record: &Record) -> Response {
    Json(json!({
        "run_id": run_id,
        "status": terminal_event_status(record),
        "output": terminal_output(record),
        "error": terminal_error(record),
    }))
    .into_response()
}

fn record_to_json(record: &Record) -> Value {
    let mut metadata = record.metadata.clone();
    metadata.remove("worker_session_id");
    let data = if engine_core::activation::has_activation_marker(record) {
        engine_core::activation::public_view(record)
    } else {
        parse_json_or_null(&record.data)
    };
    json!({
        "event_type": record.event_type,
        "run_id": record.run_id,
        "data": data,
        "timestamp_ns": record.timestamp_ns,
        "offset": record.offset.0,
        "step_key": record.step_key,
        "metadata": metadata,
        "correlation_id": record.correlation_id,
        "parent_event_id": record.parent_event_id,
    })
}

fn record_sse_event(record: &Record) -> Event {
    let event = Event::default()
        .event(&record.event_type)
        .json_data(record_to_json(record))
        .unwrap_or_else(|_| Event::default().data("{}"));
    if is_terminal(record) {
        event.id("done")
    } else {
        event.id(record.offset.0.to_string())
    }
}

fn run_to_json(run: &Run) -> Value {
    json!({
        "run_id": run.run_id,
        "project_id": run.project_id,
        "workspace_id": run.workspace_id,
        "deployment_id": run.deployment_id,
        "component_name": run.component_name,
        "component_type": run.component_type,
        "status": run.status,
        "attempt": run.attempt,
        "input_data": parse_json_or_null(&run.input_data),
        "output_data": parse_json_or_null(&run.output_data),
        "error_message": run.error_message,
        "error_code": run.error_code,
        "trace_id": run.trace_id,
        "metadata": run.metadata,
    })
}

fn parse_json_or_null(bytes: &[u8]) -> Value {
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(bytes).unwrap_or(Value::Null)
    }
}

fn nonempty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn component_not_found(collection: &str, component: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": format!("unsupported component collection '{collection}'"),
            "component": component,
        })),
    )
        .into_response()
}

fn not_found(run_id: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error": "run not found", "run_id": run_id})),
    )
        .into_response()
}

fn conflict(run_id: &str, status: &str, operation: &str) -> Response {
    (
        StatusCode::CONFLICT,
        Json(json!({
            "error": format!("cannot {operation} run in '{status}' state"),
            "run_id": run_id,
            "status": status,
        })),
    )
        .into_response()
}

fn internal_error(context: &str, error: impl std::fmt::Display) -> Response {
    tracing::error!(%error, context, "OSS HTTP request failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": format!("{context}: {error}")})),
    )
        .into_response()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as i64)
        .unwrap_or_default()
}

fn new_traceparent() -> String {
    let trace_id = uuid::Uuid::new_v4().simple().to_string();
    let span_source = uuid::Uuid::new_v4().simple().to_string();
    let span_id = &span_source[..16];
    format!("00-{trace_id}-{span_id}-01")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_collections_normalize_to_canonical_types() {
        assert_eq!(normalize_component_type("functions"), Some("function"));
        assert_eq!(normalize_component_type("WORKFLOWS"), Some("workflow"));
        assert_eq!(normalize_component_type("unknown"), None);
    }

    fn lifecycle(event_type: &str) -> Record {
        Record {
            offset: Offset::ZERO,
            project_id: "project".to_string(),
            run_id: "run".to_string(),
            event_type: event_type.to_string(),
            data: Bytes::new(),
            timestamp_ns: 0,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: HashMap::new(),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    fn replay(events: &[&str]) -> Option<String> {
        let records: Vec<Record> = events.iter().map(|event| lifecycle(event)).collect();
        replay_status(&records).map(|status| status.as_str().to_string())
    }

    #[test]
    fn a_recorded_pause_wins_over_stored_state_that_has_not_caught_up() {
        let journal = replay(&[
            "run.queued",
            "run.assigned",
            "run.started",
            "workflow.paused",
        ]);
        assert_eq!(journal.as_deref(), Some("paused"));
        let journal = RunStatus::from_str(journal.as_deref().unwrap());
        assert_eq!(
            reported_status(Some("started"), journal).as_deref(),
            Some("paused")
        );
    }

    #[test]
    fn a_resumed_run_is_not_reported_as_still_paused() {
        let events = [
            "run.queued",
            "run.assigned",
            "run.started",
            "run.paused",
            "run.resumed",
        ];
        assert_eq!(replay(&events).as_deref(), Some("queued"));
        let mut picked_up = events.to_vec();
        picked_up.extend(["run.assigned", "run.started"]);
        assert_eq!(replay(&picked_up).as_deref(), Some("started"));
    }

    #[test]
    fn a_just_submitted_run_is_queued_not_missing() {
        let journal = RunStatus::from_str(replay(&["run.queued"]).as_deref().unwrap());
        assert_eq!(reported_status(None, journal).as_deref(), Some("queued"));
        assert_eq!(reported_status(None, None), None);
        assert_eq!(
            reported_status(Some("started"), None).as_deref(),
            Some("started")
        );
    }

    #[test]
    fn a_cancel_that_loses_the_race_to_completion_leaves_the_run_completed() {
        let events = [
            "run.queued",
            "run.assigned",
            "run.started",
            "run.completed",
            "run.cancelled",
        ];
        assert_eq!(replay(&events).as_deref(), Some("completed"));
        let journal = RunStatus::from_str("completed");
        assert_eq!(
            reported_status(Some("completed"), journal).as_deref(),
            Some("completed")
        );
    }

    #[test]
    fn a_retryable_failure_is_a_retry_not_a_failure() {
        let mut failed = lifecycle("run.failed");
        failed
            .metadata
            .insert("max_attempts".to_string(), "3".to_string());
        failed
            .metadata
            .insert("attempt".to_string(), "0".to_string());
        let records = vec![
            lifecycle("run.queued"),
            lifecycle("run.assigned"),
            lifecycle("run.started"),
            failed,
        ];
        assert_ne!(replay_status(&records), Some(RunStatus::Failed));
    }

    #[test]
    fn terminal_output_unwraps_sdk_completion_envelope() {
        let record = Record {
            offset: Offset::ZERO,
            project_id: "project".to_string(),
            run_id: "run".to_string(),
            event_type: "run.completed".to_string(),
            data: Bytes::from_static(br#"{"output_data":{"ok":true}}"#),
            timestamp_ns: 0,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: HashMap::new(),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };
        assert_eq!(terminal_output(&record), Some(json!({"ok": true})));
    }

    #[test]
    fn event_json_redacts_signed_worker_session_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert(
            "worker_session_id".to_string(),
            "agnt5ws1.signed-session".to_string(),
        );
        metadata.insert("worker_id".to_string(), "worker-1".to_string());
        let record = Record {
            offset: Offset::ZERO,
            project_id: "project".to_string(),
            run_id: "run".to_string(),
            event_type: "run.started".to_string(),
            data: Bytes::new(),
            timestamp_ns: 0,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata,
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };

        let value = record_to_json(&record);

        assert!(value["metadata"].get("worker_session_id").is_none());
        assert_eq!(value["metadata"]["worker_id"], "worker-1");
    }

    #[test]
    fn event_json_never_exposes_raw_marked_activation_payloads() {
        let mut record = Record {
            offset: Offset::ZERO,
            project_id: "project".to_string(),
            run_id: "run".to_string(),
            event_type: "workflow.step.started".to_string(),
            data: Bytes::from_static(br#"{"event":{"fence_token":"secret"},"version":2}"#),
            timestamp_ns: 0,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: HashMap::new(),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        };
        record.metadata.insert(
            "activation_contract".to_string(),
            "durable_activation_v2".to_string(),
        );

        let value = record_to_json(&record);

        assert!(value["data"].is_null());
    }
}
