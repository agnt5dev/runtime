//! SDK-compatible HTTP ingress for one single-tenant runtime.

use std::{sync::Arc, time::SystemTime};

use agnt5_core::{MaterializedStore, NewJournalRecord, RuntimeEvent, Segment};
use agnt5_processor::{decode_run, run_key, RUNS};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

pub struct GatewayState<S: Segment, M: MaterializedStore> {
    project_id: String,
    segment: Arc<S>,
    store: Arc<M>,
}

impl<S: Segment, M: MaterializedStore> Clone for GatewayState<S, M> {
    fn clone(&self) -> Self {
        Self {
            project_id: self.project_id.clone(),
            segment: Arc::clone(&self.segment),
            store: Arc::clone(&self.store),
        }
    }
}

pub fn router<S: Segment, M: MaterializedStore>(
    project_id: impl Into<String>,
    segment: Arc<S>,
    store: Arc<M>,
) -> Router {
    let state = GatewayState {
        project_id: project_id.into(),
        segment,
        store,
    };
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(health))
        .route(
            "/v1/{component_type}/{component}/submit",
            post(submit::<S, M>),
        )
        .route("/v1/status/{run_id}", get(status::<S, M>))
        .route("/v1/result/{run_id}", get(result::<S, M>))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn submit<S: Segment, M: MaterializedStore>(
    State(state): State<GatewayState<S, M>>,
    Path((component_type, component_name)): Path<(String, String)>,
    Json(input): Json<Value>,
) -> Result<impl IntoResponse, ApiError> {
    let run_id = Uuid::now_v7().to_string();
    let event = RuntimeEvent::RunQueued {
        project_id: state.project_id,
        run_id: run_id.clone(),
        component_type: component_type.trim_end_matches('s').to_string(),
        component_name,
        input_data: serde_json::to_vec(&input).map_err(ApiError::internal)?,
        submitted_at_ms: now_ms(),
    };
    let payload = serde_json::to_vec(&event).map_err(ApiError::internal)?;
    state
        .segment
        .append_batch(&[NewJournalRecord {
            idempotency_key: Some(format!("submit:{run_id}").into_bytes()),
            payload: Bytes::from(payload),
        }])
        .await
        .map_err(ApiError::internal)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "run_id": run_id, "status": "enqueued", "status_code": 202 })),
    ))
}

async fn status<S: Segment, M: MaterializedStore>(
    State(state): State<GatewayState<S, M>>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let run = load_run(&*state.store, &run_id).await?;
    let public_status = if run.status == "queued" {
        "enqueued"
    } else {
        run.status.as_str()
    };
    Ok(Json(json!({
        "run_id": run.run_id,
        "status": public_status,
        "status_code": if run.status == "completed" { 200 } else if run.status == "failed" { 500 } else { 202 },
        "component_type": run.component_type,
        "component_name": run.component_name,
        "submitted_at_ms": run.submitted_at_ms,
        "completed_at_ms": run.completed_at_ms,
        "attempt": run.attempt
    })))
}

async fn result<S: Segment, M: MaterializedStore>(
    State(state): State<GatewayState<S, M>>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let run = load_run(&*state.store, &run_id).await?;
    match run.status.as_str() {
        "completed" => {
            let output = run
                .output_data
                .as_deref()
                .map(serde_json::from_slice)
                .transpose()
                .map_err(ApiError::internal)?
                .unwrap_or(Value::Null);
            Ok(Json(json!({
                "run_id": run.run_id,
                "status": "completed",
                "status_code": 200,
                "output": output
            })))
        }
        "failed" => Err(ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            run.error_message.unwrap_or_else(|| "run failed".into()),
        )),
        _ => Err(ApiError(
            StatusCode::NOT_FOUND,
            "result is not ready".into(),
        )),
    }
}

async fn load_run<M: MaterializedStore>(
    store: &M,
    run_id: &str,
) -> Result<agnt5_core::RunState, ApiError> {
    let value = store
        .get(RUNS, &run_key(run_id))
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "run not found".into()))?;
    decode_run(&value).map_err(ApiError::internal)
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

struct ApiError(StatusCode, String);

impl ApiError {
    fn internal(error: impl std::fmt::Display) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.0, Json(ErrorBody { error: self.1 })).into_response()
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
