//! Durable pull-queue metric refresh helpers.

use std::sync::Arc;
use std::time::Duration;

use engine_core::error::EngineError;
use engine_core::storage_contract::QueueQueryStore;
use engine_core::traits::Segment;
use engine_core::types::{QueueCounter, Record};
use engine_proto::Run;

use super::pull_workers::QueueMetricRefreshKey;
use super::{
    now_ms, EngineServiceImpl, EngineServiceStore, QUEUE_METRIC_REFRESH_DELAY_MS,
    QUEUE_METRIC_REFRESH_INTERVAL_MS,
};

impl<S: Segment> EngineServiceImpl<S> {
    pub(super) fn observe_queue_metrics_for_record(&self, record: &Record) {
        let Some((project_id, deployment_id, priority)) = queue_metric_target_from_record(record)
        else {
            return;
        };
        self.spawn_queue_metric_refresh(project_id, deployment_id, priority);
    }

    pub(super) fn observe_queue_metrics_for_run(&self, project_id: &str, run: &Run) {
        if run.deployment_id.is_empty() {
            return;
        }
        self.spawn_queue_metric_refresh(
            project_id.to_string(),
            run.deployment_id.clone(),
            Some(canonical_queue_priority(run.priority)),
        );
    }

    fn spawn_queue_metric_refresh(
        &self,
        project_id: String,
        deployment_id: String,
        priority: Option<u8>,
    ) {
        let (Some(store), Some(metrics)) = (self.store.clone(), self.metrics.clone()) else {
            return;
        };
        let key = QueueMetricRefreshKey {
            project_id,
            deployment_id,
            priority,
        };
        {
            let mut active = self
                .queue_metric_refreshes
                .active
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !active.insert(key.clone()) {
                return;
            }
        }
        let registry = self.queue_metric_refreshes.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(QUEUE_METRIC_REFRESH_DELAY_MS)).await;
            loop {
                match record_queue_metric_snapshot(
                    store.clone(),
                    metrics.clone(),
                    key.project_id.clone(),
                    key.deployment_id.clone(),
                    key.priority,
                )
                .await
                {
                    Ok(pending_count) if pending_count > 0 => {
                        tokio::time::sleep(Duration::from_millis(QUEUE_METRIC_REFRESH_INTERVAL_MS))
                            .await;
                    }
                    Ok(_) => break,
                    Err(err) => {
                        tracing::debug!(error = %err, "queue metric refresh failed");
                        break;
                    }
                }
            }
            registry
                .active
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key);
        });
    }
}

pub(super) fn queue_counter_oldest_ready_age_ms(counter: &QueueCounter, now_ms: i64) -> u64 {
    if counter.pending_count == 0 {
        return 0;
    }
    counter
        .oldest_ready_at_ms
        .map(|oldest| now_ms.saturating_sub(oldest).max(0) as u64)
        .unwrap_or_default()
}

fn queue_metric_event_affects_counter(event_type: &str) -> bool {
    matches!(
        event_type,
        "run.queued"
            | "run.resumed"
            | "run.lease_expired"
            | "run.assigned"
            | "run.started"
            | "run.paused"
            | "workflow.paused"
            | "run.completed"
            | "run.failed"
            | "run.cancelled"
    )
}

pub(super) fn canonical_queue_priority(priority: i32) -> u8 {
    match priority {
        1 => 1,
        3 => 3,
        _ => 2,
    }
}

pub(super) fn queue_priority_label(priority: u8) -> &'static str {
    match priority {
        1 => "interactive",
        2 => "normal",
        3 => "batch",
        _ => "unspecified",
    }
}

pub(super) fn queue_metric_target_from_record(
    record: &Record,
) -> Option<(String, String, Option<u8>)> {
    if !queue_metric_event_affects_counter(&record.event_type) {
        return None;
    }
    let deployment_id = record.metadata.get("deployment_id")?;
    if deployment_id.is_empty() {
        return None;
    }
    let priority = record
        .metadata
        .get("priority")
        .and_then(|v| v.parse::<i32>().ok())
        .map(canonical_queue_priority);
    Some((record.project_id.clone(), deployment_id.clone(), priority))
}

async fn record_queue_metric_snapshot(
    store: Arc<dyn EngineServiceStore>,
    metrics: crate::telemetry::EngineMetrics,
    project_id: String,
    deployment_id: String,
    priority: Option<u8>,
) -> Result<u64, EngineError> {
    let now = now_ms();
    let deployment_counter =
        QueueQueryStore::load_deployment_queue_counter(store.as_ref(), &project_id, &deployment_id)
            .await?;
    let deployment_pending_count = deployment_counter.pending_count;
    record_queue_counter_gauges(
        &metrics,
        &deployment_counter,
        &project_id,
        &deployment_id,
        "all",
        now,
    );

    if let Some(priority) = priority {
        let priority_counter = QueueQueryStore::load_priority_queue_counter(
            store.as_ref(),
            &project_id,
            &deployment_id,
            priority,
        )
        .await?;
        record_queue_counter_gauges(
            &metrics,
            &priority_counter,
            &project_id,
            &deployment_id,
            queue_priority_label(priority),
            now,
        );
    }

    Ok(deployment_pending_count)
}

fn record_queue_counter_gauges(
    metrics: &crate::telemetry::EngineMetrics,
    counter: &QueueCounter,
    project_id: &str,
    deployment_id: &str,
    priority: &'static str,
    now_ms: i64,
) {
    let labels = [
        opentelemetry::KeyValue::new("agnt5.project.id", project_id.to_string()),
        opentelemetry::KeyValue::new("agnt5.deployment.id", deployment_id.to_string()),
        opentelemetry::KeyValue::new("priority", priority),
    ];
    metrics.queue_pending.record(counter.pending_count, &labels);
    metrics
        .queue_oldest_ready_age_ms
        .record(queue_counter_oldest_ready_age_ms(counter, now_ms), &labels);
}
