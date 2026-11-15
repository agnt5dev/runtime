//! Which pause a resume request may answer.
//!
//! A resume used to be accepted whenever a run was paused, whatever it was
//! waiting for. An answer sent during a durable sleep was kept, and the next
//! wait-for-user took it without asking anyone, so one "approve" granted
//! approvals nobody saw. Both the managed gateway and the OSS
//! server check a resume with [`check_resume_target`] before writing
//! `run.resumed`.

use serde_json::{json, Map, Value};

use crate::lease_fence::META_RESUME_SOURCE_ID;
use crate::types::Record;

/// Pause reasons a person resolves: an SDK's wait-for-user and a guardrail
/// review. Only these take an answer.
pub const ANSWERABLE_PAUSE_REASONS: &[&str] = &["user_input_required", "guardrail_human_review"];

/// Prefix of the resume source the OSS server's `/suspend` gives a manual
/// suspension. Such a pause is resumed without an answer.
pub const MANUAL_SUSPEND_SOURCE_PREFIX: &str = "manual-suspend:";

/// A refused resume: a message for the caller, and fields that say what the
/// run is waiting for.
#[derive(Debug, Clone, PartialEq)]
pub struct ResumeRejection {
    pub error: String,
    pub detail: Map<String, Value>,
}

impl ResumeRejection {
    fn new(error: impl Into<String>, detail: Value) -> Self {
        Self {
            error: error.into(),
            detail: detail.as_object().cloned().unwrap_or_default(),
        }
    }

    /// The JSON body of the 409 response.
    pub fn response_body(&self, run_id: &str) -> Value {
        let mut body = Map::new();
        body.insert("error".to_string(), Value::String(self.error.clone()));
        body.insert("run_id".to_string(), Value::String(run_id.to_string()));
        body.extend(self.detail.clone());
        Value::Object(body)
    }
}

/// Why a `workflow.paused` record is waiting: the `pause_reason` the SDKs,
/// guardrails and workerless runs set, else the reason in its payload (a timer
/// suspension's `"reason": "timer"`).
pub fn pause_reason(paused: &Record) -> Option<String> {
    paused
        .metadata
        .get("pause_reason")
        .or_else(|| paused.metadata.get("workerless_suspend_reason"))
        .filter(|reason| !reason.is_empty())
        .cloned()
        .or_else(|| {
            serde_json::from_slice::<Value>(&paused.data)
                .ok()?
                .get("reason")?
                .as_str()
                .filter(|reason| !reason.is_empty())
                .map(str::to_string)
        })
}

fn pause_field(paused: &Record, key: &str) -> Option<String> {
    paused.metadata.get(key).cloned().or_else(|| {
        let data = serde_json::from_slice::<Value>(&paused.data).ok()?;
        match data.get(key)? {
            Value::String(value) => Some(value.clone()),
            Value::Null => None,
            other => Some(other.to_string()),
        }
    })
}

fn is_manual_suspension(paused: &Record) -> bool {
    paused
        .metadata
        .get(META_RESUME_SOURCE_ID)
        .is_some_and(|source| source.starts_with(MANUAL_SUSPEND_SOURCE_PREFIX))
}

/// Check that a resume request fits the run's latest pause.
///
/// - A pause that asked a question (wait-for-user or a guardrail review) takes
///   the resume. An unlabelled pause that carries a question is an older SDK's
///   wait-for-user and is accepted too.
/// - If the request names a `pause_index`, the pause must carry the same one.
/// - A manual suspension resumes without an answer, and refuses one.
/// - Anything else, such as a durable sleep or a signal wait, is refused: the
///   run continues on its own and an answer sent now would be kept for a
///   question nobody has been asked.
pub fn check_resume_target(paused: &Record, body: &Value) -> Result<(), ResumeRejection> {
    let answer = body.get("user_response").filter(|value| !value.is_null());

    if is_manual_suspension(paused) {
        return match answer {
            None => Ok(()),
            Some(_) => Err(ResumeRejection::new(
                "the run was suspended manually and isn't waiting for an answer; \
                 resume it without user_response",
                json!({ "pause_reason": pause_reason(paused).unwrap_or_else(|| "manual".to_string()) }),
            )),
        };
    }

    let reason = pause_reason(paused);
    let answerable = match reason.as_deref() {
        Some(reason) => ANSWERABLE_PAUSE_REASONS.contains(&reason),
        None => {
            !paused.metadata.contains_key("timer_id") && pause_field(paused, "question").is_some()
        }
    };
    if !answerable {
        let reason = reason.unwrap_or_else(|| "unknown".to_string());
        let error = match reason.as_str() {
            "timer" => "the run is paused on a durable sleep and continues when its timer \
                        fires; it isn't waiting for an answer"
                .to_string(),
            "signal" => "the run is waiting for a signal; send it with \
                         POST /v1/runs/{runId}/signals/{name}"
                .to_string(),
            other => format!("the run is paused for '{other}', not waiting for an answer"),
        };
        return Err(ResumeRejection::new(
            error,
            json!({ "pause_reason": reason }),
        ));
    }

    if let Some(requested) = body.get("approval_id").and_then(Value::as_str) {
        let waiting = pause_field(paused, "approval_id").unwrap_or_else(|| {
            format!(
                "{}:{}",
                paused.run_id,
                pause_field(paused, "pause_index").unwrap_or_else(|| paused.offset.0.to_string())
            )
        });
        if waiting != requested {
            return Err(ResumeRejection::new(
                "this approval is not the question the run is waiting for",
                json!({"approval_id":pause_field(paused, "approval_id")}),
            ));
        }
    }

    let requested = match body.get("pause_index") {
        None | Some(Value::Null) => return Ok(()),
        Some(Value::String(value)) => value.trim().to_string(),
        Some(other) => other.to_string(),
    };
    match pause_field(paused, "pause_index") {
        Some(waiting) if waiting.trim() == requested => Ok(()),
        Some(waiting) => Err(ResumeRejection::new(
            format!(
                "this answer is for pause {requested}, but the run is waiting at pause {}",
                waiting.trim()
            ),
            json!({ "pause_index": waiting.trim() }),
        )),
        // The request names a pause, and this one has no index to match it
        // against, so it can't be shown to be the same question.
        None => Err(ResumeRejection::new(
            format!(
                "this answer is for pause {requested}, but the pause the run is waiting at \
                 has no index; resume without pause_index to answer it"
            ),
            json!({ "pause_index": Value::Null }),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Offset;
    use bytes::Bytes;

    fn pause(data: Value, metadata: &[(&str, &str)]) -> Record {
        Record {
            offset: Offset::ZERO,
            project_id: "project-1".to_string(),
            run_id: "run-1".to_string(),
            event_type: "workflow.paused".to_string(),
            data: Bytes::from(data.to_string()),
            timestamp_ns: 0,
            step_key: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: metadata
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            data_type: "json".to_string(),
            data_checksum: None,
            data_compressed: false,
        }
    }

    // A durable sleep's pause: an answer sent now was kept and given to the
    // next wait-for-user, which completed without asking anyone.
    #[test]
    fn resume_is_refused_while_the_run_sleeps() {
        let sleeping = pause(
            json!({"status": "suspended", "reason": "timer", "timer_key": "sleep:cooldown"}),
            &[("timer_id", "timer-1"), ("timer_key", "sleep:cooldown")],
        );
        let rejection =
            check_resume_target(&sleeping, &json!({"user_response": "approve"})).unwrap_err();
        assert!(
            rejection.error.contains("durable sleep"),
            "{}",
            rejection.error
        );
        assert_eq!(rejection.detail["pause_reason"], "timer");
        let body = rejection.response_body("run-1");
        assert_eq!(body["run_id"], "run-1");
        assert_eq!(body["pause_reason"], "timer");

        // A sleep pause without a payload reason is still recognised by its timer.
        let bare_timer = pause(json!({}), &[("timer_id", "timer-1")]);
        assert!(check_resume_target(&bare_timer, &json!({"user_response": "x"})).is_err());
    }

    #[test]
    fn resume_is_refused_while_the_run_waits_for_a_signal() {
        let waiting = pause(
            json!({}),
            &[("pause_reason", "signal"), ("signal_name", "paid")],
        );
        let rejection = check_resume_target(&waiting, &json!({"user_response": "x"})).unwrap_err();
        assert!(rejection.error.contains("signal"), "{}", rejection.error);
    }

    #[test]
    fn resume_answers_a_question_or_a_guardrail_review() {
        for paused in [
            // Go, TypeScript and Python all label wait-for-user this way.
            pause(
                json!({"_paused": true, "question": "Approve?", "pause_index": "0"}),
                &[
                    ("pause_reason", "user_input_required"),
                    ("pause_index", "0"),
                ],
            ),
            // Workerless runs record the reason under both keys.
            pause(
                json!({"status": "suspended", "reason": "user_input_required"}),
                &[("workerless_suspend_reason", "user_input_required")],
            ),
            pause(
                json!({"reason": "guardrail_human_review", "question": "Allow?"}),
                &[
                    ("pause_reason", "guardrail_human_review"),
                    ("approval_id", "guardrail-1"),
                ],
            ),
            // An older SDK's unlabelled wait-for-user still carries its question.
            pause(json!({"_paused": true, "question": "Approve?"}), &[]),
        ] {
            assert!(check_resume_target(&paused, &json!({"user_response": "approve"})).is_ok());
        }
    }

    #[test]
    fn resume_is_refused_for_a_pause_with_no_question_or_known_reason() {
        let unknown = pause(json!({"status": "suspended"}), &[]);
        let rejection = check_resume_target(&unknown, &json!({"user_response": "x"})).unwrap_err();
        assert_eq!(rejection.detail["pause_reason"], "unknown");
        let budget = pause(json!({}), &[("workerless_suspend_reason", "budget")]);
        assert!(check_resume_target(&budget, &json!({"user_response": "x"})).is_err());
    }

    // An answer names the pause it is for; one sent for an earlier question
    // must not complete the current one.
    #[test]
    fn resume_must_match_the_pause_it_names() {
        let second_question = pause(
            json!({"question": "Release note?", "pause_index": 1}),
            &[
                ("pause_reason", "user_input_required"),
                ("pause_index", "1"),
            ],
        );
        for body in [
            json!({"user_response": "ship it", "pause_index": 1}),
            json!({"user_response": "ship it", "pause_index": "1"}),
            json!({"user_response": "ship it"}),
        ] {
            assert!(
                check_resume_target(&second_question, &body).is_ok(),
                "{body}"
            );
        }
        let rejection = check_resume_target(
            &second_question,
            &json!({"user_response": "approve", "pause_index": 0}),
        )
        .unwrap_err();
        assert!(
            rejection.error.contains("pause 0") && rejection.error.contains("pause 1"),
            "{}",
            rejection.error
        );
        assert_eq!(rejection.detail["pause_index"], "1");
    }

    // A request that names a pause can't be matched to one without an index,
    // such as a guardrail review, so it is refused rather than assumed.
    #[test]
    fn resume_that_names_a_pause_is_refused_when_the_pause_has_no_index() {
        let review = pause(
            json!({"reason": "guardrail_human_review", "question": "Allow?"}),
            &[("pause_reason", "guardrail_human_review")],
        );
        let rejection = check_resume_target(
            &review,
            &json!({"user_response": "approve", "pause_index": 0}),
        )
        .unwrap_err();
        assert!(
            rejection.error.contains("has no index"),
            "{}",
            rejection.error
        );
        assert!(check_resume_target(&review, &json!({"user_response": "approve"})).is_ok());
    }

    // The OSS server's manual suspension resumes without an answer; an answer
    // sent to it would be kept for a question nobody has been asked.
    #[test]
    fn manual_suspension_resumes_without_an_answer() {
        let suspended = pause(
            json!({"pause_reason": "manual"}),
            &[
                ("pause_reason", "manual"),
                (META_RESUME_SOURCE_ID, "manual-suspend:0190-abc"),
            ],
        );
        assert!(check_resume_target(&suspended, &json!({})).is_ok());
        assert!(check_resume_target(&suspended, &json!({"user_response": null})).is_ok());
        let rejection =
            check_resume_target(&suspended, &json!({"user_response": "approve"})).unwrap_err();
        assert!(
            rejection.error.contains("suspended manually"),
            "{}",
            rejection.error
        );
    }
}
