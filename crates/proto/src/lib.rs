/// Generated protobuf types for the AGNT5 Engine.
///
/// Contains the `EngineService` and `ExecutionEngineService` gRPC definitions,
/// along with core message types: `Record`, `Run`, and all RPC request/response
/// types. The `managed` feature adds the hosted cluster protocols: coordinator
/// gossip, runtime dispatch and `ReplicationService`.
pub mod api {
    pub mod v1 {
        tonic::include_proto!("api.v1");
    }
}

pub mod buckets;
pub mod labels;
pub mod triggers;

/// Encoded descriptors for gRPC reflection in runtime profiles.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("engine_descriptor");

/// Whether this build compiled the hosted cluster protocols. They are not
/// part of this repository, so this is always false.
pub const HOSTED_PROTOS: bool = false;

// Re-export at crate root for convenience.
pub use api::v1::*;

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{JobAssignment, PollJobResponse, SlotScalingDecision, SlotScalingHint};

    #[derive(Clone, PartialEq, Message)]
    struct LegacyPollJobResponse {
        #[prost(message, optional, tag = "1")]
        job: Option<JobAssignment>,
    }

    #[test]
    fn slot_scaling_is_additive_on_the_poll_response_wire() {
        let legacy_bytes = LegacyPollJobResponse { job: None }.encode_to_vec();
        let decoded_new = PollJobResponse::decode(legacy_bytes.as_slice()).unwrap();
        assert!(decoded_new.job.is_none());
        assert!(decoded_new.slot_scaling.is_none());

        let new_bytes = PollJobResponse {
            job: None,
            slot_scaling: Some(SlotScalingHint {
                decision: SlotScalingDecision::ScaleUp as i32,
                suggested_delta: 3,
            }),
        }
        .encode_to_vec();
        let decoded_legacy = LegacyPollJobResponse::decode(new_bytes.as_slice()).unwrap();
        assert!(decoded_legacy.job.is_none());
    }
}
