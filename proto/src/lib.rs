//! Generated Rust bindings for the public AGNT5 SDK/runtime protocol.
//!
//! The default feature set exposes only [`protocol::v2`]. Community-runtime
//! and transition-only APIs require explicit features so SDKs cannot acquire
//! those implementation contracts accidentally.

#[cfg(feature = "legacy-api")]
pub mod api {
    pub mod v1 {
        tonic::include_proto!("api.v1");
    }
}

pub mod protocol {
    pub mod v2 {
        tonic::include_proto!("agnt5.protocol.v2");
    }
}

#[cfg(feature = "runtime-api")]
pub mod runtime {
    pub mod v1 {
        tonic::include_proto!("agnt5.runtime.v1");
    }
}

#[cfg(test)]
mod tests {
    use super::protocol::v2::{
        payload_service_client::PayloadServiceClient, worker_service_client::WorkerServiceClient,
        ComponentDescriptor, MethodDescriptor,
    };

    #[test]
    fn default_artifact_exports_public_descriptors_and_clients() {
        let descriptor = ComponentDescriptor {
            name: "cart".into(),
            version: "v1".into(),
            methods: vec![MethodDescriptor {
                name: "add_item".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(descriptor.methods[0].name, "add_item");

        let _: Option<WorkerServiceClient<tonic::transport::Channel>> = None;
        let _: Option<PayloadServiceClient<tonic::transport::Channel>> = None;
    }
}
