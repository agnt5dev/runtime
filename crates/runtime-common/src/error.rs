/// Errors that can occur in the dataplane.
#[derive(Debug, thiserror::Error)]
pub enum DataplaneError {
    #[error("engine error: {0}")]
    Engine(#[from] engine_core::error::EngineError),

    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("internal error: {0}")]
    Internal(String),
}

pub type DataplaneResult<T> = Result<T, DataplaneError>;

impl From<DataplaneError> for tonic::Status {
    fn from(e: DataplaneError) -> Self {
        match e {
            DataplaneError::Engine(engine_core::error::EngineError::InvalidArgument(msg)) => {
                tonic::Status::invalid_argument(msg)
            }
            DataplaneError::Engine(engine_core::error::EngineError::InvocationConflict {
                run_id,
            }) => tonic::Status::already_exists(format!(
                "invocation idempotency key conflicts with the accepted run {run_id}"
            )),
            DataplaneError::Engine(
                e @ engine_core::error::EngineError::LeaseFenceMismatch { .. },
            ) => tonic::Status::failed_precondition(e.to_string()),
            DataplaneError::Engine(e) => tonic::Status::internal(e.to_string()),
            DataplaneError::Grpc(s) => s,
            DataplaneError::Internal(msg) => tonic::Status::internal(msg),
        }
    }
}
