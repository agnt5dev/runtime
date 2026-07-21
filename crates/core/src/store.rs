use async_trait::async_trait;

use crate::{JournalError, Offset};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvOp {
    Put {
        namespace: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        namespace: String,
        key: Vec<u8>,
    },
}

#[async_trait]
pub trait MaterializedStore: Send + Sync + 'static {
    async fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, JournalError>;
    async fn write_batch(&self, operations: &[KvOp]) -> Result<(), JournalError>;
    async fn get_checkpoint(&self, processor: &str) -> Result<Option<Offset>, JournalError>;
    async fn set_checkpoint(&self, processor: &str, offset: Offset) -> Result<(), JournalError>;
}
