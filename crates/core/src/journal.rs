use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(pub u64);

impl Offset {
    pub const ZERO: Self = Self(0);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewJournalRecord {
    pub idempotency_key: Option<Vec<u8>>,
    pub payload: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalRecord {
    pub partition_id: u32,
    pub offset: Offset,
    pub idempotency_key: Option<Vec<u8>>,
    pub payload: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendOutcome {
    pub offsets: Vec<Offset>,
    pub appended: Vec<bool>,
}

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("segment is sealed")]
    Sealed,
    #[error("offset {0} is outside the retained journal range")]
    OffsetOutOfRange(u64),
    #[error("storage error: {0}")]
    Storage(String),
}

pub type RecordStream =
    Pin<Box<dyn Stream<Item = Result<JournalRecord, JournalError>> + Send + 'static>>;

#[async_trait]
pub trait Segment: Send + Sync + 'static {
    async fn append_batch(
        &self,
        records: &[NewJournalRecord],
    ) -> Result<AppendOutcome, JournalError>;

    async fn tail(&self, from: Offset) -> Result<RecordStream, JournalError>;

    async fn read_range(
        &self,
        from: Offset,
        to: Offset,
    ) -> Result<Vec<JournalRecord>, JournalError>;

    async fn tail_offset(&self) -> Result<Offset, JournalError>;
    async fn seal(&self) -> Result<(), JournalError>;
    async fn trim(&self, through: Offset) -> Result<(), JournalError>;
}
