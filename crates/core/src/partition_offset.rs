//! Stable encoding for partition-aware public journal offsets.
//!
//! The encoding belongs to the shared runtime contract rather than any
//! physical segment adapter. Managed routing and the PostgreSQL profile
//! therefore interpret offsets identically without depending on RocksDB.

use crate::error::{EngineError, EngineResult};
use crate::types::Offset;

/// Partition ID encoded in the high 16 bits of the offset.
const PARTITION_SHIFT: u32 = 48;
pub const MAX_PARTITION_COUNT: usize = 1usize << 16;
pub const MAX_LOCAL_OFFSET: u64 = (1u64 << PARTITION_SHIFT) - 1;

/// Encode a partition-aware offset: `(partition_id << 48) | local_offset`.
pub fn encode_offset(partition: usize, local: u64) -> EngineResult<Offset> {
    if partition >= MAX_PARTITION_COUNT {
        return Err(EngineError::InvalidArgument(format!(
            "partition {partition} exceeds the 16-bit encoded offset domain"
        )));
    }
    if local > MAX_LOCAL_OFFSET {
        return Err(EngineError::InvalidArgument(format!(
            "local offset {local} exceeds the 48-bit encoded offset domain"
        )));
    }
    Ok(Offset(((partition as u64) << PARTITION_SHIFT) | local))
}

/// Decode partition ID and local offset from a partition-aware offset.
pub fn decode_offset(offset: Offset) -> (usize, u64) {
    let partition = (offset.0 >> PARTITION_SHIFT) as usize;
    let local = offset.0 & MAX_LOCAL_OFFSET;
    (partition, local)
}

/// Decode an offset and require its partition to exist in this topology.
pub fn decode_offset_for_partition_count(
    offset: Offset,
    partition_count: usize,
) -> EngineResult<(usize, u64)> {
    if !(1..=MAX_PARTITION_COUNT).contains(&partition_count) {
        return Err(EngineError::InvalidArgument(format!(
            "partition_count {partition_count} is outside 1..={MAX_PARTITION_COUNT}"
        )));
    }
    let (partition, local) = decode_offset(offset);
    if partition >= partition_count {
        return Err(EngineError::InvalidArgument(format!(
            "offset {} encodes partition {partition}, but topology has {partition_count} partitions",
            offset.0
        )));
    }
    Ok((partition, local))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_partition_offset() {
        let encoded = encode_offset(42, 99).unwrap();
        assert_eq!(decode_offset(encoded), (42, 99));
        assert_eq!(
            decode_offset_for_partition_count(encoded, 43).unwrap(),
            (42, 99)
        );
    }

    #[test]
    fn rejects_out_of_domain_values() {
        assert!(encode_offset(MAX_PARTITION_COUNT, 0).is_err());
        assert!(encode_offset(0, MAX_LOCAL_OFFSET + 1).is_err());
        assert!(decode_offset_for_partition_count(Offset::ZERO, 0).is_err());
    }
}
