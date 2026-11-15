//! The durable per-partition segment chain.
//!
//! `SegmentConfig` in `engine-replication` already describes a complete chain
//! entry — partition, segment id, epoch, sequencer, replica set, start offset,
//! sealed flag — and its doc comment says it is "Stored in the metadata store
//! (Raft) and propagated to all nodes". There is no such store. The struct is
//! built in memory when an election resolves and is gone when the process is.
//!
//! Decision D2 makes the chain durable, so leadership is something a booting
//! node *reads* rather than something three nodes have to re-derive by running
//! an election against each other. This module is the representation and its
//! validation only. Nothing here writes: BOOT-601 is deliberately read-only so
//! the chain a running cluster would have written can be compared against what
//! its elections actually produced, before anything depends on it.

use serde::{Deserialize, Serialize};

use crate::error::{EngineError, EngineResult};

/// On-disk format for a segment chain object.
pub const SEGMENT_CHAIN_FORMAT_VERSION: u32 = 1;

/// One configuration of one partition: who sequences it, who replicates it, and
/// the offset range it covers.
///
/// An entry is immutable once sealed. The tail entry is the live configuration
/// and is the only one whose `sealed_through` is `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentChainEntry {
    /// Monotonic per partition. One entry per reconfiguration, which is what
    /// makes epoch growth countable instead of emergent.
    pub epoch: u64,
    pub segment_id: u64,
    /// The node that may sequence writes for this entry's range.
    pub sequencer_node: String,
    /// Every node that replicates it, including the sequencer.
    pub replica_nodes: Vec<String>,
    /// First offset this entry covers.
    pub start_offset: u64,
    /// Exclusive offset it was sealed at. `None` marks the live tail.
    ///
    /// A sealed boundary is what lets a superseded node step down at an exact
    /// offset rather than at whatever it had reached when it noticed.
    pub sealed_through: Option<u64>,
}

impl SegmentChainEntry {
    /// Exclusive end of the range this entry covers, if it is sealed.
    pub fn end_offset_exclusive(&self) -> Option<u64> {
        self.sealed_through
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed_through.is_some()
    }
}

/// The ordered chain for one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentChain {
    pub format_version: u32,
    pub partition_id: u32,
    /// Ordered oldest first. Only the last entry may be unsealed.
    pub entries: Vec<SegmentChainEntry>,
}

impl SegmentChain {
    pub fn new(partition_id: u32, entries: Vec<SegmentChainEntry>) -> Self {
        Self {
            format_version: SEGMENT_CHAIN_FORMAT_VERSION,
            partition_id,
            entries,
        }
    }

    /// The live configuration: the unsealed tail.
    pub fn active(&self) -> Option<&SegmentChainEntry> {
        self.entries.last().filter(|entry| !entry.is_sealed())
    }

    /// The entry covering `offset`, if the chain covers it.
    pub fn entry_for_offset(&self, offset: u64) -> Option<&SegmentChainEntry> {
        self.entries.iter().find(|entry| {
            offset >= entry.start_offset
                && entry.sealed_through.is_none_or(|through| offset < through)
        })
    }

    /// The highest epoch the chain records, or zero for an empty chain.
    pub fn tail_epoch(&self) -> u64 {
        self.entries.last().map_or(0, |entry| entry.epoch)
    }

    /// Check every invariant a reader depends on.
    ///
    /// Validation is deliberately total rather than incremental: a chain is
    /// small — one entry per reconfiguration, not per record — so there is no
    /// reason to trust any part of it that has not been checked.
    pub fn validate(&self) -> EngineResult<()> {
        if self.format_version != SEGMENT_CHAIN_FORMAT_VERSION {
            return Err(EngineError::Storage(format!(
                "segment chain for partition {} has unsupported format version {}",
                self.partition_id, self.format_version
            )));
        }
        let mut previous: Option<&SegmentChainEntry> = None;
        for (index, entry) in self.entries.iter().enumerate() {
            if entry.sequencer_node.is_empty() {
                return Err(EngineError::Storage(format!(
                    "segment chain entry {index} for partition {} names no sequencer",
                    self.partition_id
                )));
            }
            if !entry.replica_nodes.contains(&entry.sequencer_node) {
                return Err(EngineError::Storage(format!(
                    "segment chain entry {index} for partition {} sequences on {} which is not in its replica set",
                    self.partition_id, entry.sequencer_node
                )));
            }
            if let Some(through) = entry.sealed_through {
                if through < entry.start_offset {
                    return Err(EngineError::Storage(format!(
                        "segment chain entry {index} for partition {} seals at {through}, below its start {}",
                        self.partition_id, entry.start_offset
                    )));
                }
            }
            if let Some(previous) = previous {
                // Only the tail may be open. An unsealed entry in the middle
                // means two configurations both believe they are live.
                let Some(previous_through) = previous.sealed_through else {
                    return Err(EngineError::Storage(format!(
                        "segment chain entry {index} for partition {} follows an unsealed entry",
                        self.partition_id
                    )));
                };
                if entry.epoch <= previous.epoch {
                    return Err(EngineError::Storage(format!(
                        "segment chain epoch does not advance for partition {}: {} then {}",
                        self.partition_id, previous.epoch, entry.epoch
                    )));
                }
                // Contiguity is the property that makes an offset resolve to
                // exactly one entry. A gap loses records; an overlap makes two
                // entries claim the same ones.
                if entry.start_offset != previous_through {
                    return Err(EngineError::Storage(format!(
                        "segment chain for partition {} is not contiguous: entry {index} starts at {} after a seal at {previous_through}",
                        self.partition_id, entry.start_offset
                    )));
                }
            }
            previous = Some(entry);
        }
        Ok(())
    }
}

/// What one election produced, in the terms the chain records.
///
/// This is the shape of `engine_replication::SegmentConfig` without the
/// dependency: `engine-core` sits below replication, and the chain has to be
/// describable without it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectedConfiguration {
    pub partition_id: u32,
    pub segment_id: u64,
    pub epoch: u64,
    pub sequencer_node: String,
    pub replica_nodes: Vec<String>,
    pub start_offset: u64,
    pub sealed: bool,
}

/// Derive the chain a cluster would have written from the configurations its
/// elections actually produced.
///
/// This is the whole point of shipping the reader before the writer: run it
/// over a recorded election sequence and the result either matches what the
/// cluster did, or it names the step where the chain model and the election
/// disagree — while nothing yet depends on the answer.
///
/// `sealed_at` gives the offset each superseded configuration was sealed at.
/// The election does not record one, which is precisely the gap BOOT-603
/// closes; supplying it here keeps the derivation honest about needing it.
pub fn derive_chain(
    partition_id: u32,
    elected: &[ElectedConfiguration],
    sealed_at: &[u64],
) -> EngineResult<SegmentChain> {
    if elected
        .iter()
        .any(|config| config.partition_id != partition_id)
    {
        return Err(EngineError::InvalidArgument(format!(
            "elected configuration for another partition cannot join partition {partition_id}'s chain"
        )));
    }
    if !elected.is_empty() && sealed_at.len() + 1 < elected.len() {
        return Err(EngineError::InvalidArgument(format!(
            "deriving partition {partition_id}'s chain needs a seal offset for each superseded configuration: {} configurations, {} seals",
            elected.len(),
            sealed_at.len()
        )));
    }
    let mut entries = Vec::with_capacity(elected.len());
    for (index, config) in elected.iter().enumerate() {
        let sealed_through = if index + 1 < elected.len() {
            Some(sealed_at[index])
        } else {
            // The last configuration is live unless the election sealed it.
            config.sealed.then_some(config.start_offset)
        };
        entries.push(SegmentChainEntry {
            epoch: config.epoch,
            segment_id: config.segment_id,
            sequencer_node: config.sequencer_node.clone(),
            replica_nodes: config.replica_nodes.clone(),
            start_offset: config.start_offset,
            sealed_through,
        });
    }
    let chain = SegmentChain::new(partition_id, entries);
    chain.validate()?;
    Ok(chain)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(epoch: u64, start: u64, sealed_through: Option<u64>) -> SegmentChainEntry {
        SegmentChainEntry {
            epoch,
            segment_id: epoch,
            sequencer_node: "node-1".to_string(),
            replica_nodes: vec!["node-1".to_string(), "node-2".to_string()],
            start_offset: start,
            sealed_through,
        }
    }

    fn elected(epoch: u64, start: u64, sequencer: &str, sealed: bool) -> ElectedConfiguration {
        ElectedConfiguration {
            partition_id: 0,
            segment_id: epoch,
            epoch,
            sequencer_node: sequencer.to_string(),
            replica_nodes: vec![
                "node-1".to_string(),
                "node-2".to_string(),
                "node-3".to_string(),
            ],
            start_offset: start,
            sealed,
        }
    }

    #[test]
    fn a_chain_resolves_every_offset_to_exactly_one_entry() {
        let chain = SegmentChain::new(
            0,
            vec![
                entry(1, 0, Some(100)),
                entry(2, 100, Some(250)),
                entry(3, 250, None),
            ],
        );
        chain.validate().unwrap();

        assert_eq!(chain.entry_for_offset(0).unwrap().epoch, 1);
        assert_eq!(chain.entry_for_offset(99).unwrap().epoch, 1);
        // The seal boundary is exclusive, so it belongs to the next entry.
        assert_eq!(chain.entry_for_offset(100).unwrap().epoch, 2);
        assert_eq!(chain.entry_for_offset(249).unwrap().epoch, 2);
        // The live tail is open-ended.
        assert_eq!(chain.entry_for_offset(1_000_000).unwrap().epoch, 3);
        assert_eq!(chain.active().unwrap().epoch, 3);
        assert_eq!(chain.tail_epoch(), 3);
    }

    #[test]
    fn a_sealed_tail_has_no_live_configuration() {
        let chain = SegmentChain::new(0, vec![entry(1, 0, Some(10))]);
        chain.validate().unwrap();
        assert!(chain.active().is_none());
        assert!(chain.entry_for_offset(10).is_none());
    }

    #[test]
    fn a_gap_or_overlap_between_entries_is_rejected() {
        let gap = SegmentChain::new(0, vec![entry(1, 0, Some(100)), entry(2, 150, None)]);
        assert!(gap
            .validate()
            .unwrap_err()
            .to_string()
            .contains("not contiguous"));

        let overlap = SegmentChain::new(0, vec![entry(1, 0, Some(100)), entry(2, 50, None)]);
        assert!(overlap
            .validate()
            .unwrap_err()
            .to_string()
            .contains("not contiguous"));
    }

    #[test]
    fn only_the_tail_may_be_unsealed() {
        // Two live configurations is the split-brain the chain exists to make
        // unrepresentable.
        let chain = SegmentChain::new(0, vec![entry(1, 0, None), entry(2, 0, None)]);
        assert!(chain
            .validate()
            .unwrap_err()
            .to_string()
            .contains("follows an unsealed entry"));
    }

    #[test]
    fn epochs_must_advance_and_a_sequencer_must_replicate_what_it_sequences() {
        let stale = SegmentChain::new(0, vec![entry(5, 0, Some(10)), entry(5, 10, None)]);
        assert!(stale
            .validate()
            .unwrap_err()
            .to_string()
            .contains("epoch does not advance"));

        let mut outsider = entry(1, 0, None);
        outsider.sequencer_node = "node-9".to_string();
        let chain = SegmentChain::new(0, vec![outsider]);
        assert!(chain
            .validate()
            .unwrap_err()
            .to_string()
            .contains("not in its replica set"));
    }

    #[test]
    fn a_derived_chain_matches_the_configuration_elected_at_every_step() {
        // The recorded sequence: three successive elections, each superseding
        // the last, with the offsets the previous leader was sealed at.
        let sequence = [
            elected(1, 0, "node-1", false),
            elected(2, 100, "node-2", false),
            elected(3, 250, "node-3", false),
        ];
        let chain = derive_chain(0, &sequence, &[100, 250]).unwrap();

        assert_eq!(chain.entries.len(), sequence.len());
        for (entry, config) in chain.entries.iter().zip(&sequence) {
            assert_eq!(entry.epoch, config.epoch);
            assert_eq!(entry.segment_id, config.segment_id);
            assert_eq!(entry.sequencer_node, config.sequencer_node);
            assert_eq!(entry.replica_nodes, config.replica_nodes);
            assert_eq!(entry.start_offset, config.start_offset);
        }
        // At every step the chain names the leader that election chose.
        assert_eq!(chain.entry_for_offset(0).unwrap().sequencer_node, "node-1");
        assert_eq!(
            chain.entry_for_offset(100).unwrap().sequencer_node,
            "node-2"
        );
        assert_eq!(
            chain.entry_for_offset(250).unwrap().sequencer_node,
            "node-3"
        );
        assert_eq!(chain.active().unwrap().sequencer_node, "node-3");
    }

    #[test]
    fn deriving_without_a_seal_offset_for_a_superseded_leader_is_refused() {
        // The election records no seal offset, which is the gap BOOT-603
        // closes. Refusing here keeps the derivation honest about needing one
        // rather than inventing a boundary.
        let sequence = [
            elected(1, 0, "node-1", false),
            elected(2, 100, "node-2", false),
        ];
        let error = derive_chain(0, &sequence, &[]).unwrap_err();
        assert!(error.to_string().contains("needs a seal offset"));
    }

    #[test]
    fn a_derived_chain_is_validated_not_merely_assembled() {
        // Elections that leave a gap must not produce a chain that reads as if
        // the missing range never existed.
        let sequence = [
            elected(1, 0, "node-1", false),
            elected(2, 500, "node-2", false),
        ];
        assert!(derive_chain(0, &sequence, &[100])
            .unwrap_err()
            .to_string()
            .contains("not contiguous"));
    }

    #[test]
    fn a_configuration_from_another_partition_cannot_join_the_chain() {
        let mut foreign = elected(1, 0, "node-1", false);
        foreign.partition_id = 7;
        assert!(derive_chain(0, &[foreign], &[])
            .unwrap_err()
            .to_string()
            .contains("another partition"));
    }

    #[test]
    fn a_chain_round_trips_through_its_on_disk_form() {
        let chain = SegmentChain::new(3, vec![entry(1, 0, Some(10)), entry(2, 10, None)]);
        let encoded = serde_json::to_vec(&chain).unwrap();
        let decoded: SegmentChain = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, chain);
        decoded.validate().unwrap();
    }
}
