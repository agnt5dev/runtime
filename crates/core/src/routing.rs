/// Stable FNV-1a hash of a run ID to a journal partition.
///
/// Routing belongs in the protocol-neutral core so append clients, routers,
/// and replication handlers all validate against the same topology rule.
pub fn partition_for_run(run_id: &str, partition_count: usize) -> usize {
    if partition_count <= 1 {
        return 0;
    }
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in run_id.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) % partition_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_is_stable_and_single_partition_safe() {
        assert_eq!(partition_for_run("run-1", 4), partition_for_run("run-1", 4));
        assert_eq!(partition_for_run("run-1", 0), 0);
        assert_eq!(partition_for_run("run-1", 1), 0);
    }
}
