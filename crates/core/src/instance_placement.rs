//! Where an always-on agent instance's records live, and how its run ids are
//! built.
//!
//! Every journal record is stored on the partition its `run_id` hashes to
//! (`routing::partition_for_run`). An instance needs its inbox and every run it
//! starts on one partition, so that one leader sees and orders all of them. We
//! keep the routing rule as it is and instead choose ids that hash to the
//! instance's partition, by trying salts in a fixed order, the way
//! `runtime_events::configuration_run_id` does.
//!
//! The instance's partition comes from a keyed hash of its project, environment,
//! agent and inbox key under a per-cluster secret, so a sender who controls the
//! inbox key can't pile instances onto one partition. The generation is left out
//! of that hash: the gateway must route an item before it can know the
//! generation, which lives on the partition it's looking for.
//!
//! Instance run ids are UUIDv8 with a fixed marker in their custom bits, so the
//! journal's append guard can recognise an instance-owned run from the record
//! alone, and the control plane can still parse the id as a UUID. These formats
//! are journal data: once an instance exists they can't change, which is why
//! the run id carries a format byte.

use crate::{
    error::{EngineError, EngineResult},
    routing::partition_for_run,
};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// The first four bytes of every instance run id: ASCII `agn5`, so the ids read
/// as `61676e35-…` in logs.
pub const INSTANCE_RUN_MARKER: [u8; 4] = *b"agn5";

/// The fifth byte of an instance run id. A later format bumps it; the append
/// guard recognises every format by the marker alone.
pub const INSTANCE_RUN_FORMAT_V1: u8 = 1;

/// Inbox keys come from outside (a user id, a customer id, an email), and they
/// are part of every inbox record's `run_id`, so they are bounded.
pub const MAX_INBOX_KEY_BYTES: usize = 256;

/// Project, environment and agent names are ours, but they are bounded too,
/// for the same reason.
pub const MAX_INSTANCE_NAME_BYTES: usize = 256;

/// The placement secret keys the partition hash. Shorter secrets are refused.
pub const MIN_PLACEMENT_SECRET_BYTES: usize = 32;

/// The same topology bound `configuration_run_id` uses. The salt search tries
/// up to `count * 256` salts; a miss at that bound is about e^-256 likely.
const MAX_PARTITIONS: usize = 256;
const SALTS_PER_PARTITION: usize = 256;

const INBOX_PREFIX: &str = "inbox";
const RUN_ID_DOMAIN: &str = "agnt5.instance-run.v1";
const PARTITION_DOMAIN: &str = "agnt5.instance-partition.v1";

/// Who an instance is: one per (project, environment, agent, inbox key), and a
/// generation that counts up each time an instance for that key is deleted and
/// created again, so nothing from a deleted instance matches a new one.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InstanceIdentity {
    project_id: String,
    environment_id: String,
    agent: String,
    inbox_key: String,
    generation: u64,
}

impl InstanceIdentity {
    /// Project, environment and agent may not contain `:`, which keeps the
    /// inbox run id unambiguous: those three are read from the left, the
    /// generation and salt are the last two `::` fields, and the inbox key,
    /// which may contain anything printable, is what's left between them.
    pub fn new(
        project_id: impl Into<String>,
        environment_id: impl Into<String>,
        agent: impl Into<String>,
        inbox_key: impl Into<String>,
        generation: u64,
    ) -> EngineResult<Self> {
        let identity = Self {
            project_id: project_id.into(),
            environment_id: environment_id.into(),
            agent: agent.into(),
            inbox_key: inbox_key.into(),
            generation,
        };
        validate_name("project id", &identity.project_id)?;
        validate_name("environment id", &identity.environment_id)?;
        validate_name("agent", &identity.agent)?;
        validate_inbox_key(&identity.inbox_key)?;
        Ok(identity)
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn environment_id(&self) -> &str {
        &self.environment_id
    }

    pub fn agent(&self) -> &str {
        &self.agent
    }

    pub fn inbox_key(&self) -> &str {
        &self.inbox_key
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The key the instance's record is stored under, which leaves out the
    /// generation: there is one live record per key.
    pub fn instance_key(&self) -> crate::instance::InstanceKey {
        crate::instance::InstanceKey {
            project_id: self.project_id.clone(),
            environment_id: self.environment_id.clone(),
            agent_name: self.agent.clone(),
            inbox_key: self.inbox_key.clone(),
        }
    }
}

/// The partition an instance lives on, under this cluster's placement secret.
///
/// The secret must not change for the life of the cluster: a new secret moves
/// every instance, the same way a new partition count does.
pub fn instance_partition(
    secret: &[u8],
    identity: &InstanceIdentity,
    count: usize,
) -> EngineResult<u32> {
    if secret.len() < MIN_PLACEMENT_SECRET_BYTES {
        return Err(invalid(&format!(
            "instance placement secret must be at least {MIN_PLACEMENT_SECRET_BYTES} bytes"
        )));
    }
    validate_topology(0, count)?;
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|_| invalid("invalid instance placement secret"))?;
    mac.update(&length_prefixed(&[
        PARTITION_DOMAIN.as_bytes(),
        identity.project_id.as_bytes(),
        identity.environment_id.as_bytes(),
        identity.agent.as_bytes(),
        identity.inbox_key.as_bytes(),
    ]));
    let digest = mac.finalize().into_bytes();
    let mut head = [0u8; 8];
    head.copy_from_slice(&digest[..8]);
    // Modulo bias over a 64-bit value is negligible for any partition count we
    // support.
    Ok((u64::from_be_bytes(head) % count as u64) as u32)
}

/// The made-up `run_id` that carries the instance's inbox records:
/// `inbox::{project}::{environment}::{agent}::{inbox key}::{generation}::{salt}`.
///
/// The salt is the first, in a fixed order, that lands the id on `partition`,
/// so anyone who knows the instance and its partition derives the same id.
pub fn inbox_run_id(
    identity: &InstanceIdentity,
    partition: u32,
    count: usize,
) -> EngineResult<String> {
    validate_topology(partition, count)?;
    let base = format!(
        "{INBOX_PREFIX}::{}::{}::{}::{}::{}",
        identity.project_id,
        identity.environment_id,
        identity.agent,
        identity.inbox_key,
        identity.generation
    );
    for salt in 0..salt_bound(count) {
        let candidate = format!("{base}::{salt}");
        if partition_for_run(&candidate, count) == partition as usize {
            return Ok(candidate);
        }
    }
    Err(invalid(
        "could not place the instance inbox on its partition",
    ))
}

/// The id of the instance's run with this sequence number, on `partition`.
///
/// It is deterministic, so two attempts to start the same run (a supervisor
/// retrying after failover, or two triggers deciding at once) produce the same
/// id and the journal keeps the first. A start the projection rejects on apply
/// uses up its sequence number; the next decision starts the next sequence.
///
/// The ids are ordered by nothing: unlike the UUIDv7 ids chat and MCP runs get,
/// they carry no time.
pub fn instance_run_id(
    identity: &InstanceIdentity,
    sequence: u64,
    partition: u32,
    count: usize,
) -> EngineResult<String> {
    validate_topology(partition, count)?;
    let generation = identity.generation.to_be_bytes();
    let sequence = sequence.to_be_bytes();
    for salt in 0..salt_bound(count) {
        let salt = (salt as u64).to_be_bytes();
        let digest = Sha256::digest(length_prefixed(&[
            RUN_ID_DOMAIN.as_bytes(),
            identity.project_id.as_bytes(),
            identity.environment_id.as_bytes(),
            identity.agent.as_bytes(),
            identity.inbox_key.as_bytes(),
            &generation,
            &sequence,
            &salt,
        ]));
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        bytes[..4].copy_from_slice(&INSTANCE_RUN_MARKER);
        bytes[4] = INSTANCE_RUN_FORMAT_V1;
        let candidate = uuid::Builder::from_custom_bytes(bytes)
            .into_uuid()
            .hyphenated()
            .to_string();
        if partition_for_run(&candidate, count) == partition as usize {
            return Ok(candidate);
        }
    }
    Err(invalid("could not place the instance run on its partition"))
}

/// Whether `run_id` names an instance-owned run, judged from the id alone.
///
/// It accepts any textual form of the UUID, not only the lowercase hyphenated
/// one the supervisor writes, so a start under a re-cased or re-formatted id is
/// still treated as instance-owned and refused by the append guard rather than
/// slipping past it.
pub fn is_instance_run_id(run_id: &str) -> bool {
    let Ok(uuid) = uuid::Uuid::try_parse(run_id) else {
        return false;
    };
    uuid.get_version_num() == 8
        && uuid.get_variant() == uuid::Variant::RFC4122
        && uuid.as_bytes()[..4] == INSTANCE_RUN_MARKER
}

/// The instance an inbox record belongs to, read back from its `run_id`.
///
/// Project, environment and agent are the first three `::` fields, since none
/// of them may contain `:`. The generation and the salt are the last two, both
/// decimal. The inbox key is everything between. Only the canonical spelling
/// is accepted, so one instance never has two inbox ids.
pub fn parse_inbox_run_id(run_id: &str) -> Option<InstanceIdentity> {
    let rest = run_id.strip_prefix(INBOX_PREFIX)?.strip_prefix("::")?;
    let mut head = rest.splitn(4, "::");
    let (project_id, environment_id, agent, tail) =
        (head.next()?, head.next()?, head.next()?, head.next()?);
    let mut tail = tail.rsplitn(3, "::");
    let (salt, generation, inbox_key) = (tail.next()?, tail.next()?, tail.next()?);
    let canonical = |text: &str| {
        text.parse::<u64>()
            .ok()
            .filter(|value| value.to_string() == text)
    };
    canonical(salt)?;
    let generation = canonical(generation)?;
    InstanceIdentity::new(project_id, environment_id, agent, inbox_key, generation).ok()
}

/// The id of the item a source delivers to an inbox. It is derived, so a
/// retried delivery gets the same id and the caller's answer doesn't change
/// when the journal absorbs the duplicate.
pub fn inbox_item_id(inbox_run_id: &str, source_id: &str) -> String {
    crate::runtime_work::RuntimeWork::stable_id(&["inbox-item", inbox_run_id, source_id])
}

fn validate_name(what: &str, value: &str) -> EngineResult<()> {
    if value.is_empty() {
        return Err(invalid(&format!("instance {what} must not be empty")));
    }
    if value.len() > MAX_INSTANCE_NAME_BYTES {
        return Err(invalid(&format!(
            "instance {what} exceeds {MAX_INSTANCE_NAME_BYTES} bytes"
        )));
    }
    if value.contains(':') || value.chars().any(char::is_control) {
        return Err(invalid(&format!(
            "instance {what} must not contain ':' or control characters"
        )));
    }
    Ok(())
}

fn validate_inbox_key(value: &str) -> EngineResult<()> {
    if value.is_empty() {
        return Err(invalid("inbox key must not be empty"));
    }
    if value.len() > MAX_INBOX_KEY_BYTES {
        return Err(invalid(&format!(
            "inbox key exceeds {MAX_INBOX_KEY_BYTES} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid("inbox key must not contain control characters"));
    }
    Ok(())
}

fn validate_topology(partition: u32, count: usize) -> EngineResult<()> {
    if count == 0 || count > MAX_PARTITIONS || partition as usize >= count {
        return Err(invalid("unsupported instance partition topology"));
    }
    Ok(())
}

fn salt_bound(count: usize) -> usize {
    count * SALTS_PER_PARTITION
}

/// Length-prefixing keeps field boundaries in the hash input, so ("ab", "c")
/// and ("a", "bc") hash differently.
fn length_prefixed(parts: &[&[u8]]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for part in parts {
        bytes.extend_from_slice(&(part.len() as u64).to_be_bytes());
        bytes.extend_from_slice(part);
    }
    bytes
}

fn invalid(message: &str) -> EngineError {
    EngineError::InvalidArgument(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

    fn identity(key: &str, generation: u64) -> InstanceIdentity {
        InstanceIdentity::new("proj-1", "env-prod", "triage", key, generation).unwrap()
    }

    #[test]
    fn identity_refuses_ambiguous_or_unbounded_parts() {
        let ok = |p: &str, e: &str, a: &str, k: &str| InstanceIdentity::new(p, e, a, k, 1);
        assert!(ok("", "e", "a", "k").is_err());
        assert!(ok("p", "", "a", "k").is_err());
        assert!(ok("p", "e", "", "k").is_err());
        assert!(ok("p", "e", "a", "").is_err());
        assert!(ok("p:x", "e", "a", "k").is_err());
        assert!(ok("p", "e:x", "a", "k").is_err());
        assert!(ok("p", "e", "a:x", "k").is_err());
        assert!(ok("p", "e", "a", "k\n").is_err());
        assert!(ok("p", "e", "a", &"k".repeat(MAX_INBOX_KEY_BYTES + 1)).is_err());
        assert!(ok("p", "e", &"a".repeat(MAX_INSTANCE_NAME_BYTES + 1), "k").is_err());
        // Inbox keys may contain separators; they come from outside systems.
        assert!(ok("p", "e", "a", "user::42:x").is_ok());
        assert!(ok("p", "e", "a", &"k".repeat(MAX_INBOX_KEY_BYTES)).is_ok());
    }

    #[test]
    fn partition_is_keyed_stable_and_ignores_generation() {
        let a = instance_partition(SECRET, &identity("U123", 1), 16).unwrap();
        assert_eq!(
            a,
            instance_partition(SECRET, &identity("U123", 1), 16).unwrap()
        );
        assert_eq!(
            a,
            instance_partition(SECRET, &identity("U123", 7), 16).unwrap()
        );
        assert!(a < 16);
        assert_eq!(
            instance_partition(SECRET, &identity("U123", 1), 1).unwrap(),
            0
        );

        // A different secret places keys differently: over 64 keys, at least one moves.
        let other = b"fedcba9876543210fedcba9876543210";
        let moved = (0..64).any(|i| {
            let id = identity(&format!("user-{i}"), 1);
            instance_partition(SECRET, &id, 16).unwrap()
                != instance_partition(other, &id, 16).unwrap()
        });
        assert!(moved);
    }

    #[test]
    fn partition_refuses_short_secrets_and_bad_topology() {
        let id = identity("U123", 1);
        assert!(instance_partition(&SECRET[..31], &id, 16).is_err());
        assert!(instance_partition(SECRET, &id, 0).is_err());
        assert!(instance_partition(SECRET, &id, MAX_PARTITIONS + 1).is_err());
    }

    #[test]
    fn partitions_spread_keys_evenly() {
        let count = 16;
        let mut buckets = vec![0usize; count];
        for i in 0..16_000 {
            let p = instance_partition(SECRET, &identity(&format!("user-{i}"), 1), count).unwrap();
            buckets[p as usize] += 1;
        }
        // Expected 1,000 each; a uniform hash stays well inside ±20%.
        assert!(
            buckets.iter().all(|&n| (800..=1200).contains(&n)),
            "{buckets:?}"
        );
    }

    #[test]
    fn inbox_ids_land_on_their_partition_for_every_topology() {
        for count in [1usize, 2, 3, 7, 16, 64, 256] {
            for partition in 0..count as u32 {
                let id = identity("U123", 2);
                let run_id = inbox_run_id(&id, partition, count).unwrap();
                assert!(run_id.starts_with("inbox::proj-1::env-prod::triage::U123::2::"));
                assert_eq!(partition_for_run(&run_id, count), partition as usize);
                assert_eq!(run_id, inbox_run_id(&id, partition, count).unwrap());
            }
        }
        assert!(inbox_run_id(&identity("U123", 1), 16, 16).is_err());
    }

    #[test]
    fn inbox_ids_differ_by_generation_and_key() {
        let a = inbox_run_id(&identity("U123", 1), 3, 16).unwrap();
        let b = inbox_run_id(&identity("U123", 2), 3, 16).unwrap();
        let c = inbox_run_id(&identity("U124", 1), 3, 16).unwrap();
        assert_ne!(a, b);
        assert_ne!(a, c);
        // A key that ends like a generation can't stand in for another instance:
        // the last two fields are always the generation and the salt.
        let tricky = inbox_run_id(&identity("U123::1", 1), 3, 16).unwrap();
        assert_ne!(tricky, a);
        let (rest, _salt) = tricky.rsplit_once("::").unwrap();
        let (rest, generation) = rest.rsplit_once("::").unwrap();
        assert_eq!(generation, "1");
        assert!(rest.ends_with("::U123::1"));
    }

    #[test]
    fn run_ids_are_marked_v8_uuids_on_their_partition() {
        for count in [1usize, 2, 7, 64, 256] {
            for partition in [0, (count as u32) - 1] {
                let run_id = instance_run_id(&identity("U123", 1), 5, partition, count).unwrap();
                let uuid = uuid::Uuid::parse_str(&run_id).unwrap();
                assert_eq!(uuid.get_version_num(), 8);
                assert_eq!(uuid.get_variant(), uuid::Variant::RFC4122);
                assert_eq!(&uuid.as_bytes()[..4], b"agn5");
                assert_eq!(uuid.as_bytes()[4], INSTANCE_RUN_FORMAT_V1);
                assert!(run_id.starts_with("61676e35-01"));
                assert_eq!(run_id, uuid.hyphenated().to_string());
                assert_eq!(partition_for_run(&run_id, count), partition as usize);
                assert!(is_instance_run_id(&run_id));
            }
        }
    }

    #[test]
    fn run_ids_are_deterministic_and_distinct() {
        let id = identity("U123", 1);
        let first = instance_run_id(&id, 1, 4, 16).unwrap();
        assert_eq!(first, instance_run_id(&id, 1, 4, 16).unwrap());
        let mut seen = std::collections::HashSet::new();
        for sequence in 0..500 {
            assert!(seen.insert(instance_run_id(&id, sequence, 4, 16).unwrap()));
        }
        assert_ne!(
            first,
            instance_run_id(&identity("U123", 2), 1, 4, 16).unwrap()
        );
        assert_ne!(
            first,
            instance_run_id(&identity("U124", 1), 1, 4, 16).unwrap()
        );
    }

    #[test]
    fn inbox_ids_parse_back_to_their_instance() {
        for key in ["U123", "user::42", "::lead", "trail::", ":x:", "a b"] {
            for generation in [1, 7, u64::MAX] {
                let id = identity(key, generation);
                let run_id = inbox_run_id(&id, 5, 16).unwrap();
                assert_eq!(parse_inbox_run_id(&run_id), Some(id), "{run_id}");
            }
        }
    }

    #[test]
    fn only_canonical_inbox_ids_parse() {
        let run_id = inbox_run_id(&identity("U123", 2), 5, 16).unwrap();
        let (base, salt) = run_id.rsplit_once("::").unwrap();
        for bad in [
            String::new(),
            "session::abc".to_string(),
            "inbox::proj-1::env-prod::triage".to_string(),
            "inbox::proj-1::env-prod::triage::U123::2".to_string(),
            format!("{base}::0{salt}"),
            format!("{base}::+{salt}"),
            run_id.replace("::2::", "::02::"),
            run_id.replace("::2::", "::0::").replace("::U123::", "::::"),
            run_id.replacen("inbox::", "inbox:::", 1),
        ] {
            assert_eq!(parse_inbox_run_id(&bad), None, "{bad}");
        }
    }

    #[test]
    fn item_ids_are_derived_uuids() {
        let inbox = inbox_run_id(&identity("U123", 1), 5, 16).unwrap();
        let item = inbox_item_id(&inbox, "evt_1");
        assert_eq!(item, inbox_item_id(&inbox, "evt_1"));
        assert_ne!(item, inbox_item_id(&inbox, "evt_2"));
        assert_ne!(
            item,
            inbox_item_id(&inbox_run_id(&identity("U123", 2), 5, 16).unwrap(), "evt_1")
        );
        let parsed = uuid::Uuid::parse_str(&item).unwrap();
        assert_eq!(parsed.hyphenated().to_string(), item);
    }

    #[test]
    fn only_marked_v8_ids_are_instance_runs() {
        let run_id = instance_run_id(&identity("U123", 1), 1, 0, 1).unwrap();
        assert!(is_instance_run_id(&run_id));
        assert!(is_instance_run_id(&run_id.to_uppercase()));
        assert!(is_instance_run_id(&run_id.replace('-', "")));

        assert!(!is_instance_run_id(""));
        assert!(!is_instance_run_id("not-a-uuid"));
        assert!(!is_instance_run_id("session::abc"));
        assert!(!is_instance_run_id(
            &inbox_run_id(&identity("U123", 1), 0, 1).unwrap()
        ));
        assert!(!is_instance_run_id(&uuid::Uuid::new_v4().to_string()));
        assert!(!is_instance_run_id(&uuid::Uuid::now_v7().to_string()));
        assert!(!is_instance_run_id(
            &crate::runtime_work::RuntimeWork::stable_id(&["x"])
        ));

        // A v8 id without the marker is someone else's.
        let mut bytes = *uuid::Uuid::parse_str(&run_id).unwrap().as_bytes();
        bytes[0] ^= 0xff;
        let unmarked = uuid::Builder::from_custom_bytes(bytes)
            .into_uuid()
            .to_string();
        assert!(!is_instance_run_id(&unmarked));

        // The marker on a non-v8 id doesn't count either.
        let mut bytes = *uuid::Uuid::new_v4().as_bytes();
        bytes[..4].copy_from_slice(&INSTANCE_RUN_MARKER);
        let v4 = uuid::Builder::from_random_bytes(bytes)
            .into_uuid()
            .to_string();
        assert!(!is_instance_run_id(&v4));
    }
}
