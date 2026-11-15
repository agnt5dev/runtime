//! Persisted storage-format compatibility contracts.
//!
//! The journal, materialized projection, and snapshot manifest evolve
//! independently. Journal remains exact version 1. Projection format 2 fences
//! versioned archived-run retention. Snapshot format 2 binds exact projection
//! progress and source provenance; format 3 additionally binds projection
//! lineage and snapshot kind while retaining v1/v2 readability for explicit
//! rejection or forensic inspection.

use crate::error::{EngineError, EngineResult};

/// First persisted format supported by the beta runtime.
pub const STORAGE_FORMAT_V1: u32 = 1;
/// Projection format with versioned, lossless archived-run retention.
pub const STORAGE_FORMAT_V2: u32 = 2;
/// Snapshot format with explicit projection lineage and publication kind.
pub const STORAGE_FORMAT_V3: u32 = 3;

/// Independently versioned persisted-data domains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageVersionDomain {
    Journal,
    Projection,
    Snapshot,
}

impl StorageVersionDomain {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Journal => "journal",
            Self::Projection => "projection",
            Self::Snapshot => "snapshot",
        }
    }
}

/// Reader/writer compatibility advertised for one persisted-data domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageVersionSupport {
    pub domain: StorageVersionDomain,
    pub writer: u32,
    pub min_readable: u32,
    pub max_readable: u32,
}

impl StorageVersionSupport {
    pub const fn exact_v1(domain: StorageVersionDomain) -> Self {
        Self {
            domain,
            writer: STORAGE_FORMAT_V1,
            min_readable: STORAGE_FORMAT_V1,
            max_readable: STORAGE_FORMAT_V1,
        }
    }

    pub const fn projection_v2() -> Self {
        Self {
            domain: StorageVersionDomain::Projection,
            writer: STORAGE_FORMAT_V2,
            min_readable: STORAGE_FORMAT_V1,
            max_readable: STORAGE_FORMAT_V2,
        }
    }

    pub const fn snapshot_v3() -> Self {
        Self {
            domain: StorageVersionDomain::Snapshot,
            writer: STORAGE_FORMAT_V3,
            min_readable: STORAGE_FORMAT_V1,
            max_readable: STORAGE_FORMAT_V3,
        }
    }

    /// Reject an unreadable persisted version before opening the data domain.
    pub fn validate_readable(self, persisted: u32) -> EngineResult<()> {
        if (self.min_readable..=self.max_readable).contains(&persisted) {
            Ok(())
        } else {
            Err(EngineError::UnsupportedStorageVersion {
                domain: self.domain.as_str(),
                persisted,
                min_readable: self.min_readable,
                max_readable: self.max_readable,
                writer: self.writer,
            })
        }
    }

    /// Reject a writer version that this process could not read back.
    pub fn validate_writer(self) -> EngineResult<()> {
        self.validate_readable(self.writer)
    }
}

pub const JOURNAL_VERSION_SUPPORT: StorageVersionSupport =
    StorageVersionSupport::exact_v1(StorageVersionDomain::Journal);
pub const PROJECTION_VERSION_SUPPORT: StorageVersionSupport =
    StorageVersionSupport::projection_v2();
pub const SNAPSHOT_VERSION_SUPPORT: StorageVersionSupport = StorageVersionSupport::snapshot_v3();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_accepts_exact_v1() {
        JOURNAL_VERSION_SUPPORT.validate_writer().unwrap();
        JOURNAL_VERSION_SUPPORT
            .validate_readable(STORAGE_FORMAT_V1)
            .unwrap();
    }

    #[test]
    fn journal_rejects_older_and_newer_versions() {
        for persisted in [0, STORAGE_FORMAT_V1 + 1] {
            let error = JOURNAL_VERSION_SUPPORT
                .validate_readable(persisted)
                .unwrap_err();
            assert!(matches!(
                error,
                EngineError::UnsupportedStorageVersion {
                    persisted: actual,
                    min_readable: STORAGE_FORMAT_V1,
                    max_readable: STORAGE_FORMAT_V1,
                    writer: STORAGE_FORMAT_V1,
                    ..
                } if actual == persisted
            ));
        }
    }

    #[test]
    fn snapshot_reads_v1_v2_and_writes_v3() {
        SNAPSHOT_VERSION_SUPPORT.validate_writer().unwrap();
        SNAPSHOT_VERSION_SUPPORT
            .validate_readable(STORAGE_FORMAT_V1)
            .unwrap();
        SNAPSHOT_VERSION_SUPPORT
            .validate_readable(STORAGE_FORMAT_V2)
            .unwrap();
        SNAPSHOT_VERSION_SUPPORT
            .validate_readable(STORAGE_FORMAT_V3)
            .unwrap();
        for persisted in [0, STORAGE_FORMAT_V3 + 1] {
            assert!(SNAPSHOT_VERSION_SUPPORT
                .validate_readable(persisted)
                .is_err());
        }
    }

    #[test]
    fn projection_reads_v1_and_writes_v2() {
        PROJECTION_VERSION_SUPPORT.validate_writer().unwrap();
        PROJECTION_VERSION_SUPPORT
            .validate_readable(STORAGE_FORMAT_V1)
            .unwrap();
        PROJECTION_VERSION_SUPPORT
            .validate_readable(STORAGE_FORMAT_V2)
            .unwrap();
        for persisted in [0, STORAGE_FORMAT_V2 + 1] {
            assert!(PROJECTION_VERSION_SUPPORT
                .validate_readable(persisted)
                .is_err());
        }
    }
}
