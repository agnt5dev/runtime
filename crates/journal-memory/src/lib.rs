//! An in-memory journal segment.
//!
//! `MemorySegment` keeps the journal in process memory. Tests across the
//! runtime use it in place of durable storage, and it carries no storage
//! engine of its own, so both the single-process and hosted runtimes can depend on it.

#![forbid(unsafe_code)]

mod memory;

pub use memory::MemorySegment;

/// Where an append puts its records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Placement {
    /// A new append: records take the next offsets at the local tail.
    AtTail,
    /// A follower persisting a leader's batch: each record must land at the
    /// offset the leader assigned, or the batch is rejected unwritten.
    AtRecordOffsets,
}

#[cfg(feature = "test-hooks")]
mod hooks {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};

    /// One-shot deterministic pause after an append has acquired the lifecycle
    /// lock and passed the sealed check.
    ///
    /// Tests use this to prove that `seal` cannot return while an accepted append
    /// is still between its lifecycle check and commit.
    pub struct AppendLifecycleTestHook {
        armed: AtomicBool,
        append_paused: Barrier,
        resume_append: Barrier,
        stop_import_after_mark: AtomicBool,
    }

    impl AppendLifecycleTestHook {
        pub fn new() -> Arc<Self> {
            Arc::new(Self {
                armed: AtomicBool::new(true),
                append_paused: Barrier::new(2),
                resume_append: Barrier::new(2),
                stop_import_after_mark: AtomicBool::new(false),
            })
        }

        /// Stop the next journal-index import right after it writes its mark,
        /// as a crash there would.
        pub fn stopping_import_after_mark() -> Arc<Self> {
            Arc::new(Self {
                armed: AtomicBool::new(false),
                append_paused: Barrier::new(2),
                resume_append: Barrier::new(2),
                stop_import_after_mark: AtomicBool::new(true),
            })
        }

        pub fn fail_journal_index_import_after_mark(&self) -> bool {
            self.stop_import_after_mark.swap(false, Ordering::AcqRel)
        }

        pub fn pause_once(&self) {
            if self.armed.swap(false, Ordering::AcqRel) {
                self.append_paused.wait();
                self.resume_append.wait();
            }
        }

        pub fn wait_until_append_paused(&self) {
            self.append_paused.wait();
        }

        pub fn resume_append(&self) {
            self.resume_append.wait();
        }
    }
}

#[cfg(feature = "test-hooks")]
pub use hooks::AppendLifecycleTestHook;
