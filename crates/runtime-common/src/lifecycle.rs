//! Shared runtime lifecycle state.

/// Node-level lifecycle state for admission and shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeLifecycleState {
    Starting,
    Serving,
    Draining,
    Stopping,
    Stopped,
}

impl NodeLifecycleState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Serving => "serving",
            Self::Draining => "draining",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        }
    }

    pub fn is_ready(self) -> bool {
        matches!(self, Self::Serving)
    }
}
