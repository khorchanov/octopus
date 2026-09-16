use chrono::{DateTime, Utc};

/// Error type threaded through workflow and step execution.
///
/// `Suspend` is produced internally by [`crate::WorkflowContext::step`] and
/// [`crate::WorkflowContext::sleep`] when a checkpoint isn't ready to
/// complete yet; it is not meant to be constructed by workflow code, but
/// workflow functions must propagate it (typically via `?`) rather than
/// swallowing it, or the run will never be rescheduled correctly.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    #[error("suspended until {0}")]
    Suspend(DateTime<Utc>),

    #[error("step failed after retries: {0}")]
    StepFailed(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("{0}")]
    Custom(String),
}

/// The error type step closures return. Any `std::error::Error` can be
/// converted into it via `?`.
pub type StepResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
