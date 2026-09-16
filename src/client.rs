use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::WorkflowError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    fn parse(s: &str) -> Self {
        match s {
            "pending" => RunStatus::Pending,
            "running" => RunStatus::Running,
            "completed" => RunStatus::Completed,
            "failed" => RunStatus::Failed,
            "cancelled" => RunStatus::Cancelled,
            other => panic!("unknown workflow_runs.status value: {other}"),
        }
    }
}

/// Entry point for starting workflows and observing their progress.
/// Cheap to clone (wraps a `PgPool`).
#[derive(Clone)]
pub struct Client {
    pool: PgPool,
}

impl Client {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Applies the crate's embedded migrations. Safe to call repeatedly.
    pub async fn migrate(&self) -> Result<(), sqlx::migrate::MigrateError> {
        sqlx::migrate!("./migrations").run(&self.pool).await
    }

    /// Starts a new run of the workflow registered under `workflow_type`,
    /// returning its run id immediately (the run executes asynchronously
    /// once a [`crate::Worker`] picks it up).
    pub async fn start<I: Serialize>(
        &self,
        workflow_type: &str,
        input: &I,
    ) -> Result<Uuid, WorkflowError> {
        let id = Uuid::new_v4();
        let input_json = serde_json::to_value(input)?;
        sqlx::query("INSERT INTO workflow_runs (id, workflow_type, input) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(workflow_type)
            .bind(input_json)
            .execute(&self.pool)
            .await?;
        Ok(id)
    }

    /// Distinct `workflow_version`s that still have a non-terminal run
    /// (pending/running) of `workflow_type`. Once this returns empty for a
    /// version, it's safe to stop registering the `Workflow` impl for it
    /// and delete the code — see [`crate::Workflow::version`].
    ///
    /// A freshly-started run that no worker has picked up yet isn't
    /// pinned to a version and won't appear here until it is.
    pub async fn active_versions(&self, workflow_type: &str) -> Result<Vec<u32>, WorkflowError> {
        let rows: Vec<(i32,)> = sqlx::query_as(
            "SELECT DISTINCT workflow_version FROM workflow_runs \
             WHERE workflow_type = $1 AND status IN ('pending', 'running') \
             AND workflow_version IS NOT NULL",
        )
        .bind(workflow_type)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(v,)| v as u32).collect())
    }

    pub async fn status(&self, run_id: Uuid) -> Result<RunStatus, WorkflowError> {
        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM workflow_runs WHERE id = $1")
                .bind(run_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(RunStatus::parse(&status))
    }

    /// Polls until the run reaches a terminal state, then returns its
    /// output (deserialized as `O`) or an error if it failed/was cancelled.
    pub async fn result<O: DeserializeOwned>(&self, run_id: Uuid) -> Result<O, WorkflowError> {
        loop {
            let (status, output, error): (String, Option<serde_json::Value>, Option<serde_json::Value>) =
                sqlx::query_as(
                    "SELECT status, output, error FROM workflow_runs WHERE id = $1",
                )
                .bind(run_id)
                .fetch_one(&self.pool)
                .await?;

            match RunStatus::parse(&status) {
                RunStatus::Completed => {
                    let output = output
                        .ok_or_else(|| WorkflowError::Custom("completed run missing output".into()))?;
                    return Ok(serde_json::from_value(output)?);
                }
                RunStatus::Failed | RunStatus::Cancelled => {
                    let msg = error
                        .and_then(|e| e.get("message").and_then(|m| m.as_str()).map(str::to_string))
                        .unwrap_or_else(|| format!("run ended in status {status}"));
                    return Err(WorkflowError::Custom(msg));
                }
                RunStatus::Pending | RunStatus::Running => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
}
