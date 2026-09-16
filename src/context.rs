use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::{StepResult, WorkflowError};
use crate::retry::RetryPolicy;

#[derive(sqlx::FromRow)]
struct StepRow {
    status: String,
    output: Option<serde_json::Value>,
    error: Option<serde_json::Value>,
    attempt: i32,
    max_attempts: i32,
    resume_at: Option<DateTime<Utc>>,
}

/// Passed to a workflow's `run` method. Provides the durable primitives
/// (`step`, `sleep`) that checkpoint progress to Postgres.
///
/// The workflow function is re-invoked from the top every time a run is
/// resumed; code between `step`/`sleep` calls is *not* memoized, so it
/// must be cheap and side-effect-free (or naturally idempotent). Anything
/// with a real side effect belongs inside a `step`.
pub struct WorkflowContext {
    run_id: Uuid,
    pool: PgPool,
    occurrences: Mutex<HashMap<String, u32>>,
}

impl WorkflowContext {
    pub(crate) fn new(run_id: Uuid, pool: PgPool) -> Self {
        Self {
            run_id,
            pool,
            occurrences: Mutex::new(HashMap::new()),
        }
    }

    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    fn next_key(&self, name: &str) -> String {
        let mut occ = self.occurrences.lock().unwrap();
        let count = occ.entry(name.to_string()).or_insert(0);
        let key = if *count == 0 {
            name.to_string()
        } else {
            format!("{name}#{count}")
        };
        *count += 1;
        key
    }

    async fn load_step(&self, key: &str) -> Result<Option<StepRow>, WorkflowError> {
        let row = sqlx::query_as::<_, StepRow>(
            "SELECT status, output, error, attempt, max_attempts, resume_at \
             FROM workflow_steps WHERE run_id = $1 AND step_key = $2",
        )
        .bind(self.run_id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// A durable, memoized unit of work. On the first execution of a given
    /// `name`, the closure runs and its result is persisted; on replay
    /// (resume after crash, suspend, or retry), a completed step returns
    /// its cached output instantly without re-running the closure.
    ///
    /// Calling `step` with the same `name` multiple times in one workflow
    /// (e.g. in a loop) is fine — occurrences are disambiguated automatically,
    /// as long as the *order* of calls is stable across replays.
    pub async fn step<F, Fut, T>(
        &self,
        name: &str,
        policy: RetryPolicy,
        f: F,
    ) -> Result<T, WorkflowError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = StepResult<T>>,
        T: Serialize + DeserializeOwned,
    {
        let key = self.next_key(name);

        if let Some(row) = self.load_step(&key).await? {
            match row.status.as_str() {
                "completed" => {
                    let output = row
                        .output
                        .ok_or_else(|| WorkflowError::Custom(format!("step {key} completed with no output")))?;
                    return Ok(serde_json::from_value(output)?);
                }
                "failed" => {
                    let msg = row
                        .error
                        .and_then(|e| e.get("message").and_then(|m| m.as_str()).map(str::to_string))
                        .unwrap_or_else(|| "unknown error".to_string());
                    return Err(WorkflowError::StepFailed(msg));
                }
                _ => {
                    if let Some(resume_at) = row.resume_at
                        && resume_at > Utc::now()
                    {
                        return Err(WorkflowError::Suspend(resume_at));
                    }
                    return self
                        .run_step_attempt(&key, row.attempt, row.max_attempts, policy, f)
                        .await;
                }
            }
        }

        sqlx::query(
            "INSERT INTO workflow_steps (run_id, step_key, kind, status, max_attempts, backoff_base_ms, backoff_factor) \
             VALUES ($1, $2, 'step', 'waiting', $3, $4, $5) \
             ON CONFLICT (run_id, step_key) DO NOTHING",
        )
        .bind(self.run_id)
        .bind(&key)
        .bind(policy.max_attempts as i32)
        .bind(policy.base.as_millis() as i64)
        .bind(policy.factor)
        .execute(&self.pool)
        .await?;

        self.run_step_attempt(&key, 0, policy.max_attempts as i32, policy, f)
            .await
    }

    async fn run_step_attempt<F, Fut, T>(
        &self,
        key: &str,
        prev_attempt: i32,
        max_attempts: i32,
        policy: RetryPolicy,
        f: F,
    ) -> Result<T, WorkflowError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = StepResult<T>>,
        T: Serialize + DeserializeOwned,
    {
        let attempt = prev_attempt + 1;
        match f().await {
            Ok(value) => {
                let output = serde_json::to_value(&value)?;
                sqlx::query(
                    "UPDATE workflow_steps SET status = 'completed', output = $1, attempt = $2, completed_at = now() \
                     WHERE run_id = $3 AND step_key = $4",
                )
                .bind(&output)
                .bind(attempt)
                .bind(self.run_id)
                .bind(key)
                .execute(&self.pool)
                .await?;
                Ok(value)
            }
            Err(err) => {
                let err_json = serde_json::json!({ "message": err.to_string() });
                if attempt < max_attempts {
                    let backoff = policy.backoff_for_attempt(attempt as u32);
                    let resume_at = Utc::now()
                        + chrono::Duration::from_std(backoff).unwrap_or_else(|_| chrono::Duration::zero());
                    sqlx::query(
                        "UPDATE workflow_steps SET status = 'retrying', attempt = $1, error = $2, resume_at = $3 \
                         WHERE run_id = $4 AND step_key = $5",
                    )
                    .bind(attempt)
                    .bind(&err_json)
                    .bind(resume_at)
                    .bind(self.run_id)
                    .bind(key)
                    .execute(&self.pool)
                    .await?;
                    Err(WorkflowError::Suspend(resume_at))
                } else {
                    sqlx::query(
                        "UPDATE workflow_steps SET status = 'failed', attempt = $1, error = $2, completed_at = now() \
                         WHERE run_id = $3 AND step_key = $4",
                    )
                    .bind(attempt)
                    .bind(&err_json)
                    .bind(self.run_id)
                    .bind(key)
                    .execute(&self.pool)
                    .await?;
                    Err(WorkflowError::StepFailed(err.to_string()))
                }
            }
        }
    }

    /// A durable timer. Suspends the run until at least `duration` has
    /// elapsed since the first call, without holding a worker task or
    /// thread blocked in the meantime.
    pub async fn sleep(&self, name: &str, duration: Duration) -> Result<(), WorkflowError> {
        let key = self.next_key(name);

        if let Some(row) = self.load_step(&key).await? {
            if row.status == "completed" {
                return Ok(());
            }
            let resume_at = row
                .resume_at
                .ok_or_else(|| WorkflowError::Custom(format!("timer {key} missing resume_at")))?;
            if resume_at > Utc::now() {
                return Err(WorkflowError::Suspend(resume_at));
            }
            sqlx::query(
                "UPDATE workflow_steps SET status = 'completed', completed_at = now() \
                 WHERE run_id = $1 AND step_key = $2",
            )
            .bind(self.run_id)
            .bind(&key)
            .execute(&self.pool)
            .await?;
            return Ok(());
        }

        let resume_at =
            Utc::now() + chrono::Duration::from_std(duration).unwrap_or_else(|_| chrono::Duration::zero());
        sqlx::query(
            "INSERT INTO workflow_steps (run_id, step_key, kind, status, resume_at) \
             VALUES ($1, $2, 'timer', 'waiting', $3) \
             ON CONFLICT (run_id, step_key) DO NOTHING",
        )
        .bind(self.run_id)
        .bind(&key)
        .bind(resume_at)
        .execute(&self.pool)
        .await?;

        Err(WorkflowError::Suspend(resume_at))
    }
}
