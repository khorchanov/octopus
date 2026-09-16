use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::context::WorkflowContext;
use crate::error::WorkflowError;
use crate::workflow::{DynWorkflow, Workflow};

/// `workflow_type -> version -> implementation`. Kept as a `BTreeMap` so
/// the highest key is the "latest" version handed to new runs.
type Registry = HashMap<String, BTreeMap<u32, Arc<dyn DynWorkflow>>>;

#[derive(sqlx::FromRow)]
struct LeasedRun {
    id: Uuid,
    workflow_type: String,
    workflow_version: Option<i32>,
    input: serde_json::Value,
}

/// Polls `workflow_runs` for due work and executes registered workflows.
/// Multiple `Worker`s (in one process or many) can run concurrently
/// against the same Postgres database; leasing via `FOR UPDATE SKIP
/// LOCKED` ensures each run is only picked up by one worker at a time.
pub struct Worker {
    pool: PgPool,
    registry: Registry,
    worker_id: String,
    concurrency: usize,
    poll_interval: Duration,
    lease_duration: Duration,
}

impl Worker {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            registry: HashMap::new(),
            worker_id: Uuid::new_v4().to_string(),
            concurrency: 10,
            poll_interval: Duration::from_millis(500),
            lease_duration: Duration::from_secs(30),
        }
    }

    /// Registers a workflow implementation under its `name()`/`version()`.
    /// Registering more than one implementation for the same name lets
    /// in-flight runs pinned to an older version keep executing it after
    /// a newer version is deployed — see [`Workflow::version`].
    ///
    /// # Panics
    /// Panics if two registered implementations share both the same
    /// name and version, since that's always a programming error.
    pub fn register<W: Workflow>(mut self, workflow: W) -> Self {
        let name = Workflow::name(&workflow).to_string();
        let version = Workflow::version(&workflow);
        let versions = self.registry.entry(name.clone()).or_default();
        if versions.insert(version, Arc::new(workflow)).is_some() {
            panic!("duplicate registration for workflow '{name}' version {version}");
        }
        self
    }

    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// How long a lease is held before a crashed worker's runs are
    /// reclaimed by another worker. Should comfortably exceed the time a
    /// single workflow replay (up to its next suspend point) can take.
    pub fn lease_duration(mut self, d: Duration) -> Self {
        self.lease_duration = d;
        self
    }

    /// Runs the poll loop forever (until the process is killed or an
    /// unrecoverable database error occurs).
    pub async fn run(self) -> Result<(), WorkflowError> {
        let workflow_types: Vec<String> = self.registry.keys().cloned().collect();
        let registry = Arc::new(self.registry);
        let semaphore = Arc::new(Semaphore::new(self.concurrency));

        loop {
            reap_expired_leases(&self.pool).await?;

            let permits = semaphore.available_permits().max(1);
            let due = lease_batch(
                &self.pool,
                &self.worker_id,
                self.lease_duration,
                permits,
                &workflow_types,
            )
            .await?;

            if due.is_empty() {
                tokio::time::sleep(self.poll_interval).await;
                continue;
            }

            let mut handles = Vec::with_capacity(due.len());
            for run in due {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let pool = self.pool.clone();
                let registry = registry.clone();
                handles.push(tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(err) = execute_run(&pool, &registry, run).await {
                        tracing::error!(error = %err, "workflow run execution failed");
                    }
                }));
            }
            for handle in handles {
                let _ = handle.await;
            }
        }
    }
}

async fn lease_batch(
    pool: &PgPool,
    worker_id: &str,
    lease_duration: Duration,
    limit: usize,
    workflow_types: &[String],
) -> Result<Vec<LeasedRun>, WorkflowError> {
    // A worker only leases runs for workflow types it has registered, so
    // it never claims (and fails) runs belonging to other workflows
    // sharing the same database.
    if workflow_types.is_empty() {
        return Ok(Vec::new());
    }

    let lease_secs = lease_duration.as_secs_f64();
    let rows = sqlx::query_as::<_, LeasedRun>(
        "WITH picked AS ( \
             SELECT id FROM workflow_runs \
             WHERE status = 'pending' AND run_after <= now() AND workflow_type = ANY($4) \
             ORDER BY run_after \
             FOR UPDATE SKIP LOCKED \
             LIMIT $1 \
         ) \
         UPDATE workflow_runs \
         SET status = 'running', locked_by = $2, \
             locked_until = now() + make_interval(secs => $3), \
             updated_at = now() \
         WHERE id IN (SELECT id FROM picked) \
         RETURNING id, workflow_type, workflow_version, input",
    )
    .bind(limit as i64)
    .bind(worker_id)
    .bind(lease_secs)
    .bind(workflow_types)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

async fn reap_expired_leases(pool: &PgPool) -> Result<(), WorkflowError> {
    sqlx::query(
        "UPDATE workflow_runs SET status = 'pending', locked_by = NULL, locked_until = NULL \
         WHERE status = 'running' AND locked_until < now()",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn execute_run(pool: &PgPool, registry: &Registry, run: LeasedRun) -> Result<(), WorkflowError> {
    let Some(versions) = registry.get(&run.workflow_type) else {
        mark_failed(
            pool,
            run.id,
            &format!("no workflow registered for type '{}'", run.workflow_type),
        )
        .await?;
        return Ok(());
    };

    // First pickup: pin the run to whichever version is latest right now.
    // Persisted before running anything so a crash between this write and
    // the run's first step can't cause a later resume to pick a different
    // (by-then latest) version and desync from steps already recorded.
    let version = match run.workflow_version {
        Some(v) => v as u32,
        None => {
            let (latest, _) = versions
                .last_key_value()
                .expect("registry never holds an empty version map");
            persist_version(pool, run.id, *latest).await?;
            *latest
        }
    };

    let Some(workflow) = versions.get(&version) else {
        mark_failed(
            pool,
            run.id,
            &format!(
                "run pinned to '{}' version {version}, but that version is no longer registered",
                run.workflow_type
            ),
        )
        .await?;
        return Ok(());
    };

    tracing::debug!(workflow = workflow.name(), version, run_id = %run.id, "executing run");
    let ctx = WorkflowContext::new(run.id, pool.clone());
    match workflow.run_json(&ctx, run.input).await {
        Ok(output) => mark_completed(pool, run.id, output).await,
        Err(WorkflowError::Suspend(resume_at)) => suspend(pool, run.id, resume_at).await,
        Err(err) => mark_failed(pool, run.id, &err.to_string()).await,
    }
}

async fn persist_version(pool: &PgPool, run_id: Uuid, version: u32) -> Result<(), WorkflowError> {
    sqlx::query("UPDATE workflow_runs SET workflow_version = $1 WHERE id = $2")
        .bind(version as i32)
        .bind(run_id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn mark_completed(
    pool: &PgPool,
    run_id: Uuid,
    output: serde_json::Value,
) -> Result<(), WorkflowError> {
    sqlx::query(
        "UPDATE workflow_runs SET status = 'completed', output = $1, updated_at = now() WHERE id = $2",
    )
    .bind(output)
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_failed(pool: &PgPool, run_id: Uuid, message: &str) -> Result<(), WorkflowError> {
    let err_json = serde_json::json!({ "message": message });
    sqlx::query(
        "UPDATE workflow_runs SET status = 'failed', error = $1, updated_at = now() WHERE id = $2",
    )
    .bind(err_json)
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn suspend(pool: &PgPool, run_id: Uuid, resume_at: DateTime<Utc>) -> Result<(), WorkflowError> {
    sqlx::query(
        "UPDATE workflow_runs \
         SET status = 'pending', run_after = $1, locked_by = NULL, locked_until = NULL, updated_at = now() \
         WHERE id = $2",
    )
    .bind(resume_at)
    .bind(run_id)
    .execute(pool)
    .await?;
    Ok(())
}
