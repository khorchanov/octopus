use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use octopus::{Client, RetryPolicy, StepResult, Worker, Workflow, WorkflowContext, WorkflowError};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

async fn test_pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping: DATABASE_URL not set (see docker-compose.yml)");
        return None;
    };
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await
        .expect("connect to test database");
    let client = Client::new(pool.clone());
    client.migrate().await.expect("run migrations");
    Some(pool)
}

struct SequentialWorkflow;

#[async_trait]
impl Workflow for SequentialWorkflow {
    type Input = i64;
    type Output = i64;

    fn name(&self) -> &str {
        "it_sequential"
    }

    async fn run(&self, ctx: &WorkflowContext, input: i64) -> Result<i64, WorkflowError> {
        let a: i64 = ctx
            .step("add_one", RetryPolicy::none(), move || async move { Ok(input + 1) })
            .await?;
        let b: i64 = ctx
            .step("double", RetryPolicy::none(), move || async move { Ok(a * 2) })
            .await?;
        let c: i64 = ctx
            .step("sub_three", RetryPolicy::none(), move || async move { Ok(b - 3) })
            .await?;
        Ok(c)
    }
}

#[tokio::test]
async fn sequential_steps_complete_end_to_end() {
    let Some(pool) = test_pool().await else { return };
    let client = Client::new(pool.clone());

    let run_id = client.start("it_sequential", &10i64).await.unwrap();

    let worker = Worker::new(pool)
        .register(SequentialWorkflow)
        .poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(worker.run());

    let output: i64 = client.result(run_id).await.unwrap();
    handle.abort();

    assert_eq!(output, 19);
}

struct ResumeWorkflow {
    counter_a: Arc<AtomicUsize>,
    counter_b: Arc<AtomicUsize>,
}

#[async_trait]
impl Workflow for ResumeWorkflow {
    type Input = ();
    type Output = ();

    fn name(&self) -> &str {
        "it_resume"
    }

    async fn run(&self, ctx: &WorkflowContext, _input: ()) -> Result<(), WorkflowError> {
        let counter_a = self.counter_a.clone();
        ctx.step("a", RetryPolicy::none(), move || async move {
            counter_a.fetch_add(1, Ordering::SeqCst);
            StepResult::Ok(())
        })
        .await?;

        ctx.sleep("wait", Duration::from_millis(600)).await?;

        let counter_b = self.counter_b.clone();
        ctx.step("b", RetryPolicy::none(), move || async move {
            counter_b.fetch_add(1, Ordering::SeqCst);
            StepResult::Ok(())
        })
        .await?;

        Ok(())
    }
}

#[tokio::test]
async fn resume_does_not_rerun_completed_steps() {
    let Some(pool) = test_pool().await else { return };
    let client = Client::new(pool.clone());

    let counter_a = Arc::new(AtomicUsize::new(0));
    let counter_b = Arc::new(AtomicUsize::new(0));

    let run_id = client.start("it_resume", &()).await.unwrap();

    let worker = Worker::new(pool)
        .register(ResumeWorkflow {
            counter_a: counter_a.clone(),
            counter_b: counter_b.clone(),
        })
        .poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(worker.run());

    let _: () = client.result(run_id).await.unwrap();
    handle.abort();

    assert_eq!(counter_a.load(Ordering::SeqCst), 1);
    assert_eq!(counter_b.load(Ordering::SeqCst), 1);
}

struct FlakyWorkflow {
    attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl Workflow for FlakyWorkflow {
    type Input = ();
    type Output = u32;

    fn name(&self) -> &str {
        "it_flaky"
    }

    async fn run(&self, ctx: &WorkflowContext, _input: ()) -> Result<u32, WorkflowError> {
        let attempts = self.attempts.clone();
        ctx.step(
            "flaky",
            RetryPolicy::exponential(5, Duration::from_millis(100)),
            move || {
                let attempts = attempts.clone();
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if n < 3 {
                        Err(format!("attempt {n} failed").into())
                    } else {
                        Ok(n as u32)
                    }
                }
            },
        )
        .await
    }
}

#[tokio::test]
async fn step_retries_with_backoff_then_succeeds() {
    let Some(pool) = test_pool().await else { return };
    let client = Client::new(pool.clone());

    let attempts = Arc::new(AtomicUsize::new(0));
    let run_id = client.start("it_flaky", &()).await.unwrap();

    let worker = Worker::new(pool)
        .register(FlakyWorkflow {
            attempts: attempts.clone(),
        })
        .poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(worker.run());

    let output: u32 = client.result(run_id).await.unwrap();
    handle.abort();

    assert_eq!(output, 3);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

struct SleeperWorkflow;

#[async_trait]
impl Workflow for SleeperWorkflow {
    type Input = ();
    type Output = ();

    fn name(&self) -> &str {
        "it_sleeper"
    }

    async fn run(&self, ctx: &WorkflowContext, _input: ()) -> Result<(), WorkflowError> {
        ctx.sleep("nap", Duration::from_secs(2)).await
    }
}

struct QuickWorkflow;

#[async_trait]
impl Workflow for QuickWorkflow {
    type Input = ();
    type Output = ();

    fn name(&self) -> &str {
        "it_quick"
    }

    async fn run(&self, _ctx: &WorkflowContext, _input: ()) -> Result<(), WorkflowError> {
        Ok(())
    }
}

#[tokio::test]
async fn sleep_does_not_block_other_runs() {
    let Some(pool) = test_pool().await else { return };
    let client = Client::new(pool.clone());

    let sleeper_id = client.start("it_sleeper", &()).await.unwrap();

    let worker = Worker::new(pool)
        .register(SleeperWorkflow)
        .register(QuickWorkflow)
        .concurrency(1)
        .poll_interval(Duration::from_millis(50));
    let handle = tokio::spawn(worker.run());

    tokio::time::sleep(Duration::from_millis(200)).await;
    let quick_id = client.start("it_quick", &()).await.unwrap();

    let quick_start = std::time::Instant::now();
    let _: () = client.result(quick_id).await.unwrap();
    let quick_elapsed = quick_start.elapsed();

    let _: () = client.result(sleeper_id).await.unwrap();
    handle.abort();

    assert!(
        quick_elapsed < Duration::from_secs(1),
        "quick workflow should not wait on sleeper's timer, took {quick_elapsed:?}"
    );
}

struct GreeterV1;

#[async_trait]
impl Workflow for GreeterV1 {
    type Input = ();
    type Output = String;

    fn name(&self) -> &str {
        "it_greeter"
    }

    fn version(&self) -> u32 {
        1
    }

    async fn run(&self, ctx: &WorkflowContext, _input: ()) -> Result<String, WorkflowError> {
        ctx.step("say_hi", RetryPolicy::none(), || async { Ok("v1".to_string()) })
            .await?;
        ctx.sleep("pause", Duration::from_millis(500)).await?;
        Ok("v1-result".to_string())
    }
}

struct GreeterV2;

#[async_trait]
impl Workflow for GreeterV2 {
    type Input = ();
    type Output = String;

    fn name(&self) -> &str {
        "it_greeter"
    }

    fn version(&self) -> u32 {
        2
    }

    async fn run(&self, ctx: &WorkflowContext, _input: ()) -> Result<String, WorkflowError> {
        ctx.step("say_hello", RetryPolicy::none(), || async { Ok("v2".to_string()) })
            .await?;
        Ok("v2-result".to_string())
    }
}

#[tokio::test]
async fn in_flight_run_stays_pinned_to_its_version_after_redeploy() {
    let Some(pool) = test_pool().await else { return };
    let client = Client::new(pool.clone());

    let old_run_id = client.start("it_greeter", &()).await.unwrap();
    let old_worker = Worker::new(pool.clone())
        .register(GreeterV1)
        .poll_interval(Duration::from_millis(50));
    let old_handle = tokio::spawn(old_worker.run());

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        client.active_versions("it_greeter").await.unwrap(),
        vec![1]
    );
    old_handle.abort();

    let new_run_id = client.start("it_greeter", &()).await.unwrap();
    let new_worker = Worker::new(pool)
        .register(GreeterV1)
        .register(GreeterV2)
        .poll_interval(Duration::from_millis(50));
    let new_handle = tokio::spawn(new_worker.run());

    let old_output: String = client.result(old_run_id).await.unwrap();
    let new_output: String = client.result(new_run_id).await.unwrap();
    new_handle.abort();

    assert_eq!(old_output, "v1-result", "in-flight run must keep running V1");
    assert_eq!(new_output, "v2-result", "new run must get the latest version, V2");

    assert!(client.active_versions("it_greeter").await.unwrap().is_empty());
}
