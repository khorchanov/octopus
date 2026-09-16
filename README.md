# Octopus

**Work in progress, unfinished, unreleased, API and schema may change.**

Durable workflow execution for Rust, backed by Postgres. No server to run,
just a library and a table.

Workflows are async functions that call `ctx.step(...)` for anything with a
side effect. Progress is checkpointed to Postgres, so a run survives process
crashes: on resume, the workflow replays from the top, but completed steps
return their cached result instantly instead of re-running.

## Install (still not published yet)

```toml
[dependencies]
octopus = "0.1"
```

## Example

```rust
use std::time::Duration;
use async_trait::async_trait;
use octopus::{Client, RetryPolicy, Worker, Workflow, WorkflowContext, WorkflowError};

struct Checkout;

#[async_trait]
impl Workflow for Checkout {
    type Input = String;
    type Output = String;

    fn name(&self) -> &str {
        "checkout"
    }

    async fn run(&self, ctx: &WorkflowContext, order_id: String) -> Result<String, WorkflowError> {
        ctx.step("charge_card", RetryPolicy::exponential(5, Duration::from_secs(1)), || async {
            Ok("ch_123".to_string())
        }).await?;

        ctx.sleep("cooldown", Duration::from_secs(3600)).await?;

        Ok(order_id)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = sqlx::PgPool::connect("postgres://localhost/mydb").await?;

    let client = Client::new(pool.clone());
    client.migrate().await?;

    let run_id = client.start("checkout", &"order-1".to_string()).await?;

    tokio::spawn(Worker::new(pool).register(Checkout).run());

    let output: String = client.result(run_id).await?;
    println!("{output}");
    Ok(())
}
```

See [`examples/checkout.rs`](examples/checkout.rs) for a fuller version.

## Core ideas

- **`ctx.step(name, retry_policy, closure)`**, a durable, memoized unit of
  work. The first time it runs, the closure executes and its result is
  saved; on replay, a completed step returns the saved result without
  re-running the closure. Failed steps retry per `RetryPolicy` with
  backoff, without holding a worker task blocked while waiting.

- **`ctx.sleep(name, duration)`**, a durable timer. Suspends the run until
  the duration elapses, freeing the worker to do other work in the
  meantime.

- **Code between `step`/`sleep` calls is not memoized** and may run
  multiple times (once per replay), so it must be cheap and
  deterministic. Side effects belong inside a `step`.

- **Versioning**, a run is pinned to whichever `Workflow` implementation
  was "latest" (highest `version()`) when a worker first picked it up.
  Deploying a new version doesn't change in-flight runs: keep the old
  `impl Workflow` registered (as a separate type) until
  `Client::active_versions` shows no runs left on it, then delete it.

## Determinism

Engines like Temporal replay your whole workflow function against a
recorded event history, so every line of it must be deterministic, no
`rand`, no `SystemTime::now()`, no unordered map iteration, anywhere in
the function.

Octopus doesn't work that way. A `step` runs its closure at most once and
caches the result; on replay, the cached result is returned and the
closure doesn't run again. So non-determinism *inside* a step is fine,
it only ever executes for real once:

```rust
// fine: runs once, the random id is cached and reused on every replay
let payment_id = ctx.step("charge", RetryPolicy::none(), || async {
    Ok(format!("ch_{}", rand::random::<u32>()))
}).await?;
```

What still has to be stable across replays is the *sequence of `step`/
`sleep` calls*, their names and the order they're called in, because
that sequence is how a resumed run is matched back up to its saved
progress:

```rust
// breaks resumption: on replay this may pick a different branch and
// call a step name/order that doesn't match what was already saved
if rand::random::<bool>() {
    ctx.step("path_a", ..., ...).await?;
} else {
    ctx.step("path_b", ..., ...).await?;
}
```

Branch on the *input* or on previous *step results* (both stable across
replays), not on anything computed fresh outside a step.

## Deploying a new version of a workflow

Changing a workflow's step sequence (renaming, reordering, adding, or
removing steps) would desync any run that's already in flight, since a
resumed run looks up its old steps by name. Instead of editing the
`Workflow` impl in place, add a new one and bump `version()`:

```rust
struct CheckoutV1;

#[async_trait]
impl Workflow for CheckoutV1 {
    type Input = String;
    type Output = String;
    fn name(&self) -> &str { "checkout" }
    fn version(&self) -> u32 { 1 }
    async fn run(&self, ctx: &WorkflowContext, order_id: String) -> Result<String, WorkflowError> {
        ctx.step("charge_card", RetryPolicy::none(), || async { Ok(()) }).await?;
        Ok(order_id)
    }
}

struct CheckoutV2;

#[async_trait]
impl Workflow for CheckoutV2 {
    type Input = String;
    type Output = String;
    fn name(&self) -> &str { "checkout" }
    fn version(&self) -> u32 { 2 }
    async fn run(&self, ctx: &WorkflowContext, order_id: String) -> Result<String, WorkflowError> {
        ctx.step("charge_card", RetryPolicy::none(), || async { Ok(()) }).await?;
        ctx.step("send_receipt", RetryPolicy::none(), || async { Ok(()) }).await?; // new step
        Ok(order_id)
    }
}
```

Register both on the worker:

```rust
Worker::new(pool)
    .register(CheckoutV1) // keep this around only until old runs drain
    .register(CheckoutV2) // new runs get this automatically (highest version)
    .run()
```

A run is pinned to whichever version is latest the moment a worker first
picks it up, and stays pinned to it for its whole lifetime, a redeploy
never switches an in-flight run's logic out from under it. Once
`client.active_versions("checkout").await?` no longer returns `1`, every
`v1` run has finished and it's safe to delete `CheckoutV1` and its
`.register(...)` call.

## Development

```bash
docker-compose up -d
DATABASE_URL=postgres://octopus:octopus@localhost:5432/octopus cargo test
```

## Status

Work in progress, not released. Not yet handling: signals, queries, child
workflows, cron-scheduled workflows, run-level timeouts.

## License

MIT
