//! Octopus: a Postgres-backed durable execution library.
//!
//! Workflows are plain async Rust functions that call [`WorkflowContext::step`]
//! for anything with a side effect and [`WorkflowContext::sleep`] for durable
//! timers. Progress is checkpointed to Postgres so a run survives process
//! crashes and restarts: on resume, the workflow function is replayed from
//! the top, but completed steps return their cached output instantly
//! instead of re-executing.
//!
//! This means workflow code between `step`/`sleep` calls must be
//! deterministic (same inputs + same step results -> same sequence of
//! calls) and cheap — it may run many times. Side effects belong inside a
//! `step`.
//!
//! ```ignore
//! use octopus::{Client, RetryPolicy, Worker, Workflow, WorkflowContext, WorkflowError};
//!
//! struct Checkout;
//!
//! #[async_trait::async_trait]
//! impl Workflow for Checkout {
//!     type Input = String;
//!     type Output = String;
//!     fn name(&self) -> &str { "checkout" }
//!     async fn run(&self, ctx: &WorkflowContext, order_id: String) -> Result<String, WorkflowError> {
//!         ctx.step("charge_card", RetryPolicy::exponential(5, std::time::Duration::from_secs(1)), || async move {
//!             Ok(())
//!         }).await?;
//!         Ok(order_id)
//!     }
//! }
//! ```

mod client;
mod context;
mod error;
mod retry;
mod worker;
mod workflow;

pub use client::{Client, RunStatus};
pub use context::WorkflowContext;
pub use error::{StepResult, WorkflowError};
pub use retry::RetryPolicy;
pub use worker::Worker;
pub use workflow::Workflow;
