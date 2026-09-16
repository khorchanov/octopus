use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::context::WorkflowContext;
use crate::error::WorkflowError;

/// A durable workflow. Implementors describe orchestration logic in
/// `run`, using [`WorkflowContext::step`] and [`WorkflowContext::sleep`]
/// for anything that must survive a crash.
///
/// `run` is re-invoked from the top on every resume; only the results of
/// `ctx.step`/`ctx.sleep` calls are memoized, so control flow between them
/// must be deterministic given the same input and the same sequence of
/// step results (see the crate-level docs for the full constraint).
#[async_trait]
pub trait Workflow: Send + Sync + 'static {
    type Input: Serialize + DeserializeOwned + Send;
    type Output: Serialize + DeserializeOwned + Send;

    /// Unique name this workflow is registered and started under.
    fn name(&self) -> &str;

    /// Version this implementation represents. A run is pinned to
    /// whichever version was latest when a worker first picked it up
    /// (see the crate-level docs on versioning), so redeploying a new
    /// version doesn't change how in-flight runs execute: keep the old
    /// `Workflow` impl registered (as a distinct type) until no runs
    /// reference its version, checkable via [`crate::Client::active_versions`].
    ///
    /// Defaults to `1`; bump it in a new impl (or override it) whenever
    /// you need to change a workflow's step sequence in a way that would
    /// break in-flight runs replaying against the old sequence.
    fn version(&self) -> u32 {
        1
    }

    async fn run(
        &self,
        ctx: &WorkflowContext,
        input: Self::Input,
    ) -> Result<Self::Output, WorkflowError>;
}

/// Object-safe, type-erased view of a [`Workflow`] used by the registry
/// and worker so heterogeneous workflow types can be dispatched by name.
#[async_trait]
pub(crate) trait DynWorkflow: Send + Sync {
    fn name(&self) -> &str;

    async fn run_json(
        &self,
        ctx: &WorkflowContext,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, WorkflowError>;
}

#[async_trait]
impl<W: Workflow> DynWorkflow for W {
    fn name(&self) -> &str {
        Workflow::name(self)
    }

    async fn run_json(
        &self,
        ctx: &WorkflowContext,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, WorkflowError> {
        let typed_input: W::Input = serde_json::from_value(input)?;
        let output = Workflow::run(self, ctx, typed_input).await?;
        Ok(serde_json::to_value(output)?)
    }
}
