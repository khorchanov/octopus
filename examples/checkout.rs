//! Run against a local Postgres (see docker-compose.yml):
//!
//!   docker-compose up -d
//!   DATABASE_URL=postgres://octopus:octopus@localhost:5432/octopus cargo run --example checkout

use std::time::Duration;

use async_trait::async_trait;
use octopus::{Client, RetryPolicy, StepResult, Worker, Workflow, WorkflowContext, WorkflowError};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;

#[derive(Debug, Serialize, Deserialize)]
struct CheckoutInput {
    order_id: String,
    amount_cents: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct CheckoutOutput {
    order_id: String,
    receipt: String,
}

struct CheckoutWorkflow;

#[async_trait]
impl Workflow for CheckoutWorkflow {
    type Input = CheckoutInput;
    type Output = CheckoutOutput;

    fn name(&self) -> &str {
        "checkout"
    }

    async fn run(
        &self,
        ctx: &WorkflowContext,
        input: Self::Input,
    ) -> Result<Self::Output, WorkflowError> {
        let charge_id = ctx
            .step(
                "charge_card",
                RetryPolicy::exponential(5, Duration::from_secs(1)),
                || charge_card(input.amount_cents),
            )
            .await?;

        ctx.step("reserve_inventory", RetryPolicy::none(), || reserve_inventory(&input.order_id))
            .await?;

        // Give the customer a window to cancel before we ship, without
        // holding a worker task blocked for the whole hour.
        ctx.sleep("cooldown", Duration::from_secs(1)).await?;

        let receipt = ctx
            .step("send_receipt", RetryPolicy::exponential(3, Duration::from_millis(500)), || {
                send_receipt(&input.order_id, &charge_id)
            })
            .await?;

        Ok(CheckoutOutput {
            order_id: input.order_id,
            receipt,
        })
    }
}

async fn charge_card(amount_cents: u64) -> StepResult<String> {
    println!("charging card for {amount_cents} cents");
    Ok(format!("ch_{amount_cents}"))
}

async fn reserve_inventory(order_id: &str) -> StepResult<()> {
    println!("reserving inventory for order {order_id}");
    Ok(())
}

async fn send_receipt(order_id: &str, charge_id: &str) -> StepResult<String> {
    println!("emailing receipt for order {order_id} (charge {charge_id})");
    Ok(format!("receipt-{order_id}"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://octopus:octopus@localhost:5432/octopus".to_string());
    let pool = PgPoolOptions::new().connect(&database_url).await?;

    let client = Client::new(pool.clone());
    client.migrate().await?;

    let run_id = client
        .start(
            "checkout",
            &CheckoutInput {
                order_id: "order-123".to_string(),
                amount_cents: 4999,
            },
        )
        .await?;
    println!("started run {run_id}");

    let worker = Worker::new(pool).register(CheckoutWorkflow).concurrency(4);
    tokio::spawn(worker.run());

    let output: CheckoutOutput = client.result(run_id).await?;
    println!("workflow completed: {output:?}");

    Ok(())
}
