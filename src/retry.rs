use std::time::Duration;

/// Retry policy for a single durable step.
///
/// Backoff is computed as `base * factor^(attempt - 1)`, where `attempt`
/// is the 1-based number of attempts made so far.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base: Duration,
    pub factor: f32,
}

impl RetryPolicy {
    /// No retries: a single attempt, fail immediately on error.
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            base: Duration::ZERO,
            factor: 1.0,
        }
    }

    /// Exponential backoff starting at `base`, doubling each attempt, up
    /// to `max_attempts` total attempts.
    pub fn exponential(max_attempts: u32, base: Duration) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            base,
            factor: 2.0,
        }
    }

    pub fn with_factor(mut self, factor: f32) -> Self {
        self.factor = factor;
        self
    }

    pub(crate) fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }
        let millis = self.base.as_millis() as f64 * (self.factor as f64).powi(attempt as i32 - 1);
        Duration::from_millis(millis.max(0.0) as u64)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::none()
    }
}
