use rand::Rng;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub initial_interval_ms: u64,
    pub multiplier: f64,
    pub max_interval_ms: u64,
    pub jitter_percent: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_interval_ms: 1_000,
            multiplier: 2.0,
            max_interval_ms: 60_000,
            jitter_percent: 0.25,
        }
    }
}

pub fn base_backoff_duration(retry_count: u32, policy: &RetryPolicy) -> Duration {
    let exp = policy.multiplier.powi(retry_count as i32);
    let raw = (policy.initial_interval_ms as f64) * exp;
    let bounded = raw.min(policy.max_interval_ms as f64).max(0.0);
    Duration::from_millis(bounded.round() as u64)
}

pub fn calculate_backoff(retry_count: u32, policy: &RetryPolicy) -> Duration {
    let mut rng = rand::thread_rng();
    calculate_backoff_with_rng(retry_count, policy, &mut rng)
}

pub fn calculate_backoff_with_rng<R: Rng + ?Sized>(
    retry_count: u32,
    policy: &RetryPolicy,
    rng: &mut R,
) -> Duration {
    let base_ms = base_backoff_duration(retry_count, policy).as_millis() as f64;
    let jitter_percent = policy.jitter_percent.max(0.0);
    let jitter_scalar = rng.gen_range(-jitter_percent..=jitter_percent);
    let delay = (base_ms * (1.0 + jitter_scalar)).max(0.0);
    Duration::from_millis(delay.round() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{SeedableRng, rngs::StdRng};

    #[test]
    fn base_backoff_for_retry_zero_to_five() {
        let p = RetryPolicy {
            jitter_percent: 0.0,
            ..RetryPolicy::default()
        };
        let got: Vec<u64> = (0..=5)
            .map(|i| base_backoff_duration(i, &p).as_millis() as u64)
            .collect();
        assert_eq!(got, vec![1_000, 2_000, 4_000, 8_000, 16_000, 32_000]);
    }

    #[test]
    fn backoff_is_capped_at_max_interval() {
        let p = RetryPolicy::default();
        let d = base_backoff_duration(30, &p);
        assert_eq!(d.as_millis() as u64, p.max_interval_ms);
    }

    #[test]
    fn jitter_stays_within_expected_bounds() {
        let p = RetryPolicy {
            initial_interval_ms: 1_000,
            multiplier: 2.0,
            max_interval_ms: 60_000,
            jitter_percent: 0.25,
        };
        let base = base_backoff_duration(2, &p).as_millis() as i64;
        let min = (base as f64 * 0.75).round() as i64;
        let max = (base as f64 * 1.25).round() as i64;
        let mut rng = StdRng::seed_from_u64(7);

        for _ in 0..100 {
            let d = calculate_backoff_with_rng(2, &p, &mut rng).as_millis() as i64;
            assert!(d >= min && d <= max, "delay {d} not in [{min}, {max}]");
        }
    }
}
