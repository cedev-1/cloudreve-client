//! Per-key log rate limiting: a failure that repeats (a file whose upload
//! keeps failing, an error every READ of a broken file hits) is logged
//! once per period, not once per attempt.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Most keys remembered; past it, the expired ones are forgotten.
const MAX_KEYS: usize = 4096;

#[derive(Default)]
pub(crate) struct RateLimiter {
    last: Mutex<HashMap<String, Instant>>,
}

impl RateLimiter {
    /// Whether `key` may be logged now: true at most once per `period`.
    pub(crate) fn allow(&self, key: &str, period: Duration) -> bool {
        let now = Instant::now();
        let mut last = self.last.lock().unwrap();
        if let Some(at) = last.get(key) {
            if now.duration_since(*at) < period {
                return false;
            }
        }
        if last.len() >= MAX_KEYS {
            last.retain(|_, at| now.duration_since(*at) < period);
        }
        last.insert(key.to_string(), now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_allowed_once_per_period_and_keys_are_independent() {
        let r = RateLimiter::default();
        assert!(r.allow("a.txt", Duration::from_secs(60)));
        assert!(!r.allow("a.txt", Duration::from_secs(60)));
        assert!(r.allow("b.txt", Duration::from_secs(60)));
        assert!(r.allow("a.txt", Duration::ZERO));
    }
}
