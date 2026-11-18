use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::config::MemoryConfig;
use crate::error::{Error, Result};

/// Tracks buffered bytes against the node's memory target. At the target, writes wait for memory
/// to be released and fail with [`Error::Backpressure`] if none is released in time. (The delays
/// that slow writers down before that point come from [`write_delay`].)
pub(crate) struct Governor {
    used: AtomicUsize,
    hard: usize,
    max_wait: Duration,
    released: Notify,
}

impl Governor {
    pub fn new(cfg: &MemoryConfig) -> Self {
        Self { used: AtomicUsize::new(0), hard: cfg.target_bytes.max(1), max_wait: cfg.max_admission_wait, released: Notify::new() }
    }

    /// Buffered bytes as a fraction of the node's target.
    pub fn utilization(&self) -> f64 {
        self.used() as f64 / self.hard as f64
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn add(&self, bytes: usize) {
        self.used.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
        self.released.notify_waiters();
    }

    /// Waits until a write may proceed. Admission is a gate, not a reservation: a write is let in
    /// whenever usage is below the target, so usage can overshoot by the size of in-flight writes.
    pub async fn admit(&self) -> Result<()> {
        if self.used() < self.hard {
            return Ok(());
        }
        let start = Instant::now();
        loop {
            let waited = start.elapsed();
            if waited >= self.max_wait {
                return Err(Error::Backpressure { waited });
            }
            // The short timeout covers a release that lands between the check and the wait.
            let slice = (self.max_wait - waited).min(Duration::from_millis(20));
            let _ = tokio::time::timeout(slice, self.released.notified()).await;
            if self.used() < self.hard {
                return Ok(());
            }
        }
    }
}

/// First delay, at the soft watermark.
const FIRST_DELAY: Duration = Duration::from_millis(10);
/// The delay doubles for every further step of this much utilization (in units of 0.01%).
const STEP: u64 = 500;
/// Utilization of a table's cap at which its writes are held instead of delayed. The cap is
/// therefore soft: a table can fill up to this multiple of it.
pub(crate) const HOLD_AT: f64 = 1.2;

/// How long a write is delayed at a given memory utilization (1.0 is the limit).
/// Nothing below the soft watermark; 10 ms at it; doubled for every further 5%, up to `max`.
/// With the defaults: 70% 10 ms, 75% 20 ms, 80% 40 ms, 85% 80 ms, 90% 160 ms, 95% 320 ms,
/// 100% 640 ms, 105% 1.28 s, 110% 2.56 s, 115% 5.12 s, then 10 s.
pub(crate) fn write_delay(utilization: f64, cfg: &MemoryConfig) -> Duration {
    let soft = (cfg.soft_watermark.clamp(0.0, 1.0) * 10_000.0).round() as u64;
    let used = (utilization.max(0.0) * 10_000.0).round().min(1e9) as u64;
    if used < soft {
        return Duration::ZERO;
    }
    let steps = ((used - soft) / STEP).min(20) as u32;
    (FIRST_DELAY * (1 << steps)).min(cfg.max_pacing_delay)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_delay_doubles_every_five_percent_from_the_soft_watermark() {
        let cfg = MemoryConfig::default();
        let ms = |u: f64| write_delay(u, &cfg).as_millis();
        assert_eq!([ms(0.0), ms(0.5), ms(0.699)], [0, 0, 0]);
        assert_eq!([ms(0.70), ms(0.749), ms(0.75), ms(0.80), ms(0.85), ms(0.90), ms(0.95)], [10, 10, 20, 40, 80, 160, 320]);
        assert_eq!([ms(1.0), ms(1.05), ms(1.10), ms(1.15)], [640, 1280, 2560, 5120]);
        // Never more than the configured maximum, however full.
        assert_eq!([ms(1.2), ms(5.0), ms(f64::INFINITY)], [10_000, 10_000, 10_000]);
    }
}
