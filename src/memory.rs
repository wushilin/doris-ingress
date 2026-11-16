use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::config::MemoryConfig;
use crate::error::{Error, Result};

/// Tracks buffered bytes against the node's memory target and decides how long a writer waits.
///
/// Below the soft watermark writes are admitted at once. Between soft and hard they are paced
/// with a delay that grows linearly. At the hard watermark they wait for memory to be released,
/// and fail with [`Error::Backpressure`] if none is released in time.
pub(crate) struct Governor {
    used: AtomicUsize,
    soft: usize,
    hard: usize,
    max_pacing_delay: Duration,
    max_wait: Duration,
    released: Notify,
}

impl Governor {
    pub fn new(cfg: &MemoryConfig) -> Self {
        let hard = cfg.target_bytes.max(1);
        let soft = ((hard as f64) * cfg.soft_watermark.clamp(0.0, 1.0)) as usize;
        Self {
            used: AtomicUsize::new(0),
            soft: soft.min(hard),
            hard,
            max_pacing_delay: cfg.max_pacing_delay,
            max_wait: cfg.max_admission_wait,
            released: Notify::new(),
        }
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
        let used = self.used();
        if used < self.soft {
            return Ok(());
        }
        if used < self.hard {
            let span = (self.hard - self.soft).max(1) as f64;
            let fraction = (used - self.soft) as f64 / span;
            tokio::time::sleep(self.max_pacing_delay.mul_f64(fraction)).await;
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
