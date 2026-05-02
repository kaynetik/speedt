use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::task::JoinHandle;
use tokio::time;

use crate::ui::{UiEvent, UiEventTx};

/// One throughput sample on the timeline.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Sample {
    /// Seconds elapsed since the sampler started.
    pub t_secs: f64,
    /// Instantaneous Mbps observed during this interval.
    pub mbps: f64,
    /// Cumulative bytes transferred up to this sample.
    pub cumulative_bytes: u64,
}

/// Streams a shared `AtomicU64` byte counter into a per-interval throughput
/// timeline. Spawns a tokio task; cancel via the returned `Arc<AtomicBool>`.
pub struct Sampler {
    pub bytes: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Vec<Sample>>>,
    pub started_at: Instant,
}

impl Sampler {
    pub fn start(interval_ms: u64, tx: Option<UiEventTx>) -> Self {
        let bytes = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let bytes_c = bytes.clone();
        let stop_c = stop.clone();
        let started_at = Instant::now();

        let handle = tokio::spawn(async move {
            let mut samples = Vec::with_capacity(1024);
            let mut last_bytes: u64 = 0;
            let mut last_at = Instant::now();
            let mut ticker = time::interval(Duration::from_millis(interval_ms));
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if stop_c.load(Ordering::Relaxed) {
                    break;
                }
                let now = Instant::now();
                let cur = bytes_c.load(Ordering::Relaxed);
                let delta_b = cur.saturating_sub(last_bytes);
                let delta_t = now.duration_since(last_at).as_secs_f64();
                let mbps = if delta_t > 0.0 {
                    (delta_b as f64 * 8.0) / delta_t / 1_000_000.0
                } else {
                    0.0
                };
                let sample = Sample {
                    t_secs: now.duration_since(started_at).as_secs_f64(),
                    mbps,
                    cumulative_bytes: cur,
                };
                samples.push(sample);
                if let Some(tx) = &tx {
                    let _ = tx.send(UiEvent::Throughput(sample));
                }
                last_bytes = cur;
                last_at = now;
            }
            samples
        });

        Self {
            bytes,
            stop,
            handle: Some(handle),
            started_at,
        }
    }

    pub fn counter(&self) -> Arc<AtomicU64> {
        self.bytes.clone()
    }

    pub async fn stop(mut self) -> (Vec<Sample>, u64, Duration) {
        self.stop.store(true, Ordering::Relaxed);
        let total_bytes = self.bytes.load(Ordering::Relaxed);
        let elapsed = self.started_at.elapsed();
        let samples = match self.handle.take() {
            Some(h) => h.await.unwrap_or_default(),
            None => Vec::new(),
        };
        (samples, total_bytes, elapsed)
    }
}
