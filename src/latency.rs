use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::time;

use crate::config;
use crate::ui::{ProbeKind, UiEvent, UiEventTx};

/// Returns latency samples in microseconds. Each probe is a small HTTP GET
/// against `__down?bytes=0` that fully reads the response, so the sample
/// captures the full RTT including TLS negotiation reuse, write of the
/// request, and read of the (zero-byte) body.
///
/// If `cancel` is set, the measurement stops at the next probe boundary.
/// If `tx` is `Some`, each successful probe is fanned out as a
/// [`UiEvent::LatencyProbe`] tagged with `kind`.
pub async fn measure(
    client: &reqwest::Client,
    probes: u32,
    spacing: Duration,
    cancel: Option<Arc<AtomicBool>>,
    tx: Option<&UiEventTx>,
    kind: ProbeKind,
) -> Result<Vec<u64>> {
    let url = config::download_url(config::LATENCY_BYTES);
    let mut samples = Vec::with_capacity(probes as usize);
    let mut ticker = time::interval(spacing);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    for _ in 0..probes {
        if let Some(c) = &cancel
            && c.load(Ordering::Relaxed)
        {
            break;
        }
        let t = Instant::now();
        match client.get(&url).send().await {
            Ok(resp) => {
                let _ = resp.bytes().await;
                let rtt_us = t.elapsed().as_micros() as u64;
                samples.push(rtt_us);
                if let Some(tx) = tx {
                    let _ = tx.send(UiEvent::LatencyProbe { kind, rtt_us });
                }
            }
            Err(_) => continue,
        }
        ticker.tick().await;
    }
    Ok(samples)
}
