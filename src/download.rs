use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use futures_util::StreamExt;
use tokio::task::JoinSet;

use crate::config;

/// Spawns `streams` parallel download tasks that keep re-requesting fixed-size
/// chunks until `duration` elapses. Bytes received are accumulated into the
/// shared `counter`, which the sampler reads on its own cadence.
///
/// We enforce the deadline by wrapping the join in `tokio::time::timeout`:
/// when the timeout fires, the spawned tasks are dropped and reqwest aborts
/// any in-flight requests. This is critical for getting a clean phase boundary
/// instead of waiting for big chunks to drain.
///
/// Returns: (total_bytes, requests_completed, errors)
pub async fn run(
    client: &reqwest::Client,
    counter: Arc<AtomicU64>,
    duration: Duration,
    streams: usize,
) -> Result<(u64, u64, u64)> {
    let url = config::download_url(config::DOWNLOAD_CHUNK_BYTES);

    let total_bytes = Arc::new(AtomicU64::new(0));
    let requests = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let mut set: JoinSet<()> = JoinSet::new();
    for _ in 0..streams {
        let client = client.clone();
        let url = url.clone();
        let counter = counter.clone();
        let total_bytes = total_bytes.clone();
        let requests = requests.clone();
        let errors = errors.clone();
        set.spawn(async move {
            loop {
                requests.fetch_add(1, Ordering::Relaxed);
                let Ok(resp) = client.get(&url).send().await else {
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                let mut stream = resp.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(b) => {
                            let n = b.len() as u64;
                            counter.fetch_add(n, Ordering::Relaxed);
                            total_bytes.fetch_add(n, Ordering::Relaxed);
                        }
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }
        });
    }

    let _ =
        tokio::time::timeout(duration, async { while set.join_next().await.is_some() {} }).await;
    set.shutdown().await;

    Ok((
        total_bytes.load(Ordering::Relaxed),
        requests.load(Ordering::Relaxed),
        errors.load(Ordering::Relaxed),
    ))
}
