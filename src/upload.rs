use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures_util::stream;
use tokio::task::JoinSet;

use crate::config;

/// Spawns `streams` parallel upload tasks. Each task posts a streamed body
/// of zeroed chunks; bytes are counted as the body stream emits them. This
/// is what fast.com / librespeed do too: it tracks application-side push
/// rate, which under steady-state matches the wire rate within one TCP
/// send-buffer worth of error.
///
/// We enforce the deadline by wrapping the join in `tokio::time::timeout`,
/// which drops in-flight uploads cleanly instead of waiting for them to
/// drain (important on slow links where a single request could outlast the
/// whole phase budget).
///
/// Returns: (total_bytes, requests_completed, errors)
pub async fn run(
    client: &reqwest::Client,
    counter: Arc<AtomicU64>,
    duration: Duration,
    streams: usize,
) -> Result<(u64, u64, u64)> {
    let url = config::upload_url();

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
                let body_stream = build_body_stream(counter.clone(), total_bytes.clone());
                let resp = client
                    .post(&url)
                    .header("content-type", "application/octet-stream")
                    .header("content-length", config::UPLOAD_CHUNK_BYTES.to_string())
                    .body(reqwest::Body::wrap_stream(body_stream))
                    .send()
                    .await;
                match resp {
                    Ok(r) => {
                        let _ = r.bytes().await;
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    let _ = tokio::time::timeout(duration, async {
        while set.join_next().await.is_some() {}
    })
    .await;
    set.shutdown().await;

    Ok((
        total_bytes.load(Ordering::Relaxed),
        requests.load(Ordering::Relaxed),
        errors.load(Ordering::Relaxed),
    ))
}

fn build_body_stream(
    counter: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
) -> impl futures_util::Stream<Item = std::io::Result<Bytes>> + Send + 'static {
    // 256 KiB chunks match typical kernel TCP send-buffer sizes more closely
    // than 64 KiB. Larger chunks mean hyper pulls less aggressively, so the
    // byte counter tracks the wire rate more faithfully.
    let chunk: Bytes = Bytes::from(vec![0u8; 256 * 1024]);
    let max_total = config::UPLOAD_CHUNK_BYTES;
    let state = (0usize, chunk);
    stream::unfold(state, move |(sent, chunk)| {
        let counter = counter.clone();
        let total_bytes = total_bytes.clone();
        async move {
            if sent >= max_total {
                return None;
            }
            // Yield so hyper can interleave socket writes between chunks.
            // Without this, the unfold can produce several chunks per poll.
            tokio::task::yield_now().await;
            let n = chunk.len() as u64;
            counter.fetch_add(n, Ordering::Relaxed);
            total_bytes.fetch_add(n, Ordering::Relaxed);
            Some((Ok(chunk.clone()), (sent + chunk.len(), chunk)))
        }
    })
}
