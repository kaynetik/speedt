use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::watch;

use crate::cli::{DeepOpts, LatencyOpts, QuickOpts};
use crate::report::{LatencyReport, PhaseReport, SessionReport};
use crate::sampler::{Sample, Sampler};
use crate::stats::{BufferbloatGrade, LatencySummary, Summary, bytes_to_mbps};
use crate::ui::{PhaseKind, ProbeKind, UiEvent, UiEventTx};
use crate::{download, latency, metadata, report, upload};

const PROBE_SPACING_MS: u64 = 100;

pub async fn run_info(client: &reqwest::Client, json: bool) -> Result<()> {
    let md = metadata::fetch(client).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&md)?);
    } else {
        let dummy = SessionReport {
            mode: "info",
            started_at: chrono::Utc::now(),
            ended_at: chrono::Utc::now(),
            metadata: md,
            latency: LatencyReport {
                idle: None,
                loaded_download: None,
                loaded_upload: None,
                bufferbloat_download: None,
                bufferbloat_upload: None,
            },
            download: None,
            upload: None,
        };
        report::print_human(&dummy);
    }
    Ok(())
}

pub async fn run_latency_only(
    client: &reqwest::Client,
    opts: LatencyOpts,
    json: bool,
    tx: Option<&UiEventTx>,
    cancel: Option<watch::Receiver<bool>>,
) -> Result<()> {
    let started_at = chrono::Utc::now();
    let pb = make_spinner(&format!("latency: {} probes", opts.probes));
    let md = metadata::fetch(client).await?;
    let total_planned_secs = f64::from(opts.probes) * (opts.spacing_ms as f64) / 1_000.0;
    if let Some(tx) = tx {
        let _ = tx.send(UiEvent::SessionStarted {
            mode: "latency",
            total_planned_secs,
            metadata: Box::new(md.clone()),
            started_at,
            idle_probes_planned: opts.probes,
            loaded_probes_planned: 0,
        });
    }
    let samples = latency::measure(
        client,
        opts.probes,
        Duration::from_millis(opts.spacing_ms),
        cancel.clone(),
        tx,
        ProbeKind::Idle,
    )
    .await?;
    pb.finish_and_clear();

    let idle = if samples.is_empty() {
        None
    } else {
        Some(LatencySummary::from_micros(&samples))
    };
    let rep = SessionReport {
        mode: "latency",
        started_at,
        ended_at: chrono::Utc::now(),
        metadata: md,
        latency: LatencyReport {
            idle,
            loaded_download: None,
            loaded_upload: None,
            bufferbloat_download: None,
            bufferbloat_upload: None,
        },
        download: None,
        upload: None,
    };
    if let Some(tx) = tx {
        let _ = tx.send(UiEvent::SessionFinished(Box::new(rep.clone())));
    }
    if json {
        report::print_json(&rep)?;
    } else if tx.is_none() {
        report::print_human(&rep);
    }
    Ok(())
}

pub async fn run_quick(
    client: &reqwest::Client,
    opts: QuickOpts,
    json: bool,
    tx: Option<&UiEventTx>,
    cancel: Option<watch::Receiver<bool>>,
) -> Result<()> {
    let started_at = chrono::Utc::now();
    let md = metadata::fetch(client).await?;

    let idle_planned_secs = f64::from(opts.latency_probes) * (PROBE_SPACING_MS as f64) / 1_000.0;
    let dl_planned_secs = opts.download_secs as f64;
    let ul_planned_secs = if opts.no_upload {
        0.0
    } else {
        opts.upload_secs as f64
    };
    if let Some(tx) = tx {
        let _ = tx.send(UiEvent::SessionStarted {
            mode: "quick",
            total_planned_secs: idle_planned_secs + dl_planned_secs + ul_planned_secs,
            metadata: Box::new(md.clone()),
            started_at,
            idle_probes_planned: opts.latency_probes,
            loaded_probes_planned: 0,
        });
    }

    let pb = make_spinner("idle latency");
    let idle_samples = latency::measure(
        client,
        opts.latency_probes,
        Duration::from_millis(PROBE_SPACING_MS),
        cancel.clone(),
        tx,
        ProbeKind::Idle,
    )
    .await?;
    pb.finish_and_clear();
    let idle = if idle_samples.is_empty() {
        None
    } else {
        Some(LatencySummary::from_micros(&idle_samples))
    };

    let download = if cancelled(cancel.as_ref()) {
        None
    } else {
        let pb = make_spinner(&format!(
            "download {}s @ {} streams",
            opts.download_secs, opts.streams
        ));
        let r = run_phase(
            client,
            "download",
            Duration::from_secs(opts.download_secs),
            opts.streams,
            100,
            PhaseKind::Download,
            tx,
            cancel.clone(),
        )
        .await?;
        pb.finish_and_clear();
        r
    };

    let upload = if opts.no_upload || cancelled(cancel.as_ref()) {
        None
    } else {
        let pb = make_spinner(&format!(
            "upload {}s @ {} streams",
            opts.upload_secs, opts.streams
        ));
        let r = run_phase(
            client,
            "upload",
            Duration::from_secs(opts.upload_secs),
            opts.streams,
            100,
            PhaseKind::Upload,
            tx,
            cancel.clone(),
        )
        .await?;
        pb.finish_and_clear();
        r
    };

    let rep = SessionReport {
        mode: "quick",
        started_at,
        ended_at: chrono::Utc::now(),
        metadata: md,
        latency: LatencyReport {
            idle,
            loaded_download: None,
            loaded_upload: None,
            bufferbloat_download: None,
            bufferbloat_upload: None,
        },
        download,
        upload,
    };

    if let Some(tx) = tx {
        let _ = tx.send(UiEvent::SessionFinished(Box::new(rep.clone())));
    }

    if json {
        report::print_json(&rep)?;
    } else if tx.is_none() {
        report::print_human(&rep);
    }
    Ok(())
}

pub async fn run_deep(
    client: &reqwest::Client,
    opts: DeepOpts,
    json: bool,
    tx: Option<&UiEventTx>,
    cancel: Option<watch::Receiver<bool>>,
) -> Result<()> {
    let started_at = chrono::Utc::now();
    let md = metadata::fetch(client).await?;

    let total = opts.duration;
    // Allocate the budget. We spend ~10% on idle latency (split start/end),
    // 45% on download (with bufferbloat overlap), 45% on upload.
    let idle_each = total.mul_f64(0.05);
    let dl_dur = total.mul_f64(0.45);
    let ul_dur = if opts.no_upload {
        Duration::ZERO
    } else {
        total.mul_f64(0.45)
    };

    let probes_per_idle = opts.latency_probes / 2;

    // Loaded probes: 2s warmup skip + 3s tail skip + 250ms spacing, run during
    // each throughput phase. Reflect the engine's actual schedule so the TUI
    // fraction lines up with what eventually arrives.
    let loaded_probes_planned = if opts.no_bufferbloat {
        0
    } else {
        let est = |d: Duration| -> u32 {
            ((d.as_secs_f64() - 5.0).max(0.0) / 0.25) as u32
        };
        est(dl_dur) + est(ul_dur)
    };

    if let Some(tx) = tx {
        let _ = tx.send(UiEvent::SessionStarted {
            mode: "deep",
            total_planned_secs: total.as_secs_f64(),
            metadata: Box::new(md.clone()),
            started_at,
            idle_probes_planned: probes_per_idle * 2,
            loaded_probes_planned,
        });
    }

    let pb = make_spinner(&format!("idle latency (start, ~{}s)", idle_each.as_secs()));
    let idle_start = latency::measure(
        client,
        probes_per_idle,
        Duration::from_millis(PROBE_SPACING_MS),
        cancel.clone(),
        tx,
        ProbeKind::Idle,
    )
    .await?;
    pb.finish_and_clear();

    let (download, loaded_dl_samples) = if cancelled(cancel.as_ref()) {
        (None, Vec::new())
    } else {
        let pb = make_spinner(&format!(
            "download {:.0}s @ {} streams (sampling every {}ms)",
            dl_dur.as_secs_f64(),
            opts.streams,
            opts.sample_ms
        ));
        let (p, s) = run_phase_with_loaded_latency(
            client,
            "download",
            dl_dur,
            opts.streams,
            opts.sample_ms,
            PhaseKind::Download,
            !opts.no_bufferbloat,
            tx,
            cancel.clone(),
        )
        .await?;
        pb.finish_and_clear();
        (p, s)
    };

    let (upload, loaded_ul_samples) = if opts.no_upload || cancelled(cancel.as_ref()) {
        (None, Vec::new())
    } else {
        let pb = make_spinner(&format!(
            "upload {:.0}s @ {} streams (sampling every {}ms)",
            ul_dur.as_secs_f64(),
            opts.streams,
            opts.sample_ms
        ));
        let (p, s) = run_phase_with_loaded_latency(
            client,
            "upload",
            ul_dur,
            opts.streams,
            opts.sample_ms,
            PhaseKind::Upload,
            !opts.no_bufferbloat,
            tx,
            cancel.clone(),
        )
        .await?;
        pb.finish_and_clear();
        (p, s)
    };

    let idle_end = if cancelled(cancel.as_ref()) {
        Vec::new()
    } else {
        let pb = make_spinner(&format!("idle latency (end, ~{}s)", idle_each.as_secs()));
        let r = latency::measure(
            client,
            probes_per_idle,
            Duration::from_millis(PROBE_SPACING_MS),
            cancel.clone(),
            tx,
            ProbeKind::Idle,
        )
        .await?;
        pb.finish_and_clear();
        r
    };

    let mut idle_all = idle_start;
    idle_all.extend(idle_end);
    let idle = if idle_all.is_empty() {
        None
    } else {
        Some(LatencySummary::from_micros(&idle_all))
    };

    let loaded_dl = if loaded_dl_samples.is_empty() {
        None
    } else {
        Some(LatencySummary::from_micros(&loaded_dl_samples))
    };
    let loaded_ul = if loaded_ul_samples.is_empty() {
        None
    } else {
        Some(LatencySummary::from_micros(&loaded_ul_samples))
    };

    let bb_dl = match (idle.as_ref(), loaded_dl.as_ref()) {
        (Some(i), Some(l)) if i.p50_ms > 0.0 => {
            Some(BufferbloatGrade::from_added((l.p50_ms - i.p50_ms).max(0.0)))
        }
        _ => None,
    };
    let bb_ul = match (idle.as_ref(), loaded_ul.as_ref()) {
        (Some(i), Some(l)) if i.p50_ms > 0.0 => {
            Some(BufferbloatGrade::from_added((l.p50_ms - i.p50_ms).max(0.0)))
        }
        _ => None,
    };

    let rep = SessionReport {
        mode: "deep",
        started_at,
        ended_at: chrono::Utc::now(),
        metadata: md,
        latency: LatencyReport {
            idle,
            loaded_download: loaded_dl,
            loaded_upload: loaded_ul,
            bufferbloat_download: bb_dl,
            bufferbloat_upload: bb_ul,
        },
        download,
        upload,
    };

    if let Some(tx) = tx {
        let _ = tx.send(UiEvent::SessionFinished(Box::new(rep.clone())));
    }

    if json {
        report::print_json(&rep)?;
    } else if tx.is_none() {
        report::print_human(&rep);
    }
    Ok(())
}

/// Run a throughput phase. Returns `Some(report)` when the phase completed
/// (or its deadline elapsed). Returns `None` only if the user cancelled
/// before any work happened, so the caller can omit the phase from the
/// session report rather than emit a near-empty placeholder.
#[allow(clippy::too_many_arguments)]
async fn run_phase(
    client: &reqwest::Client,
    label: &'static str,
    duration: Duration,
    streams: usize,
    sample_ms: u64,
    kind: PhaseKind,
    tx: Option<&UiEventTx>,
    cancel: Option<watch::Receiver<bool>>,
) -> Result<Option<PhaseReport>> {
    if cancelled(cancel.as_ref()) {
        return Ok(None);
    }
    if let Some(tx) = tx {
        let _ = tx.send(UiEvent::PhaseStarted {
            kind,
            label,
            planned_secs: duration.as_secs_f64(),
        });
    }
    let sampler = Sampler::start(sample_ms, tx.cloned());
    let counter = sampler.counter();

    let phase_fut = async {
        match kind {
            PhaseKind::Download => download::run(client, counter, duration, streams).await,
            PhaseKind::Upload => upload::run(client, counter, duration, streams).await,
        }
    };

    let outcome = race_with_cancel(phase_fut, cancel.clone()).await;
    let (timeline, final_bytes, elapsed) = sampler.stop().await;

    let (total_bytes, requests, errors) = match outcome {
        Some(res) => res?,
        None => (final_bytes, 0, 0),
    };

    let report = build_phase_report(label, total_bytes, requests, errors, elapsed, timeline);
    if let Some(tx) = tx {
        let _ = tx.send(UiEvent::PhaseFinished(report.clone()));
    }
    Ok(Some(report))
}

#[allow(clippy::too_many_arguments)]
async fn run_phase_with_loaded_latency(
    client: &reqwest::Client,
    label: &'static str,
    duration: Duration,
    streams: usize,
    sample_ms: u64,
    kind: PhaseKind,
    measure_loaded: bool,
    tx: Option<&UiEventTx>,
    cancel: Option<watch::Receiver<bool>>,
) -> Result<(Option<PhaseReport>, Vec<u64>)> {
    if cancelled(cancel.as_ref()) {
        return Ok((None, Vec::new()));
    }
    if let Some(tx) = tx {
        let _ = tx.send(UiEvent::PhaseStarted {
            kind,
            label,
            planned_secs: duration.as_secs_f64(),
        });
    }
    let sampler = Sampler::start(sample_ms, tx.cloned());
    let counter = sampler.counter();

    let probe_stop = Arc::new(AtomicBool::new(false));

    // Spawn loaded-latency probe in the background; it runs against the same
    // server using a separate connection so it sees the queue depth that
    // bulk transfers create.
    let loaded_handle = if measure_loaded {
        let probe_client = client.clone();
        let stop = probe_stop.clone();
        let probe_tx = tx.cloned();
        let probe_kind = match kind {
            PhaseKind::Download => ProbeKind::LoadedDownload,
            PhaseKind::Upload => ProbeKind::LoadedUpload,
        };
        Some(tokio::spawn(async move {
            // Skip the first 2 seconds to avoid TCP/TLS warmup contamination.
            tokio::time::sleep(Duration::from_secs(2)).await;
            let mut samples = Vec::with_capacity(256);
            let stop_at =
                std::time::Instant::now() + duration.saturating_sub(Duration::from_secs(3));
            while std::time::Instant::now() < stop_at && !stop.load(Ordering::Relaxed) {
                let t = std::time::Instant::now();
                let url = crate::config::download_url(crate::config::LATENCY_BYTES);
                if let Ok(resp) = probe_client.get(url).send().await {
                    let _ = resp.bytes().await;
                    let rtt_us = t.elapsed().as_micros() as u64;
                    samples.push(rtt_us);
                    if let Some(tx) = &probe_tx {
                        let _ = tx.send(UiEvent::LatencyProbe {
                            kind: probe_kind,
                            rtt_us,
                        });
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            samples
        }))
    } else {
        None
    };

    let phase_fut = async {
        match kind {
            PhaseKind::Download => download::run(client, counter, duration, streams).await,
            PhaseKind::Upload => upload::run(client, counter, duration, streams).await,
        }
    };

    let outcome = race_with_cancel(phase_fut, cancel.clone()).await;

    // Always signal the loaded-latency probe to stop, whether the phase
    // ended naturally or via user cancel.
    probe_stop.store(true, Ordering::Relaxed);

    let (timeline, final_bytes, elapsed) = sampler.stop().await;
    let loaded = match loaded_handle {
        Some(h) => h.await.unwrap_or_default(),
        None => Vec::new(),
    };

    let (total_bytes, requests, errors) = match outcome {
        Some(res) => res?,
        None => (final_bytes, 0, 0),
    };

    let report = build_phase_report(label, total_bytes, requests, errors, elapsed, timeline);
    if let Some(tx) = tx {
        let _ = tx.send(UiEvent::PhaseFinished(report.clone()));
    }
    Ok((Some(report), loaded))
}

/// Race `fut` against the cancel signal. Returns `Some(value)` if `fut`
/// completed first; `None` if cancel fired (the future is dropped, which
/// aborts any tasks it owned via `JoinSet`).
async fn race_with_cancel<F, T>(fut: F, cancel: Option<watch::Receiver<bool>>) -> Option<T>
where
    F: std::future::Future<Output = T>,
{
    match cancel {
        Some(mut c) => {
            tokio::select! {
                biased;
                _ = c.wait_for(|v| *v) => None,
                v = fut => Some(v),
            }
        }
        None => Some(fut.await),
    }
}

fn cancelled(cancel: Option<&watch::Receiver<bool>>) -> bool {
    cancel.is_some_and(|c| *c.borrow())
}

fn build_phase_report(
    label: &'static str,
    total_bytes: u64,
    requests: u64,
    errors: u64,
    elapsed: Duration,
    timeline: Vec<Sample>,
) -> PhaseReport {
    let secs = elapsed.as_secs_f64();
    let mean_mbps = bytes_to_mbps(total_bytes, secs);

    // Discard the first second of samples for percentile work: TCP slow start
    // and TLS handshake artifacts make those samples not representative of
    // steady-state throughput.
    let warmup_secs = 1.0;
    let post_warmup: Vec<&Sample> = timeline
        .iter()
        .filter(|s| s.t_secs >= warmup_secs)
        .collect();
    let mbps_samples: Vec<f64> = post_warmup.iter().map(|s| s.mbps).collect();
    let timeline_summary = Summary::from_samples(&mbps_samples);

    let stable_mbps = if post_warmup.len() >= 4 {
        let half = post_warmup.len() / 2;
        let tail: Vec<f64> = post_warmup[half..].iter().map(|s| s.mbps).collect();
        let s = Summary::from_samples(&tail);
        Some(s.mean)
    } else {
        None
    };

    let peak = timeline_summary.max;
    let time_to_p90_secs = if peak > 0.0 {
        timeline
            .iter()
            .find(|s| s.mbps >= 0.9 * peak)
            .map(|s| s.t_secs)
    } else {
        None
    };
    let time_to_saturation_secs = stable_mbps.and_then(|stable| {
        if stable <= 0.0 {
            return None;
        }
        timeline
            .iter()
            .find(|s| s.mbps >= 0.95 * stable)
            .map(|s| s.t_secs)
    });

    PhaseReport {
        label,
        duration_secs: secs,
        total_bytes,
        requests,
        errors,
        mean_mbps,
        timeline_summary,
        timeline,
        stable_mbps,
        time_to_p90_secs,
        time_to_saturation_secs,
    }
}

fn make_spinner(label: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("  {spinner} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
    );
    pb.set_message(label.to_string());
    pb.enable_steady_tick(Duration::from_millis(120));
    pb
}
