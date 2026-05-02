use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};

use crate::cli::{DeepOpts, LatencyOpts, QuickOpts};
use crate::report::{LatencyReport, PhaseReport, SessionReport};
use crate::sampler::{Sample, Sampler};
use crate::stats::{BufferbloatGrade, LatencySummary, Summary, bytes_to_mbps};
use crate::ui::PhaseKind;
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
) -> Result<()> {
    let started_at = chrono::Utc::now();
    let pb = make_spinner(&format!("latency: {} probes", opts.probes));
    let md = metadata::fetch(client).await?;
    let samples = latency::measure(
        client,
        opts.probes,
        Duration::from_millis(opts.spacing_ms),
        None,
    )
    .await?;
    pb.finish_and_clear();

    let idle = LatencySummary::from_micros(&samples);
    let rep = SessionReport {
        mode: "latency",
        started_at,
        ended_at: chrono::Utc::now(),
        metadata: md,
        latency: LatencyReport {
            idle: Some(idle),
            loaded_download: None,
            loaded_upload: None,
            bufferbloat_download: None,
            bufferbloat_upload: None,
        },
        download: None,
        upload: None,
    };
    if json {
        report::print_json(&rep)?;
    } else {
        report::print_human(&rep);
    }
    Ok(())
}

pub async fn run_quick(client: &reqwest::Client, opts: QuickOpts, json: bool) -> Result<()> {
    let started_at = chrono::Utc::now();
    let md = metadata::fetch(client).await?;

    let pb = make_spinner("idle latency");
    let idle_samples = latency::measure(
        client,
        opts.latency_probes,
        Duration::from_millis(PROBE_SPACING_MS),
        None,
    )
    .await?;
    pb.finish_and_clear();
    let idle = LatencySummary::from_micros(&idle_samples);

    let pb = make_spinner(&format!("download {}s @ {} streams", opts.download_secs, opts.streams));
    let download = run_phase(
        client,
        "download",
        Duration::from_secs(opts.download_secs),
        opts.streams,
        100,
        PhaseKind::Download,
    )
    .await?;
    pb.finish_and_clear();

    let upload = if opts.no_upload {
        None
    } else {
        let pb = make_spinner(&format!("upload {}s @ {} streams", opts.upload_secs, opts.streams));
        let r = run_phase(
            client,
            "upload",
            Duration::from_secs(opts.upload_secs),
            opts.streams,
            100,
            PhaseKind::Upload,
        )
        .await?;
        pb.finish_and_clear();
        Some(r)
    };

    let rep = SessionReport {
        mode: "quick",
        started_at,
        ended_at: chrono::Utc::now(),
        metadata: md,
        latency: LatencyReport {
            idle: Some(idle),
            loaded_download: None,
            loaded_upload: None,
            bufferbloat_download: None,
            bufferbloat_upload: None,
        },
        download: Some(download),
        upload,
    };

    if json {
        report::print_json(&rep)?;
    } else {
        report::print_human(&rep);
    }
    Ok(())
}

pub async fn run_deep(client: &reqwest::Client, opts: DeepOpts, json: bool) -> Result<()> {
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

    let pb = make_spinner(&format!("idle latency (start, ~{}s)", idle_each.as_secs()));
    let idle_start = latency::measure(
        client,
        probes_per_idle,
        Duration::from_millis(PROBE_SPACING_MS),
        None,
    )
    .await?;
    pb.finish_and_clear();

    let pb = make_spinner(&format!(
        "download {:.0}s @ {} streams (sampling every {}ms)",
        dl_dur.as_secs_f64(),
        opts.streams,
        opts.sample_ms
    ));
    let (download, loaded_dl_samples) = run_phase_with_loaded_latency(
        client,
        "download",
        dl_dur,
        opts.streams,
        opts.sample_ms,
        PhaseKind::Download,
        !opts.no_bufferbloat,
    )
    .await?;
    pb.finish_and_clear();

    let (upload, loaded_ul_samples) = if opts.no_upload {
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
        )
        .await?;
        pb.finish_and_clear();
        (Some(p), s)
    };

    let pb = make_spinner(&format!("idle latency (end, ~{}s)", idle_each.as_secs()));
    let idle_end = latency::measure(
        client,
        probes_per_idle,
        Duration::from_millis(PROBE_SPACING_MS),
        None,
    )
    .await?;
    pb.finish_and_clear();

    let mut idle_all = idle_start;
    idle_all.extend(idle_end);
    let idle = LatencySummary::from_micros(&idle_all);

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

    let bb_dl = match (&loaded_dl, idle.p50_ms) {
        (Some(l), idle_p50) if idle_p50 > 0.0 => {
            Some(BufferbloatGrade::from_added((l.p50_ms - idle_p50).max(0.0)))
        }
        _ => None,
    };
    let bb_ul = match (&loaded_ul, idle.p50_ms) {
        (Some(l), idle_p50) if idle_p50 > 0.0 => {
            Some(BufferbloatGrade::from_added((l.p50_ms - idle_p50).max(0.0)))
        }
        _ => None,
    };

    let rep = SessionReport {
        mode: "deep",
        started_at,
        ended_at: chrono::Utc::now(),
        metadata: md,
        latency: LatencyReport {
            idle: Some(idle),
            loaded_download: loaded_dl,
            loaded_upload: loaded_ul,
            bufferbloat_download: bb_dl,
            bufferbloat_upload: bb_ul,
        },
        download: Some(download),
        upload,
    };

    if json {
        report::print_json(&rep)?;
    } else {
        report::print_human(&rep);
    }
    Ok(())
}

async fn run_phase(
    client: &reqwest::Client,
    label: &'static str,
    duration: Duration,
    streams: usize,
    sample_ms: u64,
    kind: PhaseKind,
) -> Result<PhaseReport> {
    let sampler = Sampler::start(sample_ms);
    let counter = sampler.counter();
    let (total_bytes, requests, errors) = match kind {
        PhaseKind::Download => download::run(client, counter, duration, streams).await?,
        PhaseKind::Upload => upload::run(client, counter, duration, streams).await?,
    };
    let (timeline, _final_bytes, elapsed) = sampler.stop().await;
    Ok(build_phase_report(label, total_bytes, requests, errors, elapsed, timeline))
}

async fn run_phase_with_loaded_latency(
    client: &reqwest::Client,
    label: &'static str,
    duration: Duration,
    streams: usize,
    sample_ms: u64,
    kind: PhaseKind,
    measure_loaded: bool,
) -> Result<(PhaseReport, Vec<u64>)> {
    let sampler = Sampler::start(sample_ms);
    let counter = sampler.counter();

    let cancel = Arc::new(AtomicBool::new(false));

    // Spawn loaded-latency probe in the background; it runs against the same
    // server using a separate connection so it sees the queue depth that
    // bulk transfers create.
    let loaded_handle = if measure_loaded {
        let probe_client = client.clone();
        let cancel_c = cancel.clone();
        Some(tokio::spawn(async move {
            // Skip the first 2 seconds to avoid TCP/TLS warmup contamination.
            tokio::time::sleep(Duration::from_secs(2)).await;
            let mut samples = Vec::with_capacity(256);
            let stop_at = std::time::Instant::now()
                + duration.saturating_sub(Duration::from_secs(3));
            while std::time::Instant::now() < stop_at
                && !cancel_c.load(std::sync::atomic::Ordering::Relaxed)
            {
                let t = std::time::Instant::now();
                let url = crate::config::download_url(crate::config::LATENCY_BYTES);
                if let Ok(resp) = probe_client.get(url).send().await {
                    let _ = resp.bytes().await;
                    samples.push(t.elapsed().as_micros() as u64);
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            samples
        }))
    } else {
        None
    };

    let (total_bytes, requests, errors) = match kind {
        PhaseKind::Download => download::run(client, counter, duration, streams).await?,
        PhaseKind::Upload => upload::run(client, counter, duration, streams).await?,
    };

    cancel.store(true, std::sync::atomic::Ordering::Relaxed);
    let (timeline, _final_bytes, elapsed) = sampler.stop().await;
    let loaded = match loaded_handle {
        Some(h) => h.await.unwrap_or_default(),
        None => Vec::new(),
    };

    Ok((
        build_phase_report(label, total_bytes, requests, errors, elapsed, timeline),
        loaded,
    ))
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
    let post_warmup: Vec<&Sample> = timeline.iter().filter(|s| s.t_secs >= warmup_secs).collect();
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
