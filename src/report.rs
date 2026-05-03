use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_FULL};
use serde::Serialize;

use crate::metadata::Metadata;
use crate::sampler::Sample;
use crate::stats::{BufferbloatGrade, LatencySummary, Summary};

#[derive(Debug, Clone, Serialize)]
pub struct PhaseReport {
    pub label: &'static str,
    pub duration_secs: f64,
    pub total_bytes: u64,
    pub requests: u64,
    pub errors: u64,
    pub mean_mbps: f64,
    pub timeline_summary: Summary,
    pub timeline: Vec<Sample>,
    pub stable_mbps: Option<f64>,
    pub time_to_p90_secs: Option<f64>,
    pub time_to_saturation_secs: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyReport {
    pub idle: Option<LatencySummary>,
    pub loaded_download: Option<LatencySummary>,
    pub loaded_upload: Option<LatencySummary>,
    pub bufferbloat_download: Option<BufferbloatGrade>,
    pub bufferbloat_upload: Option<BufferbloatGrade>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionReport {
    pub mode: &'static str,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub ended_at: chrono::DateTime<chrono::Utc>,
    pub metadata: Metadata,
    pub latency: LatencyReport,
    pub download: Option<PhaseReport>,
    pub upload: Option<PhaseReport>,
}

pub fn print_human(rep: &SessionReport) {
    print_metadata(&rep.metadata);
    if rep.latency.idle.is_some()
        || rep.latency.loaded_download.is_some()
        || rep.latency.loaded_upload.is_some()
    {
        print_latency(&rep.latency);
    }
    if let Some(d) = &rep.download {
        print_phase(d);
    }
    if let Some(u) = &rep.upload {
        print_phase(u);
    }
    if rep.download.is_some() || rep.upload.is_some() || rep.latency.idle.is_some() {
        print_summary_card(rep);
    }
}

pub fn print_json(rep: &SessionReport) -> anyhow::Result<()> {
    let s = serde_json::to_string_pretty(rep)?;
    println!("{s}");
    Ok(())
}

fn print_metadata(md: &Metadata) {
    let mut t = Table::new();
    t.load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![Cell::new("Connection"), Cell::new("Value")]);

    let na = || "-".to_string();
    t.add_row(vec!["Client IP", md.client_ip.as_deref().unwrap_or("-")]);
    let asn = md
        .asn
        .map(|a| format!("AS{a}"))
        .unwrap_or_else(na);
    let org = md.as_organization.clone().unwrap_or_else(na);
    t.add_row(vec!["ISP / ASN", &format!("{org} ({asn})")]);
    let geo = format!(
        "{}, {}, {}",
        md.city.as_deref().unwrap_or("-"),
        md.region.as_deref().unwrap_or("-"),
        md.country.as_deref().unwrap_or("-")
    );
    t.add_row(vec!["Geo", &geo]);
    t.add_row(vec!["Cloudflare colo", md.colo.as_deref().unwrap_or("-")]);
    t.add_row(vec!["HTTP", md.http_protocol.as_deref().unwrap_or("-")]);
    t.add_row(vec!["TLS", md.tls.as_deref().unwrap_or("-")]);

    println!("\n{t}");
}

fn print_latency(lat: &LatencyReport) {
    let mut t = Table::new();
    t.load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("Latency"),
            Cell::new("count"),
            Cell::new("min"),
            Cell::new("p50"),
            Cell::new("p95"),
            Cell::new("p99"),
            Cell::new("max"),
            Cell::new("jitter"),
        ]);
    let row = |label: &str, s: &Option<LatencySummary>| -> Vec<String> {
        match s {
            Some(s) => vec![
                label.to_string(),
                s.count.to_string(),
                fmt_ms(s.min_ms),
                fmt_ms(s.p50_ms),
                fmt_ms(s.p95_ms),
                fmt_ms(s.p99_ms),
                fmt_ms(s.max_ms),
                fmt_ms(s.jitter_ms),
            ],
            None => vec![
                label.to_string(),
                "-".into(),
                "-".into(),
                "-".into(),
                "-".into(),
                "-".into(),
                "-".into(),
                "-".into(),
            ],
        }
    };
    t.add_row(row("idle", &lat.idle));
    t.add_row(row("under download", &lat.loaded_download));
    t.add_row(row("under upload", &lat.loaded_upload));
    println!("\n{t}");

    if let Some(b) = lat.bufferbloat_download {
        println!(
            "  Bufferbloat (download): +{:.1} ms  →  grade {}",
            b.added_latency_ms, b.grade
        );
    }
    if let Some(b) = lat.bufferbloat_upload {
        println!(
            "  Bufferbloat (upload):   +{:.1} ms  →  grade {}",
            b.added_latency_ms, b.grade
        );
    }
}

fn print_phase(p: &PhaseReport) {
    let mut t = Table::new();
    t.load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![Cell::new(p.label), Cell::new("Mbps")]);
    t.add_row(vec!["mean", &fmt_mbps(p.mean_mbps)]);
    t.add_row(vec!["p50", &fmt_mbps(p.timeline_summary.p50)]);
    t.add_row(vec!["p75", &fmt_mbps(p.timeline_summary.p75)]);
    t.add_row(vec!["p90", &fmt_mbps(p.timeline_summary.p90)]);
    t.add_row(vec!["p95", &fmt_mbps(p.timeline_summary.p95)]);
    t.add_row(vec!["p99", &fmt_mbps(p.timeline_summary.p99)]);
    t.add_row(vec!["max", &fmt_mbps(p.timeline_summary.max)]);
    t.add_row(vec!["min", &fmt_mbps(p.timeline_summary.min)]);
    t.add_row(vec!["stdev", &fmt_mbps(p.timeline_summary.stdev)]);
    if let Some(s) = p.stable_mbps {
        t.add_row(vec!["stable (last 50%)", &fmt_mbps(s)]);
    }
    if let Some(s) = p.time_to_saturation_secs {
        t.add_row(vec!["time to saturation", &format!("{s:.2} s")]);
    }
    if let Some(s) = p.time_to_p90_secs {
        t.add_row(vec!["time to 90% peak", &format!("{s:.2} s")]);
    }
    t.add_row(vec!["bytes", &fmt_bytes(p.total_bytes)]);
    t.add_row(vec!["duration", &format!("{:.2} s", p.duration_secs)]);
    t.add_row(vec!["requests issued", &p.requests.to_string()]);
    t.add_row(vec!["errors", &p.errors.to_string()]);

    println!("\n{t}");
}

/// Pure-data view of the headline numbers shown in the bottom summary card.
/// Shared between the plain renderer and the TUI's results screen so they
/// stay byte-for-byte identical.
pub struct SummaryCard {
    pub dl_mean_mbps: f64,
    pub ul_mean_mbps: f64,
    pub idle_p50_ms: f64,
    pub bufferbloat: String,
}

pub fn summary_card(rep: &SessionReport) -> SummaryCard {
    SummaryCard {
        dl_mean_mbps: rep.download.as_ref().map(|d| d.mean_mbps).unwrap_or(0.0),
        ul_mean_mbps: rep.upload.as_ref().map(|u| u.mean_mbps).unwrap_or(0.0),
        idle_p50_ms: rep
            .latency
            .idle
            .as_ref()
            .map(|s| s.p50_ms)
            .unwrap_or(0.0),
        bufferbloat: rep
            .latency
            .bufferbloat_download
            .map(|b| format!("+{:.0} ms ({})", b.added_latency_ms, b.grade))
            .unwrap_or_else(|| "-".to_string()),
    }
}

/// Format the summary card as a single line — matches the bottom card of the
/// human renderer (without the leading newline, so callers can place it).
pub fn format_summary_card(card: &SummaryCard) -> String {
    format!(
        "  ⇣ {:>8.2} Mbps   ⇡ {:>8.2} Mbps   ping {:>5.1} ms   bufferbloat {}",
        card.dl_mean_mbps, card.ul_mean_mbps, card.idle_p50_ms, card.bufferbloat
    )
}

fn print_summary_card(rep: &SessionReport) {
    println!("\n{}", format_summary_card(&summary_card(rep)));
}

fn fmt_mbps(v: f64) -> String {
    format!("{v:.2} Mbps")
}

fn fmt_ms(v: f64) -> String {
    format!("{v:.2} ms")
}

fn fmt_bytes(b: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{:.2} {}", v, UNITS[i])
}
