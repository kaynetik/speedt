use hdrhistogram::Histogram;
use serde::Serialize;

/// A serializable percentile / dispersion summary.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Summary {
    pub count: u64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub stdev: f64,
    pub p50: f64,
    pub p75: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
}

impl Summary {
    pub fn from_samples(samples: &[f64]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        let mut sorted: Vec<f64> = samples.iter().copied().filter(|x| x.is_finite()).collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let count = sorted.len() as u64;
        let min = sorted.first().copied().unwrap_or(0.0);
        let max = sorted.last().copied().unwrap_or(0.0);
        let sum: f64 = sorted.iter().sum();
        let mean = sum / count as f64;
        let var: f64 = sorted.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / count as f64;
        let stdev = var.sqrt();
        Self {
            count,
            min,
            max,
            mean,
            stdev,
            p50: percentile(&sorted, 50.0),
            p75: percentile(&sorted, 75.0),
            p90: percentile(&sorted, 90.0),
            p95: percentile(&sorted, 95.0),
            p99: percentile(&sorted, 99.0),
        }
    }
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (pct / 100.0) * (sorted.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        return sorted[lo];
    }
    let frac = rank - lo as f64;
    sorted[lo] + (sorted[hi] - sorted[lo]) * frac
}

/// Latency summary expressed in milliseconds. Backed by an HdrHistogram so
/// that we can also fold long-running sessions cheaply.
#[derive(Debug, Clone, Serialize)]
pub struct LatencySummary {
    pub count: u64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub jitter_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub raw_samples_us: Vec<u64>,
}

impl LatencySummary {
    pub fn from_micros(samples: &[u64]) -> Self {
        if samples.is_empty() {
            return Self {
                count: 0,
                min_ms: 0.0,
                max_ms: 0.0,
                mean_ms: 0.0,
                jitter_ms: 0.0,
                p50_ms: 0.0,
                p90_ms: 0.0,
                p95_ms: 0.0,
                p99_ms: 0.0,
                raw_samples_us: Vec::new(),
            };
        }
        let mut hist: Histogram<u64> = Histogram::new(3).expect("histogram new");
        for &v in samples {
            let _ = hist.record(v.max(1));
        }
        let count = hist.len();
        let mean_us = hist.mean();
        let stdev_us = hist.stdev();
        Self {
            count,
            min_ms: hist.min() as f64 / 1000.0,
            max_ms: hist.max() as f64 / 1000.0,
            mean_ms: mean_us / 1000.0,
            jitter_ms: stdev_us / 1000.0,
            p50_ms: hist.value_at_quantile(0.50) as f64 / 1000.0,
            p90_ms: hist.value_at_quantile(0.90) as f64 / 1000.0,
            p95_ms: hist.value_at_quantile(0.95) as f64 / 1000.0,
            p99_ms: hist.value_at_quantile(0.99) as f64 / 1000.0,
            raw_samples_us: samples.to_vec(),
        }
    }
}

/// Waveform-style bufferbloat grading from added latency under load.
/// <https://www.waveform.com/tools/bufferbloat>
#[derive(Debug, Clone, Copy, Serialize)]
pub struct BufferbloatGrade {
    pub added_latency_ms: f64,
    pub grade: char,
}

impl BufferbloatGrade {
    pub fn from_added(added_ms: f64) -> Self {
        let grade = if added_ms < 5.0 {
            'A'
        } else if added_ms < 30.0 {
            'B'
        } else if added_ms < 60.0 {
            'C'
        } else if added_ms < 200.0 {
            'D'
        } else {
            'F'
        };
        Self {
            added_latency_ms: added_ms,
            grade,
        }
    }
}

#[must_use]
pub fn bytes_to_mbps(bytes: u64, secs: f64) -> f64 {
    if secs <= 0.0 {
        return 0.0;
    }
    (bytes as f64 * 8.0) / secs / 1_000_000.0
}
