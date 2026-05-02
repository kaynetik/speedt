# speedt

A Rust CLI for measuring internet speed and connection quality, modeled after
fast.com. Uses Cloudflare's public speed-test backend (`speed.cloudflare.com`),
the same one used by `cloudflare-speedtest` and `librespeed` derivatives.

Two modes:

- `quick` -- fast.com-style snapshot (~20-30s).
- `deep` -- 5-minute precise session with full quality diagnostics:
  loaded latency, bufferbloat grade, throughput percentiles, time-to-saturation,
  stability, jitter, errors.

## Build

```bash
cargo build --release
./target/release/speedt --help
```

Requires Rust 1.85+ (edition 2024).

## Usage

```bash
speedt quick                       # ~20-30s fast.com-style run
speedt deep                        # 5-minute precise run
speedt deep --duration 10m         # 10-minute precise run
speedt deep --no-upload            # download-only deep run
speedt latency --probes 100        # latency only
speedt info                        # client IP / ISP / ASN / Cloudflare colo

speedt --json deep | jq            # machine-readable output
speedt -v quick                    # verbose
```

## What it measures

### Throughput (download and upload)

- Mean, p50, p75, p90, p95, p99, max, min, stdev (per 100 ms timeline samples).
- Stable rate (mean over the last 50% of the phase, after warm-up).
- Time-to-saturation (when the rate first reaches 95% of stable).
- Time to 90% of peak.
- Total bytes transferred, requests issued, errors.

### Latency and jitter

- Idle latency: HTTP round-trip against `__down?bytes=0`. Reports
  count, min, p50, p95, p99, max, and stdev (jitter).
- Loaded latency under download and upload (deep mode).

### Bufferbloat

- Added latency under load = loaded p50 minus idle p50.
- Waveform-style grade: A (<5 ms), B (<30 ms), C (<60 ms), D (<200 ms), F.

### Connection metadata

- Client public IP, ISP, ASN.
- Geolocation (city, region, country).
- Cloudflare PoP (colo / IATA code).
- Negotiated HTTP version, TLS version.

## Output

Default: pretty tables. With `--json`: a single JSON document containing all
metadata, every timeline sample, raw latency samples (microseconds), and every
derived statistic, suitable for ingestion or trend analysis.

## Design notes

- I/O is structured with `tokio` and `reqwest` (rustls). Throughput is
  measured by parallel streaming HTTP requests against `__down` and `__up`,
  with an atomic byte counter sampled at 100 ms (configurable in deep mode).
- Phase deadlines are enforced by `tokio::time::timeout` over a `JoinSet`,
  which drops in-flight requests cleanly when the phase ends, avoiding
  the "long tail" that simpler implementations suffer from on slow links.
- Loaded-latency probes run on a separate connection alongside the bulk
  workload, skipping the first 2 s of warm-up. This is what makes
  bufferbloat detection meaningful.
- Latency stats are backed by an `HdrHistogram` so they remain accurate
  over long sessions without unbounded memory.

## Endpoints

- `https://speed.cloudflare.com/__down?bytes=N` -- download N zero bytes.
- `https://speed.cloudflare.com/__up` -- accept arbitrary POST body.
- `https://speed.cloudflare.com/meta` -- ISP / ASN / geo (with Origin header).
- `https://speed.cloudflare.com/cdn-cgi/trace` -- IP / colo / TLS / HTTP.

These are public, unauthenticated endpoints.

## License

MIT OR Apache-2.0
