/// Cloudflare's public speed-test backend.
/// These are the same endpoints used by `speed.cloudflare.com` and the
/// `cloudflare-speedtest` family of CLIs / libraries.
pub const HOST: &str = "https://speed.cloudflare.com";

/// `?bytes=N` returns N zero-filled bytes.
pub const DOWNLOAD_PATH: &str = "/__down";
/// Accepts a body of arbitrary size; discards it.
pub const UPLOAD_PATH: &str = "/__up";
/// Returns connection metadata (colo, country, ASN, client IP, ...).
pub const TRACE_PATH: &str = "/cdn-cgi/trace";
/// Returns ISP/ASN/lat/lon as JSON.
pub const META_PATH: &str = "/meta";

#[must_use]
pub fn download_url(bytes: u64) -> String {
    format!("{HOST}{DOWNLOAD_PATH}?bytes={bytes}")
}

#[must_use]
pub fn upload_url() -> String {
    format!("{HOST}{UPLOAD_PATH}")
}

#[must_use]
pub fn trace_url() -> String {
    format!("{HOST}{TRACE_PATH}")
}

#[must_use]
pub fn meta_url() -> String {
    format!("{HOST}{META_PATH}")
}

/// Tiny payload for latency probes (we still want a real HTTP round-trip).
pub const LATENCY_BYTES: u64 = 0;

/// Per-stream chunk size for download requests. Cloudflare caps `__down` at
/// a few hundred MB; we re-issue requests as streams complete.
pub const DOWNLOAD_CHUNK_BYTES: u64 = 25 * 1024 * 1024;
/// Per-stream chunk size for upload requests. Kept small enough that we
/// re-issue often (good rate samples) and so post-deadline drain is bounded.
pub const UPLOAD_CHUNK_BYTES: usize = 4 * 1024 * 1024;
