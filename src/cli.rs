use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "speedt",
    version,
    about = "fast.com-style speed + quality CLI",
    long_about = "Measures internet download/upload throughput, latency, jitter, and bufferbloat \
                  using Cloudflare's public speed-test backend. Supports a quick fast.com-style \
                  session and a deep 5-minute precise session."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Increase verbosity (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    pub verbose: u8,

    /// Emit a single JSON document instead of a human report
    #[arg(long, global = true)]
    pub json: bool,

    /// UI mode: `auto` picks `tui` when stdout+stderr are TTYs, else `plain`.
    /// `--json` always forces `plain`.
    #[arg(long, value_enum, default_value_t = UiMode::Auto, global = true)]
    pub ui: UiMode,

    /// Debug-only: panic shortly after the TUI is up so the terminal-restore
    /// hook can be exercised end-to-end. Hidden from --help.
    #[cfg(debug_assertions)]
    #[arg(long, global = true, hide = true)]
    pub panic_test: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum UiMode {
    /// Pick `tui` if stdout and stderr are both TTYs, otherwise `plain`.
    Auto,
    /// Force the live terminal UI.
    Tui,
    /// Plain line-oriented output suitable for pipes and logs.
    Plain,
}

/// Concrete UI mode after resolving `auto` and `--json` overrides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedUiMode {
    Tui,
    Plain,
}

impl Cli {
    /// Resolve the effective UI mode. `--json` forces `Plain`; `Auto` becomes
    /// `Tui` only when both stdout and stderr are attached to a terminal.
    pub fn resolved_ui(&self) -> ResolvedUiMode {
        if self.json {
            return ResolvedUiMode::Plain;
        }
        match self.ui {
            UiMode::Tui => ResolvedUiMode::Tui,
            UiMode::Plain => ResolvedUiMode::Plain,
            UiMode::Auto => {
                use is_terminal::IsTerminal;
                if std::io::stdout().is_terminal() && std::io::stderr().is_terminal() {
                    ResolvedUiMode::Tui
                } else {
                    ResolvedUiMode::Plain
                }
            }
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Quick fast.com-style session (~20-30s)
    Quick(QuickOpts),
    /// Deep 5-minute precise session with full quality diagnostics
    Deep(DeepOpts),
    /// Latency / jitter only (idle ping test)
    Latency(LatencyOpts),
    /// Print connection metadata (IP, ISP, ASN, colo) and exit
    Info,
}

#[derive(Debug, Args, Clone)]
pub struct QuickOpts {
    /// Total wall-clock seconds for download phase
    #[arg(long, default_value_t = 12)]
    pub download_secs: u64,
    /// Total wall-clock seconds for upload phase
    #[arg(long, default_value_t = 8)]
    pub upload_secs: u64,
    /// Number of latency probes (idle)
    #[arg(long, default_value_t = 20)]
    pub latency_probes: u32,
    /// Parallel streams
    #[arg(long, default_value_t = 6)]
    pub streams: usize,
    /// Skip upload phase
    #[arg(long)]
    pub no_upload: bool,
}

#[derive(Debug, Args, Clone)]
pub struct DeepOpts {
    /// Total session duration (e.g. "5m", "300s"). Phases share this budget.
    #[arg(long, default_value = "5m", value_parser = parse_duration)]
    pub duration: std::time::Duration,
    /// Parallel streams during throughput phases
    #[arg(long, default_value_t = 8)]
    pub streams: usize,
    /// Number of unloaded latency probes (start + end)
    #[arg(long, default_value_t = 60)]
    pub latency_probes: u32,
    /// Sampling interval in milliseconds for throughput timeline
    #[arg(long, default_value_t = 100)]
    pub sample_ms: u64,
    /// Skip upload phase
    #[arg(long)]
    pub no_upload: bool,
    /// Skip loaded-latency / bufferbloat measurement
    #[arg(long)]
    pub no_bufferbloat: bool,
}

#[derive(Debug, Args, Clone)]
pub struct LatencyOpts {
    /// Number of probes
    #[arg(long, default_value_t = 50)]
    pub probes: u32,
    /// Inter-probe spacing in milliseconds
    #[arg(long, default_value_t = 100)]
    pub spacing_ms: u64,
}

fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    humantime::parse_duration(s).map_err(|e| e.to_string())
}
