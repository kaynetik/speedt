mod cli;
mod config;
mod metadata;
mod latency;
mod sampler;
mod download;
mod upload;
mod stats;
mod report;
mod session;
mod ui;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command, ResolvedUiMode};
use tracing_subscriber::EnvFilter;

/// Bus capacity. At 100 ms sampling that's > 100 s of headroom, so a slow
/// subscriber transient stall won't cause `Lagged` for the live TUI.
const UI_BUS_CAPACITY: usize = 1024;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    let client = build_client()?;

    match cli.resolved_ui() {
        ResolvedUiMode::Tui => run_with_tui(&client, cli).await,
        ResolvedUiMode::Plain => run_plain(&client, cli).await,
    }
}

async fn run_plain(client: &reqwest::Client, cli: Cli) -> Result<()> {
    match cli.command {
        Command::Quick(opts) => session::run_quick(client, opts, cli.json, None, None).await,
        Command::Deep(opts) => session::run_deep(client, opts, cli.json, None, None).await,
        Command::Latency(opts) => {
            session::run_latency_only(client, opts, cli.json, None, None).await
        }
        Command::Info => session::run_info(client, cli.json).await,
    }
}

async fn run_with_tui(client: &reqwest::Client, cli: Cli) -> Result<()> {
    // `Info` doesn't produce streaming events, so don't bring up the TUI for it.
    if matches!(cli.command, Command::Info) {
        return run_plain(client, cli).await;
    }

    let (tx, rx) = tokio::sync::broadcast::channel::<ui::UiEvent>(UI_BUS_CAPACITY);
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let tui = tokio::spawn(ui::tui::run(rx, cancel_tx));

    let session_result = match cli.command {
        Command::Quick(opts) => {
            session::run_quick(client, opts, cli.json, Some(&tx), Some(cancel_rx)).await
        }
        Command::Deep(opts) => {
            session::run_deep(client, opts, cli.json, Some(&tx), Some(cancel_rx)).await
        }
        Command::Latency(opts) => {
            session::run_latency_only(client, opts, cli.json, Some(&tx), Some(cancel_rx)).await
        }
        Command::Info => unreachable!("handled above"),
    };

    // Closing the bus signals the TUI to drain and exit (if the user hasn't
    // already pressed `q`, in which case the TUI is already gone).
    drop(tx);
    let _ = tui.await;
    session_result
}

fn init_tracing(verbose: u8) {
    let level = match verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("speed_tester={level},reqwest=warn")));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .try_init();
}

fn build_client() -> Result<reqwest::Client> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("speedt/", env!("CARGO_PKG_VERSION")))
        .pool_max_idle_per_host(0)
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(600))
        .build()?;
    Ok(client)
}
