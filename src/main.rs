mod cli;
mod config;
mod download;
mod latency;
mod metadata;
mod report;
mod sampler;
mod session;
mod stats;
mod ui;
mod upload;

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

    // Install the panic hook *before* spawning the TUI so a panic in the
    // spawned task — or in this one — finds the restore path already wired.
    ui::tui::install_panic_hook();

    let (tx, rx) = tokio::sync::broadcast::channel::<ui::UiEvent>(UI_BUS_CAPACITY);
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let tui = tokio::spawn(ui::tui::run(rx, cancel_tx));

    #[cfg(debug_assertions)]
    if cli.panic_test {
        // Give the TUI a moment to enter raw mode + alt screen so the panic
        // hook has something meaningful to undo when it fires.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        panic!("--panic-test: deliberate panic to exercise the terminal-restore hook");
    }

    let session_result = tokio::select! {
        // Race the session against SIGINT so the process exits as soon as the
        // user hits Ctrl-C. The TUI also listens for SIGINT and breaks its
        // loop, taking the same `leave()` restore path as `q`.
        res = run_session(client, cli.command, cli.json, &tx, cancel_rx) => res,
        _ = tokio::signal::ctrl_c() => Ok(()),
    };

    // Closing the bus signals the TUI to drain and exit (if the user hasn't
    // already pressed `q`, in which case the TUI is already gone).
    drop(tx);
    let _ = tui.await;
    session_result
}

async fn run_session(
    client: &reqwest::Client,
    command: Command,
    json: bool,
    tx: &ui::UiEventTx,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    match command {
        Command::Quick(opts) => {
            session::run_quick(client, opts, json, Some(tx), Some(cancel_rx)).await
        }
        Command::Deep(opts) => {
            session::run_deep(client, opts, json, Some(tx), Some(cancel_rx)).await
        }
        Command::Latency(opts) => {
            session::run_latency_only(client, opts, json, Some(tx), Some(cancel_rx)).await
        }
        Command::Info => unreachable!("handled by run_with_tui caller"),
    }
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
