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

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    let client = build_client()?;

    match cli.command {
        Command::Quick(opts) => session::run_quick(&client, opts, cli.json).await,
        Command::Deep(opts) => session::run_deep(&client, opts, cli.json).await,
        Command::Latency(opts) => session::run_latency_only(&client, opts, cli.json).await,
        Command::Info => session::run_info(&client, cli.json).await,
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
