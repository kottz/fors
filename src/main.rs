mod hls;
mod providers;

use anyhow::{Context, Result};
use clap::{ArgAction, Parser};
use env_logger::Env;
use log::{debug, info};
use providers::Provider;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use std::fs::File;
use std::io::{self, BufWriter, Write};

use crate::hls::{StreamVariant, stream_to_writer};

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "A lightweight Rust port of streamlink supporting Twitch and YouTube"
)]
struct Cli {
    /// Stream URL
    url: String,

    /// Desired quality (best, worst, or a specific label like 720p60)
    #[arg(default_value = "best")]
    quality: String,

    /// List available streams and exit
    #[arg(short, long, action = ArgAction::SetTrue)]
    list: bool,

    /// Print the selected stream URL instead of streaming
    #[arg(long, action = ArgAction::SetTrue)]
    stream_url: bool,

    /// Write stream data to a file instead of stdout
    #[arg(short, long, value_name = "FILE")]
    output: Option<String>,

    /// Override the default user agent
    #[arg(long, value_name = "AGENT")]
    user_agent: Option<String>,

    /// Enable Twitch low latency mode (prefetch HLS segments)
    #[arg(long, action = ArgAction::SetTrue)]
    twitch_low_latency: bool,

    /// Use on-disk cache to speed up startup (tokens/playlists)
    #[arg(long, action = ArgAction::SetTrue)]
    cache: bool,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().filter_or("RUST_LOG", "info"))
        .format_timestamp(None)
        .init();

    let cli = Cli::parse();
    let client = build_client(cli.user_agent.clone())?;

    let provider = Provider::from_url(&cli.url, cli.twitch_low_latency, cli.cache)?;
    info!("Selected provider: {}", provider.name());

    let streams = provider.load_streams(&client)?;
    debug!("Found {} variants from playlist", streams.variants.len());

    if cli.list {
        print_variants(&streams.variants);
        return Ok(());
    }

    let variant = select_variant(&streams.variants, &cli.quality)
        .with_context(|| format!("Quality '{}' is not available", cli.quality))?;

    if cli.stream_url {
        println!("{}", variant.uri);
        return Ok(());
    }

    let mut writer: Box<dyn Write> = match cli.output {
        Some(path) => Box::new(BufWriter::new(File::create(path)?)),
        None => Box::new(io::stdout()),
    };

    info!("Streaming {} ({})", variant.label, variant.uri);
    stream_to_writer(
        &client,
        &variant.uri,
        &mut writer,
        streams.is_live,
        streams.low_latency,
    )?;

    Ok(())
}

fn build_client(user_agent: Option<String>) -> Result<Client> {
    let mut headers = HeaderMap::new();
    let agent = user_agent.unwrap_or_else(|| "streamlink-rs/0.1".to_string());
    headers.insert(
        USER_AGENT,
        HeaderValue::from_str(&agent).context("Invalid user agent value")?,
    );

    Client::builder()
        .default_headers(headers)
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .context("Failed to build HTTP client")
}

fn select_variant<'a>(variants: &'a [StreamVariant], quality: &str) -> Option<&'a StreamVariant> {
    let q = quality.to_lowercase();
    match q.as_str() {
        "best" => variants
            .iter()
            .max_by(|a, b| a.bandwidth.cmp(&b.bandwidth))
            .or_else(|| variants.first()),
        "worst" => variants.iter().min_by(|a, b| a.bandwidth.cmp(&b.bandwidth)),
        _ => variants
            .iter()
            .find(|variant| variant.aliases.iter().any(|alias| alias == &q)),
    }
}

fn print_variants(variants: &[StreamVariant]) {
    let mut sorted = variants.to_vec();
    sorted.sort_by(|a, b| b.bandwidth.cmp(&a.bandwidth));

    println!("Available streams:");
    for variant in sorted {
        let res = if variant.is_audio_only {
            "audio".into()
        } else {
            variant
                .resolution
                .map(|(w, h)| format!("{w}x{h}"))
                .unwrap_or_else(|| "unknown".into())
        };
        let bandwidth_kbps = if variant.bandwidth > 0 {
            format!("{} kbps", variant.bandwidth / 1000)
        } else {
            "unknown".into()
        };
        let frame = variant
            .frame_rate
            .map(|fr| format!(" @ {:.0}fps", fr))
            .unwrap_or_default();

        println!(
            "- {:<10} {:<12} {}{}",
            variant.label, res, bandwidth_kbps, frame
        );
    }
}
