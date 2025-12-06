use anyhow::{Context, Result, bail};
use log::{debug, info};
use reqwest::blocking::Client;
use std::io::Write;
use std::time::Duration;
use url::Url;

#[derive(Debug, Clone)]
pub struct StreamVariant {
    pub label: String,
    pub aliases: Vec<String>,
    pub bandwidth: u64,
    pub resolution: Option<(u64, u64)>,
    pub frame_rate: Option<f64>,
    pub uri: Url,
    pub is_audio_only: bool,
}

#[derive(Debug)]
struct MediaPlaylist {
    target_duration: f64,
    end_list: bool,
    segments: Vec<MediaSegment>,
}

#[derive(Debug)]
struct MediaSegment {
    uri: Url,
    sequence: u64,
    duration: f64,
}

pub fn parse_master_playlist(base_url: &Url, body: &str) -> Result<Vec<StreamVariant>> {
    let mut variants = Vec::new();
    let mut pending_attrs: Option<Vec<(String, String)>> = None;

    for line in body.lines().map(str::trim) {
        if line.starts_with("#EXT-X-STREAM-INF:") {
            let attrs = parse_attribute_line(line.trim_start_matches("#EXT-X-STREAM-INF:"));
            pending_attrs = Some(attrs);
            continue;
        }

        if let Some(attrs) = pending_attrs.take() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let uri = resolve_url(base_url, line)
                .with_context(|| format!("Resolving stream URI from master playlist: {line}"))?;

            let mut bandwidth = 0;
            let mut resolution = None;
            let mut frame_rate = None;
            let mut name = None;
            let mut audio_only = false;

            for (key, value) in attrs {
                match key.as_str() {
                    "BANDWIDTH" => bandwidth = value.parse().unwrap_or(0),
                    "AVERAGE-BANDWIDTH" if bandwidth == 0 => bandwidth = value.parse().unwrap_or(0),
                    "RESOLUTION" => resolution = parse_resolution(&value),
                    "FRAME-RATE" => frame_rate = value.parse().ok(),
                    "NAME" => name = Some(value),
                    "VIDEO" if name.is_none() => name = Some(value),
                    "AUDIO" if value.contains("audio") => audio_only = true,
                    _ => {}
                }
            }

            if resolution.is_none() && name.as_deref() == Some("audio_only") {
                audio_only = true;
            }

            let (label, mut aliases) =
                build_labels(name.as_deref(), resolution, frame_rate, audio_only);
            if bandwidth == 0 && !audio_only {
                // fall back to rough estimate based on height
                if let Some((_, h)) = resolution {
                    bandwidth = h * 1000;
                }
            }

            aliases.sort();
            aliases.dedup();

            variants.push(StreamVariant {
                label,
                aliases,
                bandwidth,
                resolution,
                frame_rate,
                uri,
                is_audio_only: audio_only,
            });
        }
    }

    if variants.is_empty() {
        bail!("No playable variants found in playlist");
    }

    Ok(variants)
}

pub fn stream_to_writer(
    client: &Client,
    media_url: &Url,
    writer: &mut dyn Write,
    is_live: bool,
) -> Result<()> {
    let mut last_sequence: Option<u64> = None;
    let mut current_url = media_url.clone();

    loop {
        let response = client
            .get(current_url.clone())
            .send()
            .context("Failed to fetch media playlist")?
            .error_for_status()
            .context("Media playlist request failed")?;

        let playlist_url = response.url().clone();
        let body = response.text().context("Reading media playlist failed")?;
        let playlist = parse_media_playlist(&playlist_url, &body)?;

        let mut wrote_segment = false;

        for segment in playlist.segments {
            if let Some(last) = last_sequence {
                if segment.sequence <= last {
                    continue;
                }
            }

            debug!(
                "Downloading segment {} ({}s) {}",
                segment.sequence, segment.duration, segment.uri
            );
            let mut segment_response = client
                .get(segment.uri.clone())
                .send()
                .with_context(|| format!("Requesting segment {}", segment.uri))?
                .error_for_status()
                .with_context(|| format!("Segment download failed: {}", segment.uri))?;

            std::io::copy(&mut segment_response, writer)
                .context("Writing segment to output failed")?;
            writer.flush().ok();
            last_sequence = Some(segment.sequence);
            wrote_segment = true;
        }

        if playlist.end_list && !is_live {
            info!("End of VOD reached");
            break;
        }

        if !is_live && !wrote_segment {
            // VOD without end marker, break after one empty reload
            break;
        }

        current_url = playlist_url;
        let sleep_ms = ((playlist.target_duration * 750.0).max(500.0)) as u64;
        std::thread::sleep(Duration::from_millis(sleep_ms));
    }

    Ok(())
}

fn parse_media_playlist(base_url: &Url, body: &str) -> Result<MediaPlaylist> {
    let mut target_duration = 4.0;
    let mut media_sequence: u64 = 0;
    let mut end_list = false;
    let mut segments = Vec::new();
    let mut pending_duration: Option<f64> = None;

    for line in body.lines().map(str::trim) {
        if line.starts_with("#EXT-X-TARGETDURATION:") {
            if let Some(value) = line.split_once(':').map(|(_, v)| v) {
                if let Ok(parsed) = value.parse::<f64>() {
                    target_duration = parsed;
                }
            }
        } else if line.starts_with("#EXT-X-MEDIA-SEQUENCE:") {
            if let Some(value) = line.split_once(':').map(|(_, v)| v) {
                if let Ok(parsed) = value.parse::<u64>() {
                    media_sequence = parsed;
                }
            }
        } else if line.starts_with("#EXTINF:") {
            let value = line.trim_start_matches("#EXTINF:");
            let duration_part = value.split(',').next().unwrap_or(value);
            pending_duration = duration_part.parse::<f64>().ok();
        } else if line.starts_with("#EXT-X-TWITCH-PREFETCH:") {
            // ignore prefetch segments for now
            continue;
        } else if line.starts_with("#EXT-X-ENDLIST") {
            end_list = true;
        } else if line.starts_with('#') {
            continue;
        } else if let Some(duration) = pending_duration.take() {
            let uri = resolve_url(base_url, line)
                .with_context(|| format!("Resolving segment URL: {line}"))?;
            let sequence = media_sequence + segments.len() as u64;
            segments.push(MediaSegment {
                uri,
                sequence,
                duration,
            });
        }
    }

    if segments.is_empty() {
        bail!("No segments found in media playlist");
    }

    Ok(MediaPlaylist {
        target_duration,
        end_list,
        segments,
    })
}

fn resolve_url(base: &Url, input: &str) -> Result<Url> {
    if let Ok(url) = Url::parse(input) {
        return Ok(url);
    }

    base.join(input).context("Failed to resolve relative URL")
}

fn parse_attribute_line(value: &str) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in value.chars() {
        match ch {
            ',' if !in_quotes => {
                if !current.is_empty() {
                    pairs.push(current.trim().to_string());
                    current.clear();
                }
            }
            '"' => {
                in_quotes = !in_quotes;
                current.push(ch);
            }
            _ => current.push(ch),
        }
    }

    if !current.is_empty() {
        pairs.push(current.trim().to_string());
    }

    pairs
        .into_iter()
        .filter_map(|pair| {
            pair.split_once('=').map(|(k, v)| {
                let val = v.trim().trim_matches('"').to_string();
                (k.trim().to_string(), val)
            })
        })
        .collect()
}

fn parse_resolution(value: &str) -> Option<(u64, u64)> {
    let (w, h) = value.split_once('x')?;
    let width = w.parse().ok()?;
    let height = h.parse().ok()?;
    Some((width, height))
}

fn build_labels(
    name: Option<&str>,
    resolution: Option<(u64, u64)>,
    frame_rate: Option<f64>,
    audio_only: bool,
) -> (String, Vec<String>) {
    let mut aliases = Vec::new();

    if let Some(name) = name {
        aliases.push(name.to_lowercase());
    }

    let resolution_label = resolution.map(|(_, height)| {
        let suffix = if frame_rate.map(|fr| fr >= 59.5).unwrap_or(false) {
            "60"
        } else {
            ""
        };
        let label = format!("{height}p{suffix}");
        aliases.push(label.to_lowercase());
        label
    });

    if audio_only {
        aliases.push("audio_only".into());
        aliases.push("audio".into());
    }

    let primary = name
        .map(|n| n.to_string())
        .or(resolution_label)
        .unwrap_or_else(|| {
            if audio_only {
                "audio_only".into()
            } else {
                "unknown".into()
            }
        });

    aliases.push(primary.to_lowercase());
    aliases.sort();
    aliases.dedup();

    (primary, aliases)
}
