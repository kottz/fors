use anyhow::{Context, Result, bail};
use log::{debug, info};
use reqwest::blocking::Client;
use std::io::Write;
use std::time::Duration;
use url::Url;

#[cfg(test)]
mod tests;
pub mod twitch_policy;
use crate::hls::twitch_policy::TwitchHlsPolicy;

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
pub struct MediaPlaylist {
    pub target_duration: f64,
    pub end_list: bool,
    pub segments: Vec<MediaSegment>,
    pub ads_active: bool,
    pub ad_daterange: Option<(Option<String>, Option<f64>)>,
}

#[derive(Debug)]
pub struct MediaSegment {
    pub uri: Url,
    pub init: Option<Url>,
    pub sequence: u64,
    pub duration: f64,
    pub prefetch: bool,
    pub ad: bool,
    pub discontinuity: bool,
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
    low_latency: bool,
    debug_ads: bool,
) -> Result<()> {
    let mut last_sequence: Option<u64> = None;
    let mut current_url = media_url.clone();
    let mut consecutive_errors = 0u32;
    let mut last_init: Option<Url> = None;
    let mut initial = true;
    let mut in_ads = false;
    let mut had_content = false;

    loop {
        let response = match client.get(current_url.clone()).send() {
            Ok(resp) => resp,
            Err(err) => {
                consecutive_errors += 1;
                if consecutive_errors >= 3 && had_content {
                    info!("Stream ended (failed to reload playlist after errors)");
                    break;
                }
                debug!("Failed to fetch media playlist: {err}");
                std::thread::sleep(Duration::from_millis(750));
                continue;
            }
        };

        if !response.status().is_success() {
            consecutive_errors += 1;
            if response.status().as_u16() == 404 && had_content {
                info!("Stream ended (playlist not found)");
                break;
            }
            if consecutive_errors >= 3 && had_content {
                info!("Stream ended (playlist unavailable)");
                break;
            }
            debug!(
                "Media playlist returned status {} - retrying",
                response.status()
            );
            std::thread::sleep(Duration::from_millis(750));
            continue;
        }

        consecutive_errors = 0;

        let playlist_url = response.url().clone();
        let body = response.text().context("Reading media playlist failed")?;
        let playlist = match parse_media_playlist(&playlist_url, &body, low_latency, debug_ads) {
            Ok(pl) => pl,
            Err(err) => {
                consecutive_errors += 1;
                if consecutive_errors >= 3 && had_content {
                    info!("Stream ended (unreadable playlist)");
                    break;
                }
                debug!("Failed to parse media playlist: {err}");
                std::thread::sleep(Duration::from_millis(500));
                continue;
            }
        };

        if !in_ads && playlist.ads_active {
            in_ads = true;
            if let Some((_, Some(duration))) = &playlist.ad_daterange {
                info!("Entering ad break ({}s)", duration.ceil() as u64);
            } else {
                info!("Entering ad break");
            }
        }

        if in_ads && !playlist.ads_active {
            in_ads = false;
            info!("Exiting ad break");
            if had_content {
                if let Some(max_seq) = playlist.segments.iter().map(|s| s.sequence).max() {
                    let live_edge = if low_latency { 2 } else { 3 };
                    last_sequence = Some(max_seq.saturating_sub(live_edge));
                } else {
                    last_sequence = None;
                }
                last_init = None;
            } else {
                last_sequence = None;
                last_init = None;
            }
        }

        let mut wrote_segment = false;

        // Fast-start: on first load of a live playlist, jump to the latest edge rather than older segments
        if initial && is_live {
            if let Some(max_seq) = playlist.segments.iter().map(|s| s.sequence).max() {
                let live_edge = if low_latency { 2 } else { 3 };
                last_sequence = Some(max_seq.saturating_sub(live_edge));
                debug!(
                    "Starting near live edge at sequence {} (max {})",
                    last_sequence.unwrap_or(0),
                    max_seq
                );
            }
            initial = false;
        }

        let mut warned_discontinuity = false;
        for segment in &playlist.segments {
            if segment.discontinuity && !in_ads {
                last_sequence = None;
                last_init = None;
            }

            if let Some(last) = last_sequence
                && segment.sequence <= last
            {
                continue;
            }

            if segment.ad {
                if debug_ads {
                    info!(
                        "[ads] skipping ad segment seq={}{} uri={}",
                        segment.sequence,
                        if segment.prefetch { " (prefetch)" } else { "" },
                        segment.uri
                    );
                }
                if !in_ads && segment.discontinuity && !warned_discontinuity {
                    log::warn!("Encountered a stream discontinuity while filtering ads");
                    warned_discontinuity = true;
                }
                wrote_segment = true;
                last_sequence = Some(segment.sequence);
                continue;
            }

            if let Some(init_url) = &segment.init {
                let needs_init = last_init
                    .as_ref()
                    .map(|url| url != init_url)
                    .unwrap_or(true);
                if needs_init {
                    debug!("Downloading initialization segment {}", init_url);
                    let mut init_response = client
                        .get(init_url.clone())
                        .send()
                        .with_context(|| format!("Requesting initialization segment {}", init_url))?
                        .error_for_status()
                        .with_context(|| {
                            format!("Initialization segment download failed: {}", init_url)
                        })?;
                    std::io::copy(&mut init_response, writer)
                        .context("Writing initialization segment failed")?;
                    writer.flush().ok();
                    last_init = Some(init_url.clone());
                    had_content = true;
                    wrote_segment = true;
                }
            }

            debug!(
                "Downloading segment {}{}{} ({}s) {}",
                segment.sequence,
                if segment.prefetch { " (prefetch)" } else { "" },
                if segment.discontinuity {
                    " (discontinuity)"
                } else {
                    ""
                },
                segment.duration,
                segment.uri
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
            if debug_ads {
                info!(
                    "[ads] advanced to sequence {}{}",
                    segment.sequence,
                    if segment.prefetch { " (prefetch)" } else { "" }
                );
            }
            last_sequence = Some(segment.sequence);
            if !had_content {
                had_content = true;
            }
            if !wrote_segment {
                wrote_segment = true;
            }
        }

        if playlist.end_list && !is_live {
            info!("End of VOD reached");
            break;
        }

        if !is_live && !wrote_segment {
            break;
        }

        current_url = playlist_url;
        let last_real_duration = playlist
            .segments
            .iter()
            .rev()
            .find(|s| s.duration > 0.0)
            .map(|s| s.duration);
        let reload = if in_ads {
            0.5
        } else if low_latency {
            last_real_duration.unwrap_or(playlist.target_duration)
        } else {
            playlist.target_duration * 0.75
        };
        if debug_ads {
            info!("[ads] polling every {:.3}s (ads_active={})", reload, in_ads);
        }
        let sleep_ms = (reload * 1000.0) as u64;
        std::thread::sleep(Duration::from_millis(sleep_ms));
    }

    Ok(())
}

fn parse_media_playlist(
    base_url: &Url,
    body: &str,
    low_latency: bool,
    debug_ads: bool,
) -> Result<MediaPlaylist> {
    let mut target_duration = 4.0;
    let mut media_sequence: u64 = 0;
    let mut end_list = false;
    let mut segments = Vec::new();
    let mut pending_duration: Option<f64> = None;
    let mut pending_title: Option<String> = None;
    let mut last_duration: Option<f64> = None;
    let mut discontinuity_next = false;
    let mut current_init: Option<Url> = None;
    let mut policy = TwitchHlsPolicy::new();

    for line in body.lines().map(str::trim) {
        if line.starts_with("#EXT-X-TARGETDURATION:") {
            if let Some(value) = line.split_once(':').map(|(_, v)| v)
                && let Ok(parsed) = value.parse::<f64>()
            {
                target_duration = parsed;
            }
        } else if line.starts_with("#EXT-X-MEDIA-SEQUENCE:") {
            if let Some(value) = line.split_once(':').map(|(_, v)| v)
                && let Ok(parsed) = value.parse::<u64>()
            {
                media_sequence = parsed;
            }
        } else if line.starts_with("#EXTINF:") {
            let value = line.trim_start_matches("#EXTINF:");
            let mut parts = value.splitn(2, ',');
            if let Some(duration_part) = parts.next() {
                pending_duration = duration_part.parse::<f64>().ok();
            }
            pending_title = parts
                .next()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            last_duration = pending_duration;
        } else if line.starts_with("#EXT-X-DISCONTINUITY") {
            discontinuity_next = true;
        } else if line.starts_with("#EXT-X-TWITCH-PREFETCH:") {
            if !low_latency {
                continue;
            }
            let uri = resolve_url(base_url, line.trim_start_matches("#EXT-X-TWITCH-PREFETCH:"))
                .with_context(|| format!("Resolving prefetch segment URL: {line}"))?;
            let sequence = media_sequence + segments.len() as u64;
            let duration = last_duration.unwrap_or(target_duration);
            let ad_flag = policy.classify_segment(&uri, None, true);
            if debug_ads {
                info!(
                    "[ads] segment={} classified={} prefetch=true",
                    sequence,
                    if ad_flag { "AD" } else { "CONTENT" }
                );
            }
            segments.push(MediaSegment {
                uri,
                init: current_init.clone(),
                sequence,
                duration: if ad_flag { 0.0 } else { duration },
                prefetch: true,
                ad: ad_flag,
                discontinuity: discontinuity_next,
            });
            if discontinuity_next {
                discontinuity_next = false;
            }
            continue;
        } else if line.starts_with("#EXT-X-DATERANGE:") {
            let attrs = parse_attribute_line(line.trim_start_matches("#EXT-X-DATERANGE:"));
            policy.on_daterange(&attrs);
            if debug_ads && let Some((id, duration)) = policy.last_daterange.clone() {
                match duration {
                    Some(d) => info!(
                        "[ads] playlist contains stitched ad daterange id={} duration={:.0}",
                        id.unwrap_or_else(|| "unknown".into()),
                        d
                    ),
                    None => info!(
                        "[ads] playlist contains stitched ad daterange id={} duration=unknown",
                        id.unwrap_or_else(|| "unknown".into()),
                    ),
                }
            }
        } else if line.starts_with("#EXT-X-ENDLIST") {
            end_list = true;
        } else if line.starts_with("#EXT-X-MAP:") {
            let attrs = parse_attribute_line(line.trim_start_matches("#EXT-X-MAP:"));
            if let Some((_, uri_value)) = attrs.iter().find(|(k, _)| k == "URI") {
                let map_url = resolve_url(base_url, uri_value)
                    .with_context(|| format!("Resolving init segment URL: {uri_value}"))?;
                current_init = Some(map_url);
            }
        } else if line.starts_with('#') {
            continue;
        } else if let Some(duration) = pending_duration.take() {
            let uri = resolve_url(base_url, line)
                .with_context(|| format!("Resolving segment URL: {line}"))?;
            let sequence = media_sequence + segments.len() as u64;
            let title = pending_title.take();
            let ad_flag = policy.classify_segment(&uri, title.as_deref(), false);
            if debug_ads {
                info!(
                    "[ads] segment={} classified={} prefetch=false",
                    sequence,
                    if ad_flag { "AD" } else { "CONTENT" }
                );
            }
            segments.push(MediaSegment {
                uri,
                init: current_init.clone(),
                sequence,
                duration: if ad_flag { 0.0 } else { duration },
                prefetch: false,
                ad: ad_flag,
                discontinuity: discontinuity_next,
            });
            if discontinuity_next {
                discontinuity_next = false;
            }
        }
    }

    if segments.is_empty() {
        bail!("No segments found in media playlist");
    }

    let ads_active = segments.iter().any(|s| s.ad);

    Ok(MediaPlaylist {
        target_duration,
        end_list,
        segments,
        ads_active,
        ad_daterange: policy.last_daterange,
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
