use anyhow::{Context, Result, bail};
use log::{debug, info};
use reqwest::blocking::Client;
use std::collections::HashSet;
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
    ads: Vec<AdNotice>,
}

#[derive(Debug)]
struct MediaSegment {
    uri: Url,
    sequence: u64,
    duration: f64,
    prefetch: bool,
    ad: bool,
    discontinuity: bool,
}

#[derive(Debug)]
struct AdNotice {
    id: String,
    duration: Option<f64>,
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
) -> Result<()> {
    let mut last_sequence: Option<u64> = None;
    let mut current_url = media_url.clone();
    let mut logged_ads: HashSet<String> = HashSet::new();
    let mut ad_hold = false;
    let mut had_content = false;
    let mut consecutive_errors = 0u32;

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
        let playlist = match parse_media_playlist(&playlist_url, &body, low_latency) {
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

        let mut wrote_segment = false;

        for ad in &playlist.ads {
            if logged_ads.insert(ad.id.clone()) {
                if let Some(duration) = ad.duration {
                    info!(
                        "Detected advertisement break of {} second{}",
                        duration.ceil() as u64,
                        if duration.ceil() as u64 == 1 { "" } else { "s" },
                    );
                } else {
                    info!("Detected advertisement break");
                }
            }
        }

        let playlist_all_ads =
            !playlist.segments.is_empty() && playlist.segments.iter().all(|s| s.ad);
        if playlist_all_ads || !playlist.ads.is_empty() {
            if !ad_hold {
                info!("Filtering out segments and pausing stream output");
            }
            ad_hold = true;
        } else if ad_hold && playlist.segments.iter().any(|s| !s.ad) {
            info!("Resuming stream output");
            ad_hold = false;
            // skip past ad sequences
            if let Some(max_seq) = playlist.segments.iter().map(|s| s.sequence).max() {
                last_sequence = Some(max_seq);
            }
        }

        for segment in playlist.segments {
            if let Some(last) = last_sequence {
                if segment.sequence <= last {
                    continue;
                }
            }

            if ad_hold || segment.ad {
                // stay silent during ad segments but keep polling playlists
                if segment.discontinuity {
                    log::warn!("Encountered a stream discontinuity while filtering ads");
                }
                wrote_segment = true;
                continue;
            }

            had_content = true;

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
        let reload = if low_latency { 0.4 } else { 0.75 };
        let sleep_ms = ((playlist.target_duration * 1000.0 * reload).max(300.0)) as u64;
        std::thread::sleep(Duration::from_millis(sleep_ms));
    }

    Ok(())
}

fn parse_media_playlist(base_url: &Url, body: &str, low_latency: bool) -> Result<MediaPlaylist> {
    let mut target_duration = 4.0;
    let mut media_sequence: u64 = 0;
    let mut end_list = false;
    let mut segments = Vec::new();
    let mut pending_duration: Option<f64> = None;
    let mut pending_title: Option<String> = None;
    let mut last_duration: Option<f64> = None;
    let mut discontinuity_next = false;
    let mut ad_state: Option<AdState> = None;
    let mut ads = Vec::new();

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
            let ad_flag = consume_ad_state(&mut ad_state);
            segments.push(MediaSegment {
                uri,
                sequence,
                duration,
                prefetch: true,
                ad: ad_flag,
                discontinuity: discontinuity_next,
            });
            if discontinuity_next {
                discontinuity_next = false;
            }
            continue;
        } else if line.starts_with("#EXT-X-DATERANGE:") {
            if let Some(ad) = parse_daterange(line, last_duration, target_duration) {
                if let Some(id) = ad.id.clone() {
                    ads.push(AdNotice {
                        id,
                        duration: ad.duration,
                    });
                }
                ad_state = Some(ad);
            }
        } else if line.starts_with("#EXT-X-ENDLIST") {
            end_list = true;
        } else if line.starts_with('#') {
            continue;
        } else if let Some(duration) = pending_duration.take() {
            let uri = resolve_url(base_url, line)
                .with_context(|| format!("Resolving segment URL: {line}"))?;
            let sequence = media_sequence + segments.len() as u64;
            let mut ad_flag = consume_ad_state(&mut ad_state);
            if let Some(t) = pending_title.take() {
                let t_low = t.to_ascii_lowercase();
                if t_low.contains("amazon") || t_low.contains("stitched-ad") {
                    ad_flag = true;
                }
            }
            segments.push(MediaSegment {
                uri,
                sequence,
                duration,
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

    Ok(MediaPlaylist {
        target_duration,
        end_list,
        segments,
        ads,
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

#[derive(Debug, Clone)]
struct AdState {
    remaining_segments: u32,
    id: Option<String>,
    duration: Option<f64>,
}

fn consume_ad_state(ad_state: &mut Option<AdState>) -> bool {
    if let Some(state) = ad_state.as_mut() {
        if state.remaining_segments > 0 {
            state.remaining_segments -= 1;
        }
        if state.remaining_segments == 0 {
            *ad_state = None;
        }
        true
    } else {
        false
    }
}

fn parse_daterange(
    line: &str,
    last_duration: Option<f64>,
    target_duration: f64,
) -> Option<AdState> {
    let attrs = line.trim_start_matches("#EXT-X-DATERANGE:");
    let pairs = parse_attribute_line(attrs);
    let mut class = None;
    let mut id = None;
    let mut duration = None;
    let mut ad_id = None;

    for (k, v) in pairs {
        match k.as_str() {
            "CLASS" => class = Some(v),
            "ID" => id = Some(v),
            "DURATION" => duration = v.parse::<f64>().ok(),
            "X-TV-TWITCH-AD-COMMERCIAL-ID" | "X-TV-TWITCH-AD-ROLL-TYPE" => ad_id = Some(v),
            _ => {}
        }
    }

    let is_ad = class.as_deref() == Some("twitch-stitched-ad")
        || id
            .as_deref()
            .map(|id| id.starts_with("stitched-ad-"))
            .unwrap_or(false);

    if !is_ad {
        return None;
    }

    let seg_duration = last_duration.unwrap_or(target_duration).max(0.1);
    let remaining_segments = duration
        .map(|d| (d / seg_duration).ceil() as u32)
        .filter(|n| *n > 0)
        .unwrap_or(6);

    Some(AdState {
        remaining_segments,
        id: ad_id.or(id),
        duration,
    })
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
