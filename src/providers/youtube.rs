use anyhow::{Context, Result, anyhow, bail};
use log::info;
use regex::Regex;
use reqwest::blocking::Client;
use url::Url;

use super::StreamSet;
use crate::hls::parse_master_playlist;

pub struct YouTubeSource {
    watch_url: Url,
}

pub fn is_youtube_url(url: &Url) -> bool {
    url.host_str()
        .map(|host| host.contains("youtube.com") || host == "youtu.be")
        .unwrap_or(false)
}

impl YouTubeSource {
    pub fn from_url(url: Url) -> Result<Self> {
        let watch_url = canonical_watch_url(&url)
            .or_else(|| {
                extract_video_id(&url).and_then(|id| {
                    Url::parse(&format!("https://www.youtube.com/watch?v={id}")).ok()
                })
            })
            .ok_or_else(|| anyhow!("Unsupported YouTube URL"))?;

        Ok(YouTubeSource { watch_url })
    }

    pub fn load_streams(&self, client: &Client) -> Result<StreamSet> {
        info!("Fetching YouTube watch page");
        let response = client
            .get(self.watch_url.clone())
            .send()
            .context("Failed to request YouTube watch page")?
            .error_for_status()
            .context("YouTube watch page request failed")?;

        let final_url = response.url().clone();
        if final_url
            .host_str()
            .map(|h| h.contains("consent.youtube.com"))
            .unwrap_or(false)
        {
            bail!(
                "YouTube returned a consent page. Try supplying cookies or running in a browser first."
            );
        }

        let body = response
            .text()
            .context("Failed to read YouTube watch page")?;
        let manifest_url = extract_manifest_url(&body)?;

        info!("Fetching YouTube HLS manifest");
        let manifest_response = client
            .get(manifest_url.clone())
            .send()
            .context("Failed to request YouTube manifest")?
            .error_for_status()
            .context("YouTube returned an error for the manifest request")?;

        let playlist_url = manifest_response.url().clone();
        let manifest_body = manifest_response
            .text()
            .context("Failed to read YouTube manifest body")?;

        let variants = parse_master_playlist(&playlist_url, &manifest_body)?;
        Ok(StreamSet {
            variants,
            is_live: true,
            low_latency: false,
        })
    }
}

fn canonical_watch_url(url: &Url) -> Option<Url> {
    let host = url.host_str()?.to_lowercase();

    if host == "youtu.be" {
        let id = url.path_segments()?.next()?.to_string();
        return Url::parse(&format!("https://www.youtube.com/watch?v={id}")).ok();
    }

    if !host.contains("youtube.com") {
        return None;
    }

    extract_video_id(url)
        .and_then(|id| Url::parse(&format!("https://www.youtube.com/watch?v={id}")).ok())
}

fn extract_video_id(url: &Url) -> Option<String> {
    if let Some(id) = url
        .query_pairs()
        .find_map(|(k, v)| if k == "v" { Some(v.to_string()) } else { None })
    {
        return Some(id);
    }

    let segments: Vec<String> = url
        .path_segments()
        .map(|segments| {
            segments
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();

    match segments.as_slice() {
        [prefix, id] if prefix == "live" || prefix == "embed" || prefix == "shorts" => {
            Some(id.to_string())
        }
        _ => None,
    }
}

fn extract_manifest_url(body: &str) -> Result<Url> {
    let re = Regex::new(r#""hlsManifestUrl":"(?P<url>[^"]+)""#).unwrap();
    let captures = re
        .captures(body)
        .ok_or_else(|| anyhow!("No HLS manifest URL found on the page (stream may be offline)"))?;

    let raw_url = captures.name("url").unwrap().as_str();
    // Decode JSON-style escaping inside the string
    let decoded: String = serde_json::from_str(&format!("\"{raw_url}\""))
        .context("Failed to decode manifest URL from page data")?;

    Url::parse(&decoded).context("Invalid YouTube manifest URL")
}
