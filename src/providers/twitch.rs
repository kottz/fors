use anyhow::{Context, Result, anyhow, bail};
use log::info;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::json;
use url::Url;

use super::StreamSet;
use crate::hls::parse_master_playlist;

const CLIENT_ID: &str = "kimne78kx3ncx6brgo4mv6wki5h1ko";
const GQL_ENDPOINT: &str = "https://gql.twitch.tv/gql";
// Persisted query hash used by Twitch web player (2024-12)
const PLAYBACK_HASH: &str = "ed230aa1e33e07eebb8928504583da78a5173989fadfb1ac94be06a04f3cdbe9";

pub enum TwitchTarget {
    Live { channel: String },
    Vod { id: String },
}

pub struct TwitchSource {
    target: TwitchTarget,
    low_latency: bool,
}

pub fn is_twitch_url(url: &Url) -> bool {
    url.host_str()
        .map(|host| host == "twitch.tv" || host.ends_with(".twitch.tv"))
        .unwrap_or(false)
}

impl TwitchSource {
    pub fn from_url(url: Url, low_latency: bool) -> Result<Self> {
        let segments: Vec<String> = url
            .path_segments()
            .map(|segments| {
                segments
                    .filter(|p| !p.is_empty())
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();

        if segments.first().map(|s| s == "videos").unwrap_or(false) {
            let id = segments
                .get(1)
                .cloned()
                .ok_or_else(|| anyhow!("Missing VOD id in URL"))?;
            Ok(TwitchSource {
                target: TwitchTarget::Vod { id },
                low_latency,
            })
        } else if let Some(channel) = segments.first() {
            Ok(TwitchSource {
                target: TwitchTarget::Live {
                    channel: channel.clone(),
                },
                low_latency,
            })
        } else {
            bail!("Invalid Twitch URL: {}", url);
        }
    }

    pub fn load_streams(&self, client: &Client) -> Result<StreamSet> {
        let token = self.fetch_access_token(client)?;
        let manifest_url = self.build_manifest_url(&token)?;

        let response = client
            .get(manifest_url.clone())
            .header("Client-ID", CLIENT_ID)
            .send()
            .context("Failed to request Twitch master playlist")?
            .error_for_status()
            .context("Twitch returned an error for the playlist request")?;

        let playlist_url = response.url().clone();
        let body = response
            .text()
            .context("Failed to read Twitch playlist body")?;
        let variants = parse_master_playlist(&playlist_url, &body)?;

        info!("Will skip Twitch ad segments");
        if self.low_latency {
            info!("Low latency streaming (prefetch segments enabled)");
        }

        let is_live = matches!(self.target, TwitchTarget::Live { .. });
        Ok(StreamSet {
            variants,
            is_live,
            low_latency: self.low_latency,
        })
    }

    fn fetch_access_token(&self, client: &Client) -> Result<AccessToken> {
        let variables = match &self.target {
            TwitchTarget::Live { channel } => json!({
                "isLive": true,
                "login": channel,
                "isVod": false,
                "vodID": "",
                "playerType": "embed",
                "platform": "site",
            }),
            TwitchTarget::Vod { id } => json!({
                "isLive": false,
                "login": "",
                "isVod": true,
                "vodID": id,
                "playerType": "embed",
                "platform": "site",
            }),
        };

        let payload = json!({
            "operationName": "PlaybackAccessToken",
            "extensions": { "persistedQuery": { "version": 1, "sha256Hash": PLAYBACK_HASH } },
            "variables": variables,
        });

        info!("Requesting Twitch access token");
        let response = client
            .post(GQL_ENDPOINT)
            .header("Client-ID", CLIENT_ID)
            .json(&payload)
            .send()
            .context("Failed to request Twitch access token")?
            .error_for_status()
            .context("Twitch returned an error while getting an access token")?;

        let value: serde_json::Value = response
            .json()
            .context("Could not parse Twitch access token response")?;

        if let Some(errors) = value.get("errors").and_then(|v| v.as_array()) {
            if let Some(msg) = errors
                .get(0)
                .and_then(|err| err.get("message").and_then(|m| m.as_str()))
            {
                bail!("Twitch API error: {msg}");
            }
        }

        if let (Some(error), Some(message)) = (
            value.get("error").and_then(|v| v.as_str()),
            value.get("message").and_then(|v| v.as_str()),
        ) {
            bail!("Twitch API error: {error}: {message}");
        }

        let data: PlaybackData = serde_json::from_value(
            value
                .get("data")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({})),
        )
        .context("Malformed Twitch access token response")?;

        match &self.target {
            TwitchTarget::Live { .. } => data
                .streamPlaybackAccessToken
                .ok_or_else(|| anyhow!("No access token returned for live channel")),
            TwitchTarget::Vod { .. } => data
                .videoPlaybackAccessToken
                .ok_or_else(|| anyhow!("No access token returned for VOD")),
        }
    }

    fn build_manifest_url(&self, token: &AccessToken) -> Result<Url> {
        let encoded = urlencoding::encode(&token.value);
        let url = match &self.target {
            TwitchTarget::Live { channel } => format!(
                "https://usher.ttvnw.net/api/channel/hls/{channel}.m3u8?sig={sig}&token={token}&allow_source=true&allow_audio_only=true&allow_spectre=true&player=twitchweb&client_id={client}{fast_bread}",
                sig = token.signature,
                token = encoded,
                client = CLIENT_ID,
                fast_bread = if self.low_latency {
                    "&fast_bread=true"
                } else {
                    ""
                },
            ),
            TwitchTarget::Vod { id } => format!(
                "https://usher.ttvnw.net/vod/{id}.m3u8?sig={sig}&token={token}&allow_source=true&allow_spectre=true&player=twitchweb&client_id={client}",
                sig = token.signature,
                token = encoded,
                client = CLIENT_ID,
            ),
        };

        Url::parse(&url).context("Failed to build Twitch manifest URL")
    }
}

#[derive(Debug, Deserialize)]
struct AccessToken {
    signature: String,
    value: String,
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
struct PlaybackData {
    #[serde(rename = "streamPlaybackAccessToken")]
    streamPlaybackAccessToken: Option<AccessToken>,
    #[serde(rename = "videoPlaybackAccessToken")]
    videoPlaybackAccessToken: Option<AccessToken>,
}
