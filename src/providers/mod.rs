use anyhow::{Result, bail};
use reqwest::blocking::Client;
use url::Url;

use crate::hls::StreamVariant;

pub mod twitch;
pub mod youtube;

pub struct StreamSet {
    pub variants: Vec<StreamVariant>,
    pub is_live: bool,
}

pub enum Provider {
    Twitch(twitch::TwitchSource),
    YouTube(youtube::YouTubeSource),
}

impl Provider {
    pub fn from_url(input: &str) -> Result<Self> {
        let url = Url::parse(input)?;

        if twitch::is_twitch_url(&url) {
            let source = twitch::TwitchSource::from_url(url)?;
            Ok(Provider::Twitch(source))
        } else if youtube::is_youtube_url(&url) {
            let source = youtube::YouTubeSource::from_url(url)?;
            Ok(Provider::YouTube(source))
        } else {
            bail!("Unsupported URL: {input}");
        }
    }

    pub fn load_streams(&self, client: &Client) -> Result<StreamSet> {
        match self {
            Provider::Twitch(src) => src.load_streams(client),
            Provider::YouTube(src) => src.load_streams(client),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Provider::Twitch(_) => "twitch",
            Provider::YouTube(_) => "youtube",
        }
    }
}
