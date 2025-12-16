use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::TwitchTarget;

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct CacheFile {
    access_tokens: Vec<TokenEntry>,
    manifests: Vec<ManifestEntry>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TokenEntry {
    kind: String,
    key: String,
    signature: String,
    value: String,
    expires_at: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ManifestEntry {
    key: String,
    url: String,
    stored_at: u64,
}

const CACHE_TTL_TOKEN: u64 = 5 * 60; // 5 minutes
const CACHE_TTL_MANIFEST: u64 = 5 * 60; // 5 minutes

pub struct Cache {
    path: PathBuf,
    data: CacheFile,
}

impl Cache {
    pub fn new() -> Result<Self> {
        let path = dirs::cache_dir()
            .unwrap_or_else(std::env::temp_dir)
            .join("fors")
            .join("twitch_cache.json");

        let data = fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<CacheFile>(&bytes).ok())
            .unwrap_or_default();

        Ok(Cache { path, data })
    }

    pub fn load_token(&self, target: &TwitchTarget) -> Option<(String, String)> {
        let (kind, key) = cache_key(target)?;
        let now = now_secs();
        self.data
            .access_tokens
            .iter()
            .find(|entry| entry.kind == kind && entry.key == key && entry.expires_at > now)
            .map(|entry| (entry.signature.clone(), entry.value.clone()))
    }

    pub fn store_token(
        &self,
        target: &TwitchTarget,
        token: &crate::providers::twitch::AccessToken,
    ) {
        if let Some((kind, key)) = cache_key(target) {
            let mut data = self.data.clone();
            let expires_at = now_secs() + CACHE_TTL_TOKEN;
            data.access_tokens
                .retain(|entry| !(entry.kind == kind && entry.key == key));
            data.access_tokens.push(TokenEntry {
                kind,
                key,
                signature: token.signature.clone(),
                value: token.value.clone(),
                expires_at,
            });
            let _ = persist(&self.path, &data);
        }
    }

    pub fn load_manifest_url(&self, target: &TwitchTarget) -> Option<String> {
        let (_, key) = cache_key(target)?;
        let now = now_secs();
        self.data
            .manifests
            .iter()
            .find(|entry| entry.key == key && entry.stored_at + CACHE_TTL_MANIFEST > now)
            .map(|entry| entry.url.clone())
    }

    pub fn store_manifest_url(&self, target: &TwitchTarget, url: &str) {
        let (_, key) = match cache_key(target) {
            Some(val) => val,
            None => return,
        };
        let mut data = self.data.clone();
        let stored_at = now_secs();
        data.manifests.retain(|entry| entry.key != key);
        data.manifests.push(ManifestEntry {
            key,
            url: url.to_string(),
            stored_at,
        });
        let _ = persist(&self.path, &data);
    }
}

fn persist(path: &PathBuf, data: &CacheFile) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, serde_json::to_vec(data)?)?;
    fs::rename(tmp, path)?;
    Ok(())
}

fn cache_key(target: &TwitchTarget) -> Option<(String, String)> {
    match target {
        TwitchTarget::Live { channel } => Some(("live".into(), channel.to_lowercase())),
        TwitchTarget::Vod { id } => Some(("vod".into(), id.clone())),
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}
