use url::Url;

#[derive(Debug, Clone, Default)]
pub struct TwitchHlsPolicy {
    pub last_daterange: Option<(Option<String>, Option<f64>)>,
}

impl TwitchHlsPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_daterange(&mut self, attrs: &[(String, String)]) {
        let mut class = None;
        let mut id: Option<String> = None;
        let mut duration = None;

        for (k, v) in attrs {
            match k.as_str() {
                "CLASS" => class = Some(v.as_str()),
                "ID" => id = Some(v.clone()),
                "DURATION" => duration = v.parse::<f64>().ok(),
                _ => {}
            }
        }

        let is_ad = class == Some("twitch-stitched-ad")
            || id
                .as_deref()
                .map(|v| v.starts_with("stitched-ad-"))
                .unwrap_or(false);

        if is_ad {
            self.last_daterange = Some((id, duration));
        }
    }

    pub fn classify_segment(&self, uri: &Url, title: Option<&str>, _is_prefetch: bool) -> bool {
        if let Some(t) = title {
            let t = t.to_ascii_lowercase();
            if t.contains("amazon") || t.contains("stitched-ad") {
                return true;
            }
        }

        uri.as_str().contains("stitched-ad")
    }
}
