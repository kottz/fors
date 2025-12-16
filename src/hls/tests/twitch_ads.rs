use crate::hls::twitch_policy::TwitchHlsPolicy;
use url::Url;

#[test]
fn daterange_is_recorded_for_logging() {
    let mut policy = TwitchHlsPolicy::new();

    policy.on_daterange(&[
        ("CLASS".into(), "twitch-stitched-ad".into()),
        ("ID".into(), "stitched-ad-1".into()),
    ]);

    assert_eq!(
        policy.last_daterange,
        Some((Some("stitched-ad-1".into()), None))
    );

    let is_ad = policy.classify_segment(
        &Url::parse("https://example.com/seg.ts").unwrap(),
        Some("Amazon Ad"),
        false,
    );
    assert!(is_ad);
}

#[test]
fn prefetch_classification_does_not_mutate_state() {
    let policy = TwitchHlsPolicy::new();

    let is_ad = policy.classify_segment(
        &Url::parse("https://example.com/stitched-ad-prefetch.ts").unwrap(),
        None,
        true,
    );

    assert!(is_ad);
    assert!(policy.last_daterange.is_none());
}

#[test]
fn title_detection_marks_ad_without_daterange() {
    let policy = TwitchHlsPolicy::new();

    let is_ad = policy.classify_segment(
        &Url::parse("https://example.com/seg.ts").unwrap(),
        Some("Amazon Ad Spot"),
        false,
    );

    assert!(is_ad);
}
