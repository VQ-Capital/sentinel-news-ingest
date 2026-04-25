// ========== DOSYA: sentinel-news-ingest/src/main.rs ==========
use anyhow::{Context, Result};
use moka::future::Cache;
use prost::Message;
use reqwest::header::{ETAG, IF_NONE_MATCH};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{info, warn};

pub mod sentinel_protos {
    pub mod market {
        include!(concat!(env!("OUT_DIR"), "/sentinel.market.v1.rs"));
    }
}
use sentinel_protos::market::RawNewsEvent;

// -----------------------------------------------------------------------------
// 🛡️ DEDUPLICATION ENGINE (Jaccard Fuzzy Matching + SHA256 Exact)
// -----------------------------------------------------------------------------
struct NewsGuard {
    seen_cache: Cache<String, bool>,
    fuzzy_window: RwLock<VecDeque<HashSet<u64>>>,
    etags: RwLock<HashMap<String, String>>, // YENİ: ETag Hafızası
}

impl NewsGuard {
    fn new() -> Self {
        Self {
            seen_cache: Cache::builder()
                .max_capacity(5000)
                .time_to_live(Duration::from_secs(12 * 3600))
                .build(),
            fuzzy_window: RwLock::new(VecDeque::with_capacity(500)),
            etags: RwLock::new(HashMap::new()),
        }
    }

    fn tokenize_to_hashes(text: &str) -> HashSet<u64> {
        text.to_lowercase()
            .split_whitespace()
            .map(|word| {
                let clean_word = word.trim_matches(|c: char| !c.is_alphanumeric());
                let mut hasher = DefaultHasher::new();
                clean_word.hash(&mut hasher);
                hasher.finish()
            })
            .filter(|&h| h != 0)
            .collect()
    }

    fn jaccard_similarity(set1: &HashSet<u64>, set2: &HashSet<u64>) -> f64 {
        let intersection = set1.intersection(set2).count();
        let union = set1.len() + set2.len() - intersection;
        if union == 0 {
            return 0.0;
        }
        intersection as f64 / union as f64
    }

    async fn is_new_and_unique(&self, headline: &str) -> bool {
        let mut hasher = Sha256::new();
        hasher.update(headline.as_bytes());
        let exact_hash = format!("{:x}", hasher.finalize());

        if self.seen_cache.contains_key(&exact_hash) {
            return false;
        }

        let incoming_tokens = Self::tokenize_to_hashes(headline);

        if incoming_tokens.len() > 4 {
            let window = self.fuzzy_window.read().await;
            for existing_tokens in window.iter() {
                let similarity = Self::jaccard_similarity(&incoming_tokens, existing_tokens);
                if similarity > 0.75 {
                    warn!("🗑️ [FUZZY-DROP] Spam/Clickbait engellendi: {}", headline);
                    return false;
                }
            }
        }

        self.seen_cache.insert(exact_hash, true).await;

        let mut window_write = self.fuzzy_window.write().await;
        if window_write.len() == 500 {
            window_write.pop_back();
        }
        window_write.push_front(incoming_tokens);

        true
    }
}

// -----------------------------------------------------------------------------
// 🧹 DATA CLEANING
// -----------------------------------------------------------------------------
fn clean_text(raw: &str) -> String {
    raw.replace(char::is_control, "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

// -----------------------------------------------------------------------------
// 📡 SOURCE HANDLERS (ETag Optimized HFT Polling)
// -----------------------------------------------------------------------------
async fn fetch_rss_source(
    client: &reqwest::Client,
    name: &str,
    url: &str,
    nats: &async_nats::Client,
    guard: &Arc<NewsGuard>,
) -> Result<()> {
    let mut request = client.get(url);

    // 1. ETag kontrolü: Eğer daha önce ETag aldıysak header'a ekle
    {
        let etags_read = guard.etags.read().await;
        if let Some(etag) = etags_read.get(url) {
            request = request.header(IF_NONE_MATCH, etag);
        }
    }

    let response = request.send().await?;

    // 2. Eğer içerik değişmediyse (304 Not Modified), işlemi iptal et (Zero-Byte transfer)
    if response.status() == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(());
    }

    // 3. Yeni ETag varsa hafızaya kaydet
    if let Some(etag_header) = response.headers().get(ETAG) {
        if let Ok(etag_str) = etag_header.to_str() {
            guard
                .etags
                .write()
                .await
                .insert(url.to_string(), etag_str.to_string());
        }
    }

    // İçerik parse edilir
    let bytes = response.bytes().await?;
    let channel = rss::Channel::read_from(&bytes[..])
        .map_err(|e| anyhow::anyhow!("RSS Parse Error for {}: {}", name, e))?;

    for item in channel.items() {
        let title = item.title().unwrap_or("No Title");
        let content = item.description().unwrap_or("");

        if guard.is_new_and_unique(title).await {
            let event = RawNewsEvent {
                source: name.to_string(),
                headline: clean_text(title),
                content: clean_text(content),
                timestamp: chrono::Utc::now().timestamp_millis(),
            };

            let mut buf = Vec::new();
            if event.encode(&mut buf).is_ok() {
                nats.publish(format!("news.raw.{}", name), buf.into())
                    .await?;
                info!(
                    "🔥 [NEWS-INGEST] New feed from {}: {}",
                    name, event.headline
                );
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!(
        "📡 Service: {} | Version: {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let nats_client = async_nats::connect(&nats_url)
        .await
        .context("CRITICAL: NATS omurgasına bağlanılamadı.")?;

    let guard = Arc::new(NewsGuard::new());

    // YENİ: Tarayıcı taklidi yapan kalıcı HFT HTTP Client
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent(format!(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 {}/{}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION")
        ))
        .build()?;

    let sources = vec![
        (
            "coindesk",
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
        ),
        ("cointelegraph", "https://cointelegraph.com/rss"),
        ("cryptonews", "https://cryptonews.com/news/feed/"),
    ];

    loop {
        for (name, url) in &sources {
            let nats_clone = nats_client.clone();
            let name_clone = name.to_string();
            let url_clone = url.to_string();
            let guard_clone = guard.clone();

            if let Err(e) = fetch_rss_source(
                &http_client,
                &name_clone,
                &url_clone,
                &nats_clone,
                &guard_clone,
            )
            .await
            {
                warn!("⚠️ Source failure ({}): {}", name_clone, e);
            }
        }
        sleep(Duration::from_secs(60)).await;
    }
}
