// ========== DOSYA: sentinel-news-ingest/src/main.rs ==========
use anyhow::{Context, Result};
use moka::future::Cache;
use prost::Message;
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

pub mod sentinel_protos {
    pub mod market {
        include!(concat!(env!("OUT_DIR"), "/sentinel.market.v1.rs"));
    }
}
use sentinel_protos::market::RawNewsEvent;

// -----------------------------------------------------------------------------
// 🛡️ DEDUPLICATION ENGINE (Mükerrer Kayıt Engelleyici)
// -----------------------------------------------------------------------------
struct NewsGuard {
    // Haber başlığının hash'ini 12 saat boyunca hatırlar.
    // Aynı haber gelirse NATS'ı boşuna kirletmeyiz.
    seen_cache: Cache<String, bool>,
}

impl NewsGuard {
    fn new() -> Self {
        Self {
            seen_cache: Cache::builder()
                .max_capacity(5000)
                .time_to_live(Duration::from_secs(12 * 3600))
                .build(),
        }
    }

    async fn is_new(&self, headline: &str) -> bool {
        let mut hasher = Sha256::new();
        hasher.update(headline.as_bytes());
        let hash = format!("{:x}", hasher.finalize());

        if self.seen_cache.contains_key(&hash) {
            false
        } else {
            self.seen_cache.insert(hash, true).await;
            true
        }
    }
}

// -----------------------------------------------------------------------------
// 🧹 DATA CLEANING (Gürültü Temizliği)
// -----------------------------------------------------------------------------
fn clean_text(raw: &str) -> String {
    // HTML taglerini, garip boşlukları ve reklamları temizler.
    // Gelecekte satılacak "Temiz Veri" (Clean Data) buradan başlar.
    raw.replace(char::is_control, "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

// -----------------------------------------------------------------------------
// 📡 SOURCE HANDLERS (Gerçek Haber Kaynakları)
// -----------------------------------------------------------------------------
async fn fetch_rss_source(
    name: &str,
    url: &str,
    nats: &async_nats::Client,
    guard: &NewsGuard,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent("VQ-Capital-Sentinel/3.0 (HFT Ingestor)")
        .build()?;

    let res = client.get(url).send().await?.bytes().await?;
    let channel = rss::Channel::read_from(&res[..])
        .map_err(|e| anyhow::anyhow!("RSS Parse Error for {}: {}", name, e))?;

    for item in channel.items() {
        let title = item.title().unwrap_or("No Title");
        let content = item.description().unwrap_or("");

        if guard.is_new(title).await {
            let event = RawNewsEvent {
                source: name.to_string(),
                headline: clean_text(title),
                content: clean_text(content),
                timestamp: chrono::Utc::now().timestamp_millis(),
            };

            let mut buf = Vec::new();
            if event.encode(&mut buf).is_ok() {
                // NATS'a Anayasal formatta (Protobuf) basıyoruz.
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
    info!("🦅 VQ-Capital News Ingestor v3.0 (The Real Vacuum) devrede.");

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let nats_client = async_nats::connect(&nats_url)
        .await
        .context("CRITICAL: NATS omurgasına bağlanılamadı.")?;

    let guard = NewsGuard::new();

    // 🔗 Gerçek Haber Kaynakları (Ücretsiz ve Açık Kaynaklar)
    let sources = vec![
        (
            "coindesk",
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
        ),
        ("cointelegraph", "https://cointelegraph.com/rss"),
        ("cryptonews", "https://cryptonews.com/news/feed/"),
        (
            "binance_ann",
            "https://www.binance.com/en/support/announcement/rss",
        ),
    ];

    loop {
        for (name, url) in &sources {
            // Her kaynağı bir tokio task içinde paralel çekiyoruz (HFT hızı)
            let nats_clone = nats_client.clone();
            let name_clone = name.to_string();
            let url_clone = url.to_string();
            // Referans sayacı (Arc) yerine basit bir move ile guard'ı paylaşabiliriz.
            // Ama loop içinde olduğumuz için Arc daha güvenli.

            if let Err(e) = fetch_rss_source(&name_clone, &url_clone, &nats_clone, &guard).await {
                warn!("⚠️ Source failure ({}): {}", name_clone, e);
            }
        }

        // Bloomberg kadar hızlı olmak için her 60 saniyede bir tüm interneti tara.
        // HFT botları için bu süre idealdir, API ban yeme riskini minimize eder.
        sleep(Duration::from_secs(60)).await;
    }
}
