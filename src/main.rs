// ========== DOSYA: sentinel-news-ingest/src/main.rs ==========
use anyhow::{Context, Result};
use prost::Message;
use rand::seq::SliceRandom;
use rand::Rng;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

pub mod sentinel_protos {
    pub mod market {
        include!(concat!(env!("OUT_DIR"), "/sentinel.market.v1.rs"));
    }
}
use sentinel_protos::market::RawNewsEvent;

// Gürültü Filtresi: Metinleri LLM'in (Intelligence) daha rahat anlayacağı
// formata (küçük harf, noktalama işaretsiz) indirger.
fn noise_reduction(raw_text: &str) -> String {
    raw_text
        .to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let nats_client = async_nats::connect(&nats_url)
        .await
        .context("CRITICAL: NATS bağlanılamadı")?;

    info!("📡 Sentinel News Ingest (Sözel Veri Vakumu) Devrede.");

    // HFT Testleri için Sentetik Haber Havuzu (Gerçek senaryoda buralar WS'den akar)
    let coins = ["BTC", "ETH", "SOL", "BNB"];
    let actions = [
        "surges",
        "crashes",
        "breaks resistance",
        "dumps",
        "partners with tech giant",
        "faces SEC lawsuit",
        "updates mainnet",
        "whale accumulation detected",
    ];

    let mut rng = rand::thread_rng();

    loop {
        // Rastgele bir piyasa haberi oluştur (Gerçek veri entegrasyonuna hazır)
        let coin = coins.choose(&mut rng).unwrap();
        let action = actions.choose(&mut rng).unwrap();
        let headline = format!("BREAKING: {} {} amidst market volatility!", coin, action);

        let clean_headline = noise_reduction(&headline);

        let event = RawNewsEvent {
            source: "synthetic_feed".to_string(),
            headline: clean_headline.clone(),
            content: "Full article content goes here in production...".to_string(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        let mut buf = Vec::new();
        if event.encode(&mut buf).is_ok() {
            let subject = format!("news.raw.{}", event.source);
            if let Err(e) = nats_client.publish(subject.clone(), buf.into()).await {
                warn!("⚠️ Haber yayınlanamadı: {}", e);
            } else {
                info!(
                    "📰 [HABER İLETİLDİ] Konu: {} | Metin: '{}'",
                    subject, clean_headline
                );
            }
        }

        // Piyasada ortalama 15-45 saniyede bir önemli haber düşer
        let delay = rng.gen_range(15..=45);
        sleep(Duration::from_secs(delay)).await;
    }
}
