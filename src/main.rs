// ========== DOSYA: sentinel-news-ingest/src/main.rs ==========
use anyhow::{Context, Result};
use prost::Message;
use reqwest::Client;
use rss::Channel;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::sleep;
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

// Haber sistemde daha önce görüldü mü? (Memory-safe Ring Buffer)
fn update_seen_buffer(seen: &mut VecDeque<String>, guid: String) {
    if seen.len() >= 500 {
        seen.pop_front();
    }
    seen.push_back(guid);
}

// RSS Akışını İşleyen Asenkron Görev
async fn process_feed(
    client: &Client,
    source_name: &str,
    url: &str,
    nats_client: &async_nats::Client,
    seen_guids: &mut VecDeque<String>,
) -> Result<()> {
    let response_bytes = client
        .get(url)
        .send()
        .await
        .context("HTTP İsteği Başarısız")?
        .bytes()
        .await
        .context("Payload Okuma Başarısız")?;

    let channel = Channel::read_from(&response_bytes[..])
        .map_err(|e| anyhow::anyhow!("RSS Parse Hatası: {}", e))?;

    // Eski haberlerden yeni haberlere doğru işleyerek kronolojiyi koru
    for item in channel.items().iter().rev() {
        let raw_guid = item
            .guid()
            .map(|g| g.value())
            .unwrap_or_else(|| item.link().unwrap_or("unknown"));

        if raw_guid == "unknown" || seen_guids.contains(&raw_guid.to_string()) {
            continue;
        }

        let headline = item.title().unwrap_or("");
        let content = item.description().unwrap_or("");

        if headline.is_empty() {
            continue;
        }

        let clean_headline = noise_reduction(headline);
        let upper_head = clean_headline.to_uppercase();

        // Gürültü Filtresi Seviye 2: Sadece portföyümüzdeki hedef varlıkların haberlerini içeri al (Bant genişliği tasarrufu)
        if !(upper_head.contains("BTC")
            || upper_head.contains("ETH")
            || upper_head.contains("SOL")
            || upper_head.contains("BNB")
            || upper_head.contains("CRYPTO")
            || upper_head.contains("SEC"))
        {
            update_seen_buffer(seen_guids, raw_guid.to_string());
            continue;
        }

        let event = RawNewsEvent {
            source: source_name.to_string(),
            headline: clean_headline.clone(),
            content: noise_reduction(content),
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        let mut buf = Vec::new();
        if event.encode(&mut buf).is_ok() {
            let subject = format!("news.raw.{}", source_name);
            match nats_client.publish(subject.clone(), buf.into()).await {
                Ok(_) => {
                    info!("📰 [GERÇEK HABER] {} | {}", source_name, clean_headline);
                }
                Err(e) => warn!("⚠️ NATS Yayın Hatası ({}): {}", source_name, e),
            }
        }

        update_seen_buffer(seen_guids, raw_guid.to_string());
    }

    Ok(())
}

// Sürekli Çalışan Polling Loop
async fn poll_rss_feed(source_name: &str, url: &str, nats_client: async_nats::Client) {
    // Timeout eklenmiş güvenli client. (unwrap kullanılmaz, fallback uygulanır)
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| Client::new());

    let mut seen_guids: VecDeque<String> = VecDeque::with_capacity(500);

    info!("📡 Akış Dinleniyor: [{}] -> {}", source_name, url);

    loop {
        if let Err(e) = process_feed(&client, source_name, url, &nats_client, &mut seen_guids).await
        {
            warn!("⚠️ [{}] Akışında geçici hata: {}", source_name, e);
        }

        // Çok agresif poll yapıp ban yememek için 15 saniye bekle
        sleep(Duration::from_secs(15)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    let nats_client = async_nats::connect(&nats_url)
        .await
        .context("CRITICAL: NATS bağlanılamadı")?;

    info!("📡 Sentinel News Ingest (GERÇEK VERİ MODU) Devrede.");

    // Gerçek Dünya Kripto Haber Kaynakları
    let sources = vec![
        ("cointelegraph", "https://cointelegraph.com/rss"),
        (
            "coindesk",
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
        ),
    ];

    // Her kaynak için izole ve asenkron bir Worker Task başlat (Thread Bloke Etmez)
    for (name, url) in sources {
        let nats_clone = nats_client.clone();
        let name_str = name.to_string();
        let url_str = url.to_string();

        tokio::spawn(async move {
            poll_rss_feed(&name_str, &url_str, nats_clone).await;
        });
    }

    // Ana thread'i canlı tut (Uygulamanın kapanmasını engelle)
    tokio::signal::ctrl_c()
        .await
        .context("Sinyal dinleyicisi başlatılamadı")?;

    info!("🛑 Sentinel News Ingest Kapatılıyor...");
    Ok(())
}
