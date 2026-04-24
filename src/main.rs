// ========== DOSYA: sentinel-news-ingest/src/main.rs ==========
use anyhow::{Context, Result};
use futures_util::StreamExt;
use prost::Message;
use serde::Deserialize;
use std::time::Duration;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};
use tracing::{error, info, warn};

pub mod sentinel_protos {
    pub mod market {
        include!(concat!(env!("OUT_DIR"), "/sentinel.market.v1.rs"));
    }
}
use sentinel_protos::market::RawNewsEvent;

#[derive(Debug, Deserialize)]
struct GenericNewsPayload {
    source: String,
    title: String,
    #[serde(default)]
    body: String,
}

fn fast_noise_reduction(raw_text: &str) -> String {
    let mut clean_text = String::with_capacity(raw_text.len());
    let mut last_was_space = false;

    for c in raw_text.chars() {
        if c.is_alphanumeric() {
            clean_text.extend(c.to_lowercase());
            last_was_space = false;
        } else if c.is_whitespace() && !last_was_space {
            clean_text.push(' ');
            last_was_space = true;
        }
    }
    clean_text.trim().to_string()
}

#[inline]
async fn process_and_publish(payload: &[u8], nats_client: &async_nats::Client) {
    if let Ok(news) = serde_json::from_slice::<GenericNewsPayload>(payload) {
        let clean_headline = fast_noise_reduction(&news.title);
        let upper_head = clean_headline.to_uppercase();

        if !(upper_head.contains("BTC")
            || upper_head.contains("ETH")
            || upper_head.contains("SOL")
            || upper_head.contains("BNB")
            || upper_head.contains("CRYPTO")
            || upper_head.contains("SEC"))
        {
            return;
        }

        let event = RawNewsEvent {
            source: news.source.to_lowercase(),
            headline: clean_headline.clone(),
            content: fast_noise_reduction(&news.body),
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        let mut buf = Vec::new();
        if event.encode(&mut buf).is_ok() {
            let _ = nats_client
                .publish(format!("news.raw.{}", event.source), buf.into())
                .await;
            info!(
                "⚡ [GERÇEK ZAMANLI HABER] {} -> {}",
                event.source, clean_headline
            );
        }
    }
}

// -----------------------------------------------------------------------------
// DEVOPS & MLOPS HACK: SENTETİK HABER ÜRETİCİSİ (Ağ yoksa AI'ı beslemek için)
// -----------------------------------------------------------------------------
async fn run_synthetic_news_generator(nats_client: async_nats::Client) {
    warn!("🚨 WebSocket haber akışı bulunamadı! Sentetik Market Simülatörü devrede.");

    let scenarios = [
        (
            "sec_gov",
            "SEC approves spot Bitcoin ETF in historic decision",
            "Positive crypto regulation confirmed",
        ),
        (
            "whale_alert",
            "Massive amounts of BTC transferred to cold storage",
            "Institutional accumulation",
        ),
        (
            "defi_watch",
            "Major DeFi protocol exploited for 50 million",
            "Hack and security breach detected",
        ),
        (
            "binance_ann",
            "Binance announces integration with new Layer 2 network",
            "Expansion of ecosystem",
        ),
        (
            "macro_news",
            "Fed increases interest rates, markets react negatively",
            "Bearish macro economic outlook",
        ),
    ];

    let mut index = 0;
    loop {
        // AI motorunu gaza getirmek için her 30 saniyede bir sentetik haber bas
        sleep(Duration::from_secs(30)).await;

        let (src, head, body) = scenarios[index % scenarios.len()];
        index += 1;

        let event = RawNewsEvent {
            source: src.to_string(),
            headline: fast_noise_reduction(head),
            content: fast_noise_reduction(body),
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        let mut buf = Vec::new();
        if event.encode(&mut buf).is_ok() {
            let _ = nats_client
                .publish(format!("news.raw.{}", src), buf.into())
                .await;
            info!("🧪 [SENTETİK HABER] {} -> {}", src, head);
        }
    }
}

async fn run_websocket_ingestor(nats_client: async_nats::Client, ws_url: &str) {
    let mut retry_delay = Duration::from_secs(1);
    let max_delay = Duration::from_secs(30);

    loop {
        info!("🔗 WebSocket'e bağlanılıyor: {}", ws_url);

        match connect_async(ws_url).await {
            Ok((ws_stream, _)) => {
                info!("✅ [HFT-VACUUM] Canlı Haber Akışına Bağlanıldı: {}", ws_url);
                retry_delay = Duration::from_secs(1);

                let (_, mut read) = ws_stream.split();
                while let Some(msg_result) = read.next().await {
                    match msg_result {
                        Ok(WsMessage::Text(text)) => {
                            process_and_publish(text.as_bytes(), &nats_client).await
                        }
                        Ok(WsMessage::Binary(bin)) => process_and_publish(&bin, &nats_client).await,
                        Ok(WsMessage::Close(_)) => break,
                        Err(_) => break,
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!("❌ WebSocket Bağlantı Hatası: {:?}", e);
                // DNS Hatası veya Bağlantı hatası durumunda Sentetik Motoru tetikle!
                if ws_url.contains("mock") || ws_url.contains("local") {
                    // Vektörün 3. boyutu 0.0 kalsın, kârımız matematiksel olsun.
                    // Gerçek veri olmalı!!!
                    // run_synthetic_news_generator(nats_client.clone()).await;
                    return; // Sentetik loop'a girdiğinde bu fonksiyondan çık
                }
            }
        }
        warn!(
            "⏳ Bağlantı koptu. {} saniye sonra yeniden denenecek...",
            retry_delay.as_secs()
        );
        sleep(retry_delay).await;
        retry_delay = std::cmp::min(retry_delay * 2, max_delay);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let ws_url = std::env::var("NEWS_WS_URL")
        .unwrap_or_else(|_| "wss://mock.crypto-news-stream.local/ws".to_string());

    info!("🚀 VQ-Capital News Ingestor (WebSocket HFT Mode) Devrede.");
    let nats_client = async_nats::connect(&nats_url)
        .await
        .context("CRITICAL: NATS bağlanılamadı.")?;

    tokio::spawn(async move {
        run_websocket_ingestor(nats_client, &ws_url).await;
    });

    tokio::signal::ctrl_c()
        .await
        .context("Sinyal dinleyicisi başlatılamadı")?;
    info!("🛑 Sentinel News Ingest Kapatılıyor...");
    Ok(())
}
