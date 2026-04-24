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

// -----------------------------------------------------------------------------
// 1. DATA CONTRACT (Sıfır Tahsis İçin Ömür Boyu Referans Kullanımı Mümkündür
//    ancak WebSocket frame'leri için geçici struct kullanıyoruz)
// -----------------------------------------------------------------------------
#[derive(Debug, Deserialize)]
struct GenericNewsPayload {
    source: String,
    title: String,
    #[serde(default)]
    body: String,
}

// -----------------------------------------------------------------------------
// 2. HFT MİMARİSİ: NOISE REDUCTION (Gürültü Filtresi)
// Çöp toplayıcıyı yormamak için allocate (tahsis) miktarını minimize eder.
// -----------------------------------------------------------------------------
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

// -----------------------------------------------------------------------------
// 3. ASYNC WEBSOCKET WORKER (Exponential Backoff & Cancellation Safe)
// -----------------------------------------------------------------------------
async fn run_websocket_ingestor(nats_client: async_nats::Client, ws_url: &str) {
    let mut retry_delay = Duration::from_secs(1);
    let max_delay = Duration::from_secs(30);

    loop {
        info!("🔗 WebSocket'e bağlanılıyor: {}", ws_url);

        match connect_async(ws_url).await {
            Ok((ws_stream, _)) => {
                info!("✅ [HFT-VACUUM] Canlı Haber Akışına Bağlanıldı: {}", ws_url);
                retry_delay = Duration::from_secs(1); // Bağlantı başarılıysa gecikmeyi sıfırla

                let (_, mut read) = ws_stream.split();

                while let Some(msg_result) = read.next().await {
                    match msg_result {
                        Ok(WsMessage::Text(text)) => {
                            process_and_publish(text.as_bytes(), &nats_client).await;
                        }
                        Ok(WsMessage::Binary(bin)) => {
                            process_and_publish(&bin, &nats_client).await;
                        }
                        Ok(WsMessage::Close(_)) => {
                            warn!("⚠️ WebSocket sunucusu bağlantıyı kapattı.");
                            break;
                        }
                        Err(e) => {
                            error!("❌ Ağ Okuma Hatası: {:?}", e);
                            break;
                        }
                        _ => {} // Ping/Pong mesajlarını yut
                    }
                }
            }
            Err(e) => {
                error!("❌ WebSocket Bağlantı Hatası: {:?}", e);
            }
        }

        // Exponential Backoff (Katlanarak artan bekleme süresi)
        warn!(
            "⏳ Bağlantı koptu. {} saniye sonra yeniden denenecek...",
            retry_delay.as_secs()
        );
        sleep(retry_delay).await;
        retry_delay = std::cmp::min(retry_delay * 2, max_delay);
    }
}

// -----------------------------------------------------------------------------
// 4. ZERO-LATENCY PAYLOAD PROCESSING
// Gelen JSON bytelarını doğrudan okur, NATS'a Protobuf fırlatır.
// -----------------------------------------------------------------------------
#[inline]
async fn process_and_publish(payload: &[u8], nats_client: &async_nats::Client) {
    // Allocation'dan kaçınmak için from_slice kullanıyoruz
    let parsed: Result<GenericNewsPayload, _> = serde_json::from_slice(payload);

    match parsed {
        Ok(news) => {
            let clean_headline = fast_noise_reduction(&news.title);
            let upper_head = clean_headline.to_uppercase();

            // Sadece portföyümüzdeki hedef varlıkların haberlerini içeri al
            if !(upper_head.contains("BTC")
                || upper_head.contains("ETH")
                || upper_head.contains("SOL")
                || upper_head.contains("BNB")
                || upper_head.contains("CRYPTO")
                || upper_head.contains("SEC"))
            {
                return; // İlgilenmediğimiz coin, PUSULA DIŞI (Drop)
            }

            let event = RawNewsEvent {
                source: news.source.to_lowercase(),
                headline: clean_headline.clone(),
                content: fast_noise_reduction(&news.body),
                timestamp: chrono::Utc::now().timestamp_millis(),
            };

            let mut buf = Vec::new();
            if event.encode(&mut buf).is_ok() {
                let subject = format!("news.raw.{}", event.source);
                match nats_client.publish(subject, buf.into()).await {
                    Ok(_) => {
                        info!(
                            "⚡ [GERÇEK ZAMANLI HABER] {} -> {}",
                            event.source, clean_headline
                        );
                    }
                    Err(e) => warn!("⚠️ NATS Yayın Hatası: {}", e),
                }
            }
        }
        Err(_) => {
            // Malformed (hatalı) veya ilgisiz JSON yapısı, sessizce yut
        }
    }
}

// -----------------------------------------------------------------------------
// 5. BOOTSTRAP (Orkestrasyon)
// -----------------------------------------------------------------------------
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    // Gerçek bir Premium Kripto Haber WS sunucusu veya Proxy kullanıldığı varsayılmıştır.
    // (Örn: wss://stream.cryptopanic.com/ws/ VEYA özel VQ-Capital Proxy'si)
    let ws_url = std::env::var("NEWS_WS_URL")
        .unwrap_or_else(|_| "wss://mock.crypto-news-stream.local/ws".to_string());

    info!("🚀 VQ-Capital News Ingestor (WebSocket HFT Mode) Devrede.");

    let nats_client = async_nats::connect(&nats_url)
        .await
        .context("CRITICAL: NATS omurgasına bağlanılamadı. Sistem başlatılamıyor.")?;

    // Worker'ı arka planda başlat
    tokio::spawn(async move {
        run_websocket_ingestor(nats_client, &ws_url).await;
    });

    // Ana thread'i canlı tut (Daemon Mode)
    tokio::signal::ctrl_c()
        .await
        .context("Sinyal dinleyicisi başlatılamadı")?;

    info!("🛑 Sentinel News Ingest Kapatılıyor...");
    Ok(())
}
