# 🧠 sentinel-semantic-ingest (Legacy: sentinel-news-ingest)

**Domain:** Semantic & Human-Language Ingestion
**Rol:** Sistemin Kulakları (Sözel)

Bu servis sıradan bir "Haber Okuyucu" değildir. Dış dünyadan (RSS, Twitter, Telegram) akan düzensiz insan dilini (Unstructured Text) alır, Jaccard Similarity (Fuzzy Match) ve SHA256 ile spam/clickbait haberleri eler ve NLP (Yapay Zeka) motorunun işleyebileceği temiz `RawNewsEvent` formatına çevirir.

- **Kaynaklar:** CoinTelegraph, CoinDesk, Borsa Duyuruları (RSS/WSS)
- **NATS Çıktısı:** `news.raw.*`
- **SLA Hedefi:** < 100ms