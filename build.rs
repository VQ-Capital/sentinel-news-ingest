// ========== DOSYA: sentinel-news-ingest/build.rs ==========
fn main() -> std::io::Result<()> {
    prost_build::compile_protos(
        &["sentinel-spec/proto/sentinel/market/v1/market_data.proto"],
        &["sentinel-spec/proto/"],
    )?;
    Ok(())
}
