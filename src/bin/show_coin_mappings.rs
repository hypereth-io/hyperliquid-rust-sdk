use std::error::Error;

use alloy::signers::local::PrivateKeySigner;
use hyperliquid_rust_sdk::{BaseUrl, ExchangeClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Use a dummy wallet - we're just looking at mappings, not signing anything
    let wallet: PrivateKeySigner =
        "0000000000000000000000000000000000000000000000000000000000000001"
        .parse()?;

    println!("Initializing ExchangeClient...\n");

    let client = ExchangeClient::new(None, wallet, Some(BaseUrl::Mainnet), None, None).await?;

    // Collect and sort mappings
    let mut mappings: Vec<(String, u32)> = client
        .coin_to_asset
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    mappings.sort_by_key(|(_, idx)| *idx);

    println!("=== Coin to Asset Index Mappings ===\n");

    // Group by range
    println!("--- Original Perp (0-9999) ---");
    for (coin, idx) in mappings.iter().filter(|(_, idx)| *idx < 10000) {
        println!("  {}: {}", coin, idx);
    }

    println!("\n--- Spot (10000-109999) ---");
    for (coin, idx) in mappings.iter().filter(|(_, idx)| *idx >= 10000 && *idx < 110000) {
        println!("  {}: {}", coin, idx);
    }

    println!("\n--- HIP3 Perp Dexes (110000+) ---");
    let mut current_dex_start = 0u32;
    for (coin, idx) in mappings.iter().filter(|(_, idx)| *idx >= 110000) {
        let dex_start = (idx / 10000) * 10000;
        if dex_start != current_dex_start {
            current_dex_start = dex_start;
            println!("\n  [Offset {}]", dex_start);
        }
        println!("    {}: {}", coin, idx);
    }

    println!("\n\nTotal mappings: {}", client.coin_to_asset.len());

    Ok(())
}
