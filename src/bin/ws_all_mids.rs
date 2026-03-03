use hyperliquid_rust_sdk::{BaseUrl, InfoClient, Message, Subscription};
use log::info;
use tokio::{
    spawn,
    sync::mpsc::channel,
    time::{sleep, Duration},
};

#[tokio::main]
async fn main() {
    env_logger::init();

    let mut info_client = InfoClient::new(None, Some(BaseUrl::Testnet)).await.unwrap();

    let (sender, mut receiver) = channel(256);
    // Subscribe to allMids. Use dex: Some("dex_name".to_string()) for a specific HIP-3 dex
    let subscription_id = info_client
        .subscribe(Subscription::AllMids { dex: None }, sender)
        .await
        .unwrap();

    spawn(async move {
        sleep(Duration::from_secs(30)).await;
        info!("Unsubscribing from mids data");
        info_client.unsubscribe(subscription_id).await.unwrap()
    });

    // This loop ends when we unsubscribe
    while let Some(Message::AllMids(all_mids)) = receiver.recv().await {
        info!("Received mids data: {all_mids:?}");
    }
}
