use std::{
    borrow::BorrowMut,
    collections::HashMap,
    ops::DerefMut,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use alloy::primitives::Address;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    spawn,
    sync::{mpsc::UnboundedSender, oneshot, Mutex},
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, protocol},
    MaybeTlsStream, WebSocketStream,
};

use crate::{
    prelude::*,
    ws::message_types::{
        ActiveAssetData, ActiveSpotAssetCtx, AllMids, Bbo, Candle, L2Book, OrderUpdates, Trades,
        User,
    },
    ActiveAssetCtx, Error, Notification, UserFills, UserFundings, UserNonFundingLedgerUpdates,
    WebData2,
};

#[derive(Debug)]
struct SubscriptionData {
    sending_channel: UnboundedSender<Message>,
    subscription_id: u32,
    id: String,
}
#[derive(Debug)]
pub struct WsManager {
    stop_flag: Arc<AtomicBool>,
    writer: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>>>,
    subscriptions: Arc<Mutex<HashMap<String, Vec<SubscriptionData>>>>,
    subscription_id: u32,
    subscription_identifiers: HashMap<u32, String>,
    // Request/response tracking
    request_counter: Arc<AtomicU32>,
    pending_requests: Arc<Mutex<HashMap<u32, oneshot::Sender<serde_json::Value>>>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum Subscription {
    AllMids,
    Notification { user: Address },
    WebData2 { user: Address },
    Candle { coin: String, interval: String },
    L2Book { coin: String },
    Trades { coin: String },
    OrderUpdates { user: Address },
    UserEvents { user: Address },
    UserFills { user: Address },
    UserFundings { user: Address },
    UserNonFundingLedgerUpdates { user: Address },
    ActiveAssetCtx { coin: String },
    ActiveAssetData { user: Address, coin: String },
    Bbo { coin: String },
}

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "channel")]
#[serde(rename_all = "camelCase")]
pub enum Message {
    NoData,
    HyperliquidError(String),
    AllMids(AllMids),
    Trades(Trades),
    L2Book(L2Book),
    User(User),
    UserFills(UserFills),
    Candle(Candle),
    SubscriptionResponse,
    OrderUpdates(OrderUpdates),
    UserFundings(UserFundings),
    UserNonFundingLedgerUpdates(UserNonFundingLedgerUpdates),
    Notification(Notification),
    WebData2(WebData2),
    ActiveAssetCtx(ActiveAssetCtx),
    ActiveAssetData(ActiveAssetData),
    ActiveSpotAssetCtx(ActiveSpotAssetCtx),
    Bbo(Bbo),
    Pong,
}

#[derive(Serialize)]
pub struct SubscriptionSendData<'a> {
    pub method: &'static str,
    pub subscription: &'a serde_json::Value,
}

#[derive(Serialize)]
pub struct Ping {
    pub method: &'static str,
}

impl WsManager {
    const SEND_PING_INTERVAL: u64 = 50;

    pub async fn new(url: String, reconnect: bool) -> Result<WsManager> {
        let stop_flag = Arc::new(AtomicBool::new(false));

        let (writer, mut reader) = Self::connect(&url).await?.split();
        let writer = Arc::new(Mutex::new(writer));

        let subscriptions_map: HashMap<String, Vec<SubscriptionData>> = HashMap::new();
        let subscriptions = Arc::new(Mutex::new(subscriptions_map));
        let subscriptions_copy = Arc::clone(&subscriptions);

        let pending_requests = Arc::new(Mutex::new(HashMap::new()));
        let pending_requests_copy = Arc::clone(&pending_requests);

        {
            let writer = writer.clone();
            let stop_flag = Arc::clone(&stop_flag);
            let reader_fut = async move {
                while !stop_flag.load(Ordering::Relaxed) {
                    if let Some(data) = reader.next().await {
                        if let Err(err) = WsManager::parse_and_send_data(
                            data,
                            &subscriptions_copy,
                            &pending_requests_copy,
                        )
                        .await
                        {
                            error!("Error processing data received by WsManager reader: {err}");
                        }
                    } else {
                        warn!("WsManager disconnected");
                        if let Err(err) = WsManager::send_to_all_subscriptions(
                            &subscriptions_copy,
                            Message::NoData,
                        )
                        .await
                        {
                            warn!("Error sending disconnection notification err={err}");
                        }
                        if reconnect {
                            // Always sleep for 1 second before attempting to reconnect so it does not spin during reconnecting. This could be enhanced with exponential backoff.
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            info!("WsManager attempting to reconnect");
                            match Self::connect(&url).await {
                                Ok(ws) => {
                                    let (new_writer, new_reader) = ws.split();
                                    reader = new_reader;
                                    let mut writer_guard = writer.lock().await;
                                    *writer_guard = new_writer;
                                    for (identifier, v) in subscriptions_copy.lock().await.iter() {
                                        // TODO should these special keys be removed and instead use the simpler direct identifier mapping?
                                        if identifier.eq("userEvents")
                                            || identifier.eq("orderUpdates")
                                        {
                                            for subscription_data in v {
                                                if let Err(err) = Self::subscribe(
                                                    writer_guard.deref_mut(),
                                                    &subscription_data.id,
                                                )
                                                .await
                                                {
                                                    error!(
                                                        "Could not resubscribe {identifier}: {err}"
                                                    );
                                                }
                                            }
                                        } else if let Err(err) =
                                            Self::subscribe(writer_guard.deref_mut(), identifier)
                                                .await
                                        {
                                            error!("Could not resubscribe correctly {identifier}: {err}");
                                        }
                                    }
                                    info!("WsManager reconnect finished");
                                }
                                Err(err) => error!("Could not connect to websocket {err}"),
                            }
                        } else {
                            error!("WsManager reconnection disabled. Will not reconnect and exiting reader task.");
                            break;
                        }
                    }
                }
                warn!("ws message reader task stopped");
            };
            spawn(reader_fut);
        }

        {
            let stop_flag = Arc::clone(&stop_flag);
            let writer = Arc::clone(&writer);
            let ping_fut = async move {
                while !stop_flag.load(Ordering::Relaxed) {
                    match serde_json::to_string(&Ping { method: "ping" }) {
                        Ok(payload) => {
                            let mut writer = writer.lock().await;
                            if let Err(err) = writer.send(protocol::Message::Text(payload)).await {
                                error!("Error pinging server: {err}")
                            }
                        }
                        Err(err) => error!("Error serializing ping message: {err}"),
                    }
                    time::sleep(Duration::from_secs(Self::SEND_PING_INTERVAL)).await;
                }
                warn!("ws ping task stopped");
            };
            spawn(ping_fut);
        }

        Ok(WsManager {
            stop_flag,
            writer,
            subscriptions,
            subscription_id: 0,
            subscription_identifiers: HashMap::new(),
            request_counter: Arc::new(AtomicU32::new(0)),
            pending_requests,
        })
    }

    async fn connect(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        Ok(connect_async(url)
            .await
            .map_err(|e| Error::Websocket(e.to_string()))?
            .0)
    }

    fn get_identifier(message: &Message) -> Result<String> {
        match message {
            Message::AllMids(_) => serde_json::to_string(&Subscription::AllMids)
                .map_err(|e| Error::JsonParse(e.to_string())),
            Message::User(_) => Ok("userEvents".to_string()),
            Message::UserFills(fills) => serde_json::to_string(&Subscription::UserFills {
                user: fills.data.user,
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::Trades(trades) => {
                if trades.data.is_empty() {
                    Ok(String::default())
                } else {
                    serde_json::to_string(&Subscription::Trades {
                        coin: trades.data[0].coin.clone(),
                    })
                    .map_err(|e| Error::JsonParse(e.to_string()))
                }
            }
            Message::L2Book(l2_book) => serde_json::to_string(&Subscription::L2Book {
                coin: l2_book.data.coin.clone(),
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::Candle(candle) => serde_json::to_string(&Subscription::Candle {
                coin: candle.data.coin.clone(),
                interval: candle.data.interval.clone(),
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::OrderUpdates(_) => Ok("orderUpdates".to_string()),
            Message::UserFundings(fundings) => serde_json::to_string(&Subscription::UserFundings {
                user: fundings.data.user,
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::UserNonFundingLedgerUpdates(user_non_funding_ledger_updates) => {
                serde_json::to_string(&Subscription::UserNonFundingLedgerUpdates {
                    user: user_non_funding_ledger_updates.data.user,
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::Notification(_) => Ok("notification".to_string()),
            Message::WebData2(web_data2) => serde_json::to_string(&Subscription::WebData2 {
                user: web_data2.data.user,
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::ActiveAssetCtx(active_asset_ctx) => {
                serde_json::to_string(&Subscription::ActiveAssetCtx {
                    coin: active_asset_ctx.data.coin.clone(),
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::ActiveSpotAssetCtx(active_spot_asset_ctx) => {
                serde_json::to_string(&Subscription::ActiveAssetCtx {
                    coin: active_spot_asset_ctx.data.coin.clone(),
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::ActiveAssetData(active_asset_data) => {
                serde_json::to_string(&Subscription::ActiveAssetData {
                    user: active_asset_data.data.user,
                    coin: active_asset_data.data.coin.clone(),
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::Bbo(bbo) => serde_json::to_string(&Subscription::Bbo {
                coin: bbo.data.coin.clone(),
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::SubscriptionResponse | Message::Pong => Ok(String::default()),
            Message::NoData => Ok("".to_string()),
            Message::HyperliquidError(err) => Ok(format!("hyperliquid error: {err:?}")),
        }
    }

    async fn parse_and_send_data(
        data: std::result::Result<protocol::Message, tungstenite::Error>,
        subscriptions: &Arc<Mutex<HashMap<String, Vec<SubscriptionData>>>>,
        pending_requests: &Arc<
            Mutex<HashMap<u32, tokio::sync::oneshot::Sender<serde_json::Value>>>,
        >,
    ) -> Result<()> {
        match data {
            Ok(data) => match data.into_text() {
                Ok(data) => {
                    if !data.starts_with('{') {
                        return Ok(());
                    }

                    // Try to parse as general JSON first to check for POST responses
                    let json_data: serde_json::Value =
                        serde_json::from_str(&data).map_err(|e| Error::JsonParse(e.to_string()))?;

                    // Handle POST method responses (success and error cases)
                    if let Some(channel) = json_data.get("channel") {
                        if channel == "post" {
                            if let Some(response_data) = json_data.get("data") {
                                if let Some(request_id) =
                                    response_data.get("id").and_then(|id| id.as_u64())
                                {
                                    let mut pending = pending_requests.lock().await;
                                    if let Some(sender) = pending.remove(&(request_id as u32)) {
                                        let _ = sender.send(response_data.clone());
                                    }
                                }
                            }
                            return Ok(());
                        } else if channel == "error" {
                            // Handle error responses - try to extract request ID from the error data
                            if let Some(error_msg) = json_data.get("data").and_then(|d| d.as_str())
                            {
                                log::warn!("WebSocket POST error: {}", error_msg);
                                // Try to extract request ID from the error message
                                // The error message contains the original request as JSON
                                if error_msg.contains("\"id\":") {
                                    // Use regex or string parsing to find the request ID
                                    if let Some(id_start) = error_msg.find("\"id\":") {
                                        let id_substr = &error_msg[id_start + 5..];
                                        if let Some(comma_pos) = id_substr.find(',') {
                                            let id_str = &id_substr[..comma_pos];
                                            if let Ok(request_id) = id_str.parse::<u32>() {
                                                let mut pending = pending_requests.lock().await;
                                                if let Some(sender) = pending.remove(&request_id) {
                                                    // Send the error response back to the waiting request
                                                    let _ = sender.send(json_data.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            return Ok(());
                        }
                    }

                    // Handle direct POST responses (including errors with "id" field)
                    if let Some(request_id) = json_data.get("id").and_then(|id| id.as_u64()) {
                        let mut pending = pending_requests.lock().await;
                        if let Some(sender) = pending.remove(&(request_id as u32)) {
                            let _ = sender.send(json_data.clone());
                            return Ok(());
                        }
                    }

                    // Check if this looks like a POST response that we might have missed
                    if json_data.get("error").is_some() {
                        log::warn!(
                            "WebSocket received error response without matching request ID: {}",
                            data
                        );
                        // If we can't match it to a request, just ignore it rather than trying to parse as subscription
                        return Ok(());
                    }

                    // Handle normal subscription messages
                    let message = serde_json::from_str::<Message>(&data)
                        .map_err(|e| Error::JsonParse(e.to_string()))?;
                    let identifier = WsManager::get_identifier(&message)?;
                    if identifier.is_empty() {
                        return Ok(());
                    }

                    let mut subscriptions = subscriptions.lock().await;
                    let mut res = Ok(());
                    if let Some(subscription_datas) = subscriptions.get_mut(&identifier) {
                        for subscription_data in subscription_datas {
                            if let Err(e) = subscription_data
                                .sending_channel
                                .send(message.clone())
                                .map_err(|e| Error::WsSend(e.to_string()))
                            {
                                res = Err(e);
                            }
                        }
                    }
                    res
                }
                Err(err) => {
                    let error = Error::ReaderTextConversion(err.to_string());
                    Ok(WsManager::send_to_all_subscriptions(
                        subscriptions,
                        Message::HyperliquidError(error.to_string()),
                    )
                    .await?)
                }
            },
            Err(err) => {
                let error = Error::GenericReader(err.to_string());
                Ok(WsManager::send_to_all_subscriptions(
                    subscriptions,
                    Message::HyperliquidError(error.to_string()),
                )
                .await?)
            }
        }
    }

    async fn send_to_all_subscriptions(
        subscriptions: &Arc<Mutex<HashMap<String, Vec<SubscriptionData>>>>,
        message: Message,
    ) -> Result<()> {
        let mut subscriptions = subscriptions.lock().await;
        let mut res = Ok(());
        for subscription_datas in subscriptions.values_mut() {
            for subscription_data in subscription_datas {
                if let Err(e) = subscription_data
                    .sending_channel
                    .send(message.clone())
                    .map_err(|e| Error::WsSend(e.to_string()))
                {
                    res = Err(e);
                }
            }
        }
        res
    }

    async fn send_subscription_data(
        method: &'static str,
        writer: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>,
        identifier: &str,
    ) -> Result<()> {
        let payload = serde_json::to_string(&SubscriptionSendData {
            method,
            subscription: &serde_json::from_str::<serde_json::Value>(identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?,
        })
        .map_err(|e| Error::JsonParse(e.to_string()))?;

        writer
            .send(protocol::Message::Text(payload))
            .await
            .map_err(|e| Error::Websocket(e.to_string()))?;
        Ok(())
    }

    async fn subscribe(
        writer: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>,
        identifier: &str,
    ) -> Result<()> {
        Self::send_subscription_data("subscribe", writer, identifier).await
    }

    async fn unsubscribe(
        writer: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>,
        identifier: &str,
    ) -> Result<()> {
        Self::send_subscription_data("unsubscribe", writer, identifier).await
    }

    pub(crate) async fn add_subscription(
        &mut self,
        identifier: String,
        sending_channel: UnboundedSender<Message>,
    ) -> Result<u32> {
        let mut subscriptions = self.subscriptions.lock().await;

        let identifier_entry = if let Subscription::UserEvents { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "userEvents".to_string()
        } else if let Subscription::OrderUpdates { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "orderUpdates".to_string()
        } else {
            identifier.clone()
        };
        let subscriptions = subscriptions
            .entry(identifier_entry.clone())
            .or_insert(Vec::new());

        if !subscriptions.is_empty() && identifier_entry.eq("userEvents") {
            return Err(Error::UserEvents);
        }

        if subscriptions.is_empty() {
            Self::subscribe(self.writer.lock().await.borrow_mut(), identifier.as_str()).await?;
        }

        let subscription_id = self.subscription_id;
        self.subscription_identifiers
            .insert(subscription_id, identifier.clone());
        subscriptions.push(SubscriptionData {
            sending_channel,
            subscription_id,
            id: identifier,
        });

        self.subscription_id += 1;
        Ok(subscription_id)
    }

    pub(crate) async fn remove_subscription(&mut self, subscription_id: u32) -> Result<()> {
        let identifier = self
            .subscription_identifiers
            .get(&subscription_id)
            .ok_or(Error::SubscriptionNotFound)?
            .clone();

        let identifier_entry = if let Subscription::UserEvents { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "userEvents".to_string()
        } else if let Subscription::OrderUpdates { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "orderUpdates".to_string()
        } else {
            identifier.clone()
        };

        self.subscription_identifiers.remove(&subscription_id);

        let mut subscriptions = self.subscriptions.lock().await;

        let subscriptions = subscriptions
            .get_mut(&identifier_entry)
            .ok_or(Error::SubscriptionNotFound)?;
        let index = subscriptions
            .iter()
            .position(|subscription_data| subscription_data.subscription_id == subscription_id)
            .ok_or(Error::SubscriptionNotFound)?;
        subscriptions.remove(index);

        if subscriptions.is_empty() {
            Self::unsubscribe(self.writer.lock().await.borrow_mut(), identifier.as_str()).await?;
        }
        Ok(())
    }

    pub async fn send_request(
        &mut self,
        mut request: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let request_id = self.request_counter.fetch_add(1, Ordering::SeqCst) + 1;

        // Add request ID to the request
        request["id"] = serde_json::Value::Number(serde_json::Number::from(request_id));

        // Create oneshot channel for response
        let (tx, rx) = tokio::sync::oneshot::channel();

        // Store the sender in pending requests
        {
            let mut pending = self.pending_requests.lock().await;
            pending.insert(request_id, tx);
        }

        // Send the request through WebSocket
        let request_str =
            serde_json::to_string(&request).map_err(|e| Error::JsonParse(e.to_string()))?;
        {
            let mut writer = self.writer.lock().await;
            writer
                .send(tokio_tungstenite::tungstenite::protocol::Message::Text(
                    request_str,
                ))
                .await
                .map_err(|e| Error::Websocket(e.to_string()))?;
        }

        // Wait for response with timeout
        let response = tokio::time::timeout(std::time::Duration::from_secs(30), rx)
            .await
            .map_err(|_| Error::RequestTimeout)?
            .map_err(|_| Error::RequestChannelClosed)?;

        Ok(response)
    }
}

impl Drop for WsManager {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
    }
}
