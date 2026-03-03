use std::{collections::HashMap, future::Future, pin::Pin, time::Duration};

use alloy::primitives::Address;
use futures_util::future::select_all;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

use crate::{
    info::{
        ActiveAssetDataResponse, CandlesSnapshotResponse, FundingHistoryResponse,
        L2SnapshotResponse, OpenOrdersResponse, OrderInfo, RecentTradesResponse,
        UserAbstractionState, UserFillsResponse, UserStateResponse,
    },
    meta::{AssetContext, Meta, PerpDexInfo, SpotMeta, SpotMetaAndAssetCtxs},
    prelude::*,
    req::HttpClient,
    ws::{Subscription, WsManager},
    BaseUrl, Error, Message, OrderStatusResponse, ReferralResponse, UserFeesResponse,
    UserFundingResponse, UserTokenBalanceResponse,
};

/// Custom endpoint configuration for racing requests
#[derive(Debug, Clone)]
pub struct Endpoint {
    /// Short name for identifying this endpoint (e.g., "local-1", "us-west")
    pub name: String,
    /// The URL of the endpoint
    pub url: String,
}

impl Endpoint {
    pub fn new(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            url: url.into(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CandleSnapshotRequest {
    coin: String,
    interval: String,
    start_time: u64,
    end_time: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum InfoRequest {
    #[serde(rename = "clearinghouseState")]
    UserState {
        user: Address,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        dex: String,
    },
    #[serde(rename = "batchClearinghouseStates")]
    UserStates {
        users: Vec<Address>,
    },
    #[serde(rename = "spotClearinghouseState")]
    UserTokenBalances {
        user: Address,
    },
    UserFees {
        user: Address,
    },
    OpenOrders {
        user: Address,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        dex: String,
    },
    OrderStatus {
        user: Address,
        oid: u64,
    },
    Meta {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        dex: String,
    },
    MetaAndAssetCtxs {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        dex: String,
    },
    SpotMeta,
    SpotMetaAndAssetCtxs,
    AllMids {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        dex: String,
    },
    UserFills {
        user: Address,
    },
    #[serde(rename_all = "camelCase")]
    FundingHistory {
        coin: String,
        start_time: u64,
        end_time: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    UserFunding {
        user: Address,
        start_time: u64,
        end_time: Option<u64>,
    },
    L2Book {
        coin: String,
    },
    RecentTrades {
        coin: String,
    },
    #[serde(rename_all = "camelCase")]
    CandleSnapshot {
        req: CandleSnapshotRequest,
    },
    Referral {
        user: Address,
    },
    HistoricalOrders {
        user: Address,
    },
    ActiveAssetData {
        user: Address,
        coin: String,
    },
    PerpDexs,
    UserAbstraction {
        user: Address,
    },
}

impl InfoRequest {
    /// Check if this request type supports parallel querying across multiple endpoints
    fn supports_parallel_query(&self) -> bool {
        matches!(
            self,
            InfoRequest::UserState { .. }    // clearinghouseState
            | InfoRequest::Meta { .. }       // meta
            | InfoRequest::OpenOrders { .. } // openOrders
        )
    }
}

#[derive(Debug)]
pub struct InfoClient {
    pub http_client: HttpClient,
    pub(crate) ws_manager: Option<WsManager>,
    reconnect: bool,
    /// Custom endpoints for racing requests (each with 2s timeout)
    custom_endpoints: Vec<(Endpoint, Client)>,
}

impl InfoClient {
    /// Create a new InfoClient with only the official endpoint.
    pub async fn new(client: Option<Client>, base_url: Option<BaseUrl>) -> Result<InfoClient> {
        Self::new_internal(client, base_url, false, Vec::new()).await
    }

    /// Create a new InfoClient with WebSocket reconnect enabled.
    pub async fn with_reconnect(
        client: Option<Client>,
        base_url: Option<BaseUrl>,
    ) -> Result<InfoClient> {
        Self::new_internal(client, base_url, true, Vec::new()).await
    }

    /// Create a new InfoClient with custom endpoints for racing.
    /// For eligible requests (UserState, Meta, OpenOrders), all custom endpoints
    /// plus the official endpoint will be queried simultaneously, returning the
    /// first successful response.
    pub async fn with_endpoints(
        client: Option<Client>,
        base_url: Option<BaseUrl>,
        endpoints: Vec<Endpoint>,
    ) -> Result<InfoClient> {
        Self::new_internal(client, base_url, false, endpoints).await
    }

    /// Create a new InfoClient with WebSocket reconnect and custom endpoints for racing.
    pub async fn with_reconnect_and_endpoints(
        client: Option<Client>,
        base_url: Option<BaseUrl>,
        endpoints: Vec<Endpoint>,
    ) -> Result<InfoClient> {
        Self::new_internal(client, base_url, true, endpoints).await
    }

    async fn new_internal(
        client: Option<Client>,
        base_url: Option<BaseUrl>,
        reconnect: bool,
        endpoints: Vec<Endpoint>,
    ) -> Result<InfoClient> {
        let client = client.unwrap_or_default();
        let base_url_config = base_url.unwrap_or(BaseUrl::Mainnet);
        let base_url = base_url_config.get_url();

        // Create a client with 2s timeout for each custom endpoint
        let custom_endpoints: Vec<(Endpoint, Client)> = endpoints
            .into_iter()
            .map(|endpoint| {
                let client = Client::builder()
                    .timeout(Duration::from_secs(2))
                    .build()
                    .unwrap_or_default();
                (endpoint, client)
            })
            .collect();

        Ok(InfoClient {
            http_client: HttpClient {
                client,
                base_url,
                base_url_config,
            },
            ws_manager: None,
            reconnect,
            custom_endpoints,
        })
    }

    pub async fn subscribe(
        &mut self,
        subscription: Subscription,
        sender_channel: Sender<Message>,
    ) -> Result<u32> {
        if self.ws_manager.is_none() {
            let ws_url = format!("ws{}/ws", &self.http_client.base_url[4..]);

            let ws_manager = WsManager::new(ws_url, self.reconnect).await?;
            self.ws_manager = Some(ws_manager);
        }

        let identifier =
            serde_json::to_string(&subscription).map_err(|e| Error::JsonParse(e.to_string()))?;

        self.ws_manager
            .as_mut()
            .ok_or(Error::WsManagerNotFound)?
            .add_subscription(identifier, sender_channel)
            .await
    }

    pub async fn unsubscribe(&mut self, subscription_id: u32) -> Result<()> {
        if self.ws_manager.is_none() {
            let ws_url = format!("ws{}/ws", &self.http_client.base_url[4..]);
            let ws_manager = WsManager::new(ws_url, self.reconnect).await?;
            self.ws_manager = Some(ws_manager);
        }

        self.ws_manager
            .as_mut()
            .ok_or(Error::WsManagerNotFound)?
            .remove_subscription(subscription_id)
            .await
    }

    async fn send_info_request<T: for<'a> Deserialize<'a>>(
        &self,
        info_request: InfoRequest,
    ) -> Result<T> {
        let data =
            serde_json::to_string(&info_request).map_err(|e| Error::JsonParse(e.to_string()))?;

        // If request supports parallel query and we have custom endpoints, query all in parallel
        if info_request.supports_parallel_query() && !self.custom_endpoints.is_empty() {
            return self.query_parallel(&data).await;
        }

        // Official endpoint only (default path)
        let return_data = self.http_client.post("/info", data).await?;
        serde_json::from_str(&return_data).map_err(|e| Error::JsonParse(e.to_string()))
    }

    /// Query all endpoints (custom + official) in parallel and return the first successful response
    async fn query_parallel<T: for<'a> Deserialize<'a>>(&self, data: &str) -> Result<T> {
        type RaceResult = std::result::Result<(String, String), (String, String)>;
        type RaceFuture = Pin<Box<dyn Future<Output = RaceResult> + Send>>;

        let mut futures: Vec<RaceFuture> = Vec::new();

        // Add futures for custom endpoints (2s timeout each)
        for (endpoint, client) in &self.custom_endpoints {
            let url = format!("{}/info", endpoint.url.trim_end_matches('/'));
            let name = endpoint.name.clone();
            let data = data.to_string();
            let client = client.clone();

            futures.push(Box::pin(async move {
                match client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .body(data)
                    .send()
                    .await
                {
                    Ok(response) => {
                        if response.status().is_success() {
                            match response.text().await {
                                Ok(text) => Ok((name, text)),
                                Err(e) => Err((name, format!("Response read failed: {e}"))),
                            }
                        } else {
                            Err((name, format!("HTTP status: {}", response.status())))
                        }
                    }
                    Err(e) => Err((name, format!("Request failed: {e}"))),
                }
            }));
        }

        // Add future for official endpoint (no timeout)
        let official_client = self.http_client.client.clone();
        let official_url = format!("{}/info", self.http_client.base_url);
        let official_data = data.to_string();

        futures.push(Box::pin(async move {
            match official_client
                .post(&official_url)
                .header("Content-Type", "application/json")
                .body(official_data)
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        match response.text().await {
                            Ok(text) => Ok(("official".to_string(), text)),
                            Err(e) => Err(("official".to_string(), format!("Response read failed: {e}"))),
                        }
                    } else {
                        Err(("official".to_string(), format!("HTTP status: {}", response.status())))
                    }
                }
                Err(e) => Err(("official".to_string(), format!("Request failed: {e}"))),
            }
        }));

        // Collect errors as we race
        let mut errors: Vec<(String, String)> = Vec::new();

        // Race until we get a success or all fail
        while !futures.is_empty() {
            let (result, _index, remaining) = select_all(futures).await;
            futures = remaining;

            match result {
                Ok((name, response_text)) => {
                    log::info!("Endpoint '{}' responded first", name);
                    return serde_json::from_str(&response_text)
                        .map_err(|e| Error::JsonParse(e.to_string()));
                }
                Err((name, error)) => {
                    log::warn!("Endpoint '{}' failed: {}", name, error);
                    errors.push((name, error));
                }
            }
        }

        // All endpoints failed - aggregate errors
        let error_msg = errors
            .iter()
            .map(|(name, err)| format!("{}: {}", name, err))
            .collect::<Vec<_>>()
            .join("; ");

        Err(Error::AllEndpointsFailed(error_msg))
    }

    pub async fn open_orders(&self, address: Address) -> Result<Vec<OpenOrdersResponse>> {
        let input = InfoRequest::OpenOrders {
            user: address,
            dex: String::new(),
        };
        self.send_info_request(input).await
    }

    pub async fn open_orders_with_dex(
        &self,
        address: Address,
        dex: &str,
    ) -> Result<Vec<OpenOrdersResponse>> {
        let input = InfoRequest::OpenOrders {
            user: address,
            dex: dex.to_string(),
        };
        self.send_info_request(input).await
    }

    pub async fn user_state(&self, address: Address) -> Result<UserStateResponse> {
        let input = InfoRequest::UserState {
            user: address,
            dex: String::new(),
        };
        self.send_info_request(input).await
    }

    pub async fn user_state_with_dex(
        &self,
        address: Address,
        dex: &str,
    ) -> Result<UserStateResponse> {
        let input = InfoRequest::UserState {
            user: address,
            dex: dex.to_string(),
        };
        self.send_info_request(input).await
    }

    pub async fn user_states(&self, addresses: Vec<Address>) -> Result<Vec<UserStateResponse>> {
        let input = InfoRequest::UserStates { users: addresses };
        self.send_info_request(input).await
    }

    pub async fn user_token_balances(&self, address: Address) -> Result<UserTokenBalanceResponse> {
        let input = InfoRequest::UserTokenBalances { user: address };
        self.send_info_request(input).await
    }

    pub async fn user_fees(&self, address: Address) -> Result<UserFeesResponse> {
        let input = InfoRequest::UserFees { user: address };
        self.send_info_request(input).await
    }

    pub async fn meta(&self) -> Result<Meta> {
        let input = InfoRequest::Meta {
            dex: String::new(),
        };
        self.send_info_request(input).await
    }

    pub async fn meta_with_dex(&self, dex: &str) -> Result<Meta> {
        let input = InfoRequest::Meta {
            dex: dex.to_string(),
        };
        self.send_info_request(input).await
    }

    pub async fn meta_and_asset_contexts(&self) -> Result<(Meta, Vec<AssetContext>)> {
        let input = InfoRequest::MetaAndAssetCtxs {
            dex: String::new(),
        };
        self.send_info_request(input).await
    }

    pub async fn meta_and_asset_contexts_with_dex(
        &self,
        dex: &str,
    ) -> Result<(Meta, Vec<AssetContext>)> {
        let input = InfoRequest::MetaAndAssetCtxs {
            dex: dex.to_string(),
        };
        self.send_info_request(input).await
    }

    pub async fn spot_meta(&self) -> Result<SpotMeta> {
        let input = InfoRequest::SpotMeta;
        self.send_info_request(input).await
    }

    pub async fn spot_meta_and_asset_contexts(&self) -> Result<Vec<SpotMetaAndAssetCtxs>> {
        let input = InfoRequest::SpotMetaAndAssetCtxs;
        self.send_info_request(input).await
    }

    pub async fn all_mids(&self) -> Result<HashMap<String, String>> {
        let input = InfoRequest::AllMids {
            dex: String::new(),
        };
        self.send_info_request(input).await
    }

    pub async fn all_mids_with_dex(&self, dex: &str) -> Result<HashMap<String, String>> {
        let input = InfoRequest::AllMids {
            dex: dex.to_string(),
        };
        self.send_info_request(input).await
    }

    pub async fn user_fills(&self, address: Address) -> Result<Vec<UserFillsResponse>> {
        let input = InfoRequest::UserFills { user: address };
        self.send_info_request(input).await
    }

    pub async fn funding_history(
        &self,
        coin: String,
        start_time: u64,
        end_time: Option<u64>,
    ) -> Result<Vec<FundingHistoryResponse>> {
        let input = InfoRequest::FundingHistory {
            coin,
            start_time,
            end_time,
        };
        self.send_info_request(input).await
    }

    pub async fn user_funding_history(
        &self,
        user: Address,
        start_time: u64,
        end_time: Option<u64>,
    ) -> Result<Vec<UserFundingResponse>> {
        let input = InfoRequest::UserFunding {
            user,
            start_time,
            end_time,
        };
        self.send_info_request(input).await
    }

    pub async fn recent_trades(&self, coin: String) -> Result<Vec<RecentTradesResponse>> {
        let input = InfoRequest::RecentTrades { coin };
        self.send_info_request(input).await
    }

    pub async fn l2_snapshot(&self, coin: String) -> Result<L2SnapshotResponse> {
        let input = InfoRequest::L2Book { coin };
        self.send_info_request(input).await
    }

    pub async fn candles_snapshot(
        &self,
        coin: String,
        interval: String,
        start_time: u64,
        end_time: u64,
    ) -> Result<Vec<CandlesSnapshotResponse>> {
        let input = InfoRequest::CandleSnapshot {
            req: CandleSnapshotRequest {
                coin,
                interval,
                start_time,
                end_time,
            },
        };
        self.send_info_request(input).await
    }

    pub async fn candles_snapshot_ws(
        &mut self,
        coin: String,
        interval: String,
        start_time: u64,
        end_time: u64,
    ) -> Result<serde_json::Value> {
        // Ensure WebSocket manager exists
        if self.ws_manager.is_none() {
            let ws_url = format!("ws{}/ws", &self.http_client.base_url[4..]);

            let ws_manager = WsManager::new(ws_url, self.reconnect).await?;
            self.ws_manager = Some(ws_manager);
        }

        // Create WebSocket POST request for candle snapshot
        let request = serde_json::json!({
            "method": "post",
            "request": {
                "type": "info",
                "payload": {
                    "type": "candleSnapshot",
                    "req": {
                        "coin": coin,
                        "interval": interval,
                        "startTime": start_time,
                        "endTime": end_time
                    }
                }
            }
        });

        // Send via WebSocket
        let response = self
            .ws_manager
            .as_mut()
            .ok_or(Error::WsManagerNotFound)?
            .send_request(request)
            .await?;

        Ok(response)
    }

    pub async fn query_order_by_oid(
        &self,
        address: Address,
        oid: u64,
    ) -> Result<OrderStatusResponse> {
        let input = InfoRequest::OrderStatus { user: address, oid };
        self.send_info_request(input).await
    }

    pub async fn query_referral_state(&self, address: Address) -> Result<ReferralResponse> {
        let input = InfoRequest::Referral { user: address };
        self.send_info_request(input).await
    }

    pub async fn historical_orders(&self, address: Address) -> Result<Vec<OrderInfo>> {
        let input = InfoRequest::HistoricalOrders { user: address };
        self.send_info_request(input).await
    }

    pub async fn active_asset_data(
        &self,
        user: Address,
        coin: String,
    ) -> Result<ActiveAssetDataResponse> {
        let input = InfoRequest::ActiveAssetData { user, coin };
        self.send_info_request(input).await
    }

    /// Get the list of available HIP3 perp dexes.
    /// Returns a list where the first element is None (original dex) and
    /// subsequent elements are Some(PerpDexInfo) for builder-deployed dexes.
    pub async fn perp_dexs(&self) -> Result<Vec<Option<PerpDexInfo>>> {
        let input = InfoRequest::PerpDexs;
        self.send_info_request(input).await
    }

    /// Get the abstraction state for a user.
    /// Returns the user's current margin/trading mode configuration.
    pub async fn user_abstraction(&self, address: Address) -> Result<UserAbstractionState> {
        let input = InfoRequest::UserAbstraction { user: address };
        self.send_info_request(input).await
    }
}
