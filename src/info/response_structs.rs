use serde::Deserialize;

use alloy::primitives::Address;

use crate::{
    info::{AssetPosition, Level, MarginSummary},
    DailyUserVlm, Delta, FeeSchedule, Leverage, OrderInfo, Referrer, ReferrerState,
    UserTokenBalance,
};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserStateResponse {
    pub asset_positions: Vec<AssetPosition>,
    pub cross_margin_summary: MarginSummary,
    pub margin_summary: MarginSummary,
    pub withdrawable: String,
}

#[derive(Deserialize, Debug)]
pub struct UserTokenBalanceResponse {
    pub balances: Vec<UserTokenBalance>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserFeesResponse {
    pub active_referral_discount: String,
    pub daily_user_vlm: Vec<DailyUserVlm>,
    pub fee_schedule: FeeSchedule,
    pub user_add_rate: String,
    pub user_cross_rate: String,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct OpenOrdersResponse {
    pub coin: String,
    pub limit_px: String,
    pub oid: u64,
    pub side: String,
    pub sz: String,
    pub timestamp: u64,
    pub cloid: Option<String>,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserFillsResponse {
    pub closed_pnl: String,
    pub coin: String,
    pub crossed: bool,
    pub dir: String,
    pub hash: String,
    pub oid: u64,
    pub px: String,
    pub side: String,
    pub start_position: String,
    pub sz: String,
    pub time: u64,
    pub fee: String,
    pub tid: u64,
    pub fee_token: String,
    pub twap_id: Option<u64>,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FundingHistoryResponse {
    pub coin: String,
    pub funding_rate: String,
    pub premium: String,
    pub time: u64,
}

#[derive(Deserialize, Debug)]
pub struct UserFundingResponse {
    pub time: u64,
    pub hash: String,
    pub delta: Delta,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct L2SnapshotResponse {
    pub coin: String,
    pub levels: Vec<Vec<Level>>,
    pub time: u64,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RecentTradesResponse {
    pub coin: String,
    pub side: String,
    pub px: String,
    pub sz: String,
    pub time: u64,
    pub hash: String,
}

#[derive(serde::Deserialize, Debug)]
pub struct CandlesSnapshotResponse {
    #[serde(rename = "t")]
    pub time_open: u64,
    #[serde(rename = "T")]
    pub time_close: u64,
    #[serde(rename = "s")]
    pub coin: String,
    #[serde(rename = "i")]
    pub candle_interval: String,
    #[serde(rename = "o")]
    pub open: String,
    #[serde(rename = "c")]
    pub close: String,
    #[serde(rename = "h")]
    pub high: String,
    #[serde(rename = "l")]
    pub low: String,
    #[serde(rename = "v")]
    pub vlm: String,
    #[serde(rename = "n")]
    pub num_trades: u64,
}

#[derive(Deserialize, Debug)]
pub struct OrderStatusResponse {
    pub status: String,
    /// `None` if the order is not found
    #[serde(default)]
    pub order: Option<OrderInfo>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ReferralResponse {
    pub referred_by: Option<Referrer>,
    pub cum_vlm: String,
    pub unclaimed_rewards: String,
    pub claimed_rewards: String,
    pub referrer_state: ReferrerState,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ActiveAssetDataResponse {
    pub user: Address,
    pub coin: String,
    pub leverage: Leverage,
    pub max_trade_szs: Vec<String>,
    pub available_to_trade: Vec<String>,
    pub mark_px: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ExtraAgentResponse {
    pub name: String,
    pub address: Address,
    pub valid_until: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SubAccountResponse {
    pub name: String,
    pub sub_account_user: Address,
    pub master: Address,
    pub clearinghouse_state: Option<UserStateResponse>,
    pub spot_state: Option<UserTokenBalanceResponse>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserVaultEquity {
    pub vault_address: Address,
    pub equity: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UserRateLimitResponse {
    pub cum_vlm: String,
    pub n_requests_used: u64,
    pub n_requests_cap: u64,
    pub n_requests_surplus: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DelegationResponse {
    pub validator: Address,
    pub amount: String,
    pub locked_until_timestamp: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DelegatorSummaryResponse {
    pub delegated: String,
    pub undelegated: String,
    pub total_pending_withdrawal: String,
    pub n_pending_withdrawals: u64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PerpDeployAuctionStatusResponse {
    pub start_time_seconds: u64,
    pub duration_seconds: u64,
    pub start_gas: String,
    pub current_gas: Option<String>,
    pub end_gas: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpotDeploySpec {
    pub name: String,
    pub sz_decimals: u32,
    pub wei_decimals: u32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpotDeployTokenState {
    pub token: u64,
    pub spec: SpotDeploySpec,
    pub full_name: String,
    pub spots: Vec<u64>,
    pub max_supply: u64,
    pub hyperliquidity_genesis_balance: String,
    pub total_genesis_balance_wei: String,
    pub user_genesis_balances: Vec<(String, String)>,
    pub existing_token_genesis_balances: Vec<(u64, String)>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GasAuction {
    pub start_time_seconds: u64,
    pub duration_seconds: u64,
    pub start_gas: String,
    pub current_gas: Option<String>,
    pub end_gas: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpotDeployStateResponse {
    pub states: Vec<SpotDeployTokenState>,
    pub gas_auction: GasAuction,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "role")]
pub enum UserRoleResponse {
    #[serde(rename = "missing")]
    Missing,
    #[serde(rename = "user")]
    User,
    #[serde(rename = "vault")]
    Vault,
    #[serde(rename = "agent")]
    Agent {
        data: AgentRoleData,
    },
    #[serde(rename = "subAccount")]
    SubAccount {
        data: SubAccountRoleData,
    },
}

#[derive(Deserialize, Debug)]
pub struct AgentRoleData {
    pub user: Address,
}

#[derive(Deserialize, Debug)]
pub struct SubAccountRoleData {
    pub master: Address,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum UserAbstractionState {
    /// perp + spot balances combined
    UnifiedAccount,
    /// perp + spot balances combined
    PortfolioMargin,
    /// TODO: when does this appear?
    Disabled,
    /// perp + spot balances separate
    Default,
    /// perp + spot balances separate
    DexAbstraction,
}
