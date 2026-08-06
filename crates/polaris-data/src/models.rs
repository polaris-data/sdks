use std::{collections::BTreeMap, pin::Pin};

use chrono::{DateTime, Utc};
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::errors::PolarisError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimeInput {
    Iso8601(String),
    DateTime(DateTime<Utc>),
    EpochMicros(i64),
}

impl From<&str> for TimeInput {
    fn from(value: &str) -> Self {
        Self::Iso8601(value.to_owned())
    }
}

impl From<String> for TimeInput {
    fn from(value: String) -> Self {
        Self::Iso8601(value)
    }
}

impl From<DateTime<Utc>> for TimeInput {
    fn from(value: DateTime<Utc>) -> Self {
        Self::DateTime(value)
    }
}

impl From<i64> for TimeInput {
    fn from(value: i64) -> Self {
        Self::EpochMicros(value)
    }
}

impl From<u64> for TimeInput {
    fn from(value: u64) -> Self {
        Self::EpochMicros(value as i64)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatalogQuery {
    pub source: Option<String>,
    pub market: Option<String>,
    pub q: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListSnapshotsQuery {
    pub source: String,
    pub market: String,
    pub from: Option<TimeInput>,
    pub to: Option<TimeInput>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoricalQuery {
    pub source: String,
    pub market: String,
    pub from: Option<TimeInput>,
    pub to: Option<TimeInput>,
    pub allow_gaps: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayQuery {
    pub source: String,
    pub market: String,
    pub from: Option<TimeInput>,
    pub to: Option<TimeInput>,
    pub allow_gaps: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamQuery {
    pub source: String,
    pub markets: Vec<String>,
    pub include_buffer: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawQuery {
    pub source: String,
    pub market: String,
    pub from: Option<TimeInput>,
    pub to: Option<TimeInput>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawReplayQuery {
    pub source: String,
    pub market: String,
    pub from: Option<TimeInput>,
    pub to: Option<TimeInput>,
    pub limit: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OhlcvInterval {
    #[serde(rename = "100ms")]
    Ms100,
    #[serde(rename = "1s")]
    S1,
    #[serde(rename = "10s")]
    S10,
    #[serde(rename = "1m")]
    M1,
    #[serde(rename = "5m")]
    M5,
    #[serde(rename = "15m")]
    M15,
    #[serde(rename = "1h")]
    H1,
}

impl OhlcvInterval {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ms100 => "100ms",
            Self::S1 => "1s",
            Self::S10 => "10s",
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::H1 => "1h",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OhlcvFormat {
    #[default]
    Bars,
    TradingView,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OhlcvQuery {
    pub source: String,
    pub market: String,
    pub from: Option<TimeInput>,
    pub to: Option<TimeInput>,
    pub interval: OhlcvInterval,
    pub format: OhlcvFormat,
    pub allow_gaps: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogResponse {
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    pub markets: Vec<CatalogMarket>,
    #[doc(hidden)]
    #[serde(skip)]
    pub legacy_shape: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogAccess {
    pub status: String,
    pub public_cutoff_date: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogInstrument {
    pub base: Option<String>,
    pub quote: Option<String>,
    pub tick_size: Option<String>,
    pub lot_size: Option<String>,
    pub min_notional: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogMarket {
    pub source: String,
    pub market: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub source_type: Option<String>,
    pub categories: Option<Vec<String>>,
    pub access: Option<CatalogAccess>,
    pub instrument: CatalogInstrument,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub key: String,
    pub source: Option<String>,
    pub market: Option<String>,
    pub date: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub timestamp: Option<String>,
    pub hour: Option<u8>,
    pub filename: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DownloadManifestQuery {
    pub source: String,
    pub market: String,
    pub date: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadManifestResponse {
    pub source: String,
    pub market: String,
    pub date: String,
    pub total: usize,
    pub total_bytes: u64,
    pub snapshots: Vec<DownloadManifestEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DownloadManifestEntry {
    pub date: String,
    pub timestamp: String,
    pub key: String,
    pub url: String,
    pub expires_in_seconds: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StandardEvent {
    pub timestamp: i64,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub market: String,
    #[serde(default, rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub data: Value,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeData {
    pub price: f64,
    pub quantity: f64,
    #[serde(default)]
    pub side: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeEvent {
    pub timestamp: i64,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: TradeData,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OhlcvBar {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub trades: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradingViewCandle {
    pub time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradingViewVolume {
    pub time: i64,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradingViewOhlcv {
    pub candles: Vec<TradingViewCandle>,
    pub volumes: Vec<TradingViewVolume>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OhlcvOutput {
    Bars(Vec<OhlcvBar>),
    TradingView(TradingViewOhlcv),
}

pub type ReplayStream = Pin<Box<dyn Stream<Item = Result<StandardEvent, PolarisError>> + Send>>;
pub type RawReplayStream = Pin<Box<dyn Stream<Item = Result<Value, PolarisError>> + Send>>;
pub type RealtimeStream = Pin<Box<dyn Stream<Item = Result<StandardEvent, PolarisError>> + Send>>;

// Orderbook-related types
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderbookLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderbookEvent {
    pub timestamp: i64,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: OrderbookData,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderbookData {
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BboQuote {
    pub timestamp: i64,
    pub bid_price: f64,
    pub bid_quantity: f64,
    pub ask_price: f64,
    pub ask_quantity: f64,
}

// Point series types
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PointSeriesEvent {
    pub timestamp: i64,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: PointSeriesData,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PointSeriesData {
    #[serde(rename = "series")]
    pub series_name: String,
    pub value: f64,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

// Aggregated bar types
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VolumeBar {
    pub timestamp: i64,
    pub volume: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VwapBar {
    pub timestamp: i64,
    pub vwap: Option<f64>,
    pub volume: f64,
    pub quote_volume: f64,
    pub trades: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VolatilityBar {
    pub timestamp: i64,
    pub volatility: f64,
    pub returns: u64,
}

// Depth metrics
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DepthMetricsRow {
    pub timestamp: i64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub mid_price: f64,
    pub bid_ask_spread: f64,
    pub bid_ask_spread_bps: Option<f64>,
    pub depth_pct: f64,
    pub bid_depth_notional: f64,
    pub ask_depth_notional: f64,
    pub depth_imbalance: Option<f64>,
    pub slippage_notional: f64,
    pub target_base_quantity: Option<f64>,
    pub buy_average_price: Option<f64>,
    pub sell_average_price: Option<f64>,
    pub buy_slippage: Option<f64>,
    pub sell_slippage: Option<f64>,
    pub buy_slippage_bps: Option<f64>,
    pub sell_slippage_bps: Option<f64>,
}
