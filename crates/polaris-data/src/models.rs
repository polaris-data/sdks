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

/// Filters for option-contract discovery. `market` is the normalized underlying.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstrumentsQuery {
    pub source: String,
    pub market: String,
    pub instrument: Option<String>,
    /// Exact expiry as Unix milliseconds.
    pub expiry: Option<i64>,
    pub option_type: Option<String>,
    pub q: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BboQuery {
    pub source: String,
    pub market: String,
    pub start: i64,
    pub end: i64,
    pub interval: Option<OhlcvInterval>,
}

/// Filters for the direct historical row endpoints. Times are inclusive Unix milliseconds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HistoricalRowsQuery {
    pub source: Option<String>,
    pub market: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OptionTickerRowsQuery {
    pub source: Option<String>,
    pub market: Option<String>,
    pub instrument: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OhlcvRowsQuery {
    pub source: Option<String>,
    pub market: Option<String>,
    pub instrument: Option<String>,
    pub interval: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IntentRowsQuery {
    pub source: Option<String>,
    pub market: Option<String>,
    pub instrument: Option<String>,
    pub intent_id: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QuoteRowsQuery {
    pub source: Option<String>,
    pub market: Option<String>,
    pub instrument: Option<String>,
    pub observation_id: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
}

/// Filters for flat source snapshots and deltas from `/historical/l2-updates`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct L2UpdatesQuery {
    pub source: Option<String>,
    pub market: Option<String>,
    pub instrument: Option<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
}

/// Required inclusive millisecond window for `/historical/l2-orderbooks`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct L2OrderbooksQuery {
    pub source: String,
    pub market: String,
    pub instrument: Option<String>,
    pub start: i64,
    pub end: i64,
}

/// One flat top-25 L2 row with all nullable fixed level fields from the API.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderbookL2Row {
    pub event_id: String,
    pub source: String,
    pub market: String,
    pub instrument: Option<String>,
    pub collector_timestamp: i64,
    pub exchange_timestamp: Option<i64>,
    pub source_capture_id: String,
    pub schema_version: u32,
    pub source_event_is_snapshot: bool,
    pub bid_px_00: Option<f64>,
    pub bid_sz_00: Option<f64>,
    pub ask_px_00: Option<f64>,
    pub ask_sz_00: Option<f64>,
    pub bid_px_01: Option<f64>,
    pub bid_sz_01: Option<f64>,
    pub ask_px_01: Option<f64>,
    pub ask_sz_01: Option<f64>,
    pub bid_px_02: Option<f64>,
    pub bid_sz_02: Option<f64>,
    pub ask_px_02: Option<f64>,
    pub ask_sz_02: Option<f64>,
    pub bid_px_03: Option<f64>,
    pub bid_sz_03: Option<f64>,
    pub ask_px_03: Option<f64>,
    pub ask_sz_03: Option<f64>,
    pub bid_px_04: Option<f64>,
    pub bid_sz_04: Option<f64>,
    pub ask_px_04: Option<f64>,
    pub ask_sz_04: Option<f64>,
    pub bid_px_05: Option<f64>,
    pub bid_sz_05: Option<f64>,
    pub ask_px_05: Option<f64>,
    pub ask_sz_05: Option<f64>,
    pub bid_px_06: Option<f64>,
    pub bid_sz_06: Option<f64>,
    pub ask_px_06: Option<f64>,
    pub ask_sz_06: Option<f64>,
    pub bid_px_07: Option<f64>,
    pub bid_sz_07: Option<f64>,
    pub ask_px_07: Option<f64>,
    pub ask_sz_07: Option<f64>,
    pub bid_px_08: Option<f64>,
    pub bid_sz_08: Option<f64>,
    pub ask_px_08: Option<f64>,
    pub ask_sz_08: Option<f64>,
    pub bid_px_09: Option<f64>,
    pub bid_sz_09: Option<f64>,
    pub ask_px_09: Option<f64>,
    pub ask_sz_09: Option<f64>,
    pub bid_px_10: Option<f64>,
    pub bid_sz_10: Option<f64>,
    pub ask_px_10: Option<f64>,
    pub ask_sz_10: Option<f64>,
    pub bid_px_11: Option<f64>,
    pub bid_sz_11: Option<f64>,
    pub ask_px_11: Option<f64>,
    pub ask_sz_11: Option<f64>,
    pub bid_px_12: Option<f64>,
    pub bid_sz_12: Option<f64>,
    pub ask_px_12: Option<f64>,
    pub ask_sz_12: Option<f64>,
    pub bid_px_13: Option<f64>,
    pub bid_sz_13: Option<f64>,
    pub ask_px_13: Option<f64>,
    pub ask_sz_13: Option<f64>,
    pub bid_px_14: Option<f64>,
    pub bid_sz_14: Option<f64>,
    pub ask_px_14: Option<f64>,
    pub ask_sz_14: Option<f64>,
    pub bid_px_15: Option<f64>,
    pub bid_sz_15: Option<f64>,
    pub ask_px_15: Option<f64>,
    pub ask_sz_15: Option<f64>,
    pub bid_px_16: Option<f64>,
    pub bid_sz_16: Option<f64>,
    pub ask_px_16: Option<f64>,
    pub ask_sz_16: Option<f64>,
    pub bid_px_17: Option<f64>,
    pub bid_sz_17: Option<f64>,
    pub ask_px_17: Option<f64>,
    pub ask_sz_17: Option<f64>,
    pub bid_px_18: Option<f64>,
    pub bid_sz_18: Option<f64>,
    pub ask_px_18: Option<f64>,
    pub ask_sz_18: Option<f64>,
    pub bid_px_19: Option<f64>,
    pub bid_sz_19: Option<f64>,
    pub ask_px_19: Option<f64>,
    pub ask_sz_19: Option<f64>,
    pub bid_px_20: Option<f64>,
    pub bid_sz_20: Option<f64>,
    pub ask_px_20: Option<f64>,
    pub ask_sz_20: Option<f64>,
    pub bid_px_21: Option<f64>,
    pub bid_sz_21: Option<f64>,
    pub ask_px_21: Option<f64>,
    pub ask_sz_21: Option<f64>,
    pub bid_px_22: Option<f64>,
    pub bid_sz_22: Option<f64>,
    pub ask_px_22: Option<f64>,
    pub ask_sz_22: Option<f64>,
    pub bid_px_23: Option<f64>,
    pub bid_sz_23: Option<f64>,
    pub ask_px_23: Option<f64>,
    pub ask_sz_23: Option<f64>,
    pub bid_px_24: Option<f64>,
    pub bid_sz_24: Option<f64>,
    pub ask_px_24: Option<f64>,
    pub ask_sz_24: Option<f64>,
}

impl OrderbookL2Row {
    /// Return the reconstructed top-25 bid levels in API order.
    pub fn bids(&self) -> impl Iterator<Item = (f64, f64)> + '_ {
        [
            (self.bid_px_00, self.bid_sz_00),
            (self.bid_px_01, self.bid_sz_01),
            (self.bid_px_02, self.bid_sz_02),
            (self.bid_px_03, self.bid_sz_03),
            (self.bid_px_04, self.bid_sz_04),
            (self.bid_px_05, self.bid_sz_05),
            (self.bid_px_06, self.bid_sz_06),
            (self.bid_px_07, self.bid_sz_07),
            (self.bid_px_08, self.bid_sz_08),
            (self.bid_px_09, self.bid_sz_09),
            (self.bid_px_10, self.bid_sz_10),
            (self.bid_px_11, self.bid_sz_11),
            (self.bid_px_12, self.bid_sz_12),
            (self.bid_px_13, self.bid_sz_13),
            (self.bid_px_14, self.bid_sz_14),
            (self.bid_px_15, self.bid_sz_15),
            (self.bid_px_16, self.bid_sz_16),
            (self.bid_px_17, self.bid_sz_17),
            (self.bid_px_18, self.bid_sz_18),
            (self.bid_px_19, self.bid_sz_19),
            (self.bid_px_20, self.bid_sz_20),
            (self.bid_px_21, self.bid_sz_21),
            (self.bid_px_22, self.bid_sz_22),
            (self.bid_px_23, self.bid_sz_23),
            (self.bid_px_24, self.bid_sz_24),
        ]
        .into_iter()
        .filter_map(|(price, quantity)| price.zip(quantity))
        .filter(|(price, quantity)| {
            price.is_finite() && *price > 0.0 && quantity.is_finite() && *quantity > 0.0
        })
    }

    /// Return the reconstructed top-25 ask levels in API order.
    pub fn asks(&self) -> impl Iterator<Item = (f64, f64)> + '_ {
        [
            (self.ask_px_00, self.ask_sz_00),
            (self.ask_px_01, self.ask_sz_01),
            (self.ask_px_02, self.ask_sz_02),
            (self.ask_px_03, self.ask_sz_03),
            (self.ask_px_04, self.ask_sz_04),
            (self.ask_px_05, self.ask_sz_05),
            (self.ask_px_06, self.ask_sz_06),
            (self.ask_px_07, self.ask_sz_07),
            (self.ask_px_08, self.ask_sz_08),
            (self.ask_px_09, self.ask_sz_09),
            (self.ask_px_10, self.ask_sz_10),
            (self.ask_px_11, self.ask_sz_11),
            (self.ask_px_12, self.ask_sz_12),
            (self.ask_px_13, self.ask_sz_13),
            (self.ask_px_14, self.ask_sz_14),
            (self.ask_px_15, self.ask_sz_15),
            (self.ask_px_16, self.ask_sz_16),
            (self.ask_px_17, self.ask_sz_17),
            (self.ask_px_18, self.ask_sz_18),
            (self.ask_px_19, self.ask_sz_19),
            (self.ask_px_20, self.ask_sz_20),
            (self.ask_px_21, self.ask_sz_21),
            (self.ask_px_22, self.ask_sz_22),
            (self.ask_px_23, self.ask_sz_23),
            (self.ask_px_24, self.ask_sz_24),
        ]
        .into_iter()
        .filter_map(|(price, quantity)| price.zip(quantity))
        .filter(|(price, quantity)| {
            price.is_finite() && *price > 0.0 && quantity.is_finite() && *quantity > 0.0
        })
    }
}

/// One venue-published candle update from `/historical/ohlcv`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OhlcvRow {
    pub event_id: String,
    pub source: String,
    pub market: String,
    pub collector_timestamp: i64,
    pub source_capture_id: String,
    pub schema_version: u32,
    pub interval: String,
    pub open_timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    #[serde(default)]
    pub exchange_timestamp: Option<i64>,
    #[serde(default)]
    pub instrument: Option<String>,
    #[serde(default)]
    pub close_timestamp: Option<i64>,
    #[serde(default)]
    pub base_volume: Option<f64>,
    #[serde(default)]
    pub quote_volume: Option<f64>,
    #[serde(default)]
    pub trade_count: Option<u64>,
    #[serde(default)]
    pub is_closed: Option<bool>,
}

/// One pair-shaped intent observation from `/historical/intents`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentRow {
    pub event_id: String,
    pub source: String,
    pub market: String,
    pub collector_timestamp: i64,
    pub source_capture_id: String,
    pub schema_version: u32,
    #[serde(default)]
    pub exchange_timestamp: Option<i64>,
    #[serde(default)]
    pub instrument: Option<String>,
    #[serde(default)]
    pub amount_kind: Option<String>,
    #[serde(default)]
    pub expires_at: Option<i64>,
    #[serde(default)]
    pub input_amount: Option<String>,
    #[serde(default)]
    pub input_asset_id: Option<String>,
    #[serde(default)]
    pub input_chain_id: Option<String>,
    #[serde(default)]
    pub intent_id: Option<String>,
    #[serde(default)]
    pub output_amount: Option<String>,
    #[serde(default)]
    pub output_asset_id: Option<String>,
    #[serde(default)]
    pub output_chain_id: Option<String>,
    #[serde(default)]
    pub quote_id: Option<String>,
    #[serde(default)]
    pub quoted_input_amount: Option<String>,
    #[serde(default)]
    pub quoted_output_amount: Option<String>,
    #[serde(default)]
    pub rfq_id: Option<String>,
    #[serde(default)]
    pub settled_at: Option<i64>,
    #[serde(default)]
    pub status: Option<String>,
}

/// One PropAMM quote point from `/historical/quotes`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuoteRow {
    pub event_id: String,
    pub source: String,
    pub market: String,
    pub instrument: String,
    pub collector_timestamp: i64,
    pub source_capture_id: String,
    pub schema_version: u32,
    pub observation_id: String,
    pub input_asset_id: String,
    pub input_chain_id: String,
    pub input_amount: String,
    pub input_decimals: u32,
    pub output_asset_id: String,
    pub output_chain_id: String,
    pub output_amount: String,
    pub output_decimals: u32,
    pub amount_kind: String,
    pub block_number: u64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub router: String,
    #[serde(default)]
    pub exchange_timestamp: Option<i64>,
    #[serde(default)]
    pub oracle: Option<String>,
    #[serde(default)]
    pub pool: Option<String>,
}

/// One flat row from `/historical/trades`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeRow {
    pub event_id: String,
    pub source: String,
    pub market: String,
    pub collector_timestamp: i64,
    pub source_capture_id: String,
    pub schema_version: u32,
    pub price: f64,
    pub quantity: f64,
    #[serde(default)]
    pub exchange_timestamp: Option<i64>,
    #[serde(default)]
    pub instrument: Option<String>,
    #[serde(default)]
    pub liquidation: Option<bool>,
    #[serde(default)]
    pub maker: Option<String>,
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub taker: Option<String>,
}

/// One partial flat row from `/historical/options-ticker`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionTickerRow {
    pub event_id: String,
    pub source: String,
    pub market: String,
    pub instrument: String,
    pub collector_timestamp: i64,
    pub source_capture_id: String,
    pub schema_version: u32,
    #[serde(default)]
    pub exchange_timestamp: Option<i64>,
    #[serde(default)]
    pub expiry_timestamp: Option<i64>,
    #[serde(default)]
    pub ask_iv: Option<String>,
    #[serde(default)]
    pub ask_price: Option<String>,
    #[serde(default)]
    pub ask_size: Option<String>,
    #[serde(default)]
    pub bid_iv: Option<String>,
    #[serde(default)]
    pub bid_price: Option<String>,
    #[serde(default)]
    pub bid_size: Option<String>,
    #[serde(default)]
    pub delta: Option<String>,
    #[serde(default)]
    pub forward_price: Option<String>,
    #[serde(default)]
    pub gamma: Option<String>,
    #[serde(default)]
    pub index_price: Option<String>,
    #[serde(default)]
    pub last_price: Option<String>,
    #[serde(default)]
    pub mark_iv: Option<String>,
    #[serde(default)]
    pub mark_price: Option<String>,
    #[serde(default)]
    pub open_interest: Option<String>,
    #[serde(default)]
    pub option_type: Option<String>,
    #[serde(default)]
    pub premium_currency: Option<String>,
    #[serde(default)]
    pub quantity_unit: Option<String>,
    #[serde(default)]
    pub rho: Option<String>,
    #[serde(default)]
    pub strike: Option<String>,
    #[serde(default)]
    pub theta: Option<String>,
    #[serde(default)]
    pub turnover_24h: Option<String>,
    #[serde(default)]
    pub underlying: Option<String>,
    #[serde(default)]
    pub underlying_price: Option<String>,
    #[serde(default)]
    pub vega: Option<String>,
    #[serde(default)]
    pub volume_24h: Option<String>,
}

/// One partial flat row from `/historical/funding-rates`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingRateRow {
    pub event_id: String,
    pub source: String,
    pub market: String,
    pub collector_timestamp: i64,
    pub source_capture_id: String,
    pub schema_version: u32,
    #[serde(default)]
    pub exchange_timestamp: Option<i64>,
    #[serde(default)]
    pub funding_timestamp: Option<i64>,
    #[serde(default)]
    pub instrument: Option<String>,
    #[serde(default)]
    pub funding_rate: Option<String>,
    #[serde(default)]
    pub index_price: Option<String>,
    #[serde(default)]
    pub mark_price: Option<String>,
    #[serde(default)]
    pub open_interest: Option<String>,
    #[serde(default)]
    pub predicted_funding_rate: Option<String>,
    #[serde(default)]
    pub premium: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamQuery {
    pub source: String,
    pub markets: Vec<String>,
    /// Optional exact venue-native instrument filter applied to each market.
    /// `None` subscribes to the whole market (for options, the whole chain).
    pub instrument: Option<String>,
    pub include_buffer: bool,
    pub materialize_orderbooks: bool,
}

impl Default for StreamQuery {
    fn default() -> Self {
        Self {
            source: String::new(),
            markets: Vec::new(),
            instrument: None,
            include_buffer: false,
            materialize_orderbooks: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawQuery {
    pub source: String,
    pub market: String,
    pub from: Option<TimeInput>,
    pub to: Option<TimeInput>,
    pub limit: usize,
}

/// Query for exact captures from one venue-native raw channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawChannelQuery {
    pub exchange: String,
    pub event: String,
    /// Inclusive collector time in Unix milliseconds.
    pub start: i64,
    /// Inclusive collector time in Unix milliseconds.
    pub end: i64,
}

/// One capture from `GET /raw/{exchange}/{event}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RawCaptureRow {
    pub capture_id: String,
    pub collector_timestamp: i64,
    pub recorder_version: String,
    pub ingested_at: i64,
    pub additional_context: Value,
    /// Exact upstream JSON text; intentionally left unparsed.
    pub original_json: String,
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

/// One venue-native option contract in the instrument catalog.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionContract {
    pub source: String,
    pub market: String,
    pub instrument: String,
    pub status: String,
    pub option_type: String,
    pub underlying: String,
    pub strike: String,
    pub expiry_timestamp: i64,
    #[serde(default)]
    pub contract_size: Option<String>,
    #[serde(default)]
    pub exercise_style: Option<String>,
    #[serde(default)]
    pub premium_currency: Option<String>,
    #[serde(default)]
    pub quantity_unit: Option<String>,
    #[serde(default)]
    pub settlement_currency: Option<String>,
    #[serde(default)]
    pub statistics: Option<OptionContractStatistics>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionContractStatistics {
    pub source: String,
    pub market: String,
    #[serde(default)]
    pub instrument: Option<String>,
    pub fields: BTreeMap<String, ObservedStatistic>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedStatistic {
    pub value: String,
    pub observed_at: i64,
    #[serde(default)]
    pub exchange_timestamp: Option<i64>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub convention: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentsResponse {
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    pub instruments: Vec<OptionContract>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogCount {
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    pub sources: u64,
    pub markets: u64,
    pub by_source: BTreeMap<String, u64>,
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
    pub symbol: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub source_type: Option<String>,
    pub categories: Option<Vec<String>>,
    pub access: Option<CatalogAccess>,
    pub instrument: CatalogInstrument,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LegacyStandardEvent {
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
pub struct StandardEventV2 {
    pub collector_timestamp: i64,
    pub collector_sequence: u64,
    pub exchange_timestamp: Option<i64>,
    pub exchange_sequence: Option<String>,
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
#[serde(untagged)]
pub enum StandardEvent {
    Legacy(LegacyStandardEvent),
    V2(StandardEventV2),
}

impl StandardEvent {
    pub fn timestamp(&self) -> i64 {
        match self {
            Self::Legacy(event) => event.timestamp,
            Self::V2(event) => event.collector_timestamp,
        }
    }

    pub fn source(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.source,
            Self::V2(event) => &event.source,
        }
    }

    pub fn market(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.market,
            Self::V2(event) => &event.market,
        }
    }

    pub fn event_type(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.event_type,
            Self::V2(event) => &event.event_type,
        }
    }

    /// Exact venue-native instrument carried by the event envelope, when one
    /// is present. For options, `market()` is the normalized underlying and
    /// this value identifies the contract.
    pub fn instrument(&self) -> Option<&str> {
        self.extra().get("instrument").and_then(Value::as_str)
    }

    pub fn data(&self) -> &Value {
        match self {
            Self::Legacy(event) => &event.data,
            Self::V2(event) => &event.data,
        }
    }

    pub fn data_mut(&mut self) -> &mut Value {
        match self {
            Self::Legacy(event) => &mut event.data,
            Self::V2(event) => &mut event.data,
        }
    }

    pub(crate) fn source_mut(&mut self) -> &mut String {
        match self {
            Self::Legacy(event) => &mut event.source,
            Self::V2(event) => &mut event.source,
        }
    }

    pub(crate) fn market_mut(&mut self) -> &mut String {
        match self {
            Self::Legacy(event) => &mut event.market,
            Self::V2(event) => &mut event.market,
        }
    }

    pub(crate) fn event_type_mut(&mut self) -> &mut String {
        match self {
            Self::Legacy(event) => &mut event.event_type,
            Self::V2(event) => &mut event.event_type,
        }
    }

    #[doc(hidden)]
    pub fn extra(&self) -> &BTreeMap<String, Value> {
        match self {
            Self::Legacy(event) => &event.extra,
            Self::V2(event) => &event.extra,
        }
    }

    pub(crate) fn extra_mut(&mut self) -> &mut BTreeMap<String, Value> {
        match self {
            Self::Legacy(event) => &mut event.extra,
            Self::V2(event) => &mut event.extra,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LegacyTradeData {
    pub price: f64,
    pub quantity: f64,
    #[serde(default)]
    pub side: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub taker: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeDataV2 {
    pub order_id: Option<String>,
    pub price: f64,
    pub quantity: f64,
    pub side: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maker: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub taker: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LegacyTradeEvent {
    pub timestamp: i64,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: LegacyTradeData,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeEventV2 {
    pub collector_timestamp: i64,
    pub collector_sequence: u64,
    pub exchange_timestamp: Option<i64>,
    pub exchange_sequence: Option<String>,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: TradeDataV2,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TradeEvent {
    Legacy(LegacyTradeEvent),
    V2(TradeEventV2),
}

impl TradeEvent {
    pub fn is_v2(&self) -> bool {
        matches!(self, Self::V2(_))
    }

    pub fn timestamp(&self) -> i64 {
        match self {
            Self::Legacy(event) => event.timestamp,
            Self::V2(event) => event.collector_timestamp,
        }
    }

    pub fn price(&self) -> f64 {
        match self {
            Self::Legacy(event) => event.data.price,
            Self::V2(event) => event.data.price,
        }
    }

    pub fn quantity(&self) -> f64 {
        match self {
            Self::Legacy(event) => event.data.quantity,
            Self::V2(event) => event.data.quantity,
        }
    }

    pub fn source(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.source,
            Self::V2(event) => &event.source,
        }
    }

    pub fn market(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.market,
            Self::V2(event) => &event.market,
        }
    }

    pub fn side(&self) -> Option<&str> {
        match self {
            Self::Legacy(event) => {
                (!event.data.side.is_empty()).then_some(event.data.side.as_str())
            }
            Self::V2(event) => event.data.side.as_deref(),
        }
    }

    pub fn order_id(&self) -> Option<&str> {
        match self {
            Self::Legacy(event) => event.data.extra.get("order_id").and_then(Value::as_str),
            Self::V2(event) => event.data.order_id.as_deref(),
        }
    }

    pub fn maker(&self) -> Option<&str> {
        match self {
            Self::Legacy(event) => event.data.maker.as_deref(),
            Self::V2(event) => event.data.maker.as_deref(),
        }
    }

    pub fn taker(&self) -> Option<&str> {
        match self {
            Self::Legacy(event) => event.data.taker.as_deref(),
            Self::V2(event) => event.data.taker.as_deref(),
        }
    }

    pub fn extra(&self) -> &BTreeMap<String, Value> {
        match self {
            Self::Legacy(event) => &event.data.extra,
            Self::V2(event) => &event.data.extra,
        }
    }
}

/// Whether an intent fixes its inputs or outputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AmountKind {
    ExactInput,
    ExactOutput,
}

/// Canonical execution lifecycle for an intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentStatus {
    Submitted,
    Open,
    PartiallyFilled,
    Executing,
    Filled,
    Settled,
    Cancelled,
    Expired,
    Failed,
    Unknown,
}

/// One asset and its economically binding amount.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetAmount {
    pub asset_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipient: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// A solver or venue response to an RFQ.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentQuote {
    pub quote_id: String,
    #[serde(default)]
    pub response: Vec<AssetAmount>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// A chain transaction that executes or settles an intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettlementTransaction {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<String>,
    pub transaction_hash: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Venue-independent projection of an RFQ, quote, or executable intent.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rfq_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requester: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signer: Option<String>,
    #[serde(default)]
    pub inputs: Vec<AssetAmount>,
    #[serde(default)]
    pub outputs: Vec<AssetAmount>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount_kind: Option<AmountKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<IntentQuote>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<IntentStatus>,
    #[serde(default)]
    pub transactions: Vec<SettlementTransaction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled_at: Option<i64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyIntentEvent {
    pub timestamp: i64,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: IntentData,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntentEventV2 {
    pub collector_timestamp: i64,
    pub collector_sequence: u64,
    pub exchange_timestamp: Option<i64>,
    pub exchange_sequence: Option<String>,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: IntentData,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IntentEvent {
    Legacy(LegacyIntentEvent),
    V2(IntentEventV2),
}

impl IntentEvent {
    pub fn timestamp(&self) -> i64 {
        match self {
            Self::Legacy(event) => event.timestamp,
            Self::V2(event) => event.collector_timestamp,
        }
    }

    pub fn source(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.source,
            Self::V2(event) => &event.source,
        }
    }

    pub fn market(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.market,
            Self::V2(event) => &event.market,
        }
    }

    pub fn data(&self) -> &IntentData {
        match self {
            Self::Legacy(event) => &event.data,
            Self::V2(event) => &event.data,
        }
    }

    /// Exact upstream JSON token associated with this observation, when stored.
    pub fn raw(&self) -> Option<&Value> {
        match self {
            Self::Legacy(event) => event.raw.as_ref(),
            Self::V2(event) => event.raw.as_ref(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionGreeks {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gamma: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vega: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub theta: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rho: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionTickerData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub underlying: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strike: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry_timestamp: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub option_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mark_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bid_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bid_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ask_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ask_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub underlying_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forward_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mark_iv: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bid_iv: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ask_iv: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_interest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_24h: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turnover_24h: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub premium_currency: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity_unit: Option<String>,
    #[serde(default, skip_serializing_if = "option_greeks_are_empty")]
    pub greeks: OptionGreeks,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

fn option_greeks_are_empty(greeks: &OptionGreeks) -> bool {
    greeks.delta.is_none()
        && greeks.gamma.is_none()
        && greeks.vega.is_none()
        && greeks.theta.is_none()
        && greeks.rho.is_none()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyOptionTickerEvent {
    pub timestamp: i64,
    pub source: String,
    pub market: String,
    pub instrument: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: OptionTickerData,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionTickerEventV2 {
    pub collector_timestamp: i64,
    pub collector_sequence: u64,
    pub exchange_timestamp: Option<i64>,
    pub exchange_sequence: Option<String>,
    pub source: String,
    pub market: String,
    pub instrument: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: OptionTickerData,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OptionTickerEvent {
    Legacy(LegacyOptionTickerEvent),
    V2(OptionTickerEventV2),
}

impl OptionTickerEvent {
    pub fn timestamp(&self) -> i64 {
        match self {
            Self::Legacy(event) => event.timestamp,
            Self::V2(event) => event.collector_timestamp,
        }
    }

    pub fn source(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.source,
            Self::V2(event) => &event.source,
        }
    }

    pub fn market(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.market,
            Self::V2(event) => &event.market,
        }
    }

    pub fn instrument(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.instrument,
            Self::V2(event) => &event.instrument,
        }
    }

    pub fn data(&self) -> &OptionTickerData {
        match self {
            Self::Legacy(event) => &event.data,
            Self::V2(event) => &event.data,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerpetualTickerData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mark_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oracle_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mid_price: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_interest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub funding_rate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub funding_timestamp: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_funding_rate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub premium: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl PerpetualTickerData {
    pub fn is_empty(&self) -> bool {
        self.last_price.is_none()
            && self.mark_price.is_none()
            && self.index_price.is_none()
            && self.oracle_price.is_none()
            && self.mid_price.is_none()
            && self.open_interest.is_none()
            && self.funding_rate.is_none()
            && self.funding_timestamp.is_none()
            && self.predicted_funding_rate.is_none()
            && self.premium.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyPerpetualTickerEvent {
    pub timestamp: i64,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: PerpetualTickerData,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerpetualTickerEventV2 {
    pub collector_timestamp: i64,
    pub collector_sequence: u64,
    pub exchange_timestamp: Option<i64>,
    pub exchange_sequence: Option<String>,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: PerpetualTickerData,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PerpetualTickerEvent {
    Legacy(LegacyPerpetualTickerEvent),
    V2(PerpetualTickerEventV2),
}

impl PerpetualTickerEvent {
    pub fn timestamp(&self) -> i64 {
        match self {
            Self::Legacy(event) => event.timestamp,
            Self::V2(event) => event.collector_timestamp,
        }
    }

    pub fn source(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.source,
            Self::V2(event) => &event.source,
        }
    }

    pub fn market(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.market,
            Self::V2(event) => &event.market,
        }
    }

    pub fn data(&self) -> &PerpetualTickerData {
        match self {
            Self::Legacy(event) => &event.data,
            Self::V2(event) => &event.data,
        }
    }
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

pub type HistoricalStream<T> = Pin<Box<dyn Stream<Item = Result<T, PolarisError>> + Send>>;
pub type RealtimeStream = Pin<Box<dyn Stream<Item = Result<StandardEvent, PolarisError>> + Send>>;

// PropAMM quote-ladder types
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropammQuote {
    pub amount_in: String,
    pub amount_out: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropammQuoteLadderValues {
    pub event_id: String,
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub router: String,
    pub oracle: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    pub token_in: String,
    pub token_out: String,
    pub token_in_decimals: u8,
    pub token_out_decimals: u8,
    pub quotes: Vec<PropammQuote>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropammQuoteLadderData {
    #[serde(rename = "series")]
    pub series_name: String,
    pub values: PropammQuoteLadderValues,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropammQuoteLadderEvent {
    pub collector_timestamp: i64,
    pub collector_sequence: u64,
    pub exchange_timestamp: Option<i64>,
    pub exchange_sequence: Option<String>,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: PropammQuoteLadderData,
}

impl PropammQuoteLadderEvent {
    pub fn timestamp(&self) -> i64 {
        self.collector_timestamp
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn market(&self) -> &str {
        &self.market
    }

    pub fn data(&self) -> &PropammQuoteLadderData {
        &self.data
    }
}

// Orderbook-related types
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderbookLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LegacyOrderbookEvent {
    pub timestamp: i64,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: OrderbookData,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderbookDataV2 {
    pub is_snapshot: bool,
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OrderbookEventV2 {
    pub collector_timestamp: i64,
    pub collector_sequence: u64,
    pub exchange_timestamp: Option<i64>,
    pub exchange_sequence: Option<String>,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: OrderbookDataV2,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OrderbookEvent {
    Legacy(LegacyOrderbookEvent),
    V2(OrderbookEventV2),
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
pub struct LegacyPointSeriesEvent {
    pub timestamp: i64,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: PointSeriesData,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PointSeriesEventV2 {
    pub collector_timestamp: i64,
    pub collector_sequence: u64,
    pub exchange_timestamp: Option<i64>,
    pub exchange_sequence: Option<String>,
    pub source: String,
    pub market: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: PointSeriesData,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PointSeriesEvent {
    Legacy(LegacyPointSeriesEvent),
    V2(PointSeriesEventV2),
}

impl PointSeriesEvent {
    pub fn timestamp(&self) -> i64 {
        match self {
            Self::Legacy(event) => event.timestamp,
            Self::V2(event) => event.collector_timestamp,
        }
    }

    pub fn source(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.source,
            Self::V2(event) => &event.source,
        }
    }

    pub fn market(&self) -> &str {
        match self {
            Self::Legacy(event) => &event.market,
            Self::V2(event) => &event.market,
        }
    }

    pub fn data(&self) -> &PointSeriesData {
        match self {
            Self::Legacy(event) => &event.data,
            Self::V2(event) => &event.data,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PointSeriesData {
    #[serde(rename = "series")]
    pub series_name: String,
    #[serde(deserialize_with = "deserialize_f64_from_number_or_string")]
    pub value: f64,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

fn deserialize_f64_from_number_or_string<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .ok_or_else(|| serde::de::Error::custom("expected a number or numeric string"))
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
