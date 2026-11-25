use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

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
