pub mod blocking;
mod client;
mod errors;
mod ffi;
mod generated;
mod models;
mod orderbook;

pub(crate) use client::Client;
pub use client::Client as PolarisClient;
pub use client::{HistoricalStream, PolarisClientBuilder, RealtimeStream, Row, StreamQuery};
pub use errors::{Fault, PolarisError};
pub use generated::*;
pub use models::{
    LegacyOrderbookEvent, LegacyStandardEvent, OrderbookData, OrderbookDataV2, OrderbookEvent,
    OrderbookEventV2, OrderbookLevel, StandardEvent, StandardEventV2,
};
pub use orderbook::OrderbookBuilder;
