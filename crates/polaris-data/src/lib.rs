pub mod blocking;
mod builder;
mod client;
mod errors;
mod http;
mod models;
mod ohlcv;
mod realtime;
mod replay;
mod storage;
mod time;

pub use builder::PolarisClientBuilder;
pub use client::{PolarisClient, decode_ndjson_file};
pub use errors::PolarisError;
pub use models::{
    BboQuote, CatalogAccess, CatalogInstrument, CatalogMarket, CatalogQuery, CatalogResponse,
    DepthMetricsRow, Diagnostic, DownloadManifestEntry, DownloadManifestQuery,
    DownloadManifestResponse, HistoricalQuery, ListSnapshotsQuery, OhlcvBar, OhlcvFormat,
    OhlcvInterval, OhlcvOutput, OhlcvQuery, OrderbookData, OrderbookEvent, OrderbookLevel,
    PointSeriesData, PointSeriesEvent, RawQuery, RawReplayQuery, RawReplayStream, RealtimeStream,
    ReplayQuery, ReplayStream, SnapshotEntry, StandardEvent, StreamQuery, TimeInput, TradeData,
    TradeEvent, TradingViewCandle, TradingViewOhlcv, TradingViewVolume, VolatilityBar, VolumeBar,
    VwapBar,
};
