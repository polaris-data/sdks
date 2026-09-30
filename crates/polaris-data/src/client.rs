use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use async_stream::try_stream;
use chrono::{TimeZone, Utc};
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    builder::PolarisClientBuilder,
    errors::PolarisError,
    http::{AuthMode, HttpClient},
    models::{
        CatalogAccess, CatalogCount, CatalogInstrument, CatalogMarket, CatalogQuery,
        CatalogResponse, Diagnostic, EventsQuery, FundingRateRow, HistoricalRowsQuery,
        HistoricalStream, InstrumentsQuery, InstrumentsResponse, IntentRow, IntentRowsQuery,
        L2OrderbooksQuery, L2UpdatesQuery, MetaResponse, MixedEventRow, OhlcvRow, OhlcvRowsQuery,
        OptionContract, OptionTickerRow, OptionTickerRowsQuery, OrderbookL2Row, RawCaptureRow,
        RawChannelQuery, RawQuery, RealtimeStream, StreamQuery, TradeRow, TradeRowsQuery,
    },
    realtime,
    time::{DEFAULT_INFERRED_LOOKBACK, to_epoch_micros},
};

#[derive(Clone)]
pub struct PolarisClient {
    api_key: Option<String>,
    http: HttpClient,
    diagnostics: Arc<Mutex<Vec<Diagnostic>>>,
    stream_url: url::Url,
}

impl PolarisClient {
    pub fn builder() -> PolarisClientBuilder {
        PolarisClientBuilder::default()
    }

    pub(crate) fn from_parts(
        api_key: Option<String>,
        http: HttpClient,
        stream_url: url::Url,
    ) -> Self {
        Self {
            api_key,
            http,
            diagnostics: Arc::new(Mutex::new(Vec::new())),
            stream_url,
        }
    }

    pub fn take_diagnostics(&self) -> Vec<Diagnostic> {
        let mut diagnostics = self
            .diagnostics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut *diagnostics)
    }

    pub async fn health(&self) -> Result<Value, PolarisError> {
        self.http.get_json("/health", &[], AuthMode::None).await
    }

    /// Return API documentation and machine-readable resource links.
    pub async fn meta(&self) -> Result<MetaResponse, PolarisError> {
        let payload = self.http.get_json("/meta", &[], AuthMode::None).await?;
        serde_json::from_value(payload).map_err(|error| {
            PolarisError::InvalidResponse(format!("invalid /meta response: {error}"))
        })
    }

    pub async fn stream(&self, query: StreamQuery) -> Result<RealtimeStream, PolarisError> {
        realtime::open_stream(self.stream_url.clone(), self.api_key.clone(), query).await
    }

    pub async fn catalog(&self, query: CatalogQuery) -> Result<CatalogResponse, PolarisError> {
        let mut params = Vec::new();
        if let Some(source) = query.source {
            params.push(("source".to_owned(), source));
        }
        if let Some(market) = query.market {
            params.push(("market".to_owned(), market));
        }
        if let Some(q) = query.q {
            params.push(("q".to_owned(), q));
        }
        params.push(("limit".to_owned(), "1000".to_owned()));

        let mut markets = Vec::new();
        let mut seen = BTreeSet::new();
        let mut updated_at: Option<String> = None;
        let mut cursor: Option<String> = None;

        loop {
            let mut page_params = params.clone();
            if let Some(value) = &cursor {
                page_params.push(("cursor".to_owned(), value.clone()));
            }

            let payload = self
                .http
                .get_json("/catalog", &page_params, AuthMode::IfAvailable)
                .await?;

            if payload.get("markets").is_none() {
                return normalize_catalog_response(payload);
            }

            if updated_at.is_none() {
                updated_at = payload
                    .get("updatedAt")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
            }

            for market in normalize_flat_catalog_markets(&payload)? {
                if seen.insert((market.source.clone(), market.market.clone())) {
                    markets.push(market);
                }
            }

            let has_more = payload
                .get("has_more")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let next_cursor = payload
                .get("next_cursor")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            if !has_more || next_cursor.is_none() {
                break;
            }
            cursor = next_cursor;
        }

        let updated_at = updated_at.ok_or_else(|| {
            PolarisError::InvalidResponse("catalog response did not include updatedAt".to_owned())
        })?;

        Ok(CatalogResponse {
            updated_at,
            markets,
            legacy_shape: false,
        })
    }

    /// Discover venue-native option contracts for a source and underlying market.
    pub async fn instruments(
        &self,
        query: InstrumentsQuery,
    ) -> Result<InstrumentsResponse, PolarisError> {
        if query.source.trim().is_empty() || query.market.trim().is_empty() {
            return Err(PolarisError::Request(
                "instruments requires non-empty source and market".to_owned(),
            ));
        }
        if query.expiry.is_some_and(|expiry| expiry < 0) {
            return Err(PolarisError::Request(
                "expiry must be non-negative".to_owned(),
            ));
        }
        if query
            .option_type
            .as_deref()
            .is_some_and(|kind| kind != "call" && kind != "put")
        {
            return Err(PolarisError::Request(
                "option_type must be call or put".to_owned(),
            ));
        }
        let mut params = vec![
            ("source".to_owned(), query.source),
            ("market".to_owned(), query.market),
            ("limit".to_owned(), "1000".to_owned()),
        ];
        if let Some(value) = query.instrument {
            params.push(("instrument".to_owned(), value));
        }
        if let Some(value) = query.expiry {
            params.push(("expiry".to_owned(), value.to_string()));
        }
        if let Some(value) = query.option_type {
            params.push(("option_type".to_owned(), value));
        }
        if let Some(value) = query.q {
            params.push(("q".to_owned(), value));
        }

        let mut instruments = Vec::new();
        let mut updated_at = None;
        let mut cursor: Option<String> = None;
        let mut cursors = BTreeSet::new();
        loop {
            let mut page_params = params.clone();
            if let Some(value) = &cursor {
                page_params.push(("cursor".to_owned(), value.clone()));
            }
            let payload = self
                .http
                .get_json("/catalog/instruments", &page_params, AuthMode::IfAvailable)
                .await?;
            let page_updated_at = payload
                .get("updatedAt")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    PolarisError::InvalidResponse(
                        "instrument catalog response did not include updatedAt".to_owned(),
                    )
                })?;
            if updated_at.is_none() {
                updated_at = Some(page_updated_at.to_owned());
            }
            let page = payload
                .get("instruments")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    PolarisError::InvalidResponse(
                        "instrument catalog response did not include instruments".to_owned(),
                    )
                })?;
            for value in page {
                instruments.push(
                    serde_json::from_value::<OptionContract>(value.clone()).map_err(|error| {
                        PolarisError::InvalidResponse(format!("invalid option contract: {error}"))
                    })?,
                );
            }
            let next = payload
                .get("next_cursor")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty());
            let Some(next) = next else { break };
            if !cursors.insert(next.to_owned()) {
                return Err(PolarisError::InvalidResponse(
                    "instrument catalog repeated a cursor".to_owned(),
                ));
            }
            cursor = Some(next.to_owned());
        }
        Ok(InstrumentsResponse {
            updated_at: updated_at.expect("at least one page"),
            instruments,
        })
    }

    pub async fn count(&self) -> Result<CatalogCount, PolarisError> {
        let payload = self
            .http
            .get_json("/count", &[], AuthMode::IfAvailable)
            .await?;
        normalize_catalog_count(payload)
    }

    pub async fn raw(&self, query: RawQuery) -> Result<Vec<Value>, PolarisError> {
        if query.source.trim().is_empty() || matches!(query.source.as_str(), "." | "..") {
            return Err(PolarisError::InvalidResponse(
                "source must be a non-empty raw source identifier".to_owned(),
            ));
        }
        if query.limit == 0 {
            return Err(PolarisError::InvalidResponse(
                "limit must be > 0".to_owned(),
            ));
        }
        let (from_us, to_us) = resolve_raw_range(
            query.from.as_ref(),
            query.to.as_ref(),
            self.api_key.is_some(),
        )?;
        let mut range_params = vec![
            ("source".to_owned(), query.source.clone()),
            ("start".to_owned(), micros_to_iso8601(from_us)?),
            ("end".to_owned(), micros_to_iso8601(to_us)?),
            ("limit".to_owned(), query.limit.to_string()),
        ];
        if let Some(market) = validate_optional_filter("market", query.market)? {
            range_params.push(("market".to_owned(), market));
        }
        if let Some(channel) = validate_optional_filter("channel", query.channel)? {
            if matches!(channel.as_str(), "." | "..") {
                return Err(PolarisError::InvalidResponse(
                    "channel must be a native raw channel identifier".to_owned(),
                ));
            }
            range_params.push(("channel".to_owned(), channel));
        }
        let mut stream = self.paginated_rows::<Value>(
            "/raw".to_owned(),
            range_params,
            AuthMode::IfAvailable,
            "data",
        )?;
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await {
            rows.push(row?);
        }
        Ok(rows)
    }

    /// Stream exact captures from one venue-native raw channel.
    pub async fn raw_channel(
        &self,
        query: RawChannelQuery,
    ) -> Result<HistoricalStream<RawCaptureRow>, PolarisError> {
        if query.exchange.trim().is_empty()
            || query.event.trim().is_empty()
            || matches!(query.exchange.as_str(), "." | "..")
            || matches!(query.event.as_str(), "." | "..")
        {
            return Err(PolarisError::InvalidResponse(
                "exchange and event must be non-empty".to_owned(),
            ));
        }
        if query.start < 0 || query.end < 0 || query.start > query.end {
            return Err(PolarisError::InvalidResponse(
                "start and end must be non-negative inclusive milliseconds with start <= end"
                    .to_owned(),
            ));
        }
        let mut params = vec![
            ("source".to_owned(), query.exchange),
            ("channel".to_owned(), query.event),
            ("start".to_owned(), millis_to_iso8601(query.start)?),
            ("end".to_owned(), millis_to_iso8601(query.end)?),
            ("limit".to_owned(), "1000".to_owned()),
        ];
        if let Some(market) = validate_optional_filter("market", query.market)? {
            params.push(("market".to_owned(), market));
        }
        self.paginated_rows("/raw".to_owned(), params, AuthMode::IfAvailable, "data")
    }

    pub async fn trades(
        &self,
        query: impl Into<TradeRowsQuery>,
    ) -> Result<HistoricalStream<TradeRow>, PolarisError> {
        let query = query.into();
        let instrument = validate_optional_filter("instrument", query.instrument)?;
        self.historical_rows(
            "/trades",
            HistoricalRowsQuery {
                source: query.source,
                market: query.market,
                start: query.start,
                end: query.end,
            },
            instrument
                .map(|value| vec![("instrument", value)])
                .unwrap_or_default(),
        )
    }

    /// Stream typed flat rows in global collector-time order from `/events`.
    pub async fn events(
        &self,
        query: EventsQuery,
    ) -> Result<HistoricalStream<MixedEventRow>, PolarisError> {
        if query.start < 0 || query.end < query.start {
            return Err(PolarisError::InvalidResponse(
                "events requires non-negative inclusive bounds with start <= end".to_owned(),
            ));
        }
        let mut params = vec![
            ("start".to_owned(), query.start.to_string()),
            ("end".to_owned(), query.end.to_string()),
            ("limit".to_owned(), "1000".to_owned()),
        ];
        if let Some(types) = query.types {
            if types.is_empty() {
                return Err(PolarisError::InvalidResponse(
                    "events types must contain at least one type".to_owned(),
                ));
            }
            params.push((
                "types".to_owned(),
                types
                    .iter()
                    .map(|value| value.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
            ));
        }
        for (name, value) in [
            ("source", query.source),
            ("market", query.market),
            ("instrument", query.instrument),
        ] {
            if let Some(value) = validate_optional_filter(name, value)? {
                params.push((name.to_owned(), value));
            }
        }
        self.paginated_rows("/events".to_owned(), params, AuthMode::Required, "items")
    }

    fn historical_rows<T>(
        &self,
        path: &'static str,
        query: HistoricalRowsQuery,
        filters: Vec<(&str, String)>,
    ) -> Result<HistoricalStream<T>, PolarisError>
    where
        T: DeserializeOwned + Send + 'static,
    {
        if query.start.is_some_and(|value| value < 0)
            || query.end.is_some_and(|value| value < 0)
            || matches!((query.start, query.end), (Some(start), Some(end)) if start > end)
        {
            return Err(PolarisError::InvalidResponse(
                "start and end must be non-negative inclusive milliseconds with start <= end"
                    .to_owned(),
            ));
        }
        let mut params = vec![("limit".to_owned(), "1000".to_owned())];
        if let Some(source) = query.source {
            params.push(("source".to_owned(), source));
        }
        if let Some(market) = query.market {
            params.push(("market".to_owned(), market));
        }
        if let Some(start) = query.start {
            params.push(("start".to_owned(), start.to_string()));
        }
        if let Some(end) = query.end {
            params.push(("end".to_owned(), end.to_string()));
        }
        for (name, value) in filters {
            params.push((name.to_owned(), value));
        }
        self.paginated_rows(path.to_owned(), params, AuthMode::IfAvailable, "items")
    }

    fn paginated_rows<T>(
        &self,
        path: String,
        params: Vec<(String, String)>,
        auth_mode: AuthMode,
        row_key: &'static str,
    ) -> Result<HistoricalStream<T>, PolarisError>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let http = self.http.clone();
        Ok(Box::pin(try_stream! {
            let mut cursor: Option<String> = None;
            loop {
                let mut page_params = params.clone();
                if let Some(value) = &cursor {
                    page_params.push(("cursor".to_owned(), value.clone()));
                }
                let page = http.get_json(&path, &page_params, auth_mode).await?;
                let items = page.get(row_key).and_then(Value::as_array).ok_or_else(|| {
                    PolarisError::InvalidResponse(format!("{path} response did not include {row_key}"))
                })?;
                let has_more = page.get("has_more").and_then(Value::as_bool).ok_or_else(|| {
                    PolarisError::InvalidResponse(format!("{path} response did not include has_more"))
                })?;
                for item in items {
                    let row = serde_json::from_value::<T>(item.clone()).map_err(|error| {
                        PolarisError::InvalidResponse(format!("invalid {path} row: {error}"))
                    })?;
                    yield row;
                }
                if !has_more { break; }
                let next = page.get("next_cursor").and_then(Value::as_str)
                    .filter(|value| !value.is_empty()).ok_or_else(|| {
                        PolarisError::InvalidResponse(format!("{path} response has_more without next_cursor"))
                    })?;
                if cursor.as_deref() == Some(next) {
                    Err(PolarisError::InvalidResponse(format!("{path} repeated next_cursor")))?;
                }
                cursor = Some(next.to_owned());
            }
        }))
    }

    /// Return pair-shaped intent observations from the direct historical API.
    pub async fn intents(
        &self,
        query: IntentRowsQuery,
    ) -> Result<HistoricalStream<IntentRow>, PolarisError> {
        let mut filters = Vec::new();
        if let Some(value) = validate_optional_filter("instrument", query.instrument)? {
            filters.push(("instrument", value));
        }
        if let Some(value) = validate_optional_filter("intent_id", query.intent_id)? {
            filters.push(("intent_id", value));
        }
        self.historical_rows(
            "/intents",
            HistoricalRowsQuery {
                source: query.source,
                market: query.market,
                start: query.start,
                end: query.end,
            },
            filters,
        )
    }

    /// Stream every venue-published candle update from `/ohlcv`.
    pub async fn ohlcv(
        &self,
        query: OhlcvRowsQuery,
    ) -> Result<HistoricalStream<OhlcvRow>, PolarisError> {
        let mut filters = Vec::new();
        if let Some(value) = validate_optional_filter("instrument", query.instrument)? {
            filters.push(("instrument", value));
        }
        if let Some(value) = validate_optional_filter("interval", query.interval)? {
            filters.push(("interval", value));
        }
        self.historical_rows(
            "/ohlcv",
            HistoricalRowsQuery {
                source: query.source,
                market: query.market,
                start: query.start,
                end: query.end,
            },
            filters,
        )
    }

    /// Return standardized option ticker events for an underlying market.
    ///
    /// When `query.instrument` is omitted, all contracts in the option chain
    /// are returned. A non-empty instrument selects one exact contract.
    pub async fn option_tickers(
        &self,
        query: OptionTickerRowsQuery,
    ) -> Result<HistoricalStream<OptionTickerRow>, PolarisError> {
        let instrument = validate_optional_instrument(query.instrument)?;
        self.historical_rows(
            "/options-ticker",
            HistoricalRowsQuery {
                source: query.source,
                market: query.market,
                start: query.start,
                end: query.end,
            },
            instrument
                .map(|value| vec![("instrument", value)])
                .unwrap_or_default(),
        )
    }

    /// Return funding-bearing perpetual ticker observations.
    pub async fn perpetual_tickers(
        &self,
        query: HistoricalRowsQuery,
    ) -> Result<HistoricalStream<FundingRateRow>, PolarisError> {
        self.historical_rows("/perpetual-ticker", query, vec![])
    }

    // -----------------------------------------------------------------------
    // New data schema methods
    // -----------------------------------------------------------------------

    /// Return one reconstructed, sorted top-25 book after each source L2 event.
    pub async fn l2_snapshots(
        &self,
        query: L2OrderbooksQuery,
    ) -> Result<HistoricalStream<OrderbookL2Row>, PolarisError> {
        if query.source.trim().is_empty() || query.market.trim().is_empty() {
            return Err(PolarisError::InvalidResponse(
                "source and market are required for l2_snapshots".to_owned(),
            ));
        }
        if query.start < 0 || query.end < query.start {
            return Err(PolarisError::InvalidResponse(
                "l2_snapshots requires non-negative inclusive bounds with start <= end".to_owned(),
            ));
        }
        let mut params = vec![
            ("source".to_owned(), query.source.trim().to_owned()),
            ("market".to_owned(), query.market.trim().to_owned()),
            ("start".to_owned(), query.start.to_string()),
            ("end".to_owned(), query.end.to_string()),
            ("limit".to_owned(), "1000".to_owned()),
        ];
        if let Some(instrument) = validate_optional_filter("instrument", query.instrument)? {
            params.push(("instrument".to_owned(), instrument));
        }
        self.paginated_rows(
            "/l2-orderbooks".to_owned(),
            params,
            AuthMode::IfAvailable,
            "items",
        )
    }

    /// Return flat stateless source snapshots and deltas, capped at 25 levels.
    pub async fn l2_updates(
        &self,
        query: L2UpdatesQuery,
    ) -> Result<HistoricalStream<OrderbookL2Row>, PolarisError> {
        let filters = validate_optional_filter("instrument", query.instrument)?
            .map(|value| vec![("instrument", value)])
            .unwrap_or_default();
        self.historical_rows(
            "/l2-updates",
            HistoricalRowsQuery {
                source: query.source,
                market: query.market,
                start: query.start,
                end: query.end,
            },
            filters,
        )
    }

    /// Return partial flat funding observations from the direct historical API.
    pub async fn funding_rates(
        &self,
        query: HistoricalRowsQuery,
    ) -> Result<HistoricalStream<FundingRateRow>, PolarisError> {
        self.historical_rows("/funding-rates", query, vec![])
    }
}

fn normalize_catalog_response(payload: Value) -> Result<CatalogResponse, PolarisError> {
    let updated_at = payload
        .get("updatedAt")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            PolarisError::InvalidResponse("catalog response did not include updatedAt".to_owned())
        })?
        .to_owned();

    if let Some(markets) = payload.get("markets").and_then(Value::as_array) {
        let markets = markets
            .iter()
            .map(normalize_flat_market)
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(CatalogResponse {
            updated_at,
            markets,
            legacy_shape: false,
        });
    }

    if let Some(sources) = payload.get("sources").and_then(Value::as_array) {
        let mut markets = Vec::new();
        for source_entry in sources {
            let source = source_entry
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    PolarisError::InvalidResponse("legacy catalog source missing id".to_owned())
                })?;
            let source_markets = source_entry
                .get("markets")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    PolarisError::InvalidResponse(
                        "legacy catalog source missing markets".to_owned(),
                    )
                })?;
            for market_entry in source_markets {
                let market_id = market_entry
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        PolarisError::InvalidResponse("legacy catalog market missing id".to_owned())
                    })?
                    .to_owned();
                let mut normalized = market_entry.clone();
                let object = normalized.as_object_mut().ok_or_else(|| {
                    PolarisError::InvalidResponse(
                        "legacy catalog market was not an object".to_owned(),
                    )
                })?;
                object.insert("source".to_owned(), Value::String(source.to_owned()));
                object.insert("market".to_owned(), Value::String(market_id));
                let mut market = normalize_flat_market(&normalized)?;
                if market.source_type.is_none() {
                    market.source_type = market_entry
                        .get("source")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                }
                markets.push(market);
            }
        }
        return Ok(CatalogResponse {
            updated_at,
            markets,
            legacy_shape: true,
        });
    }

    Err(PolarisError::InvalidResponse(
        "catalog response did not include markets or sources".to_owned(),
    ))
}

fn normalize_flat_catalog_markets(payload: &Value) -> Result<Vec<CatalogMarket>, PolarisError> {
    let markets = payload
        .get("markets")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            PolarisError::InvalidResponse("catalog response did not include markets".to_owned())
        })?;
    markets.iter().map(normalize_flat_market).collect()
}

fn normalize_catalog_count(payload: Value) -> Result<CatalogCount, PolarisError> {
    serde_json::from_value(payload).map_err(|err| {
        PolarisError::InvalidResponse(format!("count response was not valid JSON: {err}"))
    })
}

fn normalize_flat_market(entry: &Value) -> Result<CatalogMarket, PolarisError> {
    let source = entry
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| PolarisError::InvalidResponse("catalog market missing source".to_owned()))?
        .to_owned();
    let market = entry
        .get("market")
        .and_then(Value::as_str)
        .ok_or_else(|| PolarisError::InvalidResponse("catalog market missing market".to_owned()))?
        .to_owned();
    let access = parse_access(entry.get("access"))?;
    let categories = entry
        .get("categories")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        });

    Ok(CatalogMarket {
        source,
        symbol: entry
            .get("symbol")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| market.clone()),
        market,
        start: entry
            .get("start")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        end: entry
            .get("end")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        source_type: entry
            .get("source_type")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        categories,
        access,
        instrument: parse_instrument(entry.get("instrument"))?,
    })
}

fn parse_access(value: Option<&Value>) -> Result<Option<CatalogAccess>, PolarisError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(status) = value.get("status").and_then(Value::as_str) else {
        return Ok(None);
    };
    Ok(Some(CatalogAccess {
        status: status.to_owned(),
        public_cutoff_date: match value.get("public_cutoff_date") {
            Some(Value::Null) | None => None,
            Some(Value::String(text)) => Some(text.clone()),
            Some(_) => {
                return Err(PolarisError::InvalidResponse(
                    "catalog access public_cutoff_date was not a string".to_owned(),
                ));
            }
        },
    }))
}

fn parse_instrument(value: Option<&Value>) -> Result<CatalogInstrument, PolarisError> {
    let Some(value) = value else {
        return Ok(CatalogInstrument::default());
    };
    let Some(object) = value.as_object() else {
        return Err(PolarisError::InvalidResponse(
            "catalog instrument was not an object".to_owned(),
        ));
    };

    Ok(CatalogInstrument {
        base: stringify_nullable_field(object.get("base"), "catalog instrument.base")?,
        quote: stringify_nullable_field(object.get("quote"), "catalog instrument.quote")?,
        tick_size: stringify_nullable_field(
            object.get("tick_size"),
            "catalog instrument.tick_size",
        )?,
        lot_size: stringify_nullable_field(object.get("lot_size"), "catalog instrument.lot_size")?,
        min_notional: stringify_nullable_field(
            object.get("min_notional"),
            "catalog instrument.min_notional",
        )?,
    })
}

fn stringify_nullable_field(
    value: Option<&Value>,
    field_name: &str,
) -> Result<Option<String>, PolarisError> {
    match value {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(Value::Number(number)) => Ok(Some(number.to_string())),
        Some(Value::Bool(boolean)) => Ok(Some(boolean.to_string())),
        Some(_) => Err(PolarisError::InvalidResponse(format!(
            "{field_name} was not a string, number, or null"
        ))),
    }
}

// ===========================================================================
// Helper functions
// ===========================================================================

fn validate_optional_instrument(
    instrument: Option<String>,
) -> Result<Option<String>, PolarisError> {
    validate_optional_filter("instrument", instrument)
}

fn validate_optional_filter(
    name: &str,
    value: Option<String>,
) -> Result<Option<String>, PolarisError> {
    if let Some(value) = value {
        let value = value.trim().to_owned();
        if value.is_empty() {
            return Err(PolarisError::InvalidResponse(format!(
                "{name} must be non-empty"
            )));
        }
        return Ok(Some(value));
    }
    Ok(None)
}

fn resolve_raw_range(
    from: Option<&crate::models::TimeInput>,
    to: Option<&crate::models::TimeInput>,
    authenticated: bool,
) -> Result<(i64, i64), PolarisError> {
    let window = DEFAULT_INFERRED_LOOKBACK
        .num_microseconds()
        .expect("seven days in microseconds");
    let now = Utc::now().timestamp_micros();
    let from = from.map(to_epoch_micros).transpose()?;
    let to = to.map(to_epoch_micros).transpose()?;
    let (start, end) = match (from, to) {
        (Some(start), Some(end)) => (start, end),
        (Some(start), None) => (
            start,
            start
                .checked_add(window)
                .ok_or_else(|| {
                    PolarisError::InvalidResponse("raw time range overflowed".to_owned())
                })?
                .min(now),
        ),
        (None, Some(end)) => (
            end.checked_sub(window).ok_or_else(|| {
                PolarisError::InvalidResponse("raw time range overflowed".to_owned())
            })?,
            end,
        ),
        // Leave a minute inside the rolling public cutoff so request latency does not
        // put an anonymous default query just outside the permitted window.
        (None, None) => (
            now - window + if authenticated { 0 } else { 60_000_000 },
            now,
        ),
    };
    if start > end {
        return Err(PolarisError::InvalidResponse(
            "raw start must be before or equal to end".to_owned(),
        ));
    }
    Ok((start, end))
}

fn micros_to_iso8601(value: i64) -> Result<String, PolarisError> {
    chrono::Utc
        .timestamp_micros(value)
        .single()
        .map(|value| value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        .ok_or_else(|| {
            PolarisError::InvalidResponse(format!("invalid epoch micros value '{value}'"))
        })
}

fn millis_to_iso8601(value: i64) -> Result<String, PolarisError> {
    let micros = value.checked_mul(1_000).ok_or_else(|| {
        PolarisError::InvalidResponse(format!("invalid epoch milliseconds value '{value}'"))
    })?;
    micros_to_iso8601(micros)
}
