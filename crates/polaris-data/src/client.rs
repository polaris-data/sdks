use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, BufReader, Cursor, Read},
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
        BboQuery, BboQuote, CatalogAccess, CatalogCount, CatalogInstrument, CatalogMarket,
        CatalogQuery, CatalogResponse, DepthMetricsRow, Diagnostic, FundingRateRow,
        HistoricalRowsQuery, HistoricalStream, InstrumentsQuery, InstrumentsResponse, IntentRow,
        IntentRowsQuery, L2OrderbooksQuery, L2UpdatesQuery, OhlcvBar, OhlcvFormat, OhlcvOutput,
        OhlcvQuery, OhlcvRow, OhlcvRowsQuery, OptionContract, OptionTickerRow,
        OptionTickerRowsQuery, OrderbookL2Row, QuoteRow, QuoteRowsQuery, RawCaptureRow,
        RawChannelQuery, RawQuery, RealtimeStream, StreamQuery, TradeRow, VolatilityBar, VolumeBar,
        VwapBar,
    },
    ohlcv, realtime,
    storage::StorageLayout,
    time::{DEFAULT_INFERRED_LOOKBACK, end_of_public_cutoff_day, to_datetime, to_epoch_micros},
};

#[derive(Clone)]
pub struct PolarisClient {
    api_key: Option<String>,
    layout: StorageLayout,
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
        layout: StorageLayout,
        http: HttpClient,
        stream_url: url::Url,
    ) -> Self {
        Self {
            api_key,
            layout,
            http,
            diagnostics: Arc::new(Mutex::new(Vec::new())),
            stream_url,
        }
    }

    pub fn dataset_root(&self) -> &std::path::Path {
        &self.layout.root
    }

    pub fn cache_dir(&self) -> &std::path::Path {
        &self.layout.cache_dir
    }

    pub fn daily_dir(&self) -> &std::path::Path {
        &self.layout.daily_dir
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
        if query.limit == 0 {
            return Err(PolarisError::InvalidResponse(
                "limit must be > 0".to_owned(),
            ));
        }
        let (from_us, to_us) = self
            .resolve_historical_range(
                &query.source,
                &query.market,
                query.from.as_ref(),
                query.to.as_ref(),
            )
            .await?;
        let range_params = vec![
            ("source".to_owned(), query.source.clone()),
            ("market".to_owned(), query.market.clone()),
            ("from".to_owned(), micros_to_iso8601(from_us)?),
            ("to".to_owned(), micros_to_iso8601(to_us)?),
        ];

        let mut file_params = range_params.clone();
        file_params.push(("format".to_owned(), "file".to_owned()));
        if let Ok((content_type, body)) = self
            .http
            .get_bytes("/raw", &file_params, AuthMode::Required)
            .await
        {
            let is_json = content_type
                .as_deref()
                .is_some_and(|value| value.to_ascii_lowercase().contains("application/json"));
            if !is_json {
                if let Ok(rows) = decode_ndjson(&body) {
                    return Ok(rows);
                }
            }
        }

        let mut rows = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut params = range_params.clone();
            params.push(("limit".to_owned(), query.limit.to_string()));
            if let Some(value) = &cursor {
                params.push(("cursor".to_owned(), value.clone()));
            }
            let payload = self
                .http
                .get_json("/raw", &params, AuthMode::Required)
                .await?;
            let page = payload
                .get("data")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    PolarisError::InvalidResponse(
                        "raw response did not include a data array".to_owned(),
                    )
                })?;
            rows.extend(page.iter().cloned());
            cursor = payload
                .get("next_cursor")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .filter(|value| !value.is_empty());
            if cursor.is_none() {
                break;
            }
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
        let mut url = url::Url::parse("https://polaris.invalid/").expect("valid URL");
        url.path_segments_mut()
            .expect("URL has path segments")
            .extend(["raw", query.exchange.as_str(), query.event.as_str()]);
        let path = url.path().to_owned();
        let params = vec![
            ("start".to_owned(), query.start.to_string()),
            ("end".to_owned(), query.end.to_string()),
            ("limit".to_owned(), "1000".to_owned()),
        ];
        self.paginated_rows(path, params, AuthMode::IfAvailable)
    }

    pub async fn trades(
        &self,
        query: HistoricalRowsQuery,
    ) -> Result<HistoricalStream<TradeRow>, PolarisError> {
        self.historical_rows("/historical/trades", query, vec![])
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
        self.paginated_rows(path.to_owned(), params, AuthMode::IfAvailable)
    }

    fn paginated_rows<T>(
        &self,
        path: String,
        params: Vec<(String, String)>,
        auth_mode: AuthMode,
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
                let items = page.get("items").and_then(Value::as_array).ok_or_else(|| {
                    PolarisError::InvalidResponse(format!("{path} response did not include items"))
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
        self.intent_rows(query).await
    }

    /// Stream pair-shaped intent observations from the direct historical API.
    pub async fn intent_rows(
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
            "/historical/intents",
            HistoricalRowsQuery {
                source: query.source,
                market: query.market,
                start: query.start,
                end: query.end,
            },
            filters,
        )
    }

    /// Stream venue-published candle updates from the direct historical API.
    pub async fn ohlcv_rows(
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
            "/historical/ohlcv",
            HistoricalRowsQuery {
                source: query.source,
                market: query.market,
                start: query.start,
                end: query.end,
            },
            filters,
        )
    }

    /// Stream individual PropAMM quote points from the direct historical API.
    pub async fn quote_rows(
        &self,
        query: QuoteRowsQuery,
    ) -> Result<HistoricalStream<QuoteRow>, PolarisError> {
        let mut filters = Vec::new();
        if let Some(value) = validate_optional_filter("instrument", query.instrument)? {
            filters.push(("instrument", value));
        }
        if let Some(value) = validate_optional_filter("observation_id", query.observation_id)? {
            filters.push(("observation_id", value));
        }
        self.historical_rows(
            "/historical/quotes",
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
            "/historical/options-ticker",
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
        self.funding_rates(query).await
    }

    pub async fn ohlcv(&self, query: OhlcvQuery) -> Result<OhlcvOutput, PolarisError> {
        let format = query.format;
        let rows = self
            .venue_candles(&query, Some(query.interval.as_str()))
            .await?;
        let bars = rows
            .into_iter()
            .map(|row| OhlcvBar {
                timestamp: row.open_timestamp,
                open: row.open,
                high: row.high,
                low: row.low,
                close: row.close,
                volume: row.base_volume.unwrap_or_default(),
                trades: row.trade_count.unwrap_or_default(),
            })
            .collect::<Vec<_>>();
        Ok(match format {
            OhlcvFormat::Bars => OhlcvOutput::Bars(bars),
            OhlcvFormat::TradingView => OhlcvOutput::TradingView(crate::TradingViewOhlcv {
                candles: bars
                    .iter()
                    .map(|bar| crate::TradingViewCandle {
                        time: bar.timestamp / 1_000,
                        open: bar.open,
                        high: bar.high,
                        low: bar.low,
                        close: bar.close,
                    })
                    .collect(),
                volumes: bars
                    .iter()
                    .map(|bar| crate::TradingViewVolume {
                        time: bar.timestamp / 1_000,
                        value: bar.volume,
                    })
                    .collect(),
            }),
        })
    }

    async fn venue_candles(
        &self,
        query: &OhlcvQuery,
        interval: Option<&str>,
    ) -> Result<Vec<OhlcvRow>, PolarisError> {
        let start = query
            .from
            .as_ref()
            .map(to_epoch_micros)
            .transpose()?
            .map(|value| value.div_euclid(1_000));
        let end = query
            .to
            .as_ref()
            .map(to_epoch_micros)
            .transpose()?
            .map(|value| value.div_euclid(1_000));
        let mut rows = self
            .ohlcv_rows(OhlcvRowsQuery {
                source: Some(query.source.clone()),
                market: Some(query.market.clone()),
                instrument: None,
                interval: interval.map(ToOwned::to_owned),
                start,
                end,
            })
            .await?;
        let mut latest = BTreeMap::<(String, String, String, String, i64), OhlcvRow>::new();
        while let Some(row) = rows.next().await {
            let row = row?;
            let key = (
                row.source.clone(),
                row.market.clone(),
                row.instrument.clone().unwrap_or_default(),
                row.interval.clone(),
                row.open_timestamp,
            );
            let replace = latest.get(&key).is_none_or(|old| {
                (row.collector_timestamp, &row.event_id) > (old.collector_timestamp, &old.event_id)
            });
            if replace {
                latest.insert(key, row);
            }
        }
        let mut rows = latest.into_values().collect::<Vec<_>>();
        rows.sort_by(|a, b| (a.open_timestamp, &a.event_id).cmp(&(b.open_timestamp, &b.event_id)));
        Ok(rows)
    }

    async fn resolve_historical_range(
        &self,
        source: &str,
        market: &str,
        from: Option<&crate::models::TimeInput>,
        to: Option<&crate::models::TimeInput>,
    ) -> Result<(i64, i64), PolarisError> {
        if let (Some(from), Some(to)) = (from, to) {
            let from_us = to_epoch_micros(from)?;
            let to_us = to_epoch_micros(to)?;
            if from_us >= to_us {
                return Err(PolarisError::InvalidResponse(
                    "from must be before to".to_owned(),
                ));
            }
            return Ok((from_us, to_us));
        }

        let market_bounds = self.catalog_market_bounds(source, market).await?;
        let lower_bound = market_bounds.start_us;
        let mut upper_bound = market_bounds.end_us.min(Utc::now().timestamp_micros());

        if market_bounds.access_status.as_deref() == Some("restricted") && self.api_key.is_none() {
            return Err(PolarisError::AccessDenied {
                message: format!("dataset '{source}/{market}' requires authentication"),
                status_code: None,
                body: None,
            });
        }

        if self.api_key.is_none() {
            if let Some(public_cutoff_us) = market_bounds.public_cutoff_us {
                upper_bound = upper_bound.min(public_cutoff_us);
            }
        }

        if lower_bound >= upper_bound {
            return Err(PolarisError::InvalidResponse(format!(
                "catalog reported no queryable historical range for '{source}/{market}'"
            )));
        }

        let (resolved_from, resolved_to) = match (from, to) {
            (None, None) => {
                let resolved_to = upper_bound;
                let resolved_from = (resolved_to
                    - DEFAULT_INFERRED_LOOKBACK
                        .num_microseconds()
                        .expect("7 days in micros"))
                .max(lower_bound);
                (resolved_from, resolved_to)
            }
            (None, Some(to)) => {
                let resolved_to = to_epoch_micros(to)?.min(upper_bound);
                let from_dt = (chrono::Utc
                    .timestamp_micros(resolved_to)
                    .single()
                    .ok_or_else(|| {
                        PolarisError::InvalidResponse("invalid inferred upper bound".to_owned())
                    })?
                    - DEFAULT_INFERRED_LOOKBACK)
                    .timestamp_micros();
                (lower_bound.max(from_dt), resolved_to)
            }
            (Some(from), None) => {
                let resolved_from = lower_bound.max(to_epoch_micros(from)?);
                let to_dt = (chrono::Utc
                    .timestamp_micros(resolved_from)
                    .single()
                    .ok_or_else(|| {
                        PolarisError::InvalidResponse("invalid inferred lower bound".to_owned())
                    })?
                    + DEFAULT_INFERRED_LOOKBACK)
                    .timestamp_micros();
                (resolved_from, upper_bound.min(to_dt))
            }
            (Some(_), Some(_)) => unreachable!(),
        };

        if resolved_from >= resolved_to {
            return Err(PolarisError::InvalidResponse(
                "from must resolve to a time before to".to_owned(),
            ));
        }
        Ok((resolved_from, resolved_to))
    }

    async fn catalog_market_bounds(
        &self,
        source: &str,
        market: &str,
    ) -> Result<CatalogMarketBounds, PolarisError> {
        let catalog = self
            .catalog(CatalogQuery {
                source: Some(source.to_owned()),
                market: Some(market.to_owned()),
                q: None,
            })
            .await?;
        if catalog.legacy_shape {
            return Err(PolarisError::InvalidResponse(
                "Catalog response did not include market metadata needed to infer a historical range"
                    .to_owned(),
            ));
        }

        let market_entry = catalog
            .markets
            .into_iter()
            .find(|entry| entry.source == source && entry.market == market)
            .ok_or_else(|| PolarisError::NotFound {
                message: format!("catalog did not include dataset '{source}/{market}'"),
                status_code: None,
                body: None,
            })?;

        let start = market_entry
            .start
            .as_ref()
            .ok_or_else(|| {
                PolarisError::InvalidResponse(format!(
                    "catalog entry for '{source}/{market}' is missing start"
                ))
            })?
            .clone();
        let end = market_entry
            .end
            .as_ref()
            .ok_or_else(|| {
                PolarisError::InvalidResponse(format!(
                    "catalog entry for '{source}/{market}' is missing end"
                ))
            })?
            .clone();

        let public_cutoff_us = match market_entry
            .access
            .as_ref()
            .and_then(|access| access.public_cutoff_date.as_ref())
        {
            Some(cutoff) => Some(end_of_public_cutoff_day(cutoff)?),
            None => None,
        };

        Ok(CatalogMarketBounds {
            start_us: to_datetime(&start.into())?.timestamp_micros(),
            end_us: to_datetime(&end.into())?.timestamp_micros(),
            access_status: market_entry
                .access
                .as_ref()
                .map(|access| access.status.to_lowercase()),
            public_cutoff_us,
        })
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
            "/historical/l2-orderbooks".to_owned(),
            params,
            AuthMode::IfAvailable,
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
            "/historical/l2-updates",
            HistoricalRowsQuery {
                source: query.source,
                market: query.market,
                start: query.start,
                end: query.end,
            },
            filters,
        )
    }

    /// Derive best bid / offer quotes directly from standardized orderbook updates.
    pub async fn bbo(&self, query: BboQuery) -> Result<HistoricalStream<BboQuote>, PolarisError> {
        self.bbo_inner(query, false).await
    }

    /// Derive only best bid / offer changes, suppressing deep and no-op updates.
    pub async fn bbo_changes(
        &self,
        query: BboQuery,
    ) -> Result<HistoricalStream<BboQuote>, PolarisError> {
        self.bbo_inner(query, true).await
    }

    async fn bbo_inner(
        &self,
        query: BboQuery,
        changes_only: bool,
    ) -> Result<HistoricalStream<BboQuote>, PolarisError> {
        let interval_ms = query.interval.map(ohlcv::interval_to_millis);
        let mut rows = self
            .l2_snapshots(L2OrderbooksQuery {
                source: query.source,
                market: query.market,
                instrument: None,
                start: query.start,
                end: query.end,
            })
            .await?;
        Ok(Box::pin(try_stream! {
            let mut buckets = BTreeMap::<i64, BboQuote>::new();
            let mut last_quote: Option<BboQuote> = None;
            while let Some(row) = rows.next().await {
                let row = row?;
                let (Some((bid_price, bid_quantity)), Some((ask_price, ask_quantity))) =
                    (row.bids().next(), row.asks().next()) else { continue };
                let mut quote = BboQuote {
                    timestamp: row.collector_timestamp,
                    bid_price, bid_quantity, ask_price, ask_quantity,
                };
                if changes_only {
                    let unchanged = last_quote.as_ref().is_some_and(|previous| {
                        previous.bid_price == quote.bid_price
                            && previous.bid_quantity == quote.bid_quantity
                            && previous.ask_price == quote.ask_price
                            && previous.ask_quantity == quote.ask_quantity
                    });
                    if unchanged { continue; }
                    last_quote = Some(quote.clone());
                }
                let Some(width) = interval_ms else {
                    yield quote;
                    continue;
                };
                let bucket = quote.timestamp.div_euclid(width) * width;
                quote.timestamp = bucket;
                buckets.insert(bucket, quote);
            }
            for quote in buckets.into_values() { yield quote; }
        }))
    }

    /// Return partial flat funding observations from the direct historical API.
    pub async fn funding_rates(
        &self,
        query: HistoricalRowsQuery,
    ) -> Result<HistoricalStream<FundingRateRow>, PolarisError> {
        self.historical_rows("/historical/funding-rates", query, vec![])
    }

    /// Return funding-bearing observations that include a mark price.
    pub async fn mark_prices(
        &self,
        query: HistoricalRowsQuery,
    ) -> Result<HistoricalStream<FundingRateRow>, PolarisError> {
        let mut rows = self.funding_rates(query).await?;
        Ok(Box::pin(try_stream! {
            while let Some(row) = rows.next().await {
                let row = row?;
                if row.mark_price.is_some() {
                    yield row;
                }
            }
        }))
    }

    /// Return standardized PropAMM quote-ladder records for a time range.
    pub async fn volume(&self, query: OhlcvQuery) -> Result<Vec<VolumeBar>, PolarisError> {
        let ohlcv_output = self.ohlcv(query).await?;
        match ohlcv_output {
            OhlcvOutput::Bars(bars) => Ok(bars
                .into_iter()
                .map(|bar| VolumeBar {
                    timestamp: bar.timestamp,
                    volume: bar.volume,
                })
                .collect()),
            OhlcvOutput::TradingView(tv) => Ok(tv
                .volumes
                .into_iter()
                .map(|v| VolumeBar {
                    timestamp: v.time,
                    volume: v.value,
                })
                .collect()),
        }
    }

    /// Derive candle VWAP from venue-reported quote and base volume.
    pub async fn vwap(&self, query: OhlcvQuery) -> Result<Vec<VwapBar>, PolarisError> {
        let rows = self
            .venue_candles(&query, Some(query.interval.as_str()))
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let volume = row.base_volume.unwrap_or_default();
                let quote_volume = row.quote_volume.unwrap_or(row.close * volume);
                VwapBar {
                    timestamp: row.open_timestamp,
                    vwap: (volume > 0.0).then_some(quote_volume / volume),
                    volume,
                    quote_volume,
                    trades: row.trade_count.unwrap_or_default(),
                }
            })
            .collect())
    }

    /// Estimate realized volatility from the finest available venue candle closes.
    pub async fn volatility(&self, query: OhlcvQuery) -> Result<Vec<VolatilityBar>, PolarisError> {
        let target = ohlcv::interval_to_millis(query.interval);
        let mut rows = self.venue_candles(&query, None).await?;
        let finest = rows
            .iter()
            .filter_map(|row| candle_interval_ms(&row.interval))
            .filter(|width| *width < target)
            .min();
        let Some(finest) = finest else {
            return Ok(Vec::new());
        };
        rows.retain(|row| candle_interval_ms(&row.interval) == Some(finest));
        let mut aggregator = VolatilityAggregator::new(target);
        for row in rows {
            aggregator.add(row.open_timestamp, row.close);
        }
        Ok(aggregator.finish())
    }

    /// Derive spread, depth, imbalance, and slippage metrics from orderbooks.
    pub async fn depth_metrics(
        &self,
        query: L2OrderbooksQuery,
        depth_pct: Option<f64>,
        slippage_notional: Option<f64>,
    ) -> Result<HistoricalStream<DepthMetricsRow>, PolarisError> {
        let depth_pct = depth_pct.unwrap_or(0.01);
        let slippage_notional = slippage_notional.unwrap_or(10_000.0);

        if depth_pct <= 0.0 {
            return Err(PolarisError::InvalidResponse(
                "depth_pct must be greater than 0".to_owned(),
            ));
        }
        if slippage_notional <= 0.0 {
            return Err(PolarisError::InvalidResponse(
                "slippage_notional must be greater than 0".to_owned(),
            ));
        }

        let mut rows = self.l2_snapshots(query).await?;
        Ok(Box::pin(try_stream! {
            while let Some(row) = rows.next().await {
                let row = row?;
                let bids: Vec<_> = row.bids().collect();
                let asks: Vec<_> = row.asks().collect();
                if let Some(metrics) = Self::derive_depth_metrics(
                    row.collector_timestamp, &bids, &asks,
                    depth_pct, slippage_notional,
                ) { yield metrics; }
            }
        }))
    }

    fn derive_depth_metrics(
        timestamp: i64,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        depth_pct: f64,
        slippage_notional: f64,
    ) -> Option<DepthMetricsRow> {
        let (bid_price, _bid_quantity) = *bids.first()?;
        let (ask_price, _ask_quantity) = *asks.first()?;

        if ask_price < bid_price {
            return None;
        }

        let mid_price = (bid_price + ask_price) / 2.0;
        let spread = ask_price - bid_price;
        let spread_bps = if mid_price > 0.0 {
            Some((spread / mid_price) * 10_000.0)
        } else {
            None
        };

        let bid_depth_notional =
            Self::depth_notional_within_pct(bids.iter().copied(), true, mid_price, depth_pct);
        let ask_depth_notional =
            Self::depth_notional_within_pct(asks.iter().copied(), false, mid_price, depth_pct);
        let total_depth_notional = bid_depth_notional + ask_depth_notional;
        let depth_imbalance = if total_depth_notional > 0.0 {
            Some((bid_depth_notional - ask_depth_notional) / total_depth_notional)
        } else {
            None
        };

        let target_base_quantity = if mid_price > 0.0 {
            Some(slippage_notional / mid_price)
        } else {
            None
        };

        let (buy_avg_price, buy_slippage, buy_slippage_bps) = Self::calculate_slippage(
            asks.iter().copied(),
            target_base_quantity?,
            slippage_notional,
            mid_price,
        );
        let (sell_avg_price, sell_slippage, sell_slippage_bps) = Self::calculate_slippage(
            bids.iter().copied(),
            target_base_quantity?,
            slippage_notional,
            mid_price,
        );

        Some(DepthMetricsRow {
            timestamp,
            bid_price,
            ask_price,
            mid_price,
            bid_ask_spread: spread,
            bid_ask_spread_bps: spread_bps,
            depth_pct,
            bid_depth_notional,
            ask_depth_notional,
            depth_imbalance,
            slippage_notional,
            target_base_quantity,
            buy_average_price: buy_avg_price,
            sell_average_price: sell_avg_price,
            buy_slippage,
            sell_slippage,
            buy_slippage_bps,
            sell_slippage_bps,
        })
    }

    fn depth_notional_within_pct(
        levels: impl Iterator<Item = (f64, f64)>,
        is_bid: bool,
        mid_price: f64,
        depth_pct: f64,
    ) -> f64 {
        let cutoff = if is_bid {
            mid_price * (1.0 - depth_pct)
        } else {
            mid_price * (1.0 + depth_pct)
        };

        levels
            .filter(|(price, _)| {
                if is_bid {
                    *price >= cutoff
                } else {
                    *price <= cutoff
                }
            })
            .map(|(price, quantity)| price * quantity)
            .sum()
    }

    fn calculate_slippage(
        levels: impl Iterator<Item = (f64, f64)>,
        target_quantity: f64,
        slippage_notional: f64,
        mid_price: f64,
    ) -> (Option<f64>, Option<f64>, Option<f64>) {
        let mut remaining_quantity = target_quantity;
        let mut quote_total = 0.0;

        for (price, available_quantity) in levels {
            let fill_quantity = available_quantity.min(remaining_quantity);
            quote_total += fill_quantity * price;
            remaining_quantity -= fill_quantity;
            if remaining_quantity <= 1e-12 {
                break;
            }
        }

        if remaining_quantity > 1e-12 {
            return (None, None, None);
        }

        let avg_price = quote_total / target_quantity;
        let slippage = (quote_total - slippage_notional).abs();
        let slippage_bps = (((avg_price - mid_price) / mid_price) * 10_000.0).abs();

        (Some(avg_price), Some(slippage), Some(slippage_bps))
    }
}

#[derive(Clone, Debug)]
struct CatalogMarketBounds {
    start_us: i64,
    end_us: i64,
    access_status: Option<String>,
    public_cutoff_us: Option<i64>,
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

fn candle_interval_ms(interval: &str) -> Option<i64> {
    match interval {
        "100ms" => Some(100),
        "1s" => Some(1_000),
        "10s" => Some(10_000),
        "1m" => Some(60_000),
        "5m" => Some(300_000),
        "15m" => Some(900_000),
        "1h" => Some(3_600_000),
        _ => None,
    }
}

struct VolatilityAggregator {
    interval_ms: i64,
    points: Vec<(i64, f64)>,
}

impl VolatilityAggregator {
    fn new(interval_ms: i64) -> Self {
        Self {
            interval_ms,
            points: Vec::new(),
        }
    }

    fn add(&mut self, timestamp: i64, price: f64) {
        if price <= 0.0 || !price.is_finite() {
            return;
        }
        self.points.push((timestamp, price));
    }

    fn finish(self) -> Vec<VolatilityBar> {
        let mut buckets: std::collections::HashMap<i64, VolatilityBucket> =
            std::collections::HashMap::new();

        for (timestamp, price) in self.points {
            let bucket = (timestamp / self.interval_ms) * self.interval_ms;
            let state = buckets.entry(bucket).or_insert_with(|| VolatilityBucket {
                timestamp: bucket,
                returns: 0,
                mean: 0.0,
                m2: 0.0,
                last_price: None,
            });

            if let Some(last_price) = state.last_price {
                let log_return = (price / last_price).ln();
                state.returns += 1;
                let delta = log_return - state.mean;
                state.mean += delta / state.returns as f64;
                let delta2 = log_return - state.mean;
                state.m2 += delta * delta2;
            }

            state.last_price = Some(price);
        }

        let mut result = Vec::new();
        for state in buckets.into_values() {
            if state.returns < 2 {
                continue;
            }

            let variance = state.m2 / (state.returns - 1) as f64;
            result.push(VolatilityBar {
                timestamp: state.timestamp,
                volatility: variance.sqrt(),
                returns: state.returns as u64,
            });
        }

        result.sort_by_key(|bar| bar.timestamp);
        result
    }
}

struct VolatilityBucket {
    timestamp: i64,
    returns: usize,
    mean: f64,
    m2: f64,
    last_price: Option<f64>,
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

fn micros_to_iso8601(value: i64) -> Result<String, PolarisError> {
    chrono::Utc
        .timestamp_micros(value)
        .single()
        .map(|value| value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        .ok_or_else(|| {
            PolarisError::InvalidResponse(format!("invalid epoch micros value '{value}'"))
        })
}

pub fn decode_ndjson_file(path: &std::path::Path) -> Result<Vec<Value>, PolarisError> {
    decode_ndjson(&std::fs::read(path)?)
}

fn decode_ndjson(body: &[u8]) -> Result<Vec<Value>, PolarisError> {
    const ZSTD_MAGIC: &[u8] = b"\x28\xb5\x2f\xfd";
    let reader: Box<dyn Read> = if body.starts_with(ZSTD_MAGIC) {
        Box::new(
            zstd::stream::read::Decoder::new(Cursor::new(body))
                .map_err(|err| PolarisError::Decode(format!("invalid zstd stream: {err}")))?,
        )
    } else {
        Box::new(Cursor::new(body))
    };
    let mut rows = Vec::new();
    for line in BufReader::new(reader).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let row = serde_json::from_str::<Value>(&line)
            .map_err(|err| PolarisError::Decode(format!("invalid ndjson line: {err}")))?;
        if !row.is_object() {
            return Err(PolarisError::Decode(
                "expected each NDJSON row to be an object".to_owned(),
            ));
        }
        rows.push(row);
    }
    Ok(rows)
}
