use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, BufReader, Cursor, Read},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use async_stream::try_stream;
use chrono::{Duration, NaiveDate, TimeZone, Utc};
use futures_util::StreamExt;
use serde_json::Value;

use crate::{
    OrderbookBuilder,
    builder::PolarisClientBuilder,
    errors::PolarisError,
    http::{AuthMode, HttpClient},
    models::{
        BboQuery, BboQuote, CatalogAccess, CatalogCount, CatalogInstrument, CatalogMarket,
        CatalogQuery, CatalogResponse, DepthMetricsRow, Diagnostic, DownloadManifestQuery,
        DownloadManifestResponse, HistoricalQuery, HistoricalStream, LegacyOrderbookEvent,
        LegacyPointSeriesEvent, LegacyTradeData, LegacyTradeEvent, ListSnapshotsQuery, OhlcvOutput,
        OhlcvQuery, OrderbookData, OrderbookDataV2, OrderbookEvent, OrderbookEventV2,
        OrderbookLevel, PointSeriesData, PointSeriesEvent, PointSeriesEventV2,
        PropammQuoteLadderData, PropammQuoteLadderEvent, RawQuery, RawReplayQuery, RawReplayStream,
        RealtimeStream, ReplayQuery, ReplayStream, SnapshotEntry, StandardEvent, StreamQuery,
        TradeDataV2, TradeEvent, TradeEventV2, VolatilityBar, VolumeBar, VwapBar,
    },
    ohlcv,
    orderbook::{BookUpdate, BookView, parse_level_tuple},
    realtime, replay,
    storage::{
        LocalSnapshotFile, SnapshotCoverage, StorageLayout, acquire_sync_lock, data_file_path,
        list_local_snapshot_entries, parse_snapshot_key, temp_file_path, write_coverage_sidecar,
    },
    time::{
        DEFAULT_INFERRED_LOOKBACK, end_of_public_cutoff_day, to_datetime, to_epoch_micros,
        to_iso8601,
    },
};

#[derive(Clone)]
pub struct PolarisClient {
    api_key: Option<String>,
    layout: StorageLayout,
    http: HttpClient,
    diagnostics: Arc<Mutex<Vec<Diagnostic>>>,
    stream_url: url::Url,
}

#[derive(Clone)]
pub(crate) struct PreparedReplay {
    pub(crate) paths: Vec<PathBuf>,
    pub(crate) from_us: i64,
    pub(crate) to_us: i64,
    pub(crate) gaps: Vec<(i64, i64)>,
    pub(crate) materialize_orderbooks: bool,
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

    pub async fn count(&self) -> Result<CatalogCount, PolarisError> {
        let payload = self
            .http
            .get_json("/count", &[], AuthMode::IfAvailable)
            .await?;
        normalize_catalog_count(payload)
    }

    pub async fn download_manifest(
        &self,
        query: DownloadManifestQuery,
    ) -> Result<DownloadManifestResponse, PolarisError> {
        let params = vec![
            ("source".to_owned(), query.source),
            ("market".to_owned(), query.market),
            ("date".to_owned(), query.date),
            ("mode".to_owned(), "json".to_owned()),
        ];
        let payload = self
            .http
            .get_json("/download", &params, AuthMode::IfAvailable)
            .await?;
        normalize_download_manifest_response(payload)
    }

    pub async fn list_snapshots(
        &self,
        query: ListSnapshotsQuery,
    ) -> Result<Vec<SnapshotEntry>, PolarisError> {
        if let Some(limit) = query.limit {
            if limit == 0 {
                return Err(PolarisError::InvalidResponse(
                    "limit must be > 0".to_owned(),
                ));
            }
        }

        let mut params = vec![
            ("source".to_owned(), query.source.clone()),
            ("market".to_owned(), query.market.clone()),
        ];
        if let Some(from) = &query.from {
            params.push(("from".to_owned(), to_iso8601(from)?));
        }
        if let Some(to) = &query.to {
            params.push(("to".to_owned(), to_iso8601(to)?));
        }
        if let Some(limit) = query.limit {
            params.push(("limit".to_owned(), limit.to_string()));
        }

        let mut entries = BTreeMap::<String, SnapshotEntry>::new();
        let mut cursor: Option<String> = None;
        let mut first_access: Option<CatalogAccess> = None;

        loop {
            let mut page_params = params.clone();
            if let Some(cursor_value) = &cursor {
                page_params.push(("cursor".to_owned(), cursor_value.clone()));
            }

            let payload = self
                .http
                .get_json("/snapshots", &page_params, AuthMode::IfAvailable)
                .await?;

            if first_access.is_none() {
                first_access = parse_access(payload.get("access"))?;
            }
            self.check_snapshot_access(&query, first_access.as_ref())?;

            for field in ["snapshots", "data"] {
                if let Some(items) = payload.get(field).and_then(Value::as_array) {
                    for item in items {
                        let entry = parse_snapshot_entry(item, &query.source, &query.market)?;
                        entries.insert(entry.key.clone(), entry);
                    }
                }
            }

            cursor = payload
                .get("next_cursor")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .filter(|value| !value.is_empty());
            if cursor.is_none() {
                break;
            }
        }

        Ok(entries.into_values().collect())
    }

    pub async fn events(
        &self,
        query: HistoricalQuery,
    ) -> Result<HistoricalStream<StandardEvent>, PolarisError> {
        self.replay(ReplayQuery {
            source: query.source,
            market: query.market,
            from: query.from,
            to: query.to,
            allow_gaps: query.allow_gaps,
            materialize_orderbooks: query.materialize_orderbooks,
        })
        .await
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

    pub async fn raw_replay(&self, query: RawReplayQuery) -> Result<RawReplayStream, PolarisError> {
        let rows = self
            .raw(RawQuery {
                source: query.source,
                market: query.market,
                from: query.from,
                to: query.to,
                limit: query.limit,
            })
            .await?;
        Ok(Box::pin(futures_util::stream::iter(
            rows.into_iter().map(Ok),
        )))
    }

    pub async fn trades(
        &self,
        mut query: HistoricalQuery,
    ) -> Result<HistoricalStream<TradeEvent>, PolarisError> {
        query.materialize_orderbooks = false;
        let mut events = self.events(query).await?;
        Ok(Box::pin(try_stream! {
            while let Some(event) = events.next().await {
                let event = event?;
                if event.event_type() != "trade" {
                    continue;
                }
                yield Self::parse_trade(event)?;
            }
        }))
    }

    pub async fn replay(&self, query: ReplayQuery) -> Result<ReplayStream, PolarisError> {
        let materialize_orderbooks = query.materialize_orderbooks;
        let records = self.replay_records(query).await?;
        Ok(replay::replay_stream(records, materialize_orderbooks))
    }

    async fn replay_records(
        &self,
        query: ReplayQuery,
    ) -> Result<replay::ReplayRecordStream, PolarisError> {
        let prepared = self.prepare_replay(query).await?;
        Ok(replay::replay_record_stream(
            prepared.paths,
            prepared.from_us,
            prepared.to_us,
            prepared.gaps,
        ))
    }

    pub(crate) async fn prepare_replay(
        &self,
        query: ReplayQuery,
    ) -> Result<PreparedReplay, PolarisError> {
        let (from_us, to_us) = self
            .resolve_historical_range(
                &query.source,
                &query.market,
                query.from.as_ref(),
                query.to.as_ref(),
            )
            .await?;
        let (paths, gaps) = self
            .resolve_snapshot_paths(
                &query.source,
                &query.market,
                from_us,
                to_us,
                query.allow_gaps,
            )
            .await?;
        Ok(PreparedReplay {
            paths,
            from_us,
            to_us,
            gaps,
            materialize_orderbooks: query.materialize_orderbooks,
        })
    }

    pub async fn ohlcv(&self, query: OhlcvQuery) -> Result<OhlcvOutput, PolarisError> {
        let mut trades = self
            .trades(HistoricalQuery {
                source: query.source,
                market: query.market,
                from: query.from,
                to: query.to,
                allow_gaps: query.allow_gaps,
                materialize_orderbooks: true,
            })
            .await?;
        let mut aggregator = ohlcv::OhlcvAggregator::new(query.interval);
        while let Some(trade) = trades.next().await {
            aggregator.add(&trade?);
        }
        Ok(aggregator.finish(query.format))
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

    async fn resolve_snapshot_paths(
        &self,
        source: &str,
        market: &str,
        from_us: i64,
        to_us: i64,
        allow_gaps: bool,
    ) -> Result<(Vec<PathBuf>, Vec<(i64, i64)>), PolarisError> {
        let start = Utc
            .timestamp_micros(from_us)
            .single()
            .ok_or_else(|| PolarisError::InvalidResponse("invalid replay start".to_owned()))?;
        let end = Utc
            .timestamp_micros(to_us)
            .single()
            .ok_or_else(|| PolarisError::InvalidResponse("invalid replay end".to_owned()))?;
        let mut required_dates = BTreeSet::new();
        let mut cursor = start.date_naive();
        let last = (end - Duration::microseconds(1)).date_naive();
        while cursor <= last {
            required_dates.insert(cursor);
            cursor = cursor
                .succ_opt()
                .ok_or_else(|| PolarisError::InvalidResponse("date overflow".to_owned()))?;
        }

        let direct_daily: Vec<PathBuf> = required_dates
            .iter()
            .map(|day| {
                self.layout
                    .daily_dir
                    .join(source)
                    .join(market)
                    .join(format!("{}.jsonl.zst", day.format("%Y-%m-%d")))
            })
            .collect();
        if direct_daily.iter().all(|path| path.exists()) {
            self.push_estimated_coverage_diagnostic(source, market, direct_daily.len());
            return Ok((direct_daily, Vec::new()));
        }

        let local_entries =
            list_local_snapshot_entries(&self.layout, source, market, &required_dates)?;
        let mut local_selected = Vec::new();
        let mut local_gaps = Vec::new();
        for day in &required_dates {
            let (range_start, range_end) = day_query_bounds(*day, from_us, to_us)?;
            let (selected, gaps) = select_snapshot_coverage(
                local_entries
                    .get(day)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
                *day,
                range_start,
                range_end,
            )?;
            local_selected.extend(selected);
            local_gaps.extend(gaps);
        }
        if !local_selected.is_empty() && local_gaps.is_empty() {
            let estimated = local_selected
                .iter()
                .filter(|entry| entry.coverage.is_estimated())
                .count();
            if estimated > 0 {
                self.push_estimated_coverage_diagnostic(source, market, estimated);
            }
            return Ok((
                local_selected.into_iter().map(|entry| entry.path).collect(),
                Vec::new(),
            ));
        }

        let first_day = *required_dates
            .first()
            .ok_or_else(|| PolarisError::InvalidResponse("empty replay range".to_owned()))?;
        let last_day = *required_dates
            .last()
            .ok_or_else(|| PolarisError::InvalidResponse("empty replay range".to_owned()))?;
        let query_from = Utc
            .from_utc_datetime(&first_day.and_hms_opt(0, 0, 0).expect("midnight"))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let query_to_day = last_day
            .succ_opt()
            .ok_or_else(|| PolarisError::InvalidResponse("date overflow".to_owned()))?;
        let query_to = Utc
            .from_utc_datetime(&query_to_day.and_hms_opt(0, 0, 0).expect("midnight"))
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let remote = self
            .list_snapshots(ListSnapshotsQuery {
                source: source.to_owned(),
                market: market.to_owned(),
                from: Some(query_from.into()),
                to: Some(query_to.into()),
                limit: Some(1000),
            })
            .await?;
        let mut remote_by_day = BTreeMap::<NaiveDate, Vec<LocalSnapshotFile>>::new();
        for entry in remote {
            let Some(day_text) = entry.date.as_deref() else {
                continue;
            };
            let Ok(day) = NaiveDate::parse_from_str(day_text, "%Y-%m-%d") else {
                continue;
            };
            if !required_dates.contains(&day) {
                continue;
            }
            let path = data_file_path(&self.layout.data_dir, &entry.key)?;
            let coverage = snapshot_coverage(&entry)?;
            remote_by_day
                .entry(day)
                .or_default()
                .push(LocalSnapshotFile {
                    entry,
                    path,
                    download_url: None,
                    coverage,
                });
        }

        let mut selected_entries = Vec::new();
        let mut gaps = Vec::new();
        for day in &required_dates {
            let (range_start, range_end) = day_query_bounds(*day, from_us, to_us)?;
            let (selected, day_gaps) = select_snapshot_coverage(
                remote_by_day
                    .get(day)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
                *day,
                range_start,
                range_end,
            )?;
            selected_entries.extend(selected);
            gaps.extend(day_gaps);
        }

        if !gaps.is_empty() && !allow_gaps {
            return Err(PolarisError::CoverageGap {
                dataset_source: source.to_owned(),
                market: market.to_owned(),
                intervals: gaps
                    .iter()
                    .map(|(start, end)| format_gap(*start, *end))
                    .collect(),
            });
        }

        if !gaps.is_empty() && allow_gaps {
            let message = format!(
                "Standardized snapshot coverage for '{}/{}' has gaps in {}..{}; skipped missing intervals: {}",
                source,
                market,
                chrono::Utc
                    .timestamp_micros(from_us)
                    .single()
                    .expect("valid from")
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                chrono::Utc
                    .timestamp_micros(to_us)
                    .single()
                    .expect("valid to")
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                gaps.iter()
                    .map(|(start, end)| format_gap(*start, *end))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            log::warn!("{message}");
            self.diagnostics
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(Diagnostic {
                    code: "snapshot_gaps".to_owned(),
                    message,
                });
        }

        let selected_dates: BTreeSet<NaiveDate> = selected_entries
            .iter()
            .filter(|entry| !entry.path.exists())
            .filter_map(|entry| {
                entry
                    .entry
                    .date
                    .as_deref()
                    .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
            })
            .collect();
        let mut urls = BTreeMap::new();
        for day in selected_dates {
            let manifest = self
                .download_manifest(DownloadManifestQuery {
                    source: source.to_owned(),
                    market: market.to_owned(),
                    date: day.format("%Y-%m-%d").to_string(),
                })
                .await?;
            for entry in manifest.snapshots {
                urls.insert(entry.key, entry.url);
            }
        }

        let mut selected_paths = Vec::new();
        for mut candidate in selected_entries {
            if !candidate.path.exists() {
                candidate.download_url = urls.get(&candidate.entry.key).cloned();
            }
            if !candidate.path.exists() {
                self.download_snapshot(&candidate).await?;
            } else {
                self.persist_snapshot_coverage(&candidate).await?;
            }
            selected_paths.push((coverage_sort_key(&candidate, from_us)?, candidate.path));
        }
        selected_paths
            .sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        selected_paths.dedup_by(|left, right| left.1 == right.1);
        Ok((
            selected_paths.into_iter().map(|(_, path)| path).collect(),
            gaps,
        ))
    }

    fn push_estimated_coverage_diagnostic(&self, source: &str, market: &str, files: usize) {
        let message = format!(
            "Local snapshot coverage for '{source}/{market}' was inferred from {files} file(s) without exact coverage metadata; completeness is estimated"
        );
        log::warn!("{message}");
        self.diagnostics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Diagnostic {
                code: "estimated_snapshot_coverage".to_owned(),
                message,
            });
    }

    async fn persist_snapshot_coverage(
        &self,
        snapshot: &LocalSnapshotFile,
    ) -> Result<(), PolarisError> {
        let SnapshotCoverage::Exact { start_us, end_us } = snapshot.coverage else {
            return Ok(());
        };
        let locks_dir = self.layout.locks_dir.clone();
        let path = snapshot.path.clone();
        let key = snapshot.entry.key.clone();
        tokio::task::spawn_blocking(move || {
            let _lock = acquire_sync_lock(&locks_dir)?;
            write_coverage_sidecar(&path, &key, start_us, end_us)
        })
        .await
        .map_err(|err| PolarisError::Request(format!("coverage writer join failed: {err}")))?
    }

    async fn download_snapshot(&self, snapshot: &LocalSnapshotFile) -> Result<(), PolarisError> {
        let final_path = snapshot.path.as_path();
        if final_path.exists() {
            return self.persist_snapshot_coverage(snapshot).await;
        }

        let locks_dir = self.layout.locks_dir.clone();
        let _lock = tokio::task::spawn_blocking(move || acquire_sync_lock(&locks_dir))
            .await
            .map_err(|err| PolarisError::Request(format!("lock task failed: {err}")))??;
        if final_path.exists() {
            if let SnapshotCoverage::Exact { start_us, end_us } = snapshot.coverage {
                write_coverage_sidecar(final_path, &snapshot.entry.key, start_us, end_us)?;
            }
            return Ok(());
        }

        let url = snapshot.download_url.as_ref().ok_or_else(|| {
            PolarisError::InvalidResponse(format!(
                "missing download url for snapshot '{}'",
                snapshot.entry.key
            ))
        })?;
        let bytes = self.http.download_absolute_bytes(url).await?;

        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let temp_path = temp_file_path(&self.layout.tmp_dir, &snapshot.entry.key)?;
        if let Some(parent) = temp_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(&temp_path, &bytes).await?;
        if let Err(err) = tokio::fs::rename(&temp_path, final_path).await {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(err.into());
        }
        if let SnapshotCoverage::Exact { start_us, end_us } = snapshot.coverage {
            write_coverage_sidecar(final_path, &snapshot.entry.key, start_us, end_us)?;
        }
        if snapshot.entry.start.is_none()
            && snapshot.entry.end.is_none()
            && snapshot.entry.hour.is_none()
        {
            let source = snapshot.entry.source.as_deref().unwrap_or_default();
            let market = snapshot.entry.market.as_deref().unwrap_or_default();
            if let Some(day) = snapshot.entry.date.as_deref() {
                let daily_path = self
                    .layout
                    .daily_dir
                    .join(source)
                    .join(market)
                    .join(format!("{day}.jsonl.zst"));
                if let Some(parent) = daily_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                let daily_temp = daily_path.with_extension("jsonl.zst.part");
                tokio::fs::copy(final_path, &daily_temp).await?;
                tokio::fs::rename(daily_temp, daily_path).await?;
            }
        }
        Ok(())
    }

    fn check_snapshot_access(
        &self,
        query: &ListSnapshotsQuery,
        access: Option<&CatalogAccess>,
    ) -> Result<(), PolarisError> {
        let Some(access) = access else {
            return Ok(());
        };
        if self.api_key.is_some() {
            return Ok(());
        }

        match access.status.as_str() {
            "restricted" => Err(PolarisError::AccessDenied {
                message: format!(
                    "dataset '{}/{}' requires authentication",
                    query.source, query.market
                ),
                status_code: None,
                body: None,
            }),
            "preview" => {
                if let (Some(cutoff), Some(to)) =
                    (access.public_cutoff_date.as_ref(), query.to.as_ref())
                {
                    let cutoff_us = end_of_public_cutoff_day(cutoff)?;
                    if to_epoch_micros(to)? > cutoff_us {
                        return Err(PolarisError::AccessDenied {
                            message: format!(
                                "dataset '{}/{}' data after {} requires authentication",
                                query.source, query.market, cutoff
                            ),
                            status_code: None,
                            body: None,
                        });
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    // -----------------------------------------------------------------------
    // New data schema methods
    // -----------------------------------------------------------------------

    /// Return standardized orderbook snapshot events for a time range.
    pub async fn l2_snapshots(
        &self,
        query: HistoricalQuery,
    ) -> Result<HistoricalStream<OrderbookEvent>, PolarisError> {
        let mut events = self.events(query).await?;
        Ok(Box::pin(try_stream! {
            while let Some(event) = events.next().await {
                let event = event?;
                if !matches!(
                    event.event_type(),
                    "orderbook" | "orderbook_delta" | "orderbook_snapshot" | "l2_snapshot"
                ) {
                    continue;
                }
                if let Some(orderbook) = Self::try_parse_orderbook(event) {
                    yield orderbook;
                }
            }
        }))
    }

    /// Return raw standardized orderbook snapshots and deltas for a time range.
    ///
    /// Unlike [`Self::l2_snapshots`], this method never reconstructs complete
    /// books. Feed the updates into [`OrderbookBuilder`] when application-managed
    /// book state is needed.
    pub async fn l2_updates(
        &self,
        mut query: HistoricalQuery,
    ) -> Result<HistoricalStream<StandardEvent>, PolarisError> {
        query.materialize_orderbooks = false;
        let mut events = self.events(query).await?;
        Ok(Box::pin(try_stream! {
            while let Some(event) = events.next().await {
                let event = event?;
                if matches!(
                    event.event_type(),
                    "orderbook" | "orderbook_delta" | "orderbook_snapshot" | "l2_snapshot"
                ) {
                    yield event;
                }
            }
        }))
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
        let mut records = self
            .replay_records(ReplayQuery {
                source: query.source,
                market: query.market,
                from: query.from,
                to: query.to,
                allow_gaps: query.allow_gaps,
                materialize_orderbooks: false,
            })
            .await?;
        Ok(Box::pin(try_stream! {
            let mut orderbooks = OrderbookBuilder::new();
            let mut buckets = std::collections::BTreeMap::<i64, BboQuote>::new();
            let mut last_quote: Option<BboQuote> = None;
            while let Some(record) = records.next().await {
                let event = match record? {
                    replay::ReplayRecord::Reset => {
                        orderbooks.clear();
                        last_quote = None;
                        continue;
                    }
                    replay::ReplayRecord::Event(event) => event,
                };
                if orderbooks.update_state(&event)? != BookUpdate::Applied {
                    continue;
                }
                let Some(mut quote) = orderbooks.best_bid_offer(
                    event.source(),
                    event.market(),
                    event.timestamp(),
                ) else {
                    continue;
                };
                if changes_only {
                    let unchanged = last_quote.as_ref().is_some_and(|previous| {
                        previous.bid_price == quote.bid_price
                            && previous.bid_quantity == quote.bid_quantity
                            && previous.ask_price == quote.ask_price
                            && previous.ask_quantity == quote.ask_quantity
                    });
                    if unchanged {
                        continue;
                    }
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
            for quote in buckets.into_values() {
                yield quote;
            }
        }))
    }

    /// Return standardized funding-rate point-series events for a time range.
    pub async fn funding_rates(
        &self,
        mut query: HistoricalQuery,
    ) -> Result<HistoricalStream<PointSeriesEvent>, PolarisError> {
        query.materialize_orderbooks = false;
        self.point_series(query, &["funding_rate"]).await
    }

    /// Return standardized mark-price point-series events for a time range.
    pub async fn mark_prices(
        &self,
        mut query: HistoricalQuery,
    ) -> Result<HistoricalStream<PointSeriesEvent>, PolarisError> {
        query.materialize_orderbooks = false;
        self.point_series(query, &["mark_price", "mark_px"]).await
    }

    /// Return standardized PropAMM quote-ladder records for a time range.
    pub async fn propamm_quote_ladders(
        &self,
        mut query: HistoricalQuery,
    ) -> Result<HistoricalStream<PropammQuoteLadderEvent>, PolarisError> {
        query.materialize_orderbooks = false;
        let mut events = self.events(query).await?;
        Ok(Box::pin(try_stream! {
            while let Some(event) = events.next().await {
                if let Some(ladder) = Self::parse_propamm_quote_ladder(event?)? {
                    yield ladder;
                }
            }
        }))
    }

    async fn point_series(
        &self,
        query: HistoricalQuery,
        expected_series: &'static [&'static str],
    ) -> Result<HistoricalStream<PointSeriesEvent>, PolarisError> {
        let mut events = self.events(query).await?;
        Ok(Box::pin(try_stream! {
            while let Some(event) = events.next().await {
                let event = event?;
                if event.event_type() == "point" {
                    if let Some(point) = Self::try_parse_point_series(event, expected_series) {
                        yield point;
                    }
                }
            }
        }))
    }

    /// Aggregate per-bucket trade volume from standardized trade data.
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

    /// Aggregate per-bucket VWAP from standardized trade data.
    pub async fn vwap(&self, query: OhlcvQuery) -> Result<Vec<VwapBar>, PolarisError> {
        let mut trades = self
            .trades(HistoricalQuery {
                source: query.source,
                market: query.market,
                from: query.from,
                to: query.to,
                allow_gaps: query.allow_gaps,
                materialize_orderbooks: true,
            })
            .await?;

        let interval_ms = ohlcv::interval_to_millis(query.interval);
        let mut aggregator = VwapAggregator::new(interval_ms);

        while let Some(trade) = trades.next().await {
            let trade = trade?;
            aggregator.add(trade.timestamp(), trade.price(), trade.quantity());
        }

        Ok(aggregator.finish())
    }

    /// Aggregate realized volatility from standardized trade data.
    pub async fn volatility(&self, query: OhlcvQuery) -> Result<Vec<VolatilityBar>, PolarisError> {
        let mut trades = self
            .trades(HistoricalQuery {
                source: query.source,
                market: query.market,
                from: query.from,
                to: query.to,
                allow_gaps: query.allow_gaps,
                materialize_orderbooks: true,
            })
            .await?;

        let interval_ms = ohlcv::interval_to_millis(query.interval);
        let mut aggregator = VolatilityAggregator::new(interval_ms);

        while let Some(trade) = trades.next().await {
            let trade = trade?;
            aggregator.add_trade(&trade);
        }

        Ok(aggregator.finish())
    }

    /// Derive spread, depth, imbalance, and slippage metrics from orderbooks.
    pub async fn depth_metrics(
        &self,
        query: HistoricalQuery,
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

        let mut records = self
            .replay_records(ReplayQuery {
                source: query.source,
                market: query.market,
                from: query.from,
                to: query.to,
                allow_gaps: query.allow_gaps,
                materialize_orderbooks: false,
            })
            .await?;
        Ok(Box::pin(try_stream! {
            let mut orderbooks = OrderbookBuilder::new();
            while let Some(record) = records.next().await {
                let event = match record? {
                    replay::ReplayRecord::Reset => {
                        orderbooks.clear();
                        continue;
                    }
                    replay::ReplayRecord::Event(event) => event,
                };
                if orderbooks.update_state(&event)? != BookUpdate::Applied {
                    continue;
                }
                if let Some(view) = orderbooks.view(event.source(), event.market()) {
                    if let Some(metrics) = Self::derive_depth_metrics(
                        event.timestamp(),
                        view,
                        depth_pct,
                        slippage_notional,
                    ) {
                        yield metrics;
                    }
                }
            }
        }))
    }

    // -----------------------------------------------------------------------
    // Helper methods for data derivation
    // -----------------------------------------------------------------------

    pub(crate) fn parse_trade(event: StandardEvent) -> Result<TradeEvent, PolarisError> {
        match event {
            StandardEvent::Legacy(event) => {
                let data: LegacyTradeData = serde_json::from_value(event.data).map_err(|err| {
                    PolarisError::Decode(format!("invalid legacy trade payload: {err}"))
                })?;
                Ok(TradeEvent::Legacy(LegacyTradeEvent {
                    timestamp: event.timestamp,
                    source: event.source,
                    market: event.market,
                    event_type: event.event_type,
                    data,
                }))
            }
            StandardEvent::V2(event) => {
                let data: TradeDataV2 = serde_json::from_value(event.data).map_err(|err| {
                    PolarisError::Decode(format!("invalid v2 trade payload: {err}"))
                })?;
                if data
                    .side
                    .as_deref()
                    .is_some_and(|side| !matches!(side, "buy" | "sell"))
                {
                    return Err(PolarisError::Decode(
                        "invalid v2 trade payload: data.side must be buy, sell, or null".to_owned(),
                    ));
                }
                Ok(TradeEvent::V2(TradeEventV2 {
                    collector_timestamp: event.collector_timestamp,
                    collector_sequence: event.collector_sequence,
                    exchange_timestamp: event.exchange_timestamp,
                    exchange_sequence: event.exchange_sequence,
                    source: event.source,
                    market: event.market,
                    event_type: event.event_type,
                    data,
                }))
            }
        }
    }

    fn try_parse_orderbook(event: StandardEvent) -> Option<OrderbookEvent> {
        let mut payload = event.data().clone();
        if !payload.is_object() {
            payload = Value::Object(Default::default());
        }
        let object = payload.as_object_mut()?;
        for key in ["bids", "asks"] {
            if !object.contains_key(key) {
                if let Some(value) = event.extra().get(key) {
                    object.insert(key.to_owned(), value.clone());
                }
            }
        }
        let is_delta = match &event {
            StandardEvent::Legacy(_) => event.event_type() == "orderbook_delta",
            StandardEvent::V2(_) => !object.get("is_snapshot")?.as_bool()?,
        };
        let bids = match object.get("bids") {
            Some(value) => parse_orderbook_levels(value)?,
            None if is_delta => Vec::new(),
            None => return None,
        };
        let asks = match object.get("asks") {
            Some(value) => parse_orderbook_levels(value)?,
            None if is_delta => Vec::new(),
            None => return None,
        };
        let mut extra = object.clone();
        extra.remove("bids");
        extra.remove("asks");
        match event {
            StandardEvent::Legacy(event) => Some(OrderbookEvent::Legacy(LegacyOrderbookEvent {
                timestamp: event.timestamp,
                source: event.source,
                market: event.market,
                event_type: event.event_type,
                data: OrderbookData {
                    bids,
                    asks,
                    extra: extra.into_iter().collect(),
                },
            })),
            StandardEvent::V2(event) => {
                let is_snapshot = extra.remove("is_snapshot")?.as_bool()?;
                Some(OrderbookEvent::V2(OrderbookEventV2 {
                    collector_timestamp: event.collector_timestamp,
                    collector_sequence: event.collector_sequence,
                    exchange_timestamp: event.exchange_timestamp,
                    exchange_sequence: event.exchange_sequence,
                    source: event.source,
                    market: event.market,
                    event_type: event.event_type,
                    data: OrderbookDataV2 {
                        is_snapshot,
                        bids,
                        asks,
                        extra: extra.into_iter().collect(),
                    },
                }))
            }
        }
    }

    pub(crate) fn try_parse_point_series(
        event: StandardEvent,
        expected_series: &[&str],
    ) -> Option<PointSeriesEvent> {
        let data: PointSeriesData = serde_json::from_value(event.data().clone()).ok()?;
        if !expected_series.contains(&data.series_name.as_str()) {
            return None;
        }
        match event {
            StandardEvent::Legacy(event) => {
                Some(PointSeriesEvent::Legacy(LegacyPointSeriesEvent {
                    timestamp: event.timestamp,
                    source: event.source,
                    market: event.market,
                    event_type: event.event_type,
                    data,
                }))
            }
            StandardEvent::V2(event) => Some(PointSeriesEvent::V2(PointSeriesEventV2 {
                collector_timestamp: event.collector_timestamp,
                collector_sequence: event.collector_sequence,
                exchange_timestamp: event.exchange_timestamp,
                exchange_sequence: event.exchange_sequence,
                source: event.source,
                market: event.market,
                event_type: event.event_type,
                data,
            })),
        }
    }

    pub(crate) fn parse_propamm_quote_ladder(
        event: StandardEvent,
    ) -> Result<Option<PropammQuoteLadderEvent>, PolarisError> {
        if event.event_type() != "record"
            || event.data().get("series").and_then(Value::as_str) != Some("quote_ladder")
        {
            return Ok(None);
        }
        let StandardEvent::V2(event) = event else {
            return Ok(None);
        };
        let values = event
            .data
            .get("values")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                PolarisError::Decode(
                    "invalid PropAMM quote-ladder payload: values must be an object".to_owned(),
                )
            })?;
        if !values.contains_key("oracle") {
            return Err(PolarisError::Decode(
                "invalid PropAMM quote-ladder payload: oracle is required".to_owned(),
            ));
        }
        if values.get("pool").is_some_and(|value| !value.is_string()) {
            return Err(PolarisError::Decode(
                "invalid PropAMM quote-ladder payload: pool must be a string when present"
                    .to_owned(),
            ));
        }
        let data =
            serde_json::from_value::<PropammQuoteLadderData>(event.data).map_err(|error| {
                PolarisError::Decode(format!("invalid PropAMM quote-ladder payload: {error}"))
            })?;
        if data.series_name != "quote_ladder" {
            return Ok(None);
        }
        Ok(Some(PropammQuoteLadderEvent {
            collector_timestamp: event.collector_timestamp,
            collector_sequence: event.collector_sequence,
            exchange_timestamp: event.exchange_timestamp,
            exchange_sequence: event.exchange_sequence,
            source: event.source,
            market: event.market,
            event_type: event.event_type,
            data,
        }))
    }

    fn derive_depth_metrics(
        timestamp: i64,
        orderbook: BookView<'_>,
        depth_pct: f64,
        slippage_notional: f64,
    ) -> Option<DepthMetricsRow> {
        let (bid_price, _bid_quantity) = orderbook.bids().next()?;
        let (ask_price, _ask_quantity) = orderbook.asks().next()?;

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
            Self::depth_notional_within_pct(orderbook.bids(), true, mid_price, depth_pct);
        let ask_depth_notional =
            Self::depth_notional_within_pct(orderbook.asks(), false, mid_price, depth_pct);
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
            orderbook.asks(),
            target_base_quantity?,
            slippage_notional,
            mid_price,
        );
        let (sell_avg_price, sell_slippage, sell_slippage_bps) = Self::calculate_slippage(
            orderbook.bids(),
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

fn day_query_bounds(day: NaiveDate, from_us: i64, to_us: i64) -> Result<(i64, i64), PolarisError> {
    let start = Utc
        .from_utc_datetime(&day.and_hms_opt(0, 0, 0).expect("midnight"))
        .timestamp_micros()
        .max(from_us);
    let next = day
        .succ_opt()
        .ok_or_else(|| PolarisError::InvalidResponse("date overflow".to_owned()))?;
    let end = Utc
        .from_utc_datetime(&next.and_hms_opt(0, 0, 0).expect("midnight"))
        .timestamp_micros()
        .min(to_us);
    Ok((start, end))
}

fn snapshot_start_us(entry: &SnapshotEntry, day: NaiveDate) -> Result<Option<i64>, PolarisError> {
    if let Some(start) = &entry.start {
        return Ok(Some(to_datetime(&start.clone().into())?.timestamp_micros()));
    }
    let midnight = Utc.from_utc_datetime(&day.and_hms_opt(0, 0, 0).expect("midnight"));
    if let Some(timestamp) = entry.timestamp.as_deref() {
        if timestamp.len() == 6 && timestamp.chars().all(|ch| ch.is_ascii_digit()) {
            let hour = timestamp[0..2].parse::<i64>().unwrap_or_default();
            let minute = timestamp[2..4].parse::<i64>().unwrap_or_default();
            let second = timestamp[4..6].parse::<i64>().unwrap_or_default();
            return Ok(Some(
                (midnight
                    + Duration::hours(hour)
                    + Duration::minutes(minute)
                    + Duration::seconds(second))
                .timestamp_micros(),
            ));
        }
    }
    Ok(entry
        .hour
        .map(|hour| (midnight + Duration::hours(i64::from(hour))).timestamp_micros()))
}

fn snapshot_end_us(entry: &SnapshotEntry) -> Result<Option<i64>, PolarisError> {
    entry
        .end
        .as_ref()
        .map(|end| to_datetime(&end.clone().into()).map(|value| value.timestamp_micros()))
        .transpose()
}

fn snapshot_coverage(entry: &SnapshotEntry) -> Result<SnapshotCoverage, PolarisError> {
    let (Some(start), Some(end)) = (entry.start.as_ref(), entry.end.as_ref()) else {
        return Ok(SnapshotCoverage::Estimated);
    };
    let start_us = to_datetime(&start.clone().into())?.timestamp_micros();
    let end_us = to_datetime(&end.clone().into())?.timestamp_micros();
    if start_us >= end_us {
        return Ok(SnapshotCoverage::Estimated);
    }
    Ok(SnapshotCoverage::Exact { start_us, end_us })
}

fn coverage_sort_key(entry: &LocalSnapshotFile, fallback: i64) -> Result<i64, PolarisError> {
    match entry.coverage {
        SnapshotCoverage::Exact { start_us, .. } => Ok(start_us),
        SnapshotCoverage::Estimated => {
            let day = entry
                .entry
                .date
                .as_deref()
                .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok());
            day.map(|day| snapshot_start_us(&entry.entry, day))
                .transpose()
                .map(|value| value.flatten().unwrap_or(fallback))
        }
    }
}

type SnapshotCoverageSelection = (Vec<LocalSnapshotFile>, Vec<(i64, i64)>);

fn select_snapshot_coverage(
    entries: &[LocalSnapshotFile],
    day: NaiveDate,
    range_start: i64,
    range_end: i64,
) -> Result<SnapshotCoverageSelection, PolarisError> {
    if let Some(daily) = entries
        .iter()
        .filter(|entry| {
            entry.coverage.is_estimated()
                && entry.entry.start.is_none()
                && entry.entry.end.is_none()
                && entry.entry.hour.is_none()
                && entry.entry.timestamp.is_none()
        })
        .min_by(|left, right| left.entry.key.cmp(&right.entry.key))
    {
        return Ok((vec![daily.clone()], Vec::new()));
    }

    let day_end = Utc
        .from_utc_datetime(
            &day.succ_opt()
                .ok_or_else(|| PolarisError::InvalidResponse("date overflow".to_owned()))?
                .and_hms_opt(0, 0, 0)
                .expect("midnight"),
        )
        .timestamp_micros();
    let mut ordered = entries
        .iter()
        .filter_map(|entry| {
            let start = match entry.coverage {
                SnapshotCoverage::Exact { start_us, .. } => Ok(Some(start_us)),
                SnapshotCoverage::Estimated => snapshot_start_us(&entry.entry, day),
            };
            start
                .transpose()
                .map(|result| result.map(|start| (start, entry.clone())))
        })
        .collect::<Result<Vec<_>, _>>()?;
    ordered.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.entry.key.cmp(&right.1.entry.key))
    });
    if let Some((first_start, first_entry)) = ordered.first_mut()
        && first_entry.coverage.is_estimated()
        && first_entry.entry.hour == Some(0)
    {
        *first_start = Utc
            .from_utc_datetime(&day.and_hms_opt(0, 0, 0).expect("midnight"))
            .timestamp_micros();
    }

    let mut selected = Vec::new();
    let mut covered = Vec::new();
    for (index, (entry_start, entry)) in ordered.iter().enumerate() {
        let entry_end = match entry.coverage {
            SnapshotCoverage::Exact { end_us, .. } => end_us,
            SnapshotCoverage::Estimated => snapshot_end_us(&entry.entry)?
                .or_else(|| {
                    (entry.entry.hour.is_some() && entry.entry.timestamp.is_none())
                        .then_some(*entry_start + Duration::hours(1).num_microseconds().unwrap())
                })
                .or_else(|| {
                    ordered[index + 1..]
                        .iter()
                        .map(|item| item.0)
                        .find(|next| next > entry_start)
                })
                .unwrap_or(day_end),
        };
        let overlap_start = range_start.max(*entry_start);
        let overlap_end = range_end.min(entry_end);
        if overlap_start < overlap_end {
            selected.push(entry.clone());
            covered.push((overlap_start, overlap_end));
        }
    }

    covered.sort();
    let mut gaps = Vec::new();
    let mut cursor = range_start;
    for (start, end) in covered {
        if end <= cursor {
            continue;
        }
        if start > cursor {
            gaps.push((cursor, start));
        }
        cursor = cursor.max(end);
    }
    if cursor < range_end {
        gaps.push((cursor, range_end));
    }
    Ok((selected, gaps))
}

fn format_gap(start_us: i64, end_us: i64) -> String {
    let render = |value| {
        Utc.timestamp_micros(value)
            .single()
            .expect("valid coverage timestamp")
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    };
    format!("{}..{}", render(start_us), render(end_us))
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

fn normalize_download_manifest_response(
    payload: Value,
) -> Result<DownloadManifestResponse, PolarisError> {
    serde_json::from_value(payload).map_err(|err| {
        PolarisError::InvalidResponse(format!("download response was not valid JSON: {err}"))
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

fn parse_snapshot_entry(
    value: &Value,
    source: &str,
    market: &str,
) -> Result<SnapshotEntry, PolarisError> {
    let key = value
        .get("key")
        .or_else(|| value.get("path"))
        .or_else(|| value.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            PolarisError::InvalidResponse("snapshot entry did not include a key".to_owned())
        })?
        .to_owned();
    let parsed_key = parse_snapshot_key(&key).ok();
    let timestamp = value
        .get("timestamp")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            parsed_key
                .as_ref()
                .and_then(|parsed| parsed.timestamp.clone())
        });
    let explicit_hour = value
        .get("hour")
        .and_then(Value::as_u64)
        .map(|hour| hour as u8);
    let inferred_hour = match timestamp.as_deref() {
        Some(timestamp) => Some(parse_manifest_hour(timestamp)?),
        None => None,
    };
    let hour = explicit_hour
        .or(inferred_hour)
        .or_else(|| parsed_key.as_ref().and_then(|parsed| parsed.hour));
    Ok(SnapshotEntry {
        key: key.clone(),
        source: value
            .get("source")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| Some(source.to_owned())),
        market: value
            .get("market")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| Some(market.to_owned())),
        date: value
            .get("date")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| parsed_key.as_ref().map(|parsed| parsed.date.clone())),
        start: value
            .get("start")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        end: value
            .get("end")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        timestamp,
        hour,
        filename: value
            .get("filename")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn parse_manifest_hour(timestamp: &str) -> Result<u8, PolarisError> {
    if timestamp.len() < 2 || !timestamp.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(PolarisError::InvalidResponse(format!(
            "invalid snapshot timestamp '{timestamp}'"
        )));
    }
    timestamp[0..2].parse::<u8>().map_err(|err| {
        PolarisError::InvalidResponse(format!("invalid snapshot timestamp '{timestamp}': {err}"))
    })
}

// ===========================================================================
// Aggregators
// ===========================================================================

struct VwapAggregator {
    interval_ms: i64,
    rows: std::collections::HashMap<i64, VwapRow>,
}

struct VwapRow {
    timestamp: i64,
    volume: f64,
    quote_volume: f64,
    trades: u64,
}

impl VwapAggregator {
    fn new(interval_ms: i64) -> Self {
        Self {
            interval_ms,
            rows: std::collections::HashMap::new(),
        }
    }

    fn add(&mut self, timestamp: i64, price: f64, quantity: f64) {
        if quantity <= 0.0 || !price.is_finite() {
            return;
        }

        let bucket = (timestamp / self.interval_ms) * self.interval_ms;
        let row = self.rows.entry(bucket).or_insert_with(|| VwapRow {
            timestamp: bucket,
            volume: 0.0,
            quote_volume: 0.0,
            trades: 0,
        });

        row.volume += quantity;
        row.quote_volume += price * quantity;
        row.trades += 1;
    }

    fn finish(self) -> Vec<VwapBar> {
        let mut result: Vec<_> = self
            .rows
            .into_values()
            .map(|row| VwapBar {
                timestamp: row.timestamp,
                vwap: if row.volume > 0.0 {
                    Some(row.quote_volume / row.volume)
                } else {
                    None
                },
                volume: row.volume,
                quote_volume: row.quote_volume,
                trades: row.trades,
            })
            .collect();

        result.sort_by_key(|bar| bar.timestamp);
        result
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

    fn add_trade(&mut self, trade: &TradeEvent) {
        self.add(trade.timestamp(), trade.price());
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

fn parse_orderbook_levels(value: &Value) -> Option<Vec<OrderbookLevel>> {
    let rows = value.as_array()?;
    let mut levels = Vec::new();
    for row in rows {
        let (price, quantity) = parse_level_tuple(row);
        if let (Some(price), Some(quantity)) = (price, quantity) {
            levels.push(OrderbookLevel { price, quantity });
        }
    }
    Some(levels)
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
