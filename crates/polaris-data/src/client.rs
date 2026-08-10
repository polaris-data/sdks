use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, BufReader, Cursor, Read},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use chrono::{Duration, NaiveDate, TimeZone, Utc};
use futures_util::StreamExt;
use serde_json::Value;

use crate::{
    builder::PolarisClientBuilder,
    errors::PolarisError,
    http::{AuthMode, HttpClient},
    models::{
        BboQuote, CatalogAccess, CatalogInstrument, CatalogMarket, CatalogQuery, CatalogResponse,
        DepthMetricsRow, Diagnostic, DownloadManifestQuery, DownloadManifestResponse,
        HistoricalQuery, ListSnapshotsQuery, OhlcvOutput, OhlcvQuery, OrderbookData,
        OrderbookEvent, OrderbookLevel, PointSeriesData, PointSeriesEvent, RawQuery,
        RawReplayQuery, RawReplayStream, RealtimeStream, ReplayQuery, ReplayStream, SnapshotEntry,
        StandardEvent, StreamQuery, TradeData, TradeEvent, VolatilityBar, VolumeBar, VwapBar,
    },
    ohlcv, realtime, replay,
    storage::{
        LocalSnapshotFile, StorageLayout, acquire_sync_lock, data_file_path,
        list_local_snapshot_entries, parse_snapshot_key, temp_file_path,
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

        let payload = self
            .http
            .get_json("/catalog", &params, AuthMode::IfAvailable)
            .await?;
        normalize_catalog_response(payload)
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

    pub async fn events(&self, query: HistoricalQuery) -> Result<Vec<StandardEvent>, PolarisError> {
        let replay = self
            .replay(ReplayQuery {
                source: query.source,
                market: query.market,
                from: query.from,
                to: query.to,
                allow_gaps: query.allow_gaps,
                materialize_orderbooks: query.materialize_orderbooks,
            })
            .await?;
        replay.collect::<Vec<_>>().await.into_iter().collect()
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

    pub async fn trades(&self, query: HistoricalQuery) -> Result<Vec<TradeEvent>, PolarisError> {
        let events = self.events(query).await?;
        events
            .into_iter()
            .filter(|event| event.event_type == "trade")
            .map(|event| {
                let data: TradeData = serde_json::from_value(event.data.clone())
                    .map_err(|err| PolarisError::Decode(format!("invalid trade payload: {err}")))?;
                Ok(TradeEvent {
                    timestamp: event.timestamp,
                    source: event.source,
                    market: event.market,
                    event_type: event.event_type,
                    data,
                })
            })
            .collect()
    }

    pub async fn replay(&self, query: ReplayQuery) -> Result<ReplayStream, PolarisError> {
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
        Ok(replay::replay_stream(
            paths,
            from_us,
            to_us,
            query.source,
            query.market,
            query.materialize_orderbooks,
            gaps,
        ))
    }

    pub async fn ohlcv(&self, query: OhlcvQuery) -> Result<OhlcvOutput, PolarisError> {
        let trades = self
            .trades(HistoricalQuery {
                source: query.source,
                market: query.market,
                from: query.from,
                to: query.to,
                allow_gaps: query.allow_gaps,
                materialize_orderbooks: true,
            })
            .await?;
        Ok(ohlcv::aggregate(&trades, query.interval, query.format))
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
            remote_by_day
                .entry(day)
                .or_default()
                .push(LocalSnapshotFile {
                    entry,
                    path,
                    download_url: None,
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
            }
            selected_paths.push(candidate.path);
        }
        selected_paths.sort();
        selected_paths.dedup();
        Ok((selected_paths, gaps))
    }

    async fn download_snapshot(&self, snapshot: &LocalSnapshotFile) -> Result<(), PolarisError> {
        let final_path = snapshot.path.as_path();
        if final_path.exists() {
            return Ok(());
        }

        let locks_dir = self.layout.locks_dir.clone();
        let _lock = tokio::task::spawn_blocking(move || acquire_sync_lock(&locks_dir))
            .await
            .map_err(|err| PolarisError::Request(format!("lock task failed: {err}")))??;
        if final_path.exists() {
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
    ) -> Result<Vec<OrderbookEvent>, PolarisError> {
        let events = self.events(query).await?;
        Ok(events
            .into_iter()
            .filter(|event| {
                matches!(
                    event.event_type.as_str(),
                    "orderbook" | "orderbook_delta" | "orderbook_snapshot" | "l2_snapshot"
                )
            })
            .filter_map(|event| self.try_parse_orderbook(event))
            .collect())
    }

    /// Derive best bid / offer quotes from standardized orderbook snapshots.
    pub async fn bbo(&self, query: HistoricalQuery) -> Result<Vec<BboQuote>, PolarisError> {
        let orderbooks = self
            .l2_snapshots(HistoricalQuery {
                materialize_orderbooks: true,
                ..query
            })
            .await?;
        Ok(orderbooks
            .into_iter()
            .filter_map(|orderbook| self.derive_bbo(&orderbook))
            .collect())
    }

    /// Return standardized funding-rate point-series events for a time range.
    pub async fn funding_rates(
        &self,
        query: HistoricalQuery,
    ) -> Result<Vec<PointSeriesEvent>, PolarisError> {
        let events = self.events(query).await?;
        Ok(events
            .into_iter()
            .filter(|event| event.event_type == "point")
            .filter_map(|event| self.try_parse_point_series(event, "funding_rate"))
            .collect())
    }

    /// Return standardized mark-price point-series events for a time range.
    pub async fn mark_prices(
        &self,
        query: HistoricalQuery,
    ) -> Result<Vec<PointSeriesEvent>, PolarisError> {
        let events = self.events(query).await?;
        Ok(events
            .into_iter()
            .filter(|event| event.event_type == "point")
            .filter_map(|event| self.try_parse_point_series(event, "mark_price"))
            .collect())
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
        let trades = self
            .trades(HistoricalQuery {
                source: query.source,
                market: query.market,
                from: query.from,
                to: query.to,
                allow_gaps: query.allow_gaps,
                materialize_orderbooks: true,
            })
            .await?;

        let interval_ms = interval_to_ms(query.interval.as_str())?;
        let mut aggregator = VwapAggregator::new(interval_ms);

        for trade in trades {
            aggregator.add(trade.timestamp, trade.data.price, trade.data.quantity);
        }

        Ok(aggregator.finish())
    }

    /// Aggregate realized volatility from standardized trade data.
    pub async fn volatility(&self, query: OhlcvQuery) -> Result<Vec<VolatilityBar>, PolarisError> {
        let trades = self
            .trades(HistoricalQuery {
                source: query.source,
                market: query.market,
                from: query.from,
                to: query.to,
                allow_gaps: query.allow_gaps,
                materialize_orderbooks: true,
            })
            .await?;

        let interval_ms = interval_to_ms(query.interval.as_str())?;
        let mut aggregator = VolatilityAggregator::new(interval_ms);

        for trade in trades {
            aggregator.add(trade.timestamp, trade.data.price);
        }

        Ok(aggregator.finish())
    }

    /// Derive spread, depth, imbalance, and slippage metrics from orderbooks.
    pub async fn depth_metrics(
        &self,
        query: HistoricalQuery,
        depth_pct: Option<f64>,
        slippage_notional: Option<f64>,
    ) -> Result<Vec<DepthMetricsRow>, PolarisError> {
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

        let orderbooks = self
            .l2_snapshots(HistoricalQuery {
                materialize_orderbooks: true,
                ..query
            })
            .await?;
        let mut result = Vec::new();

        for orderbook in orderbooks {
            if let Some(metrics) =
                self.derive_depth_metrics(&orderbook, depth_pct, slippage_notional)
            {
                result.push(metrics);
            }
        }

        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Helper methods for data derivation
    // -----------------------------------------------------------------------

    fn try_parse_orderbook(&self, event: StandardEvent) -> Option<OrderbookEvent> {
        let mut payload = event.data;
        if !payload.is_object() {
            payload = Value::Object(Default::default());
        }
        let object = payload.as_object_mut()?;
        for key in ["bids", "asks"] {
            if !object.contains_key(key) {
                if let Some(value) = event.extra.get(key) {
                    object.insert(key.to_owned(), value.clone());
                }
            }
        }
        let is_delta = event.event_type == "orderbook_delta";
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
        let extra = object
            .iter()
            .filter(|(key, _)| key.as_str() != "bids" && key.as_str() != "asks")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let data = OrderbookData { bids, asks, extra };
        Some(OrderbookEvent {
            timestamp: event.timestamp,
            source: event.source,
            market: event.market,
            event_type: event.event_type,
            data,
        })
    }

    fn derive_bbo(&self, orderbook: &OrderbookEvent) -> Option<BboQuote> {
        let bid = self.best_orderbook_level(&orderbook.data.bids, "bid")?;
        let ask = self.best_orderbook_level(&orderbook.data.asks, "ask")?;

        Some(BboQuote {
            timestamp: orderbook.timestamp,
            bid_price: bid.0,
            bid_quantity: bid.1,
            ask_price: ask.0,
            ask_quantity: ask.1,
        })
    }

    fn best_orderbook_level(&self, levels: &[OrderbookLevel], side: &str) -> Option<(f64, f64)> {
        let mut best_price: Option<f64> = None;
        let mut best_quantity: Option<f64> = None;

        for level in levels {
            if level.price <= 0.0 || level.quantity <= 0.0 {
                continue;
            }

            match (best_price, side) {
                (None, _) => {
                    best_price = Some(level.price);
                    best_quantity = Some(level.quantity);
                }
                (Some(bp), "bid") if level.price > bp => {
                    best_price = Some(level.price);
                    best_quantity = Some(level.quantity);
                }
                (Some(bp), "ask") if level.price < bp => {
                    best_price = Some(level.price);
                    best_quantity = Some(level.quantity);
                }
                _ => {}
            }
        }

        match (best_price, best_quantity) {
            (Some(p), Some(q)) => Some((p, q)),
            _ => None,
        }
    }

    fn try_parse_point_series(
        &self,
        event: StandardEvent,
        expected_series: &str,
    ) -> Option<PointSeriesEvent> {
        let data: PointSeriesData = serde_json::from_value(event.data).ok()?;
        if data.series_name != expected_series {
            return None;
        }
        Some(PointSeriesEvent {
            timestamp: event.timestamp,
            source: event.source,
            market: event.market,
            event_type: event.event_type,
            data,
        })
    }

    fn derive_depth_metrics(
        &self,
        orderbook: &OrderbookEvent,
        depth_pct: f64,
        slippage_notional: f64,
    ) -> Option<DepthMetricsRow> {
        let bids = self.sorted_orderbook_levels(&orderbook.data.bids, "bid");
        let asks = self.sorted_orderbook_levels(&orderbook.data.asks, "ask");

        if bids.is_empty() || asks.is_empty() {
            return None;
        }

        let (bid_price, _bid_quantity) = bids[0];
        let (ask_price, _ask_quantity) = asks[0];

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

        let bid_depth_notional = self.depth_notional_within_pct(&bids, "bid", mid_price, depth_pct);
        let ask_depth_notional = self.depth_notional_within_pct(&asks, "ask", mid_price, depth_pct);
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

        let (buy_avg_price, buy_slippage, buy_slippage_bps) =
            self.calculate_slippage(&asks, target_base_quantity?, slippage_notional, mid_price);
        let (sell_avg_price, sell_slippage, sell_slippage_bps) =
            self.calculate_slippage(&bids, target_base_quantity?, slippage_notional, mid_price);

        Some(DepthMetricsRow {
            timestamp: orderbook.timestamp,
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

    fn sorted_orderbook_levels(&self, levels: &[OrderbookLevel], side: &str) -> Vec<(f64, f64)> {
        let mut parsed: Vec<(f64, f64)> = levels
            .iter()
            .filter(|level| level.price > 0.0 && level.quantity > 0.0)
            .map(|level| (level.price, level.quantity))
            .collect();

        match side {
            "bid" => {
                parsed.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal))
            }
            "ask" => {
                parsed.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
            }
            _ => {}
        }

        parsed
    }

    fn depth_notional_within_pct(
        &self,
        levels: &[(f64, f64)],
        side: &str,
        mid_price: f64,
        depth_pct: f64,
    ) -> f64 {
        let cutoff = match side {
            "bid" => mid_price * (1.0 - depth_pct),
            "ask" => mid_price * (1.0 + depth_pct),
            _ => return 0.0,
        };

        levels
            .iter()
            .filter(|(price, _)| match side {
                "bid" => *price >= cutoff,
                "ask" => *price <= cutoff,
                _ => true,
            })
            .map(|(price, quantity)| price * quantity)
            .sum()
    }

    fn calculate_slippage(
        &self,
        levels: &[(f64, f64)],
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
            entry.entry.start.is_none()
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
            snapshot_start_us(&entry.entry, day)
                .transpose()
                .map(|result| result.map(|start| (start, entry.clone())))
        })
        .collect::<Result<Vec<_>, _>>()?;
    ordered.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.entry.key.cmp(&right.1.entry.key))
    });

    let mut selected = Vec::new();
    let mut covered = Vec::new();
    for (index, (entry_start, entry)) in ordered.iter().enumerate() {
        let entry_end = snapshot_end_us(&entry.entry)?
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
            .unwrap_or(day_end);
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

    fn finish(self) -> Vec<VolatilityBar> {
        let mut points = self.points.clone();
        points.sort_by_key(|(ts, _)| *ts);

        let mut buckets: std::collections::HashMap<i64, VolatilityBucket> =
            std::collections::HashMap::new();

        for (timestamp, price) in points {
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

fn interval_to_ms(interval: &str) -> Result<i64, PolarisError> {
    let re = regex::Regex::new(r"^(\d+)(ms|s|m|h)$").unwrap();
    let captures = re.captures(interval).ok_or_else(|| {
        PolarisError::InvalidResponse(format!("Invalid interval format: {interval}"))
    })?;

    let amount: u64 = captures[1].parse().map_err(|_| {
        PolarisError::InvalidResponse(format!("Invalid interval amount: {interval}"))
    })?;

    let unit = &captures[2];
    let milliseconds = match unit {
        "ms" => amount,
        "s" => amount * 1_000,
        "m" => amount * 60_000,
        "h" => amount * 3_600_000,
        _ => {
            return Err(PolarisError::InvalidResponse(format!(
                "Invalid interval unit: {unit}"
            )));
        }
    };

    Ok(milliseconds as i64)
}

fn parse_orderbook_levels(value: &Value) -> Option<Vec<OrderbookLevel>> {
    let rows = value.as_array()?;
    let mut levels = Vec::new();
    for row in rows {
        let (price, quantity) = if let Some(values) = row.as_array() {
            (
                values.first().and_then(parse_number),
                values.get(1).and_then(parse_number),
            )
        } else if let Some(object) = row.as_object() {
            (
                object.get("price").and_then(parse_number),
                object
                    .get("quantity")
                    .or_else(|| object.get("size"))
                    .and_then(parse_number),
            )
        } else {
            (None, None)
        };
        if let (Some(price), Some(quantity)) = (price, quantity) {
            levels.push(OrderbookLevel { price, quantity });
        }
    }
    Some(levels)
}

fn parse_number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
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
