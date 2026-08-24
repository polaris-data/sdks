use std::path::PathBuf;

mod columnar;

use polaris_data::{
    BboQuery, BboQuote, DepthMetricsRow, HistoricalQuery, ListSnapshotsQuery, OhlcvFormat,
    OhlcvInterval, OhlcvOutput, OhlcvQuery, OrderbookBuilder, PointSeriesEvent, PolarisError,
    PropammQuoteLadderEvent, RawQuery, RawReplayQuery, ReplayQuery, StandardEvent, StreamQuery,
    TimeInput, TradeEvent,
    blocking::{self, RawReplayCacheConfig},
};
use pyo3::{
    create_exception,
    exceptions::PyException,
    prelude::*,
    types::{PyAny, PyModule},
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::columnar::NativeColumnar;

create_exception!(_native, NativeError, PyException);

pub(crate) fn native_error(error: PolarisError) -> PyErr {
    let payload = match error {
        PolarisError::Unauthorized {
            message,
            status_code,
            body,
        } => json!({
            "kind": "unauthorized",
            "message": message,
            "status_code": status_code,
            "body": body,
        }),
        PolarisError::AccessDenied {
            message,
            status_code,
            body,
        } => json!({
            "kind": "access_denied",
            "message": message,
            "status_code": status_code,
            "body": body,
        }),
        PolarisError::NotFound {
            message,
            status_code,
            body,
        } => json!({
            "kind": "not_found",
            "message": message,
            "status_code": status_code,
            "body": body,
        }),
        PolarisError::RateLimited {
            message,
            reset_at,
            status_code,
            body,
        } => json!({
            "kind": "rate_limited",
            "message": message,
            "reset_at": reset_at,
            "status_code": status_code,
            "body": body,
        }),
        PolarisError::Decode(message) => {
            json!({"kind": "stream_decode", "message": message})
        }
        PolarisError::CoverageGap {
            dataset_source,
            market,
            intervals,
        } => json!({
            "kind": "coverage_gap",
            "message": format!(
                "snapshot coverage gap for {dataset_source}/{market}: {intervals:?}"
            ),
            "source": dataset_source,
            "market": market,
            "intervals": intervals,
        }),
        PolarisError::InvalidResponse(message) => {
            json!({"kind": "invalid_response", "message": message})
        }
        PolarisError::Io(error) => json!({"kind": "io", "message": error.to_string()}),
        PolarisError::Request(message) => json!({"kind": "request", "message": message}),
        PolarisError::StreamConnection(message) => {
            json!({"kind": "stream_connection", "message": message})
        }
        PolarisError::StreamProtocol { code, message } => {
            json!({"kind": "stream_protocol", "code": code, "message": message})
        }
        PolarisError::BlockingInAsyncRuntime => json!({
            "kind": "blocking_in_async_runtime",
            "message": "blocking Polaris client cannot run inside an active Tokio runtime; use the async client",
        }),
    };
    NativeError::new_err(payload.to_string())
}

fn time_input(value: Option<String>) -> Option<TimeInput> {
    value.map(TimeInput::Iso8601)
}

fn historical_query(
    source: String,
    market: String,
    from_: Option<String>,
    to: Option<String>,
    allow_gaps: bool,
) -> HistoricalQuery {
    HistoricalQuery {
        source,
        market,
        from: time_input(from_),
        to: time_input(to),
        allow_gaps,
        materialize_orderbooks: true,
    }
}

fn parse_interval(value: &str) -> PyResult<OhlcvInterval> {
    match value {
        "100ms" => Ok(OhlcvInterval::Ms100),
        "1s" => Ok(OhlcvInterval::S1),
        "10s" => Ok(OhlcvInterval::S10),
        "1m" => Ok(OhlcvInterval::M1),
        "5m" => Ok(OhlcvInterval::M5),
        "15m" => Ok(OhlcvInterval::M15),
        "1h" => Ok(OhlcvInterval::H1),
        _ => Err(pyo3::exceptions::PyValueError::new_err(
            "interval must be one of: 100ms, 1s, 10s, 1m, 5m, 15m, 1h",
        )),
    }
}

fn prune_empty_event_identity(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                prune_empty_event_identity(item);
            }
        }
        Value::Object(object) => {
            let is_event = (object.contains_key("timestamp")
                || object.contains_key("collector_timestamp"))
                && object.contains_key("type");
            if is_event {
                for key in ["source", "market"] {
                    if object.get(key).is_some_and(|value| value == "") {
                        object.remove(key);
                    }
                }
                if object.get("type").is_some_and(|value| value == "") {
                    object.remove("type");
                }
                if object.get("data").is_some_and(Value::is_null) {
                    object.remove("data");
                }
                if let Some(Value::Object(data)) = object.get_mut("data") {
                    if data.get("side").is_some_and(|value| value == "") {
                        data.remove("side");
                    }
                }
            }
            for value in object.values_mut() {
                prune_empty_event_identity(value);
            }
        }
        _ => {}
    }
}

fn to_python<'py, T: Serialize>(py: Python<'py>, value: &T) -> PyResult<Bound<'py, PyAny>> {
    let mut value = serde_json::to_value(value)
        .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
    prune_empty_event_identity(&mut value);
    Ok(pythonize::pythonize(py, &value)?)
}

fn standard_event_to_python<'py>(
    py: Python<'py>,
    event: &StandardEvent,
) -> PyResult<Bound<'py, PyAny>> {
    let mut value = serde_json::to_value(event)
        .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
    if let Value::Object(object) = &mut value {
        for key in ["source", "market"] {
            if object.get(key).is_some_and(|value| value == "") {
                object.remove(key);
            }
        }
        if object.get("type").is_some_and(|value| value == "") {
            object.remove("type");
        }
        if object.get("data").is_some_and(Value::is_null) {
            object.remove("data");
        }
        if let Some(Value::Object(data)) = object.get_mut("data") {
            if data.get("side").is_some_and(|value| value == "") {
                data.remove("side");
            }
        }
    }
    Ok(pythonize::pythonize(py, &value)?)
}

#[pyclass(module = "polaris_data._native")]
struct NativeClient {
    inner: blocking::PolarisClient,
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl NativeClient {
    #[new]
    #[pyo3(signature = (api_key=None, base_url="https://api.polaris.supply", timeout=30.0, dataset_root=None, stream_url=None))]
    fn new(
        api_key: Option<String>,
        base_url: &str,
        timeout: f64,
        dataset_root: Option<PathBuf>,
        stream_url: Option<String>,
    ) -> PyResult<Self> {
        if !timeout.is_finite() || timeout <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "timeout must be greater than 0",
            ));
        }
        let mut builder = blocking::PolarisClient::builder()
            .base_url(base_url)
            .timeout(std::time::Duration::from_secs_f64(timeout));
        if let Some(api_key) = api_key {
            builder = builder.api_key(api_key);
        }
        if let Some(dataset_root) = dataset_root {
            builder = builder.dataset_root(dataset_root);
        }
        if let Some(stream_url) = stream_url {
            builder = builder.stream_url(stream_url);
        }
        Ok(Self {
            inner: builder.build().map_err(native_error)?,
        })
    }

    #[getter]
    fn dataset_root(&self) -> String {
        self.inner.dataset_root().to_string_lossy().into_owned()
    }

    #[getter]
    fn replay_cache_dir(&self) -> String {
        self.inner
            .cache_dir()
            .join("replay")
            .to_string_lossy()
            .into_owned()
    }

    fn close(&self) {}

    #[pyo3(signature = (source, markets, include_buffer=false, materialize_orderbooks=true))]
    fn stream(
        &self,
        py: Python<'_>,
        source: String,
        markets: Vec<String>,
        include_buffer: bool,
        materialize_orderbooks: bool,
    ) -> PyResult<NativeRealtimeStream> {
        let iterator = py
            .detach(|| {
                self.inner.stream(StreamQuery {
                    source,
                    markets,
                    include_buffer,
                    materialize_orderbooks,
                })
            })
            .map_err(native_error)?;
        Ok(NativeRealtimeStream {
            iterator: Some(iterator),
        })
    }

    fn take_diagnostics(&self) -> Vec<String> {
        self.inner
            .take_diagnostics()
            .into_iter()
            .map(|diagnostic| diagnostic.message)
            .collect()
    }

    fn health<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let result = py.detach(|| self.inner.health()).map_err(native_error)?;
        to_python(py, &result)
    }

    #[pyo3(signature = (source=None, market=None, q=None))]
    fn catalog<'py>(
        &self,
        py: Python<'py>,
        source: Option<String>,
        market: Option<String>,
        q: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let result = py
            .detach(|| {
                self.inner
                    .catalog(polaris_data::CatalogQuery { source, market, q })
            })
            .map_err(native_error)?;
        to_python(py, &result)
    }

    fn count<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let result = py.detach(|| self.inner.count()).map_err(native_error)?;
        to_python(py, &result)
    }

    #[pyo3(signature = (source, market, from_, to, limit=1000))]
    fn list_snapshots<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        from_: String,
        to: String,
        limit: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let result = py
            .detach(|| {
                self.inner.list_snapshots(ListSnapshotsQuery {
                    source,
                    market,
                    from: Some(TimeInput::Iso8601(from_)),
                    to: Some(TimeInput::Iso8601(to)),
                    limit: Some(limit),
                })
            })
            .map_err(native_error)?;
        to_python(py, &result)
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false, materialize_orderbooks=true))]
    fn events<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
        materialize_orderbooks: bool,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                let mut query = historical_query(source, market, from_, to, allow_gaps);
                query.materialize_orderbooks = materialize_orderbooks;
                self.inner.events(query)
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::Events(
            iterator,
        )))
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false, materialize_orderbooks=true, batch_size=65_536))]
    fn events_columnar(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
        materialize_orderbooks: bool,
        batch_size: usize,
    ) -> PyResult<NativeColumnar> {
        validate_batch_size(batch_size)?;
        let identity_source = source.clone();
        let identity_market = market.clone();
        py.detach(|| {
            let mut query = historical_query(source, market, from_, to, allow_gaps);
            query.materialize_orderbooks = materialize_orderbooks;
            let plan = self.inner.prepare_historical(query)?;
            Ok(NativeColumnar::events(
                plan,
                identity_source,
                identity_market,
                batch_size,
            ))
        })
        .map_err(native_error)
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false))]
    fn trades<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                self.inner
                    .trades(historical_query(source, market, from_, to, allow_gaps))
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::Trades(
            iterator,
        )))
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false, batch_size=65_536))]
    fn trades_columnar(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
        batch_size: usize,
    ) -> PyResult<NativeColumnar> {
        validate_batch_size(batch_size)?;
        let identity_source = source.clone();
        let identity_market = market.clone();
        py.detach(|| {
            let mut query = historical_query(source, market, from_, to, allow_gaps);
            query.materialize_orderbooks = false;
            let plan = self.inner.prepare_historical(query)?;
            NativeColumnar::trades(plan, identity_source, identity_market, batch_size)
        })
        .map_err(native_error)
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false, materialize_orderbooks=true))]
    fn replay(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
        materialize_orderbooks: bool,
    ) -> PyResult<NativeReplay> {
        let iterator = py
            .detach(|| {
                self.inner.replay(ReplayQuery {
                    source,
                    market,
                    from: time_input(from_),
                    to: time_input(to),
                    allow_gaps,
                    materialize_orderbooks,
                })
            })
            .map_err(native_error)?;
        Ok(NativeReplay {
            iterator: Some(NativeReplayIterator::Single(iterator)),
        })
    }

    #[pyo3(signature = (source, market, from_, to, allow_gaps=false, materialize_orderbooks=true))]
    fn replay_chunked(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: String,
        to: String,
        allow_gaps: bool,
        materialize_orderbooks: bool,
    ) -> PyResult<NativeReplay> {
        let iterator = py
            .detach(|| {
                self.inner.replay_chunked(ReplayQuery {
                    source,
                    market,
                    from: Some(TimeInput::Iso8601(from_)),
                    to: Some(TimeInput::Iso8601(to)),
                    allow_gaps,
                    materialize_orderbooks,
                })
            })
            .map_err(native_error)?;
        Ok(NativeReplay {
            iterator: Some(NativeReplayIterator::Chunked(Box::new(iterator))),
        })
    }

    #[pyo3(signature = (source, market, from_=None, to=None, limit=1000))]
    fn raw<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        limit: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let result = py
            .detach(|| {
                self.inner.raw(RawQuery {
                    source,
                    market,
                    from: time_input(from_),
                    to: time_input(to),
                    limit,
                })
            })
            .map_err(native_error)?;
        to_python(py, &result)
    }

    #[pyo3(signature = (source, market, from_=None, to=None, limit=1000))]
    fn raw_replay(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        limit: usize,
    ) -> PyResult<NativeRawReplay> {
        let iterator = py
            .detach(|| {
                self.inner.raw_replay(RawReplayQuery {
                    source,
                    market,
                    from: time_input(from_),
                    to: time_input(to),
                    limit,
                })
            })
            .map_err(native_error)?;
        Ok(NativeRawReplay {
            iterator: Some(NativeRawReplayIterator::Single(iterator)),
        })
    }

    #[pyo3(signature = (source, market, from_, to, limit=1000, cache_enabled=true, cache_dir=None))]
    fn raw_replay_cached(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: String,
        to: String,
        limit: usize,
        cache_enabled: bool,
        cache_dir: Option<PathBuf>,
    ) -> PyResult<NativeRawReplay> {
        let cache = self.raw_replay_cache_config(cache_enabled, cache_dir);
        let iterator = py
            .detach(|| {
                self.inner.raw_replay_cached(
                    RawReplayQuery {
                        source,
                        market,
                        from: Some(TimeInput::Iso8601(from_)),
                        to: Some(TimeInput::Iso8601(to)),
                        limit,
                    },
                    cache,
                )
            })
            .map_err(native_error)?;
        Ok(NativeRawReplay {
            iterator: Some(NativeRawReplayIterator::Cached(iterator)),
        })
    }

    #[pyo3(signature = (source, market, from_, to, limit=1000, cache_enabled=true, cache_dir=None))]
    fn raw_replay_chunked(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: String,
        to: String,
        limit: usize,
        cache_enabled: bool,
        cache_dir: Option<PathBuf>,
    ) -> PyResult<NativeRawReplay> {
        let cache = self.raw_replay_cache_config(cache_enabled, cache_dir);
        let iterator = py
            .detach(|| {
                self.inner.raw_replay_chunked(
                    RawReplayQuery {
                        source,
                        market,
                        from: Some(TimeInput::Iso8601(from_)),
                        to: Some(TimeInput::Iso8601(to)),
                        limit,
                    },
                    cache,
                )
            })
            .map_err(native_error)?;
        Ok(NativeRawReplay {
            iterator: Some(NativeRawReplayIterator::Chunked(Box::new(iterator))),
        })
    }

    #[pyo3(signature = (source, market, interval, from_=None, to=None, format=None, allow_gaps=false))]
    fn ohlcv<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        interval: &str,
        from_: Option<String>,
        to: Option<String>,
        format: Option<&str>,
        allow_gaps: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let query = OhlcvQuery {
            source,
            market,
            from: time_input(from_),
            to: time_input(to),
            interval: parse_interval(interval)?,
            format: match format {
                None => OhlcvFormat::Bars,
                Some("tradingview") => OhlcvFormat::TradingView,
                Some(_) => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "format must be one of: None, 'tradingview'",
                    ));
                }
            },
            allow_gaps,
        };
        let output = py
            .detach(|| self.inner.ohlcv(query))
            .map_err(native_error)?;
        match output {
            OhlcvOutput::Bars(bars) => to_python(py, &bars),
            OhlcvOutput::TradingView(value) => to_python(py, &value),
        }
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false, materialize_orderbooks=true))]
    fn l2_snapshots<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
        materialize_orderbooks: bool,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                let mut query = historical_query(source, market, from_, to, allow_gaps);
                query.materialize_orderbooks = materialize_orderbooks;
                self.inner.events(query)
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::L2Events(
            iterator,
        )))
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false))]
    fn l2_updates<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                let mut query = historical_query(source, market, from_, to, allow_gaps);
                query.materialize_orderbooks = false;
                self.inner.events(query)
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::L2Events(
            iterator,
        )))
    }

    #[pyo3(signature = (source, market, from_=None, to=None, interval=None, allow_gaps=false, changes_only=false))]
    fn bbo<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        interval: Option<&str>,
        allow_gaps: bool,
        changes_only: bool,
    ) -> PyResult<NativeHistorical> {
        let interval = interval.map(parse_interval).transpose()?;
        let iterator = py
            .detach(|| {
                let query = BboQuery {
                    source,
                    market,
                    from: time_input(from_),
                    to: time_input(to),
                    allow_gaps,
                    interval,
                };
                if changes_only {
                    self.inner.bbo_changes(query)
                } else {
                    self.inner.bbo(query)
                }
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::Bbo(
            iterator,
        )))
    }

    #[pyo3(signature = (source, market, from_=None, to=None, interval=None, allow_gaps=false, changes_only=false, batch_size=65_536))]
    fn bbo_columnar(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        interval: Option<&str>,
        allow_gaps: bool,
        changes_only: bool,
        batch_size: usize,
    ) -> PyResult<NativeColumnar> {
        validate_batch_size(batch_size)?;
        let parsed_interval = interval.map(parse_interval).transpose()?;
        let identity_source = source.clone();
        let identity_market = market.clone();
        let iterator = py
            .detach(|| {
                let query = BboQuery {
                    source,
                    market,
                    from: time_input(from_),
                    to: time_input(to),
                    allow_gaps,
                    interval: parsed_interval,
                };
                if changes_only {
                    self.inner.bbo_changes(query)
                } else {
                    self.inner.bbo(query)
                }
            })
            .map_err(native_error)?;
        Ok(NativeColumnar::bbo(
            iterator,
            identity_source,
            identity_market,
            batch_size,
        ))
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false))]
    fn funding_rates<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                self.inner
                    .funding_rates(historical_query(source, market, from_, to, allow_gaps))
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::Points(
            iterator,
        )))
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false, batch_size=65_536))]
    fn funding_rates_columnar(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
        batch_size: usize,
    ) -> PyResult<NativeColumnar> {
        self.point_series_columnar(
            py,
            source,
            market,
            from_,
            to,
            allow_gaps,
            batch_size,
            "funding_rate",
            "funding_rate",
        )
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false))]
    fn mark_prices<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                self.inner
                    .mark_prices(historical_query(source, market, from_, to, allow_gaps))
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::Points(
            iterator,
        )))
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false, batch_size=65_536))]
    fn mark_prices_columnar(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
        batch_size: usize,
    ) -> PyResult<NativeColumnar> {
        self.point_series_columnar(
            py,
            source,
            market,
            from_,
            to,
            allow_gaps,
            batch_size,
            "mark_price",
            "mark_price",
        )
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false))]
    fn propamm_quote_ladders<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                self.inner
                    .propamm_quote_ladders(historical_query(source, market, from_, to, allow_gaps))
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(
            NativeHistoricalIterator::PropammQuoteLadders(iterator),
        ))
    }

    #[pyo3(signature = (source, market, from_=None, to=None, allow_gaps=false, batch_size=65_536))]
    fn propamm_quote_ladders_columnar(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
        batch_size: usize,
    ) -> PyResult<NativeColumnar> {
        validate_batch_size(batch_size)?;
        let identity_source = source.clone();
        let identity_market = market.clone();
        py.detach(|| {
            let mut query = historical_query(source, market, from_, to, allow_gaps);
            query.materialize_orderbooks = false;
            let plan = self.inner.prepare_historical(query)?;
            Ok(NativeColumnar::propamm_quote_ladders(
                plan,
                identity_source,
                identity_market,
                batch_size,
            ))
        })
        .map_err(native_error)
    }

    #[pyo3(signature = (source, market, interval, from_=None, to=None, allow_gaps=false))]
    fn volume<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        interval: &str,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let query = aggregate_query(source, market, interval, from_, to, allow_gaps)?;
        let result = py
            .detach(|| self.inner.volume(query))
            .map_err(native_error)?;
        to_python(py, &result)
    }

    #[pyo3(signature = (source, market, interval, from_=None, to=None, allow_gaps=false))]
    fn vwap<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        interval: &str,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let query = aggregate_query(source, market, interval, from_, to, allow_gaps)?;
        let result = py.detach(|| self.inner.vwap(query)).map_err(native_error)?;
        to_python(py, &result)
    }

    #[pyo3(signature = (source, market, interval, from_=None, to=None, allow_gaps=false))]
    fn volatility<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        interval: &str,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let query = aggregate_query(source, market, interval, from_, to, allow_gaps)?;
        let result = py
            .detach(|| self.inner.volatility(query))
            .map_err(native_error)?;
        to_python(py, &result)
    }

    #[pyo3(signature = (source, market, from_=None, to=None, depth_pct=0.01, slippage_notional=10_000.0, allow_gaps=false))]
    fn depth_metrics<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        depth_pct: f64,
        slippage_notional: f64,
        allow_gaps: bool,
    ) -> PyResult<NativeHistorical> {
        let query = historical_query(source, market, from_, to, allow_gaps);
        let iterator = py
            .detach(|| {
                self.inner
                    .depth_metrics(query, Some(depth_pct), Some(slippage_notional))
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::Depth(
            iterator,
        )))
    }

    #[pyo3(signature = (source, market, from_=None, to=None, depth_pct=0.01, slippage_notional=10_000.0, allow_gaps=false, batch_size=65_536))]
    fn depth_metrics_columnar(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        depth_pct: f64,
        slippage_notional: f64,
        allow_gaps: bool,
        batch_size: usize,
    ) -> PyResult<NativeColumnar> {
        validate_batch_size(batch_size)?;
        let identity_source = source.clone();
        let identity_market = market.clone();
        let iterator = py
            .detach(|| {
                self.inner.depth_metrics(
                    historical_query(source, market, from_, to, allow_gaps),
                    Some(depth_pct),
                    Some(slippage_notional),
                )
            })
            .map_err(native_error)?;
        Ok(NativeColumnar::depth(
            iterator,
            identity_source,
            identity_market,
            batch_size,
        ))
    }
}

impl NativeClient {
    fn raw_replay_cache_config(
        &self,
        enabled: bool,
        directory: Option<PathBuf>,
    ) -> RawReplayCacheConfig {
        RawReplayCacheConfig {
            enabled,
            directory: directory.unwrap_or_else(|| self.inner.cache_dir().join("replay")),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn point_series_columnar(
        &self,
        py: Python<'_>,
        source: String,
        market: String,
        from_: Option<String>,
        to: Option<String>,
        allow_gaps: bool,
        batch_size: usize,
        series_name: &'static str,
        value_name: &'static str,
    ) -> PyResult<NativeColumnar> {
        validate_batch_size(batch_size)?;
        let identity_source = source.clone();
        let identity_market = market.clone();
        py.detach(|| {
            let mut query = historical_query(source, market, from_, to, allow_gaps);
            query.materialize_orderbooks = false;
            let plan = self.inner.prepare_historical(query)?;
            NativeColumnar::points(
                plan,
                identity_source,
                identity_market,
                series_name,
                value_name,
                batch_size,
            )
        })
        .map_err(native_error)
    }
}

fn validate_batch_size(batch_size: usize) -> PyResult<()> {
    if batch_size == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "batch_size must be greater than 0",
        ));
    }
    Ok(())
}

fn aggregate_query(
    source: String,
    market: String,
    interval_value: &str,
    from_: Option<String>,
    to: Option<String>,
    allow_gaps: bool,
) -> PyResult<OhlcvQuery> {
    Ok(OhlcvQuery {
        source,
        market,
        from: time_input(from_),
        to: time_input(to),
        interval: parse_interval(interval_value)?,
        format: OhlcvFormat::Bars,
        allow_gaps,
    })
}

#[pyclass(unsendable, module = "polaris_data._native")]
struct NativeHistorical {
    iterator: Option<NativeHistoricalIterator>,
}

enum NativeHistoricalIterator {
    Events(blocking::HistoricalIterator<StandardEvent>),
    L2Events(blocking::HistoricalIterator<StandardEvent>),
    Trades(blocking::HistoricalIterator<TradeEvent>),
    Bbo(blocking::HistoricalIterator<BboQuote>),
    Points(blocking::HistoricalIterator<PointSeriesEvent>),
    PropammQuoteLadders(blocking::HistoricalIterator<PropammQuoteLadderEvent>),
    Depth(blocking::HistoricalIterator<DepthMetricsRow>),
}

impl NativeHistorical {
    fn new(iterator: NativeHistoricalIterator) -> Self {
        Self {
            iterator: Some(iterator),
        }
    }
}

fn next_historical<'py, T: Serialize + Send>(
    py: Python<'py>,
    iterator: &mut blocking::HistoricalIterator<T>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    match py.detach(|| iterator.next()) {
        Some(Ok(value)) => to_python(py, &value).map(Some),
        Some(Err(error)) => Err(native_error(error)),
        None => Ok(None),
    }
}

fn next_standard_event<'py>(
    py: Python<'py>,
    iterator: &mut blocking::HistoricalIterator<StandardEvent>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    match py.detach(|| iterator.next()) {
        Some(Ok(value)) => standard_event_to_python(py, &value).map(Some),
        Some(Err(error)) => Err(native_error(error)),
        None => Ok(None),
    }
}

fn next_l2_event<'py>(
    py: Python<'py>,
    iterator: &mut blocking::HistoricalIterator<StandardEvent>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    loop {
        match py.detach(|| iterator.next()) {
            Some(Ok(event))
                if matches!(
                    event.event_type(),
                    "orderbook" | "orderbook_delta" | "l2_snapshot" | "orderbook_snapshot"
                ) =>
            {
                return standard_event_to_python(py, &event).map(Some);
            }
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(native_error(error)),
            None => return Ok(None),
        }
    }
}

#[pymethods]
impl NativeHistorical {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let Some(iterator) = self.iterator.as_mut() else {
            return Ok(None);
        };
        let result = match iterator {
            NativeHistoricalIterator::Events(iterator) => next_standard_event(py, iterator),
            NativeHistoricalIterator::L2Events(iterator) => next_l2_event(py, iterator),
            NativeHistoricalIterator::Trades(iterator) => next_historical(py, iterator),
            NativeHistoricalIterator::Bbo(iterator) => next_historical(py, iterator),
            NativeHistoricalIterator::Points(iterator) => next_historical(py, iterator),
            NativeHistoricalIterator::PropammQuoteLadders(iterator) => {
                next_historical(py, iterator)
            }
            NativeHistoricalIterator::Depth(iterator) => next_historical(py, iterator),
        };
        if matches!(result, Ok(None)) {
            self.iterator = None;
        }
        result
    }

    fn close(&mut self) {
        self.iterator = None;
    }
}

#[pyclass(unsendable, module = "polaris_data._native")]
struct NativeReplay {
    iterator: Option<NativeReplayIterator>,
}

#[pyclass(name = "NativeOrderbookBuilder", module = "polaris_data._native")]
struct NativeOrderbookBuilder {
    inner: OrderbookBuilder,
}

#[pymethods]
impl NativeOrderbookBuilder {
    #[new]
    fn new() -> Self {
        Self {
            inner: OrderbookBuilder::new(),
        }
    }

    fn apply<'py>(
        &mut self,
        py: Python<'py>,
        event: &Bound<'py, PyAny>,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        let event: StandardEvent = pythonize::depythonize(event).map_err(|error| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid standardized event: {error}"))
        })?;
        self.inner
            .apply(event)
            .map_err(native_error)?
            .map(|event| to_python(py, &event))
            .transpose()
    }

    fn update(&mut self, event: &Bound<'_, PyAny>) -> PyResult<bool> {
        let event: StandardEvent = pythonize::depythonize(event).map_err(|error| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid standardized event: {error}"))
        })?;
        self.inner.update(&event).map_err(native_error)
    }

    fn snapshot<'py>(
        &self,
        py: Python<'py>,
        source: &str,
        market: &str,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        self.inner
            .snapshot(source, market)
            .map(|book| to_python(py, &book))
            .transpose()
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn clear_book(&mut self, source: &str, market: &str) {
        self.inner.clear_book(source, market);
    }
}

#[pyclass(unsendable, module = "polaris_data._native")]
struct NativeRealtimeStream {
    iterator: Option<blocking::RealtimeIterator>,
}

#[pymethods]
impl NativeRealtimeStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        loop {
            let Some(iterator) = self.iterator.as_mut() else {
                return Ok(None);
            };
            match py.detach(|| iterator.next_timeout(std::time::Duration::from_millis(100))) {
                blocking::RealtimePoll::Event(Ok(value)) => return to_python(py, &value).map(Some),
                blocking::RealtimePoll::Event(Err(error)) => return Err(native_error(error)),
                blocking::RealtimePoll::Pending => py.check_signals()?,
                blocking::RealtimePoll::Closed => {
                    self.iterator = None;
                    return Ok(None);
                }
            }
        }
    }

    fn close(&mut self) {
        self.iterator = None;
    }
}

enum NativeReplayIterator {
    Single(blocking::ReplayIterator),
    Chunked(Box<blocking::ChunkedReplayIterator>),
}

#[pymethods]
impl NativeReplay {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let Some(iterator) = self.iterator.as_mut() else {
            return Ok(None);
        };
        let next = py.detach(|| match iterator {
            NativeReplayIterator::Single(iterator) => iterator.next(),
            NativeReplayIterator::Chunked(iterator) => iterator.next(),
        });
        match next {
            Some(Ok(value)) => standard_event_to_python(py, &value).map(Some),
            Some(Err(error)) => Err(native_error(error)),
            None => {
                self.iterator = None;
                Ok(None)
            }
        }
    }
}

#[pyclass(unsendable, module = "polaris_data._native")]
struct NativeRawReplay {
    iterator: Option<NativeRawReplayIterator>,
}

enum NativeRawReplayIterator {
    Single(blocking::RawReplayIterator),
    Cached(blocking::CachedRawReplayIterator),
    Chunked(Box<blocking::ChunkedRawReplayIterator>),
}

#[pymethods]
impl NativeRawReplay {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let Some(iterator) = self.iterator.as_mut() else {
            return Ok(None);
        };
        let next = py.detach(|| match iterator {
            NativeRawReplayIterator::Single(iterator) => iterator.next(),
            NativeRawReplayIterator::Cached(iterator) => iterator.next(),
            NativeRawReplayIterator::Chunked(iterator) => iterator.next(),
        });
        match next {
            Some(Ok(value)) => to_python(py, &value).map(Some),
            Some(Err(error)) => Err(native_error(error)),
            None => {
                self.iterator = None;
                Ok(None)
            }
        }
    }
}

#[pyfunction]
fn decode_file<'py>(py: Python<'py>, path: PathBuf) -> PyResult<Bound<'py, PyAny>> {
    let rows = py
        .detach(|| polaris_data::decode_ndjson_file(&path))
        .map_err(native_error)?;
    to_python(py, &rows)
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeClient>()?;
    module.add_class::<NativeColumnar>()?;
    module.add_class::<NativeOrderbookBuilder>()?;
    module.add_class::<NativeHistorical>()?;
    module.add_class::<NativeReplay>()?;
    module.add_class::<NativeRealtimeStream>()?;
    module.add_class::<NativeRawReplay>()?;
    module.add_function(wrap_pyfunction!(decode_file, module)?)?;
    module.add("NativeError", module.py().get_type::<NativeError>())?;
    module.add("__native__", true)?;
    Ok(())
}
