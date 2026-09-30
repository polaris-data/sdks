use std::path::PathBuf;

use polaris_data::{
    EventsQuery, FundingRateRow, HistoricalRowsQuery, IntentRow, IntentRowsQuery,
    L2OrderbooksQuery, L2UpdatesQuery, MixedEventRow, MixedEventType, OhlcvRow, OhlcvRowsQuery,
    OptionTickerRow, OptionTickerRowsQuery, OrderbookBuilder, OrderbookL2Row, PolarisError,
    RawCaptureRow, RawChannelQuery, RawQuery, StandardEvent, StreamQuery, TimeInput, TradeRow,
    blocking::{self},
};
use pyo3::{
    create_exception,
    exceptions::PyException,
    prelude::*,
    types::{PyAny, PyModule},
};
use serde::Serialize;
use serde_json::{Value, json};

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

    fn close(&self) {}

    #[pyo3(signature = (source, markets, instrument=None, include_buffer=false, materialize_orderbooks=true))]
    fn stream(
        &self,
        py: Python<'_>,
        source: String,
        markets: Vec<String>,
        instrument: Option<String>,
        include_buffer: bool,
        materialize_orderbooks: bool,
    ) -> PyResult<NativeRealtimeStream> {
        let iterator = py
            .detach(|| {
                self.inner.stream(StreamQuery {
                    source,
                    markets,
                    instrument,
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

    #[pyo3(signature = (source, market, instrument=None, expiry=None, option_type=None, q=None))]
    fn instruments<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        instrument: Option<String>,
        expiry: Option<i64>,
        option_type: Option<String>,
        q: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let result = py
            .detach(|| {
                self.inner.instruments(polaris_data::InstrumentsQuery {
                    source,
                    market,
                    instrument,
                    expiry,
                    option_type,
                    q,
                })
            })
            .map_err(native_error)?;
        to_python(py, &result)
    }

    fn count<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let result = py.detach(|| self.inner.count()).map_err(native_error)?;
        to_python(py, &result)
    }

    #[pyo3(signature = (source=None, market=None, start=None, end=None))]
    fn trades<'py>(
        &self,
        py: Python<'py>,
        source: Option<String>,
        market: Option<String>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                self.inner.trades(HistoricalRowsQuery {
                    source,
                    market,
                    start,
                    end,
                })
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::Trades(
            iterator,
        )))
    }

    #[pyo3(signature = (start, end, types=None, source=None, market=None, instrument=None))]
    fn events(
        &self,
        py: Python<'_>,
        start: i64,
        end: i64,
        types: Option<Vec<String>>,
        source: Option<String>,
        market: Option<String>,
        instrument: Option<String>,
    ) -> PyResult<NativeHistorical> {
        let types = types
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| {
                        serde_json::from_value::<MixedEventType>(Value::String(value)).map_err(
                            |_| pyo3::exceptions::PyValueError::new_err("invalid events type"),
                        )
                    })
                    .collect::<PyResult<Vec<_>>>()
            })
            .transpose()?;
        let iterator = py
            .detach(|| {
                self.inner.events(EventsQuery {
                    start,
                    end,
                    types,
                    source,
                    market,
                    instrument,
                })
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::Events(
            iterator,
        )))
    }

    #[pyo3(signature = (source=None, market=None, instrument=None, intent_id=None, start=None, end=None))]
    fn intents<'py>(
        &self,
        py: Python<'py>,
        source: Option<String>,
        market: Option<String>,
        instrument: Option<String>,
        intent_id: Option<String>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                self.inner.intents(IntentRowsQuery {
                    source,
                    market,
                    instrument,
                    intent_id,
                    start,
                    end,
                })
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::Intents(
            iterator,
        )))
    }

    #[pyo3(signature = (source=None, market=None, instrument=None, start=None, end=None))]
    fn option_tickers(
        &self,
        py: Python<'_>,
        source: Option<String>,
        market: Option<String>,
        instrument: Option<String>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                self.inner.option_tickers(OptionTickerRowsQuery {
                    source,
                    market,
                    instrument,
                    start,
                    end,
                })
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(
            NativeHistoricalIterator::OptionTickers(iterator),
        ))
    }

    #[pyo3(signature = (source=None, market=None, start=None, end=None))]
    fn perpetual_tickers(
        &self,
        py: Python<'_>,
        source: Option<String>,
        market: Option<String>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                self.inner.perpetual_tickers(HistoricalRowsQuery {
                    source,
                    market,
                    start,
                    end,
                })
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(
            NativeHistoricalIterator::PerpetualTickers(iterator),
        ))
    }

    #[pyo3(signature = (source, market=None, from_=None, to=None, limit=1000, channel=None))]
    fn raw<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: Option<String>,
        from_: Option<String>,
        to: Option<String>,
        limit: usize,
        channel: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let result = py
            .detach(|| {
                self.inner.raw(RawQuery {
                    source,
                    market,
                    channel,
                    from: time_input(from_),
                    to: time_input(to),
                    limit,
                })
            })
            .map_err(native_error)?;
        to_python(py, &result)
    }

    #[pyo3(signature = (exchange, event, start, end, market=None))]
    fn raw_channel(
        &self,
        py: Python<'_>,
        exchange: String,
        event: String,
        start: i64,
        end: i64,
        market: Option<String>,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                self.inner.raw_channel(RawChannelQuery {
                    exchange,
                    event,
                    market,
                    start,
                    end,
                })
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(
            NativeHistoricalIterator::RawCaptures(iterator),
        ))
    }

    #[pyo3(signature = (source=None, market=None, instrument=None, interval=None, start=None, end=None))]
    fn ohlcv(
        &self,
        py: Python<'_>,
        source: Option<String>,
        market: Option<String>,
        instrument: Option<String>,
        interval: Option<String>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyResult<NativeHistorical> {
        let query = OhlcvRowsQuery {
            source,
            market,
            instrument,
            interval,
            start,
            end,
        };
        let iterator = py
            .detach(|| self.inner.ohlcv(query))
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::Ohlcv(
            iterator,
        )))
    }

    #[pyo3(signature = (source, market, start, end, instrument=None))]
    fn l2_snapshots<'py>(
        &self,
        py: Python<'py>,
        source: String,
        market: String,
        start: i64,
        end: i64,
        instrument: Option<String>,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                self.inner.l2_snapshots(L2OrderbooksQuery {
                    source,
                    market,
                    instrument,
                    start,
                    end,
                })
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::L2Rows(
            iterator,
        )))
    }

    #[pyo3(signature = (source=None, market=None, instrument=None, start=None, end=None))]
    fn l2_updates<'py>(
        &self,
        py: Python<'py>,
        source: Option<String>,
        market: Option<String>,
        instrument: Option<String>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                self.inner.l2_updates(L2UpdatesQuery {
                    source,
                    market,
                    instrument,
                    start,
                    end,
                })
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(NativeHistoricalIterator::L2Rows(
            iterator,
        )))
    }

    #[pyo3(signature = (source=None, market=None, start=None, end=None))]
    fn funding_rates<'py>(
        &self,
        py: Python<'py>,
        source: Option<String>,
        market: Option<String>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyResult<NativeHistorical> {
        let iterator = py
            .detach(|| {
                self.inner.funding_rates(HistoricalRowsQuery {
                    source,
                    market,
                    start,
                    end,
                })
            })
            .map_err(native_error)?;
        Ok(NativeHistorical::new(
            NativeHistoricalIterator::FundingRates(iterator),
        ))
    }
}

#[pyclass(unsendable, module = "polaris_data._native")]
struct NativeHistorical {
    iterator: Option<NativeHistoricalIterator>,
}

enum NativeHistoricalIterator {
    Events(blocking::HistoricalIterator<MixedEventRow>),
    Ohlcv(blocking::HistoricalIterator<OhlcvRow>),
    L2Rows(blocking::HistoricalIterator<OrderbookL2Row>),
    Trades(blocking::HistoricalIterator<TradeRow>),
    Intents(blocking::HistoricalIterator<IntentRow>),
    OptionTickers(blocking::HistoricalIterator<OptionTickerRow>),
    FundingRates(blocking::HistoricalIterator<FundingRateRow>),
    RawCaptures(blocking::HistoricalIterator<RawCaptureRow>),
    PerpetualTickers(blocking::HistoricalIterator<FundingRateRow>),
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
            NativeHistoricalIterator::Events(iterator) => next_historical(py, iterator),
            NativeHistoricalIterator::Ohlcv(iterator) => next_historical(py, iterator),
            NativeHistoricalIterator::L2Rows(iterator) => next_historical(py, iterator),
            NativeHistoricalIterator::Trades(iterator) => next_historical(py, iterator),
            NativeHistoricalIterator::Intents(iterator) => next_historical(py, iterator),
            NativeHistoricalIterator::OptionTickers(iterator) => next_historical(py, iterator),
            NativeHistoricalIterator::FundingRates(iterator) => next_historical(py, iterator),
            NativeHistoricalIterator::RawCaptures(iterator) => next_historical(py, iterator),
            NativeHistoricalIterator::PerpetualTickers(iterator) => next_historical(py, iterator),
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
    module.add_class::<NativeOrderbookBuilder>()?;
    module.add_class::<NativeHistorical>()?;
    module.add_class::<NativeRealtimeStream>()?;
    module.add_function(wrap_pyfunction!(decode_file, module)?)?;
    module.add("NativeError", module.py().get_type::<NativeError>())?;
    module.add("__native__", true)?;
    Ok(())
}
