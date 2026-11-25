use std::{
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};

use polaris_data::{
    DataQuery, OrderbookBuilder, PolarisError, StandardEvent, StreamQuery, blocking,
};
use pyo3::{
    create_exception,
    exceptions::{PyException, PyRuntimeError, PyValueError},
    prelude::*,
    types::{PyAny, PyModule},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

create_exception!(_native, NativeError, PyException);

fn native_error(error: PolarisError) -> PyErr {
    NativeError::new_err(
        serde_json::to_string(&error.into_fault()).expect("serializable Polaris error"),
    )
}

fn decode<T: DeserializeOwned>(input: &str) -> PyResult<T> {
    serde_json::from_str(input).map_err(|error| PyValueError::new_err(error.to_string()))
}

fn encode(value: &impl Serialize) -> PyResult<String> {
    serde_json::to_string(value).map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

fn to_python<'py, T: Serialize>(py: Python<'py>, value: &T) -> PyResult<Bound<'py, PyAny>> {
    let mut value =
        serde_json::to_value(value).map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    prune_empty_event_identity(&mut value);
    py.import("json")?.call_method1("loads", (encode(&value)?,))
}

#[pyclass(module = "polaris_data._native")]
struct NativeClient {
    inner: blocking::PolarisClient,
}

#[pymethods]
impl NativeClient {
    #[new]
    #[pyo3(signature = (api_key=None, base_url="https://api.polaris.supply", timeout=30.0, stream_url=None))]
    fn new(
        api_key: Option<String>,
        base_url: &str,
        timeout: f64,
        stream_url: Option<String>,
    ) -> PyResult<Self> {
        if !timeout.is_finite() || timeout <= 0.0 {
            return Err(PyValueError::new_err("timeout must be greater than 0"));
        }
        let mut builder = blocking::PolarisClient::builder()
            .base_url(base_url)
            .timeout(Duration::from_secs_f64(
                timeout.min(f64::from(u32::MAX) / 1000.0),
            ));
        if let Some(value) = api_key {
            builder = builder.api_key(value);
        }
        if let Some(value) = stream_url {
            builder = builder.stream_url(value);
        }
        Ok(Self {
            inner: builder.build().map_err(native_error)?,
        })
    }

    fn request(&self, py: Python<'_>, operation: &str, input: &str) -> PyResult<String> {
        let query: Value = decode(input)?;
        let value: Value = py
            .detach(|| self.inner.request(operation, query))
            .map_err(native_error)?;
        encode(&value)
    }

    fn data_rows(&self, input: &str) -> PyResult<NativeDataRows> {
        let query: DataQuery = decode(input)?;
        let iterator = self.inner.data_rows(query).map_err(native_error)?;
        Ok(NativeDataRows {
            iterator: Mutex::new(Some(iterator)),
        })
    }

    fn stream(&self, input: &str) -> PyResult<NativeRealtimeStream> {
        let query: StreamQuery = decode(input)?;
        let stream = self.inner.stream(query).map_err(native_error)?;
        Ok(NativeRealtimeStream {
            stream: Arc::new(stream),
            reading: Mutex::new(()),
        })
    }
}

#[pyclass(module = "polaris_data._native")]
struct NativeDataRows {
    iterator: Mutex<Option<blocking::HistoricalIterator>>,
}

#[pymethods]
impl NativeDataRows {
    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<String>> {
        self.iterator
            .get_mut()
            .expect("historical iterator lock")
            .as_mut()
            .and_then(|iterator| py.detach(|| iterator.next()))
            .transpose()
            .map_err(native_error)?
            .as_ref()
            .map(encode)
            .transpose()
    }

    fn close(&mut self) {
        *self.iterator.get_mut().expect("historical iterator lock") = None;
    }
}

#[pyclass(module = "polaris_data._native")]
struct NativeRealtimeStream {
    stream: Arc<blocking::RealtimeIterator>,
    reading: Mutex<()>,
}

#[pymethods]
impl NativeRealtimeStream {
    fn __next__(&self, py: Python<'_>) -> PyResult<Option<String>> {
        let _guard = self
            .reading
            .try_lock()
            .map_err(|_| PyRuntimeError::new_err("Realtime stream already has a reader"))?;
        let stream = Arc::clone(&self.stream);
        let (sender, mut receiver) = mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .spawn(move || {
                let _ = sender.send(stream.read());
            })
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let result = loop {
            let recv = &mut receiver;
            match py.detach(move || recv.recv_timeout(Duration::from_millis(100))) {
                Ok(result) => {
                    break result
                        .map_err(native_error)
                        .and_then(|page| page.as_ref().map(encode).transpose());
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(error) = py.check_signals() {
                        self.stream.close();
                        break Err(error);
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break Err(PyRuntimeError::new_err("Realtime stream reader stopped"));
                }
            }
        };
        if !py.detach(move || worker.join().is_ok()) {
            return Err(PyRuntimeError::new_err("Realtime stream reader panicked"));
        }
        result
    }

    fn close(&self) {
        self.stream.close();
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

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__native__", true)?;
    m.add("NativeError", m.py().get_type::<NativeError>())?;
    m.add_class::<NativeClient>()?;
    m.add_class::<NativeOrderbookBuilder>()?;
    m.add_class::<NativeDataRows>()?;
    m.add_class::<NativeRealtimeStream>()?;
    Ok(())
}
