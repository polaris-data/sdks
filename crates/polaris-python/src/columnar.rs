use std::sync::Arc;

use arrow_array::{
    ArrayRef, Float64Array, RecordBatch, TimestampMillisecondArray,
    builder::StringDictionaryBuilder, types::Int32Type,
};
use arrow_pyarrow::{IntoPyArrow, ToPyArrow};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use polaris_data::{BboQuote, DepthMetricsRow, PolarisError, blocking::HistoricalIterator};
use pyo3::{prelude::*, types::PyAny};

use crate::native_error;

const UTC: &str = "UTC";

enum ColumnarIterator {
    Bbo {
        iterator: HistoricalIterator<BboQuote>,
        source: String,
        market: String,
    },
    Depth {
        iterator: HistoricalIterator<DepthMetricsRow>,
        source: String,
        market: String,
    },
}

#[pyclass(unsendable, module = "polaris_data._native")]
pub(crate) struct NativeColumnar {
    iterator: Option<ColumnarIterator>,
    schema: SchemaRef,
    batch_size: usize,
}

impl NativeColumnar {
    pub(crate) fn bbo(
        iterator: HistoricalIterator<BboQuote>,
        source: String,
        market: String,
        batch_size: usize,
    ) -> Self {
        Self {
            iterator: Some(ColumnarIterator::Bbo {
                iterator,
                source,
                market,
            }),
            schema: bbo_schema(),
            batch_size,
        }
    }

    pub(crate) fn depth(
        iterator: HistoricalIterator<DepthMetricsRow>,
        source: String,
        market: String,
        batch_size: usize,
    ) -> Self {
        Self {
            iterator: Some(ColumnarIterator::Depth {
                iterator,
                source,
                market,
            }),
            schema: depth_schema(),
            batch_size,
        }
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, PolarisError> {
        let Some(iterator) = self.iterator.as_mut() else {
            return Ok(None);
        };
        let schema = Arc::clone(&self.schema);
        let result = match iterator {
            ColumnarIterator::Bbo {
                iterator,
                source,
                market,
            } => take_rows(iterator, self.batch_size)?
                .map(|rows| build_bbo_batch(schema, &rows, source, market)),
            ColumnarIterator::Depth {
                iterator,
                source,
                market,
            } => take_rows(iterator, self.batch_size)?
                .map(|rows| build_depth_batch(schema, &rows, source, market)),
        };
        match result {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(error)) => {
                self.iterator = None;
                Err(error)
            }
            None => {
                self.iterator = None;
                Ok(None)
            }
        }
    }
}

#[pymethods]
impl NativeColumnar {
    #[getter]
    fn schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.schema.as_ref().to_pyarrow(py)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        py.detach(|| self.next_batch())
            .map_err(native_error)?
            .map(|batch| batch.into_pyarrow(py))
            .transpose()
    }

    fn close(&mut self) {
        self.iterator = None;
    }
}

fn take_rows<T>(
    iterator: &mut HistoricalIterator<T>,
    batch_size: usize,
) -> Result<Option<Vec<T>>, PolarisError> {
    let mut rows = Vec::with_capacity(batch_size);
    while rows.len() < batch_size {
        match iterator.next() {
            Some(Ok(row)) => rows.push(row),
            Some(Err(error)) => return Err(error),
            None => break,
        }
    }
    Ok((!rows.is_empty()).then_some(rows))
}

fn timestamp_field() -> Field {
    Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Millisecond, Some(UTC.into())),
        false,
    )
}

fn dictionary_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

fn identity_fields() -> [Field; 3] {
    [
        timestamp_field(),
        Field::new("source", dictionary_type(), false),
        Field::new("market", dictionary_type(), false),
    ]
}

fn bbo_schema() -> SchemaRef {
    let mut fields = Vec::from(identity_fields());
    fields.extend(
        ["bid_price", "bid_quantity", "ask_price", "ask_quantity"]
            .map(|name| Field::new(name, DataType::Float64, false)),
    );
    Arc::new(Schema::new(fields))
}

fn depth_schema() -> SchemaRef {
    let mut fields = Vec::from(identity_fields());
    fields.extend([
        Field::new("bid_price", DataType::Float64, false),
        Field::new("ask_price", DataType::Float64, false),
        Field::new("mid_price", DataType::Float64, false),
        Field::new("bid_ask_spread", DataType::Float64, false),
        Field::new("bid_ask_spread_bps", DataType::Float64, true),
        Field::new("depth_pct", DataType::Float64, false),
        Field::new("bid_depth_notional", DataType::Float64, false),
        Field::new("ask_depth_notional", DataType::Float64, false),
        Field::new("depth_imbalance", DataType::Float64, true),
        Field::new("slippage_notional", DataType::Float64, false),
        Field::new("target_base_quantity", DataType::Float64, true),
        Field::new("buy_average_price", DataType::Float64, true),
        Field::new("sell_average_price", DataType::Float64, true),
        Field::new("buy_slippage", DataType::Float64, true),
        Field::new("sell_slippage", DataType::Float64, true),
        Field::new("buy_slippage_bps", DataType::Float64, true),
        Field::new("sell_slippage_bps", DataType::Float64, true),
    ]);
    Arc::new(Schema::new(fields))
}

fn timestamps(values: impl IntoIterator<Item = i64>) -> ArrayRef {
    Arc::new(TimestampMillisecondArray::from_iter_values(values).with_timezone(UTC))
}

fn dictionary<'a>(
    values: impl IntoIterator<Item = Option<&'a str>>,
) -> Result<ArrayRef, PolarisError> {
    let mut builder = StringDictionaryBuilder::<Int32Type>::new();
    for value in values {
        if let Some(value) = value {
            builder
                .append(value)
                .map_err(|error| PolarisError::Decode(error.to_string()))?;
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_bbo_batch(
    schema: SchemaRef,
    rows: &[BboQuote],
    source: &str,
    market: &str,
) -> Result<RecordBatch, PolarisError> {
    let columns: Vec<ArrayRef> = vec![
        timestamps(rows.iter().map(|row| row.timestamp)),
        dictionary(rows.iter().map(|_| Some(source)))?,
        dictionary(rows.iter().map(|_| Some(market)))?,
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|row| row.bid_price),
        )),
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|row| row.bid_quantity),
        )),
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|row| row.ask_price),
        )),
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|row| row.ask_quantity),
        )),
    ];
    RecordBatch::try_new(schema, columns).map_err(|error| PolarisError::Decode(error.to_string()))
}

fn build_depth_batch(
    schema: SchemaRef,
    rows: &[DepthMetricsRow],
    source: &str,
    market: &str,
) -> Result<RecordBatch, PolarisError> {
    let required = |value: fn(&DepthMetricsRow) -> f64| -> ArrayRef {
        Arc::new(Float64Array::from_iter_values(rows.iter().map(value)))
    };
    let optional = |value: fn(&DepthMetricsRow) -> Option<f64>| -> ArrayRef {
        Arc::new(Float64Array::from_iter(rows.iter().map(value)))
    };
    let columns = vec![
        timestamps(rows.iter().map(|row| row.timestamp)),
        dictionary(rows.iter().map(|_| Some(source)))?,
        dictionary(rows.iter().map(|_| Some(market)))?,
        required(|row| row.bid_price),
        required(|row| row.ask_price),
        required(|row| row.mid_price),
        required(|row| row.bid_ask_spread),
        optional(|row| row.bid_ask_spread_bps),
        required(|row| row.depth_pct),
        required(|row| row.bid_depth_notional),
        required(|row| row.ask_depth_notional),
        optional(|row| row.depth_imbalance),
        required(|row| row.slippage_notional),
        optional(|row| row.target_base_quantity),
        optional(|row| row.buy_average_price),
        optional(|row| row.sell_average_price),
        optional(|row| row.buy_slippage),
        optional(|row| row.sell_slippage),
        optional(|row| row.buy_slippage_bps),
        optional(|row| row.sell_slippage_bps),
    ];
    RecordBatch::try_new(schema, columns).map_err(|error| PolarisError::Decode(error.to_string()))
}
