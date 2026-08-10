use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray, builder::StringDictionaryBuilder, types::Int32Type,
};
use arrow_pyarrow::{IntoPyArrow, ToPyArrow};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use polaris_data::{
    BboQuote, DepthMetricsRow, PointSeriesEvent, PolarisError, TradeEvent,
    blocking::{HistoricalIterator, PreparedHistoricalReplay},
};
use pyo3::{prelude::*, types::PyAny};
use serde_json::Value;

use crate::native_error;

const UTC: &str = "UTC";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExtensionType {
    Unknown,
    Boolean,
    Int64,
    Float64,
    Utf8,
    Json,
}

impl ExtensionType {
    fn observe(self, value: &Value) -> Self {
        let observed = match value {
            Value::Null => return self,
            Value::Bool(_) => Self::Boolean,
            Value::Number(number) if number.as_i64().is_some() => Self::Int64,
            Value::Number(number)
                if number
                    .as_u64()
                    .is_some_and(|value| i64::try_from(value).is_ok()) =>
            {
                Self::Int64
            }
            Value::Number(_) => Self::Float64,
            Value::String(_) => Self::Utf8,
            Value::Array(_) | Value::Object(_) => Self::Json,
        };
        match (self, observed) {
            (Self::Unknown, value) => value,
            (current, value) if current == value => current,
            (Self::Int64, Self::Float64) | (Self::Float64, Self::Int64) => Self::Float64,
            _ => Self::Json,
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::Unknown | Self::Utf8 => DataType::Utf8,
            Self::Json => DataType::LargeUtf8,
        }
    }
}

#[derive(Clone, Debug)]
struct ExtensionField {
    key: String,
    name: String,
    data_type: ExtensionType,
}

enum ColumnarIterator {
    Trades {
        iterator: HistoricalIterator<TradeEvent>,
        source: String,
        market: String,
        extensions: Vec<ExtensionField>,
    },
    Points {
        iterator: HistoricalIterator<PointSeriesEvent>,
        source: String,
        market: String,
        value_name: &'static str,
        extensions: Vec<ExtensionField>,
    },
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
    pub(crate) fn trades(
        plan: PreparedHistoricalReplay,
        source: String,
        market: String,
        batch_size: usize,
    ) -> Result<Self, PolarisError> {
        let extensions = infer_extensions(plan.trades(), |row| &row.data.extra)?;
        let schema = trade_schema(&extensions);
        Ok(Self {
            iterator: Some(ColumnarIterator::Trades {
                iterator: plan.trades(),
                source,
                market,
                extensions,
            }),
            schema,
            batch_size,
        })
    }

    pub(crate) fn points(
        plan: PreparedHistoricalReplay,
        source: String,
        market: String,
        series_name: &'static str,
        value_name: &'static str,
        batch_size: usize,
    ) -> Result<Self, PolarisError> {
        let extensions = infer_extensions(plan.point_series(series_name), |row| &row.data.extra)?;
        let schema = point_schema(value_name, &extensions);
        Ok(Self {
            iterator: Some(ColumnarIterator::Points {
                iterator: plan.point_series(series_name),
                source,
                market,
                value_name,
                extensions,
            }),
            schema,
            batch_size,
        })
    }

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
            ColumnarIterator::Trades {
                iterator,
                source,
                market,
                extensions,
            } => take_rows(iterator, self.batch_size)?
                .map(|rows| build_trade_batch(schema, &rows, source, market, extensions)),
            ColumnarIterator::Points {
                iterator,
                source,
                market,
                value_name,
                extensions,
            } => take_rows(iterator, self.batch_size)?.map(|rows| {
                build_point_batch(schema, &rows, source, market, value_name, extensions)
            }),
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

fn infer_extensions<T>(
    iterator: HistoricalIterator<T>,
    extra: impl Fn(&T) -> &BTreeMap<String, Value>,
) -> Result<Vec<ExtensionField>, PolarisError> {
    let mut fields = BTreeMap::<String, ExtensionType>::new();
    for row in iterator {
        let row = row?;
        for (key, value) in extra(&row) {
            let current = fields.entry(key.clone()).or_insert(ExtensionType::Unknown);
            *current = current.observe(value);
        }
    }
    Ok(fields
        .into_iter()
        .map(|(key, data_type)| ExtensionField {
            name: format!("extra.{key}"),
            key,
            data_type,
        })
        .collect())
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

fn trade_schema(extensions: &[ExtensionField]) -> SchemaRef {
    let mut fields = Vec::from(identity_fields());
    fields.extend([
        Field::new("price", DataType::Float64, false),
        Field::new("quantity", DataType::Float64, false),
        Field::new("side", dictionary_type(), true),
    ]);
    fields.extend(
        extensions
            .iter()
            .map(|field| Field::new(&field.name, field.data_type.data_type(), true)),
    );
    Arc::new(Schema::new(fields))
}

fn point_schema(value_name: &str, extensions: &[ExtensionField]) -> SchemaRef {
    let mut fields = Vec::from(identity_fields());
    fields.push(Field::new(value_name, DataType::Float64, false));
    fields.extend(
        extensions
            .iter()
            .map(|field| Field::new(&field.name, field.data_type.data_type(), true)),
    );
    Arc::new(Schema::new(fields))
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

fn identity<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.is_empty() { fallback } else { value }
}

fn extension_array<T>(
    rows: &[T],
    field: &ExtensionField,
    extra: impl Fn(&T) -> &BTreeMap<String, Value>,
) -> Result<ArrayRef, PolarisError> {
    let values = rows.iter().map(|row| extra(row).get(&field.key));
    let mismatch = || {
        PolarisError::Decode(format!(
            "columnar schema changed while reading {}",
            field.name
        ))
    };
    match field.data_type {
        ExtensionType::Boolean => {
            let values = values
                .map(|value| optional_typed_value(value, Value::as_bool))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(mismatch)?;
            Ok(Arc::new(BooleanArray::from(values)))
        }
        ExtensionType::Int64 => {
            let values = values
                .map(|value| {
                    optional_typed_value(value, |value| {
                        value
                            .as_i64()
                            .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
                    })
                })
                .collect::<Option<Vec<_>>>()
                .ok_or_else(mismatch)?;
            Ok(Arc::new(Int64Array::from(values)))
        }
        ExtensionType::Float64 => {
            let values = values
                .map(|value| optional_typed_value(value, Value::as_f64))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(mismatch)?;
            Ok(Arc::new(Float64Array::from(values)))
        }
        ExtensionType::Unknown | ExtensionType::Utf8 => {
            let values = values
                .map(|value| {
                    optional_typed_value(value, |value| value.as_str().map(ToOwned::to_owned))
                })
                .collect::<Option<Vec<_>>>()
                .ok_or_else(mismatch)?;
            Ok(Arc::new(StringArray::from(values)))
        }
        ExtensionType::Json => {
            let values = values
                .map(|value| {
                    value
                        .filter(|value| !value.is_null())
                        .map(serde_json::to_string)
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| PolarisError::Decode(error.to_string()))?;
            Ok(Arc::new(arrow_array::LargeStringArray::from(values)))
        }
    }
}

fn optional_typed_value<T>(
    value: Option<&Value>,
    parse: impl FnOnce(&Value) -> Option<T>,
) -> Option<Option<T>> {
    match value {
        None | Some(Value::Null) => Some(None),
        Some(value) => parse(value).map(Some),
    }
}

fn build_trade_batch(
    schema: SchemaRef,
    rows: &[TradeEvent],
    source: &str,
    market: &str,
    extensions: &[ExtensionField],
) -> Result<RecordBatch, PolarisError> {
    validate_extension_keys(rows, extensions, |row| &row.data.extra)?;
    let mut columns = vec![
        timestamps(rows.iter().map(|row| row.timestamp)),
        dictionary(rows.iter().map(|row| Some(identity(&row.source, source))))?,
        dictionary(rows.iter().map(|row| Some(identity(&row.market, market))))?,
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|row| row.data.price),
        )),
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|row| row.data.quantity),
        )),
        dictionary(
            rows.iter()
                .map(|row| (!row.data.side.is_empty()).then_some(row.data.side.as_str())),
        )?,
    ];
    for field in extensions {
        columns.push(extension_array(rows, field, |row| &row.data.extra)?);
    }
    RecordBatch::try_new(schema, columns).map_err(|error| PolarisError::Decode(error.to_string()))
}

fn build_point_batch(
    schema: SchemaRef,
    rows: &[PointSeriesEvent],
    source: &str,
    market: &str,
    _value_name: &str,
    extensions: &[ExtensionField],
) -> Result<RecordBatch, PolarisError> {
    validate_extension_keys(rows, extensions, |row| &row.data.extra)?;
    let mut columns = vec![
        timestamps(rows.iter().map(|row| row.timestamp)),
        dictionary(rows.iter().map(|row| Some(identity(&row.source, source))))?,
        dictionary(rows.iter().map(|row| Some(identity(&row.market, market))))?,
        Arc::new(Float64Array::from_iter_values(
            rows.iter().map(|row| row.data.value),
        )),
    ];
    for field in extensions {
        columns.push(extension_array(rows, field, |row| &row.data.extra)?);
    }
    RecordBatch::try_new(schema, columns).map_err(|error| PolarisError::Decode(error.to_string()))
}

fn validate_extension_keys<T>(
    rows: &[T],
    extensions: &[ExtensionField],
    extra: impl Fn(&T) -> &BTreeMap<String, Value>,
) -> Result<(), PolarisError> {
    for row in rows {
        for key in extra(row).keys() {
            if extensions
                .binary_search_by(|field| field.key.as_str().cmp(key.as_str()))
                .is_err()
            {
                return Err(PolarisError::Decode(format!(
                    "columnar schema changed while reading extra.{key}"
                )));
            }
        }
    }
    Ok(())
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
