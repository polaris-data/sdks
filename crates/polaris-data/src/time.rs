use chrono::{DateTime, Duration, TimeZone, Utc};

use crate::{errors::PolarisError, models::TimeInput};

pub(crate) const DEFAULT_INFERRED_LOOKBACK: Duration = Duration::days(7);

pub(crate) fn to_datetime(value: &TimeInput) -> Result<DateTime<Utc>, PolarisError> {
    match value {
        TimeInput::Iso8601(raw) => {
            if raw.chars().all(|ch| ch.is_ascii_digit()) && raw.len() >= 13 {
                micros_to_datetime(raw.parse::<i64>().map_err(|err| {
                    PolarisError::InvalidResponse(format!("invalid epoch timestamp '{raw}': {err}"))
                })?)
            } else {
                DateTime::parse_from_rfc3339(raw)
                    .map(|value| value.with_timezone(&Utc))
                    .map_err(|err| {
                        PolarisError::InvalidResponse(format!("invalid timestamp '{raw}': {err}"))
                    })
            }
        }
        TimeInput::DateTime(value) => Ok(*value),
        TimeInput::EpochMicros(value) => micros_to_datetime(*value),
    }
}

pub(crate) fn to_epoch_micros(value: &TimeInput) -> Result<i64, PolarisError> {
    Ok(to_datetime(value)?.timestamp_micros())
}

pub(crate) fn micros_to_datetime(value: i64) -> Result<DateTime<Utc>, PolarisError> {
    Utc.timestamp_micros(value).single().ok_or_else(|| {
        PolarisError::InvalidResponse(format!("invalid epoch micros value '{value}'"))
    })
}
