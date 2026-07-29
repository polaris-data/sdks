use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};

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

pub(crate) fn to_iso8601(value: &TimeInput) -> Result<String, PolarisError> {
    Ok(to_datetime(value)?.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}

pub(crate) fn micros_to_datetime(value: i64) -> Result<DateTime<Utc>, PolarisError> {
    Utc.timestamp_micros(value).single().ok_or_else(|| {
        PolarisError::InvalidResponse(format!("invalid epoch micros value '{value}'"))
    })
}

pub(crate) fn end_of_public_cutoff_day(cutoff: &str) -> Result<i64, PolarisError> {
    let day = NaiveDate::parse_from_str(cutoff, "%Y-%m-%d").map_err(|err| {
        PolarisError::InvalidResponse(format!("invalid public cutoff '{cutoff}': {err}"))
    })?;
    let next_day = day.succ_opt().ok_or_else(|| {
        PolarisError::InvalidResponse(format!("invalid public cutoff day '{cutoff}'"))
    })?;
    let dt = next_day.and_hms_opt(0, 0, 0).ok_or_else(|| {
        PolarisError::InvalidResponse(format!("invalid public cutoff day '{cutoff}'"))
    })?;
    Ok(Utc.from_utc_datetime(&dt).timestamp_micros())
}
