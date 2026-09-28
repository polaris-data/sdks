use crate::models::OhlcvInterval;

pub(crate) fn interval_to_millis(interval: OhlcvInterval) -> i64 {
    match interval {
        OhlcvInterval::Ms100 => 100,
        OhlcvInterval::S1 => 1_000,
        OhlcvInterval::S10 => 10_000,
        OhlcvInterval::M1 => 60_000,
        OhlcvInterval::M5 => 300_000,
        OhlcvInterval::M15 => 900_000,
        OhlcvInterval::H1 => 3_600_000,
    }
}
