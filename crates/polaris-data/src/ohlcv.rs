use std::collections::BTreeMap;

use crate::models::{
    OhlcvBar, OhlcvFormat, OhlcvInterval, OhlcvOutput, TradeEvent, TradingViewCandle,
    TradingViewOhlcv, TradingViewVolume,
};

#[derive(Clone, Debug)]
struct Bucket {
    timestamp: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    trades: u64,
}

pub(crate) fn aggregate(
    trades: &[TradeEvent],
    interval: OhlcvInterval,
    format: OhlcvFormat,
) -> OhlcvOutput {
    let width = interval_to_millis(interval);
    let mut buckets: BTreeMap<i64, Bucket> = BTreeMap::new();
    let mut previous_close: Option<f64> = None;
    let mut previous_bucket: Option<i64> = None;

    let mut ordered = trades.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|trade| trade.timestamp);
    for trade in ordered {
        let bucket_ts = (trade.timestamp / width) * width;
        let price = trade.data.price;
        let quantity = trade.data.quantity;

        buckets
            .entry(bucket_ts)
            .and_modify(|bucket| {
                bucket.high = bucket.high.max(price);
                bucket.low = bucket.low.min(price);
                bucket.close = price;
                bucket.volume += quantity;
                bucket.trades += 1;
            })
            .or_insert(Bucket {
                timestamp: bucket_ts,
                open: if previous_bucket.is_some_and(|previous| previous + width == bucket_ts) {
                    previous_close.unwrap_or(price)
                } else {
                    price
                },
                high: price,
                low: price,
                close: price,
                volume: quantity,
                trades: 1,
            });
        previous_close = Some(price);
        previous_bucket = Some(bucket_ts);
    }

    let bars: Vec<OhlcvBar> = buckets
        .into_values()
        .map(|bucket| OhlcvBar {
            timestamp: bucket.timestamp,
            open: bucket.open,
            high: bucket.high,
            low: bucket.low,
            close: bucket.close,
            volume: bucket.volume,
            trades: bucket.trades,
        })
        .collect();

    match format {
        OhlcvFormat::Bars => OhlcvOutput::Bars(bars),
        OhlcvFormat::TradingView => {
            let candles = bars
                .iter()
                .map(|bar| TradingViewCandle {
                    time: bar.timestamp / 1_000,
                    open: bar.open,
                    high: bar.high,
                    low: bar.low,
                    close: bar.close,
                })
                .collect();
            let volumes = bars
                .iter()
                .map(|bar| TradingViewVolume {
                    time: bar.timestamp / 1_000,
                    value: bar.volume,
                })
                .collect();

            OhlcvOutput::TradingView(TradingViewOhlcv { candles, volumes })
        }
    }
}

fn interval_to_millis(interval: OhlcvInterval) -> i64 {
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
