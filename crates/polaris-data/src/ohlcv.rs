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

pub(crate) struct OhlcvAggregator {
    width: i64,
    buckets: BTreeMap<i64, Bucket>,
}

impl OhlcvAggregator {
    pub(crate) fn new(interval: OhlcvInterval) -> Self {
        Self {
            width: interval_to_millis(interval),
            buckets: BTreeMap::new(),
        }
    }

    pub(crate) fn add(&mut self, trade: &TradeEvent) {
        let timestamp = trade.timestamp();
        let bucket_ts = timestamp.div_euclid(self.width) * self.width;
        let price = trade.price();
        let quantity = trade.quantity();

        self.buckets
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
                open: price,
                high: price,
                low: price,
                close: price,
                volume: quantity,
                trades: 1,
            });
    }

    pub(crate) fn finish(self, format: OhlcvFormat) -> OhlcvOutput {
        let mut bars = Vec::with_capacity(self.buckets.len());
        let mut previous: Option<(i64, f64)> = None;
        for bucket in self.buckets.into_values() {
            let open = previous
                .filter(|(timestamp, _)| *timestamp + self.width == bucket.timestamp)
                .map(|(_, close)| close)
                .unwrap_or(bucket.open);
            previous = Some((bucket.timestamp, bucket.close));
            bars.push(OhlcvBar {
                timestamp: bucket.timestamp,
                open,
                high: bucket.high,
                low: bucket.low,
                close: bucket.close,
                volume: bucket.volume,
                trades: bucket.trades,
            });
        }

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
}

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
