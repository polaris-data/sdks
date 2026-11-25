use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::{OrderbookData, PolarisError, StandardEvent, ffi::Orderbooks};

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum BookUpdate {
    Ignored,
    Suppressed,
    Applied,
}

/// Reconstruct complete order books from standardized snapshot and delta events.
///
/// State is isolated by `(source, market)`. Deltas received before a snapshot are
/// refused by returning `Ok(None)`. Non-orderbook events pass through unchanged.
#[derive(Clone, Default)]
pub struct OrderbookBuilder {
    books: Orderbooks,
}

impl fmt::Debug for OrderbookBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OrderbookBuilder")
            .finish_non_exhaustive()
    }
}

impl OrderbookBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.books
            .change::<()>(&json!({"operation": "clear"}))
            .expect("clear native orderbook");
    }

    pub fn clear_book(&mut self, source: &str, market: &str) {
        self.books
            .change::<()>(&json!({
                "operation": "clear_book", "source": source, "market": market
            }))
            .expect("clear native book");
    }

    pub fn apply(
        &mut self,
        mut event: StandardEvent,
    ) -> Result<Option<StandardEvent>, PolarisError> {
        match self.update_state(&event)? {
            BookUpdate::Ignored => return Ok(Some(event)),
            BookUpdate::Suppressed => return Ok(None),
            BookUpdate::Applied => {}
        }

        let book: Option<OrderbookData> = self.books.snapshot(event.source(), event.market())?;
        let book = book.expect("book state initialized");
        let mut data = match std::mem::take(event.data_mut()) {
            Value::Object(object) => object,
            _ => Map::new(),
        };
        data.insert("bids".to_owned(), json!(book.bids));
        data.insert("asks".to_owned(), json!(book.asks));
        event.extra_mut().remove("bids");
        event.extra_mut().remove("asks");
        *event.event_type_mut() = "orderbook".to_owned();
        *event.data_mut() = Value::Object(data);
        Ok(Some(event))
    }

    /// Update book state without constructing a complete orderbook.
    ///
    /// Returns `true` when an orderbook snapshot or delta was applied. Deltas
    /// received before their first snapshot and non-orderbook events return
    /// `false`.
    pub fn update(&mut self, event: &StandardEvent) -> Result<bool, PolarisError> {
        Ok(matches!(self.update_state(event)?, BookUpdate::Applied))
    }

    /// Materialize the current complete book for a source and market.
    pub fn snapshot(&self, source: &str, market: &str) -> Option<OrderbookData> {
        self.books
            .snapshot(source, market)
            .expect("read native orderbook")
    }

    fn update_state(&mut self, event: &StandardEvent) -> Result<BookUpdate, PolarisError> {
        #[derive(Serialize)]
        struct BookData<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            is_snapshot: Option<&'a Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            bids: Option<&'a Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            asks: Option<&'a Value>,
        }

        #[derive(Serialize)]
        struct Update<'a> {
            operation: &'static str,
            source: &'a str,
            market: &'a str,
            kind: &'a str,
            v2: bool,
            data: Option<BookData<'a>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            bids: Option<&'a Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            asks: Option<&'a Value>,
        }

        self.books.change(&Update {
            operation: "update",
            source: event.source(),
            market: event.market(),
            kind: event.event_type(),
            v2: matches!(event, StandardEvent::V2(_)),
            data: event.data().as_object().map(|data| BookData {
                is_snapshot: data.get("is_snapshot"),
                bids: data.get("bids"),
                asks: data.get("asks"),
            }),
            bids: event.extra().get("bids"),
            asks: event.extra().get("asks"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LegacyStandardEvent;

    #[test]
    fn clones_are_independent_and_invalid_deltas_leave_the_book_intact() {
        let mut original = OrderbookBuilder::new();
        original
            .update(&event(
                "orderbook",
                "s",
                "m",
                json!({"bids": [[99, 1]], "asks": [[101, 2]]}),
            ))
            .unwrap();
        let mut copy = original.clone();
        let invalid = event(
            "orderbook_delta",
            "s",
            "m",
            json!({"bids": [[99, 0]], "asks": [[101, -1]]}),
        );
        assert!(matches!(
            copy.update(&invalid),
            Err(PolarisError::Decode(_))
        ));
        assert_eq!(copy.snapshot("s", "m"), original.snapshot("s", "m"));

        copy.update(&event(
            "orderbook_delta",
            "s",
            "m",
            json!({"bids": [[99, 0]]}),
        ))
        .unwrap();
        assert!(copy.snapshot("s", "m").unwrap().bids.is_empty());
        assert_eq!(original.snapshot("s", "m").unwrap().bids.len(), 1);
        original.clear();
        assert!(original.snapshot("s", "m").is_none());
        assert_eq!(copy.snapshot("s", "m").unwrap().asks.len(), 1);
    }

    fn event(kind: &str, source: &str, market: &str, data: Value) -> StandardEvent {
        StandardEvent::Legacy(LegacyStandardEvent {
            timestamp: 1,
            source: source.to_owned(),
            market: market.to_owned(),
            event_type: kind.to_owned(),
            data,
            extra: Default::default(),
        })
    }

    #[test]
    fn snapshot_delta_deletion_and_clear() {
        let mut builder = OrderbookBuilder::new();
        assert!(
            builder
                .apply(event(
                    "orderbook_delta",
                    "lighter",
                    "BTC-USD",
                    json!({"bids": [[100, 2]]})
                ))
                .unwrap()
                .is_none()
        );

        let snapshot = builder
            .apply(event(
                "orderbook",
                "lighter",
                "BTC-USD",
                json!({"bids": [[99, 1], [100, 2]], "asks": [{"price": 101, "size": 3}]}),
            ))
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.data()["bids"][0]["price"], 100.0);

        let updated = builder
            .apply(event(
                "orderbook_delta",
                "lighter",
                "BTC-USD",
                json!({"bids": [[100, 0], [98, 4]], "asks": [[101, 5]]}),
            ))
            .unwrap()
            .unwrap();
        assert_eq!(updated.event_type(), "orderbook");
        assert_eq!(
            updated.data()["bids"],
            json!([{"price": 99.0, "quantity": 1.0}, {"price": 98.0, "quantity": 4.0}])
        );
        assert_eq!(
            updated.data()["asks"],
            json!([{"price": 101.0, "quantity": 5.0}])
        );

        let replaced = builder
            .apply(event(
                "orderbook",
                "lighter",
                "BTC-USD",
                json!({"bids": [[97, 6]], "asks": [[103, 7]]}),
            ))
            .unwrap()
            .unwrap();
        assert_eq!(
            replaced.data()["bids"],
            json!([{"price": 97.0, "quantity": 6.0}])
        );

        builder
            .apply(event(
                "orderbook",
                "lighter",
                "ETH-USD",
                json!({"bids": [[10, 1]], "asks": [[11, 1]]}),
            ))
            .unwrap();

        builder.clear_book("lighter", "BTC-USD");
        assert!(
            builder
                .apply(event(
                    "orderbook_delta",
                    "lighter",
                    "BTC-USD",
                    json!({"asks": [[102, 1]]})
                ))
                .unwrap()
                .is_none()
        );
        assert!(
            builder
                .apply(event(
                    "orderbook_delta",
                    "lighter",
                    "ETH-USD",
                    json!({"bids": [[10, 2]]})
                ))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn malformed_snapshot_does_not_replace_valid_state() {
        let mut builder = OrderbookBuilder::new();
        builder
            .apply(event(
                "orderbook",
                "s",
                "m",
                json!({"bids": [[1, 1]], "asks": [[2, 1]]}),
            ))
            .unwrap();
        assert!(
            builder
                .apply(event("orderbook", "s", "m", json!({"bids": [[3, 1]]})))
                .is_err()
        );
        let result = builder
            .apply(event(
                "orderbook_delta",
                "s",
                "m",
                json!({"asks": [[2, 2]]}),
            ))
            .unwrap()
            .unwrap();
        assert_eq!(result.data()["bids"][0]["price"], 1.0);
    }

    #[test]
    fn lazy_update_materializes_only_on_snapshot_request() {
        let mut builder = OrderbookBuilder::new();
        assert!(
            !builder
                .update(&event(
                    "orderbook_delta",
                    "s",
                    "m",
                    json!({"bids": [[1, 2]]}),
                ))
                .unwrap()
        );
        assert!(builder.snapshot("s", "m").is_none());

        assert!(
            builder
                .update(&event(
                    "orderbook",
                    "s",
                    "m",
                    json!({"bids": [[1, 2]], "asks": [[2, 3]]}),
                ))
                .unwrap()
        );
        assert!(
            builder
                .update(&event(
                    "orderbook_delta",
                    "s",
                    "m",
                    json!({"bids": [[1, 4]]}),
                ))
                .unwrap()
        );

        let snapshot = builder.snapshot("s", "m").expect("book");
        assert_eq!(snapshot.bids[0].price, 1.0);
        assert_eq!(snapshot.bids[0].quantity, 4.0);
        assert_eq!(snapshot.asks[0].price, 2.0);
    }
}
