use std::collections::{BTreeMap, HashMap};

use ordered_float::OrderedFloat;
use serde_json::{Map, Value, json};

use crate::{BboQuote, OrderbookData, OrderbookLevel, PolarisError, StandardEvent};

const SNAPSHOT_TYPES: [&str; 3] = ["orderbook", "orderbook_snapshot", "l2_snapshot"];
const DELTA_TYPE: &str = "orderbook_delta";

#[derive(Clone, Debug, Default)]
struct BookState {
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BookUpdate {
    Ignored,
    Suppressed,
    Applied,
}

pub(crate) struct BookView<'a> {
    state: &'a BookState,
}

impl BookView<'_> {
    pub(crate) fn bids(&self) -> impl Iterator<Item = (f64, f64)> + '_ {
        self.state
            .bids
            .iter()
            .rev()
            .map(|(price, quantity)| (price.0, *quantity))
    }

    pub(crate) fn asks(&self) -> impl Iterator<Item = (f64, f64)> + '_ {
        self.state
            .asks
            .iter()
            .map(|(price, quantity)| (price.0, *quantity))
    }
}

/// Reconstruct complete order books from standardized snapshot and delta events.
///
/// State is isolated by `(source, market)`. Deltas received before a snapshot are
/// refused by returning `Ok(None)`. Non-orderbook events pass through unchanged.
#[derive(Clone, Debug, Default)]
pub struct OrderbookBuilder {
    books: HashMap<String, HashMap<String, BookState>>,
}

impl OrderbookBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.books.clear();
    }

    pub fn clear_book(&mut self, source: &str, market: &str) {
        if let Some(markets) = self.books.get_mut(source) {
            markets.remove(market);
            if markets.is_empty() {
                self.books.remove(source);
            }
        }
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

        let state = self
            .books
            .get(&event.source)
            .and_then(|markets| markets.get(&event.market))
            .expect("book state initialized");
        let mut data = match std::mem::take(&mut event.data) {
            Value::Object(object) => object,
            _ => Map::new(),
        };
        data.insert("bids".to_owned(), canonical_levels(state.bids.iter().rev()));
        data.insert("asks".to_owned(), canonical_levels(state.asks.iter()));
        event.extra.remove("bids");
        event.extra.remove("asks");
        event.event_type = "orderbook".to_owned();
        event.data = Value::Object(data);
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
        let state = self.books.get(source)?.get(market)?;
        Some(OrderbookData {
            bids: typed_levels(state.bids.iter().rev()),
            asks: typed_levels(state.asks.iter()),
            extra: Default::default(),
        })
    }

    pub(crate) fn update_state(
        &mut self,
        event: &StandardEvent,
    ) -> Result<BookUpdate, PolarisError> {
        let is_snapshot = SNAPSHOT_TYPES.contains(&event.event_type.as_str());
        let is_delta = event.event_type == DELTA_TYPE;
        if !is_snapshot && !is_delta {
            return Ok(BookUpdate::Ignored);
        }

        let data = match &event.data {
            Value::Object(object) => Some(object),
            _ if event.extra.contains_key("bids") || event.extra.contains_key("asks") => None,
            _ => {
                return Err(PolarisError::Decode(
                    "invalid orderbook payload: data must be an object".to_owned(),
                ));
            }
        };
        let bids = parse_side(side_value(data, event, "bids"), is_snapshot, "bids")?;
        let asks = parse_side(side_value(data, event, "asks"), is_snapshot, "asks")?;
        if is_snapshot {
            let mut state = BookState::default();
            apply_levels(&mut state.bids, bids.expect("snapshot bids validated"));
            apply_levels(&mut state.asks, asks.expect("snapshot asks validated"));
            self.books
                .entry(event.source.clone())
                .or_default()
                .insert(event.market.clone(), state);
        } else {
            let Some(state) = self
                .books
                .get_mut(&event.source)
                .and_then(|markets| markets.get_mut(&event.market))
            else {
                return Ok(BookUpdate::Suppressed);
            };
            if let Some(levels) = bids {
                apply_levels(&mut state.bids, levels);
            }
            if let Some(levels) = asks {
                apply_levels(&mut state.asks, levels);
            }
        }

        Ok(BookUpdate::Applied)
    }

    pub(crate) fn best_bid_offer(
        &self,
        source: &str,
        market: &str,
        timestamp: i64,
    ) -> Option<BboQuote> {
        let state = self.books.get(source)?.get(market)?;
        let (bid_price, bid_quantity) = state.bids.last_key_value()?;
        let (ask_price, ask_quantity) = state.asks.first_key_value()?;
        Some(BboQuote {
            timestamp,
            bid_price: bid_price.0,
            bid_quantity: *bid_quantity,
            ask_price: ask_price.0,
            ask_quantity: *ask_quantity,
        })
    }

    pub(crate) fn view(&self, source: &str, market: &str) -> Option<BookView<'_>> {
        self.books
            .get(source)?
            .get(market)
            .map(|state| BookView { state })
    }
}

fn side_value<'a>(
    data: Option<&'a Map<String, Value>>,
    event: &'a StandardEvent,
    side: &str,
) -> Option<&'a Value> {
    data.and_then(|object| object.get(side))
        .or_else(|| event.extra.get(side))
}

fn parse_side(
    value: Option<&Value>,
    required: bool,
    side: &str,
) -> Result<Option<Vec<OrderbookLevel>>, PolarisError> {
    let Some(value) = value else {
        if required {
            return Err(PolarisError::Decode(format!(
                "invalid orderbook snapshot: data.{side} is required"
            )));
        }
        return Ok(None);
    };
    let rows = value.as_array().ok_or_else(|| {
        PolarisError::Decode(format!(
            "invalid orderbook payload: data.{side} must be an array"
        ))
    })?;
    rows.iter()
        .map(parse_level)
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn parse_level(value: &Value) -> Result<OrderbookLevel, PolarisError> {
    let (price, quantity) = if let Some(values) = value.as_array() {
        if values.len() < 2 {
            return Err(PolarisError::Decode(
                "invalid orderbook level: expected [price, quantity]".to_owned(),
            ));
        }
        (parse_number(&values[0]), parse_number(&values[1]))
    } else if let Some(object) = value.as_object() {
        let quantity = object
            .get("quantity")
            .or_else(|| object.get("size"))
            .or_else(|| object.get("amount"));
        (
            object.get("price").and_then(parse_number),
            quantity.and_then(parse_number),
        )
    } else {
        (None, None)
    };

    let price = price.ok_or_else(|| {
        PolarisError::Decode("invalid orderbook level: price must be numeric".to_owned())
    })?;
    let quantity = quantity.ok_or_else(|| {
        PolarisError::Decode("invalid orderbook level: quantity must be numeric".to_owned())
    })?;
    if !price.is_finite() || price <= 0.0 || !quantity.is_finite() || quantity < 0.0 {
        return Err(PolarisError::Decode(
            "invalid orderbook level: price must be positive and quantity non-negative".to_owned(),
        ));
    }
    Ok(OrderbookLevel { price, quantity })
}

fn parse_number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn apply_levels(book: &mut BTreeMap<OrderedFloat<f64>, f64>, updates: Vec<OrderbookLevel>) {
    for update in updates {
        let price = OrderedFloat(update.price);
        if update.quantity == 0.0 {
            book.remove(&price);
        } else {
            book.insert(price, update.quantity);
        }
    }
}

fn canonical_levels<'a>(levels: impl Iterator<Item = (&'a OrderedFloat<f64>, &'a f64)>) -> Value {
    json!(typed_levels(levels))
}

fn typed_levels<'a>(
    levels: impl Iterator<Item = (&'a OrderedFloat<f64>, &'a f64)>,
) -> Vec<OrderbookLevel> {
    levels
        .map(|(price, quantity)| OrderbookLevel {
            price: price.0,
            quantity: *quantity,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(kind: &str, source: &str, market: &str, data: Value) -> StandardEvent {
        StandardEvent {
            timestamp: 1,
            source: source.to_owned(),
            market: market.to_owned(),
            event_type: kind.to_owned(),
            data,
            extra: Default::default(),
        }
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
        assert_eq!(snapshot.data["bids"][0]["price"], 100.0);

        let updated = builder
            .apply(event(
                "orderbook_delta",
                "lighter",
                "BTC-USD",
                json!({"bids": [[100, 0], [98, 4]], "asks": [[101, 5]]}),
            ))
            .unwrap()
            .unwrap();
        assert_eq!(updated.event_type, "orderbook");
        assert_eq!(
            updated.data["bids"],
            json!([{"price": 99.0, "quantity": 1.0}, {"price": 98.0, "quantity": 4.0}])
        );
        assert_eq!(
            updated.data["asks"],
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
            replaced.data["bids"],
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
        assert_eq!(result.data["bids"][0]["price"], 1.0);
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
