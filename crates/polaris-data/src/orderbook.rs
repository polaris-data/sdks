use std::collections::HashMap;

use serde_json::{Map, Value, json};

use crate::{OrderbookLevel, PolarisError, StandardEvent};

const SNAPSHOT_TYPES: [&str; 3] = ["orderbook", "orderbook_snapshot", "l2_snapshot"];
const DELTA_TYPE: &str = "orderbook_delta";

#[derive(Clone, Debug, Default)]
struct BookState {
    bids: Vec<OrderbookLevel>,
    asks: Vec<OrderbookLevel>,
}

/// Reconstruct complete order books from standardized snapshot and delta events.
///
/// State is isolated by `(source, market)`. Deltas received before a snapshot are
/// refused by returning `Ok(None)`. Non-orderbook events pass through unchanged.
#[derive(Clone, Debug, Default)]
pub struct OrderbookBuilder {
    books: HashMap<(String, String), BookState>,
}

impl OrderbookBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.books.clear();
    }

    pub fn clear_book(&mut self, source: &str, market: &str) {
        self.books.remove(&(source.to_owned(), market.to_owned()));
    }

    pub fn apply(
        &mut self,
        mut event: StandardEvent,
    ) -> Result<Option<StandardEvent>, PolarisError> {
        let is_snapshot = SNAPSHOT_TYPES.contains(&event.event_type.as_str());
        let is_delta = event.event_type == DELTA_TYPE;
        if !is_snapshot && !is_delta {
            return Ok(Some(event));
        }

        let mut data = match &event.data {
            Value::Object(object) => object.clone(),
            _ if event.extra.contains_key("bids") || event.extra.contains_key("asks") => Map::new(),
            _ => {
                return Err(PolarisError::Decode(
                    "invalid orderbook payload: data must be an object".to_owned(),
                ));
            }
        };
        copy_envelope_side(&event, &mut data, "bids");
        copy_envelope_side(&event, &mut data, "asks");

        let bids = parse_side(data.get("bids"), is_snapshot, "bids")?;
        let asks = parse_side(data.get("asks"), is_snapshot, "asks")?;
        let key = (event.source.clone(), event.market.clone());

        if is_snapshot {
            let mut state = BookState::default();
            apply_levels(&mut state.bids, bids.expect("snapshot bids validated"));
            apply_levels(&mut state.asks, asks.expect("snapshot asks validated"));
            self.books.insert(key.clone(), state);
        } else {
            let Some(state) = self.books.get_mut(&key) else {
                return Ok(None);
            };
            if let Some(levels) = bids {
                apply_levels(&mut state.bids, levels);
            }
            if let Some(levels) = asks {
                apply_levels(&mut state.asks, levels);
            }
        }

        let state = self.books.get(&key).expect("book state initialized");
        let mut bids = state.bids.clone();
        let mut asks = state.asks.clone();
        bids.sort_by(|left, right| right.price.total_cmp(&left.price));
        asks.sort_by(|left, right| left.price.total_cmp(&right.price));

        data.insert("bids".to_owned(), canonical_levels(&bids));
        data.insert("asks".to_owned(), canonical_levels(&asks));
        event.extra.remove("bids");
        event.extra.remove("asks");
        event.event_type = "orderbook".to_owned();
        event.data = Value::Object(data);
        Ok(Some(event))
    }
}

fn copy_envelope_side(event: &StandardEvent, data: &mut Map<String, Value>, side: &str) {
    if !data.contains_key(side) {
        if let Some(value) = event.extra.get(side) {
            data.insert(side.to_owned(), value.clone());
        }
    }
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

fn apply_levels(book: &mut Vec<OrderbookLevel>, updates: Vec<OrderbookLevel>) {
    for update in updates {
        if let Some(index) = book.iter().position(|level| level.price == update.price) {
            if update.quantity == 0.0 {
                book.remove(index);
            } else {
                book[index] = update;
            }
        } else if update.quantity > 0.0 {
            book.push(update);
        }
    }
}

fn canonical_levels(levels: &[OrderbookLevel]) -> Value {
    Value::Array(
        levels
            .iter()
            .map(|level| json!({"price": level.price, "quantity": level.quantity}))
            .collect(),
    )
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
}
