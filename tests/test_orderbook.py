from __future__ import annotations

from polaris_data import OrderbookBuilder


def _event(kind: str, market: str, data: dict[str, object]) -> dict[str, object]:
    return {
        "timestamp": 1,
        "source": "lighter",
        "market": market,
        "type": kind,
        "data": data,
    }


def test_orderbook_builder_snapshot_delta_deletion_and_reset() -> None:
    builder = OrderbookBuilder()
    assert builder.apply(
        _event("orderbook_delta", "BTC-USD", {"bids": [[100, 1]]})
    ) is None

    snapshot = builder.apply(
        _event(
            "orderbook",
            "BTC-USD",
            {
                "bids": [[99, 1], [100, 2]],
                "asks": [{"price": "101", "size": "3"}],
            },
        )
    )
    assert snapshot is not None
    assert snapshot["data"]["bids"] == [
        {"price": 100.0, "quantity": 2.0},
        {"price": 99.0, "quantity": 1.0},
    ]

    updated = builder.apply(
        _event(
            "orderbook_delta",
            "BTC-USD",
            {"bids": [[100, 0], [98, 4]], "asks": [[101, 5]]},
        )
    )
    assert updated is not None
    assert updated["type"] == "orderbook"
    assert updated["data"] == {
        "bids": [
            {"price": 99.0, "quantity": 1.0},
            {"price": 98.0, "quantity": 4.0},
        ],
        "asks": [{"price": 101.0, "quantity": 5.0}],
    }

    replaced = builder.apply(
        _event(
            "orderbook",
            "BTC-USD",
            {"bids": [[97, 6]], "asks": [[103, 7]]},
        )
    )
    assert replaced is not None
    assert replaced["data"]["bids"] == [{"price": 97.0, "quantity": 6.0}]

    builder.apply(
        _event("orderbook", "ETH-USD", {"bids": [[10, 1]], "asks": [[11, 1]]})
    )
    builder.clear_book("lighter", "BTC-USD")
    assert builder.apply(
        _event("orderbook_delta", "BTC-USD", {"asks": [[102, 1]]})
    ) is None
    assert builder.apply(
        _event("orderbook_delta", "ETH-USD", {"bids": [[10, 2]]})
    ) is not None

    builder.clear()
    assert builder.apply(
        _event("orderbook_delta", "ETH-USD", {"bids": [[10, 3]]})
    ) is None


def test_orderbook_builder_passes_non_orderbook_events_through() -> None:
    builder = OrderbookBuilder()
    trade = _event("trade", "BTC-USD", {"price": 100, "quantity": 1})
    assert builder.apply(trade) == trade
