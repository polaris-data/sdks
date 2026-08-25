"""Typed response structures returned by Polaris endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional, TypedDict, Union

JSONDict = dict[str, Any]
CatalogInstrumentValue = Optional[Union[str, int, float]]


class CatalogInstrument(TypedDict):
    base: str | None
    quote: str | None
    tick_size: CatalogInstrumentValue
    lot_size: CatalogInstrumentValue
    min_notional: CatalogInstrumentValue


class CatalogAccess(TypedDict):
    status: str
    public_cutoff_date: str | None


class CatalogMarketEntry(TypedDict):
    source: str
    market: str
    symbol: str
    start: str
    end: str
    source_type: str
    categories: list[str]
    access: CatalogAccess
    instrument: CatalogInstrument


class CatalogResponse(TypedDict):
    markets: list[CatalogMarketEntry]
    updatedAt: str


class CatalogCount(TypedDict):
    updatedAt: str
    sources: int
    markets: int
    by_source: dict[str, int]


@dataclass(frozen=True)
class SnapshotEntry:
    """Remote standardized snapshot metadata."""

    key: str
    source: str | None = None
    market: str | None = None
    date: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    hour: int | None = None
class PropammQuote(TypedDict):
    amount_in: str
    amount_out: str


class _PropammQuoteLadderValuesRequired(TypedDict):
    event_id: str
    chain_id: int
    block_number: int
    block_hash: str
    parent_hash: str
    transaction_hash: str
    transaction_index: int
    router: str
    oracle: Optional[str]
    token_in: str
    token_out: str
    token_in_decimals: int
    token_out_decimals: int
    quotes: list[PropammQuote]


class PropammQuoteLadderValues(_PropammQuoteLadderValuesRequired, total=False):
    pool: str


class PropammQuoteLadderData(TypedDict):
    series: Literal["quote_ladder"]
    values: PropammQuoteLadderValues


class PropammQuoteLadderEvent(TypedDict):
    collector_timestamp: int
    collector_sequence: int
    exchange_timestamp: Optional[int]
    exchange_sequence: Optional[str]
    source: str
    market: str
    type: Literal["record"]
    data: PropammQuoteLadderData


class OptionGreeks(TypedDict, total=False):
    delta: str
    gamma: str
    vega: str
    theta: str
    rho: str


class OptionTickerData(TypedDict, total=False):
    mark_price: str
    bid_price: str
    bid_size: str
    ask_price: str
    ask_size: str
    last_price: str
    index_price: str
    underlying_price: str
    forward_price: str
    mark_iv: str
    bid_iv: str
    ask_iv: str
    open_interest: str
    volume_24h: str
    turnover_24h: str
    premium_currency: str
    quantity_unit: str
    greeks: OptionGreeks


class LegacyOptionTickerEvent(TypedDict):
    timestamp: int
    source: str
    market: str
    instrument: str
    type: Literal["option_ticker"]
    data: OptionTickerData


class OptionTickerEventV2(TypedDict):
    collector_timestamp: int
    collector_sequence: int
    exchange_timestamp: Optional[int]
    exchange_sequence: Optional[str]
    source: str
    market: str
    instrument: str
    type: Literal["option_ticker"]
    data: OptionTickerData


OptionTickerEvent = Union[LegacyOptionTickerEvent, OptionTickerEventV2]
