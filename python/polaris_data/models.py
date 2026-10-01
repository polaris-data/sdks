"""Typed response structures returned by Polaris endpoints."""

from __future__ import annotations

from typing import Any, Literal, Optional, TypedDict, Union

JSONDict = dict[str, Any]
CatalogInstrumentValue = Optional[Union[str, int, float]]

AmountKind = Literal["exact_input", "exact_output"]
IntentStatus = Literal[
    "submitted",
    "open",
    "partially_filled",
    "executing",
    "filled",
    "settled",
    "cancelled",
    "expired",
    "failed",
    "unknown",
]


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


class MetaResponse(TypedDict):
    name: str
    docs: str
    llms: str
    openapi: str
    skill: str
    health: str
    stream: str


class ObservedStatistic(TypedDict):
    value: str
    observed_at: int
    exchange_timestamp: int | None
    unit: str | None
    convention: str | None


class OptionContractStatistics(TypedDict):
    source: str
    market: str
    instrument: str | None
    fields: dict[str, ObservedStatistic]


class OptionContract(TypedDict):
    source: str
    market: str
    instrument: str
    status: str
    option_type: str
    underlying: str
    strike: str
    expiry_timestamp: int
    contract_size: str | None
    exercise_style: str | None
    premium_currency: str | None
    quantity_unit: str | None
    settlement_currency: str | None
    statistics: OptionContractStatistics | None


class InstrumentsResponse(TypedDict):
    updatedAt: str
    instruments: list[OptionContract]


class CatalogCount(TypedDict):
    updatedAt: str
    sources: int
    markets: int
    by_source: dict[str, int]


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


class _LegacyTradeDataRequired(TypedDict):
    price: float
    quantity: float
    side: str


class LegacyTradeData(_LegacyTradeDataRequired, total=False):
    maker: str
    taker: str


class _TradeDataV2Required(TypedDict):
    order_id: Optional[str]
    price: float
    quantity: float
    side: Optional[Literal["buy", "sell"]]


class TradeDataV2(_TradeDataV2Required, total=False):
    maker: str
    taker: str


class LegacyTradeEvent(TypedDict):
    timestamp: int
    source: str
    market: str
    type: Literal["trade"]
    data: LegacyTradeData


class TradeEventV2(TypedDict):
    collector_timestamp: int
    collector_sequence: int
    exchange_timestamp: Optional[int]
    exchange_sequence: Optional[str]
    source: str
    market: str
    type: Literal["trade"]
    data: TradeDataV2


TradeEvent = Union[LegacyTradeEvent, TradeEventV2]


class _TradeRowRequired(TypedDict):
    event_id: str
    source: str
    market: str
    collector_timestamp: int
    source_capture_id: str
    schema_version: int
    price: float
    quantity: float


class TradeRow(_TradeRowRequired, total=False):
    exchange_timestamp: Optional[int]
    instrument: Optional[str]
    liquidation: Optional[bool]
    maker: Optional[str]
    order_id: Optional[str]
    side: Optional[str]
    taker: Optional[str]


class OptionGreeks(TypedDict, total=False):
    delta: str
    gamma: str
    vega: str
    theta: str
    rho: str


class OptionTickerData(TypedDict, total=False):
    underlying: str
    strike: str
    expiry_timestamp: int
    option_type: Literal["call", "put"]
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


class _OptionTickerRowRequired(TypedDict):
    event_id: str
    source: str
    market: str
    instrument: str
    collector_timestamp: int
    source_capture_id: str
    schema_version: int


class OptionTickerRow(_OptionTickerRowRequired, total=False):
    exchange_timestamp: Optional[int]
    expiry_timestamp: Optional[int]
    ask_iv: Optional[str]
    ask_price: Optional[str]
    ask_size: Optional[str]
    bid_iv: Optional[str]
    bid_price: Optional[str]
    bid_size: Optional[str]
    delta: Optional[str]
    forward_price: Optional[str]
    gamma: Optional[str]
    index_price: Optional[str]
    last_price: Optional[str]
    mark_iv: Optional[str]
    mark_price: Optional[str]
    open_interest: Optional[str]
    option_type: Optional[str]
    premium_currency: Optional[str]
    quantity_unit: Optional[str]
    rho: Optional[str]
    strike: Optional[str]
    theta: Optional[str]
    turnover_24h: Optional[str]
    underlying: Optional[str]
    underlying_price: Optional[str]
    vega: Optional[str]
    volume_24h: Optional[str]


class _FundingRateRowRequired(TypedDict):
    event_id: str
    source: str
    market: str
    collector_timestamp: int
    source_capture_id: str
    schema_version: int


class FundingRateRow(_FundingRateRowRequired, total=False):
    exchange_timestamp: Optional[int]
    funding_timestamp: Optional[int]
    instrument: Optional[str]
    funding_rate: Optional[str]
    index_price: Optional[str]
    mark_price: Optional[str]
    open_interest: Optional[str]
    predicted_funding_rate: Optional[str]
    premium: Optional[str]


class _OhlcvRowRequired(TypedDict):
    event_id: str
    source: str
    market: str
    collector_timestamp: int
    source_capture_id: str
    schema_version: int
    interval: str
    open_timestamp: int
    open: float
    high: float
    low: float
    close: float


class OhlcvRow(_OhlcvRowRequired, total=False):
    exchange_timestamp: Optional[int]
    instrument: Optional[str]
    close_timestamp: Optional[int]
    base_volume: Optional[float]
    quote_volume: Optional[float]
    trade_count: Optional[int]
    is_closed: Optional[bool]


MixedEventType = Literal[
    "trade", "l2_update", "funding_rate", "intent", "quote", "option_ticker", "ohlcv"
]


class _OrderbookL2RowRequired(TypedDict):
    event_id: str
    source: str
    market: str
    instrument: Optional[str]
    collector_timestamp: int
    exchange_timestamp: Optional[int]
    source_capture_id: str
    schema_version: int
    source_event_is_snapshot: bool


class OrderbookL2Row(_OrderbookL2RowRequired):
    """Flat top-25 source update or reconstructed book."""
    bid_px_00: Optional[float]
    bid_sz_00: Optional[float]
    ask_px_00: Optional[float]
    ask_sz_00: Optional[float]
    bid_px_01: Optional[float]
    bid_sz_01: Optional[float]
    ask_px_01: Optional[float]
    ask_sz_01: Optional[float]
    bid_px_02: Optional[float]
    bid_sz_02: Optional[float]
    ask_px_02: Optional[float]
    ask_sz_02: Optional[float]
    bid_px_03: Optional[float]
    bid_sz_03: Optional[float]
    ask_px_03: Optional[float]
    ask_sz_03: Optional[float]
    bid_px_04: Optional[float]
    bid_sz_04: Optional[float]
    ask_px_04: Optional[float]
    ask_sz_04: Optional[float]
    bid_px_05: Optional[float]
    bid_sz_05: Optional[float]
    ask_px_05: Optional[float]
    ask_sz_05: Optional[float]
    bid_px_06: Optional[float]
    bid_sz_06: Optional[float]
    ask_px_06: Optional[float]
    ask_sz_06: Optional[float]
    bid_px_07: Optional[float]
    bid_sz_07: Optional[float]
    ask_px_07: Optional[float]
    ask_sz_07: Optional[float]
    bid_px_08: Optional[float]
    bid_sz_08: Optional[float]
    ask_px_08: Optional[float]
    ask_sz_08: Optional[float]
    bid_px_09: Optional[float]
    bid_sz_09: Optional[float]
    ask_px_09: Optional[float]
    ask_sz_09: Optional[float]
    bid_px_10: Optional[float]
    bid_sz_10: Optional[float]
    ask_px_10: Optional[float]
    ask_sz_10: Optional[float]
    bid_px_11: Optional[float]
    bid_sz_11: Optional[float]
    ask_px_11: Optional[float]
    ask_sz_11: Optional[float]
    bid_px_12: Optional[float]
    bid_sz_12: Optional[float]
    ask_px_12: Optional[float]
    ask_sz_12: Optional[float]
    bid_px_13: Optional[float]
    bid_sz_13: Optional[float]
    ask_px_13: Optional[float]
    ask_sz_13: Optional[float]
    bid_px_14: Optional[float]
    bid_sz_14: Optional[float]
    ask_px_14: Optional[float]
    ask_sz_14: Optional[float]
    bid_px_15: Optional[float]
    bid_sz_15: Optional[float]
    ask_px_15: Optional[float]
    ask_sz_15: Optional[float]
    bid_px_16: Optional[float]
    bid_sz_16: Optional[float]
    ask_px_16: Optional[float]
    ask_sz_16: Optional[float]
    bid_px_17: Optional[float]
    bid_sz_17: Optional[float]
    ask_px_17: Optional[float]
    ask_sz_17: Optional[float]
    bid_px_18: Optional[float]
    bid_sz_18: Optional[float]
    ask_px_18: Optional[float]
    ask_sz_18: Optional[float]
    bid_px_19: Optional[float]
    bid_sz_19: Optional[float]
    ask_px_19: Optional[float]
    ask_sz_19: Optional[float]
    bid_px_20: Optional[float]
    bid_sz_20: Optional[float]
    ask_px_20: Optional[float]
    ask_sz_20: Optional[float]
    bid_px_21: Optional[float]
    bid_sz_21: Optional[float]
    ask_px_21: Optional[float]
    ask_sz_21: Optional[float]
    bid_px_22: Optional[float]
    bid_sz_22: Optional[float]
    ask_px_22: Optional[float]
    ask_sz_22: Optional[float]
    bid_px_23: Optional[float]
    bid_sz_23: Optional[float]
    ask_px_23: Optional[float]
    ask_sz_23: Optional[float]
    bid_px_24: Optional[float]
    bid_sz_24: Optional[float]
    ask_px_24: Optional[float]
    ask_sz_24: Optional[float]


class _IntentRowRequired(TypedDict):
    event_id: str
    source: str
    market: str
    collector_timestamp: int
    source_capture_id: str
    schema_version: int


class IntentRow(_IntentRowRequired, total=False):
    exchange_timestamp: Optional[int]
    instrument: Optional[str]
    amount_kind: Optional[str]
    expires_at: Optional[int]
    input_amount: Optional[str]
    input_asset_id: Optional[str]
    input_chain_id: Optional[str]
    intent_id: Optional[str]
    output_amount: Optional[str]
    output_asset_id: Optional[str]
    output_chain_id: Optional[str]
    quote_id: Optional[str]
    quoted_input_amount: Optional[str]
    quoted_output_amount: Optional[str]
    rfq_id: Optional[str]
    settled_at: Optional[int]
    status: Optional[str]


class _QuoteRowRequired(TypedDict):
    event_id: str
    source: str
    market: str
    instrument: str
    collector_timestamp: int
    source_capture_id: str
    schema_version: int
    observation_id: str
    input_asset_id: str
    input_chain_id: str
    input_amount: str
    input_decimals: int
    output_asset_id: str
    output_chain_id: str
    output_amount: str
    output_decimals: int
    amount_kind: str
    block_number: int
    block_hash: str
    transaction_hash: str
    transaction_index: int
    router: str


class QuoteRow(_QuoteRowRequired, total=False):
    exchange_timestamp: Optional[int]
    oracle: Optional[str]
    pool: Optional[str]


class _TradeMixedEvent(TypedDict):
    type: Literal["trade"]
    data: TradeRow


class _L2UpdateMixedEvent(TypedDict):
    type: Literal["l2_update"]
    data: OrderbookL2Row


class _FundingRateMixedEvent(TypedDict):
    type: Literal["funding_rate"]
    data: FundingRateRow


class _IntentMixedEvent(TypedDict):
    type: Literal["intent"]
    data: IntentRow


class _QuoteMixedEvent(TypedDict):
    type: Literal["quote"]
    data: QuoteRow


class _OptionTickerMixedEvent(TypedDict):
    type: Literal["option_ticker"]
    data: OptionTickerRow


class _OhlcvMixedEvent(TypedDict):
    type: Literal["ohlcv"]
    data: OhlcvRow


MixedEventRow = Union[
    _TradeMixedEvent, _L2UpdateMixedEvent, _FundingRateMixedEvent,
    _IntentMixedEvent, _QuoteMixedEvent, _OptionTickerMixedEvent,
    _OhlcvMixedEvent,
]


class RawCaptureRow(TypedDict):
    raw_table: str
    capture_id: str
    collector_timestamp: int
    recorder_version: str
    ingested_at: int
    additional_context: Any
    original_json: str


class PerpetualTickerData(TypedDict, total=False):
    last_price: str
    mark_price: str
    index_price: str
    oracle_price: str
    mid_price: str
    open_interest: str
    funding_rate: str
    funding_timestamp: int
    predicted_funding_rate: str
    premium: str


class LegacyPerpetualTickerEvent(TypedDict):
    timestamp: int
    source: str
    market: str
    type: Literal["perpetual_ticker"]
    data: PerpetualTickerData


class PerpetualTickerEventV2(TypedDict):
    collector_timestamp: int
    collector_sequence: int
    exchange_timestamp: Optional[int]
    exchange_sequence: Optional[str]
    source: str
    market: str
    type: Literal["perpetual_ticker"]
    data: PerpetualTickerData


PerpetualTickerEvent = Union[LegacyPerpetualTickerEvent, PerpetualTickerEventV2]


class _AssetAmountRequired(TypedDict):
    asset_id: str


class AssetAmount(_AssetAmountRequired, total=False):
    chain_id: str
    amount: str
    recipient: str


class IntentQuote(TypedDict):
    quote_id: str
    response: list[AssetAmount]


class _SettlementTransactionRequired(TypedDict):
    transaction_hash: str


class SettlementTransaction(_SettlementTransactionRequired, total=False):
    chain_id: str


class _IntentDataRequired(TypedDict):
    inputs: list[AssetAmount]
    outputs: list[AssetAmount]
    transactions: list[SettlementTransaction]


class IntentData(_IntentDataRequired, total=False):
    rfq_id: str
    intent_id: str
    requester: str
    signer: str
    amount_kind: AmountKind
    expires_at: int
    quote: IntentQuote
    status: IntentStatus
    settled_at: int


class _LegacyIntentEventRequired(TypedDict):
    timestamp: int
    source: str
    market: str
    type: Literal["intent"]
    data: IntentData


class LegacyIntentEvent(_LegacyIntentEventRequired, total=False):
    raw: Any


class _IntentEventV2Required(TypedDict):
    collector_timestamp: int
    collector_sequence: int
    exchange_timestamp: Optional[int]
    exchange_sequence: Optional[str]
    source: str
    market: str
    type: Literal["intent"]
    data: IntentData


class IntentEventV2(_IntentEventV2Required, total=False):
    raw: Any


IntentEvent = Union[LegacyIntentEvent, IntentEventV2]
