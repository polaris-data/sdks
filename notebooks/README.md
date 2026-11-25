# Polaris Data Example Notebooks

These six Jupyter notebooks demonstrate market data analyses using the Polaris Python SDK. Each notebook includes saved outputs and charts from its original run.

## Quickstart

From the SDK repository root, run the following commands with `uv` installed. The notebook project creates its own `notebooks/.venv` with `polaris-data[dataframe]==0.15.0`, including Pandas and PyArrow support.

```bash
cd notebooks
make install
make notebook
```

Open the JupyterLab URL printed in the terminal and select a notebook to browse its saved results. You can also view the notebooks directly on GitHub.

## Live data

All six notebooks call the legacy `/catalog` route, which returns HTTP 404 on the current production API. Running the analysis cells requires a backend compatible with SDK 0.15.0. [Production API reference](https://api.polaris.supply/openapi.json)

The original runs used public catalog windows. The shared helper, `_polaris.py`, selects those windows, caps historical iterators, and normalizes event timestamps.

## Notebook Overview

### [Hyperliquid BTC trades and OHLCV](hyperliquid_btc_trade_analysis.ipynb)

Uses a short Hyperliquid BTC window to connect one-minute OHLCV bars with typed trades, buy/sell volume, notional, and signed execution flow.

### [Hyperliquid L4 post-trade analysis](hyperliquid_l4_post_trade_analysis.ipynb)

Builds participant-attributed post-trade analytics from raw Hyperliquid BTC events, including buyer/seller wallets, aggressor and passive roles, counterparties, execution cost, and forward midpoint markouts.

### [Lighter AAPL schema tour](lighter_aapl_standardized_schema_tour.ipynb)

Follows a Lighter AAPL perpetual through mixed events, raw and reconstructed L2 books, BBO, depth and slippage, funding and mark-price coverage, and bucketed volume, VWAP, and volatility.

### [UniswapX intents and RFQs](uniswapx_intents_and_rfqs_analysis.ipynb)

Contrasts canonical UniswapX intent fields with venue-native payloads, measures field and status coverage, correlates lifecycle observations, and analyzes canonical asset-flow activity. RFQ and quote absence is reported explicitly when the selected public sample contains only executable intents.

### [Aevo BTC options surface](aevo_btc_options_surface_analysis.ipynb)

Reconstructs the latest state of an Aevo BTC option chain, demonstrates exact-contract filtering, and analyzes moneyness, expiries, implied-volatility smiles, Greeks, and open interest.

### [PropAMM quote ladder analysis](propamm_quote_ladder_analysis.ipynb)

Checks all six documented PropAMM sources, selects a comparable directed token pair, preserves decimal precision, and visualizes normalized quote curves and within-source price impact.

## Schema Coverage

| Polaris schema | Notebook |
| --- | --- |
| [Events](https://docs.polaris.supply/schemas/events) | Lighter AAPL schema tour; Hyperliquid L4 post-trade analysis |
| [Trades](https://docs.polaris.supply/schemas/trades) | Hyperliquid BTC trade analysis |
| [Intents and RFQs](https://docs.polaris.supply/schemas/intents-and-rfqs) | UniswapX intents and RFQs |
| [Option tickers](https://docs.polaris.supply/schemas/option-tickers) | Aevo BTC options surface |
| [L2 snapshots and updates](https://docs.polaris.supply/schemas/l2-snapshots) | Lighter AAPL schema tour |
| [BBO](https://docs.polaris.supply/schemas/bbo) | Lighter AAPL schema tour |
| [Depth metrics](https://docs.polaris.supply/schemas/depth-metrics) | Lighter AAPL schema tour |
| [Funding rates](https://docs.polaris.supply/schemas/funding-rates) | Lighter AAPL schema tour |
| [Mark prices](https://docs.polaris.supply/schemas/mark-prices) | Lighter AAPL schema tour |
| [OHLCV](https://docs.polaris.supply/schemas/ohlcv) | Hyperliquid BTC trade analysis |
| [Volume](https://docs.polaris.supply/schemas/volume) | Lighter AAPL schema tour |
| [VWAP](https://docs.polaris.supply/schemas/vwap) | Lighter AAPL schema tour |
| [Volatility](https://docs.polaris.supply/schemas/volatility) | Lighter AAPL schema tour |
| [PropAMM quote ladders](https://docs.polaris.supply/schemas/propamm-quote-ladders) | PropAMM quote ladder analysis |
