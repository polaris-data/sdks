use std::time::Duration;

use futures_util::StreamExt;
use polaris_data::{
    CatalogQuery, EventsQuery, HistoricalRowsQuery, HistoricalStream, InstrumentsQuery,
    IntentRowsQuery, L2OrderbooksQuery, L2UpdatesQuery, MixedEventRow, MixedEventType,
    OhlcvRowsQuery, OptionTickerRowsQuery, PolarisClient, PolarisError, RawChannelQuery, RawQuery,
    TimeInput, blocking,
};
use serde_json::json;
use tempfile::TempDir;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

fn build_client(server: &MockServer, root: &TempDir) -> PolarisClient {
    PolarisClient::builder()
        .base_url(server.uri())
        .dataset_root(root.path())
        .timeout(Duration::from_secs(5))
        .build()
        .expect("client")
}

async fn collect_stream<T>(stream: HistoricalStream<T>) -> Result<Vec<T>, PolarisError> {
    stream.collect::<Vec<_>>().await.into_iter().collect()
}

#[tokio::test]
async fn builder_creates_layout_and_uses_explicit_root() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);

    assert_eq!(client.dataset_root(), root.path());
    assert!(client.dataset_root().join("data").exists());
    assert!(client.dataset_root().join("tmp").exists());
    assert!(client.cache_dir().exists());
}

#[tokio::test]
async fn builder_uses_environment_root_override() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let env_root = root.path().join("env-root");

    // SAFETY: the test sets and removes a process env var within a single test scope.
    unsafe { std::env::set_var("POLARIS_ROOT", &env_root) };
    let client = PolarisClient::builder()
        .base_url(server.uri())
        .timeout(Duration::from_secs(5))
        .build()
        .expect("client");
    // SAFETY: see note above.
    unsafe { std::env::remove_var("POLARIS_ROOT") };

    assert_eq!(client.dataset_root(), env_root.as_path());
    assert!(client.dataset_root().join("data").exists());
}

#[tokio::test]
async fn catalog_normalizes_flat_shape() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);

    Mock::given(method("GET"))
        .and(path("/catalog"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "updatedAt": "2024-01-15T00:00:00Z",
            "markets": [{
                "source": "binance",
                "market": "BTC-USDT",
                "symbol": "BTCUSDT",
                "start": "2024-01-01T00:00:00Z",
                "end": "2024-01-15T00:00:00Z",
                "source_type": "exchange",
                "categories": ["spot"],
                "access": {"status": "open"},
                "instrument": {
                    "base": "BTC",
                    "quote": "USDT",
                    "tick_size": "0.1",
                    "lot_size": 0.001,
                    "min_notional": "10"
                }
            }]
        })))
        .mount(&server)
        .await;

    let response = client
        .catalog(CatalogQuery {
            source: Some("binance".to_owned()),
            market: Some("BTC-USDT".to_owned()),
            q: None,
        })
        .await
        .expect("catalog");

    assert_eq!(response.updated_at, "2024-01-15T00:00:00Z");
    assert_eq!(response.markets.len(), 1);
    assert_eq!(response.markets[0].source, "binance");
    assert_eq!(response.markets[0].market, "BTC-USDT");
    assert_eq!(response.markets[0].symbol, "BTCUSDT");
    assert_eq!(response.markets[0].instrument.base.as_deref(), Some("BTC"));
    assert_eq!(
        response.markets[0].instrument.quote.as_deref(),
        Some("USDT")
    );
    assert_eq!(
        response.markets[0].instrument.tick_size.as_deref(),
        Some("0.1")
    );
    assert_eq!(
        response.markets[0].instrument.lot_size.as_deref(),
        Some("0.001")
    );
    assert_eq!(
        response.markets[0].instrument.min_notional.as_deref(),
        Some("10")
    );
    assert_eq!(
        response.markets[0].access.as_ref().expect("access").status,
        "open"
    );
}

#[tokio::test]
async fn catalog_normalizes_legacy_shape() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);

    Mock::given(method("GET"))
        .and(path("/catalog"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "updatedAt": "2024-01-15T00:00:00Z",
            "sources": [{
                "id": "binance",
                "markets": [{
                    "id": "BTC-USDT",
                    "start": "2024-01-01T00:00:00Z",
                    "end": "2024-01-15T00:00:00Z",
                    "source": "exchange"
                }]
            }]
        })))
        .mount(&server)
        .await;

    let response = client
        .catalog(CatalogQuery::default())
        .await
        .expect("catalog");

    assert_eq!(response.markets.len(), 1);
    assert_eq!(response.markets[0].source, "binance");
    assert_eq!(response.markets[0].market, "BTC-USDT");
    assert_eq!(response.markets[0].symbol, "BTC-USDT");
    assert_eq!(response.markets[0].source_type.as_deref(), Some("exchange"));
    assert_eq!(response.markets[0].instrument.base, None);
}

#[tokio::test]
async fn catalog_paginates_across_cursor_pages() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);

    Mock::given(method("GET"))
        .and(path("/catalog"))
        .respond_with(|request: &wiremock::Request| {
            let has_cursor = request.url.query_pairs().any(|(key, _)| key == "cursor");
            if has_cursor {
                ResponseTemplate::new(200).set_body_json(json!({
                    "updatedAt": "2024-01-15T00:00:00Z",
                    "total": 3,
                    "limit": 1000,
                    "has_more": false,
                    "next_cursor": null,
                    "markets": [{
                        "source": "hyperliquid",
                        "market": "BTC",
                        "symbol": "BTC"
                    }]
                }))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "updatedAt": "2024-01-15T00:00:00Z",
                    "total": 3,
                    "limit": 1000,
                    "has_more": true,
                    "next_cursor": "cursor-token",
                    "markets": [
                        {
                            "source": "binance",
                            "market": "BTC-USDT",
                            "symbol": "BTCUSDT"
                        },
                        {
                            "source": "binance",
                            "market": "ETH-USDT",
                            "symbol": "ETHUSDT"
                        }
                    ]
                }))
            }
        })
        .mount(&server)
        .await;

    let response = client
        .catalog(CatalogQuery::default())
        .await
        .expect("catalog");

    assert_eq!(response.updated_at, "2024-01-15T00:00:00Z");
    assert_eq!(response.markets.len(), 3);
    assert_eq!(response.markets[0].market, "BTC-USDT");
    assert_eq!(response.markets[1].market, "ETH-USDT");
    assert_eq!(response.markets[2].market, "BTC");
}

#[tokio::test]
async fn instruments_paginates_and_preserves_contract_statistics() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    Mock::given(method("GET"))
        .and(path("/catalog/instruments"))
        .and(query_param("source", "deribit"))
        .and(query_param("market", "BTC"))
        .and(query_param("option_type", "call"))
        .respond_with(|request: &wiremock::Request| {
            let params: std::collections::BTreeMap<_, _> =
                request.url.query_pairs().into_owned().collect();
            assert_eq!(
                params.get("expiry").map(String::as_str),
                Some("1790812800000")
            );
            assert_eq!(params.get("limit").map(String::as_str), Some("1000"));
            let exact = params.get("instrument").is_some();
            if exact {
                assert_eq!(
                    params.get("instrument").map(String::as_str),
                    Some("BTC-1OCT26-70000-C")
                );
                assert_eq!(params.get("q").map(String::as_str), Some("70000"));
            } else {
                assert!(params.get("q").is_none());
            }
            let later = params.get("cursor").is_some();
            ResponseTemplate::new(200).set_body_json(json!({
                "updatedAt": "2026-09-30T00:00:00Z",
                "instruments": [{
                    "source": "deribit", "market": "BTC",
                    "instrument": if later { "BTC-1OCT26-75000-C" } else { "BTC-1OCT26-70000-C" },
                    "status": "active", "option_type": "call", "underlying": "BTC",
                    "strike": if later { "75000.0" } else { "70000.0" },
                    "expiry_timestamp": 1790812800000_i64,
                    "statistics": if later { serde_json::Value::Null } else { json!({
                        "source": "deribit", "market": "BTC",
                        "fields": { "latest_price": {
                            "value": "0.025", "observed_at": 1790800000000_i64,
                            "unit": "BTC"
                        }}
                    }) }
                }],
                "next_cursor": if later || exact { None } else { Some("contract-cursor") }
            }))
        })
        .mount(&server)
        .await;

    let response = client
        .instruments(InstrumentsQuery {
            source: "deribit".to_owned(),
            market: "BTC".to_owned(),
            instrument: None,
            expiry: Some(1790812800000),
            option_type: Some("call".to_owned()),
            q: None,
        })
        .await
        .expect("instruments");
    assert_eq!(response.updated_at, "2026-09-30T00:00:00Z");
    assert_eq!(response.instruments.len(), 2);
    assert_eq!(response.instruments[0].strike, "70000.0");
    assert_eq!(response.instruments[1].strike, "75000.0");
    assert_eq!(
        response.instruments[0].statistics.as_ref().unwrap().fields["latest_price"].value,
        "0.025"
    );
    assert!(response.instruments[1].statistics.is_none());

    let exact = client
        .instruments(InstrumentsQuery {
            source: "deribit".to_owned(),
            market: "BTC".to_owned(),
            instrument: Some("BTC-1OCT26-70000-C".to_owned()),
            expiry: Some(1790812800000),
            option_type: Some("call".to_owned()),
            q: Some("70000".to_owned()),
        })
        .await
        .expect("exact instrument");
    assert_eq!(exact.instruments.len(), 1);
    assert_eq!(exact.instruments[0].instrument, "BTC-1OCT26-70000-C");
}

#[tokio::test]
async fn count_returns_catalog_counts() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);

    Mock::given(method("GET"))
        .and(path("/count"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "updatedAt": "2024-01-15T00:00:00Z",
            "sources": 3,
            "markets": 42,
            "by_source": {
                "binance": 10,
                "hyperliquid": 32
            }
        })))
        .mount(&server)
        .await;

    let count = client.count().await.expect("count");

    assert_eq!(count.updated_at, "2024-01-15T00:00:00Z");
    assert_eq!(count.sources, 3);
    assert_eq!(count.markets, 42);
    assert_eq!(count.by_source.get("binance"), Some(&10));
    assert_eq!(count.by_source.get("hyperliquid"), Some(&32));
}

#[tokio::test]
async fn option_tickers_are_typed_and_filter_exact_instruments() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    let query = |instrument: Option<&str>| OptionTickerRowsQuery {
        source: Some("deribit".to_owned()),
        market: Some("BTC".to_owned()),
        instrument: instrument.map(ToOwned::to_owned),
        start: Some(10),
        end: Some(10),
    };

    let row = json!({
        "event_id": "o1", "source": "deribit", "market": "BTC",
        "instrument": "BTC-29MAR24-50000-C", "collector_timestamp": 10,
        "source_capture_id": "capture", "schema_version": 1,
        "mark_iv": "0.8359", "delta": "0.431", "mark_price": null
    });
    Mock::given(method("GET"))
        .and(path("/options-ticker"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [row], "has_more": false, "next_cursor": null
        })))
        .mount(&server)
        .await;

    let chain = collect_stream(
        client
            .option_tickers(query(None))
            .await
            .expect("option chain"),
    )
    .await
    .expect("option rows");
    let exact = collect_stream(
        client
            .option_tickers(query(Some("BTC-29MAR24-50000-C")))
            .await
            .expect("exact option"),
    )
    .await
    .expect("exact option rows");

    assert_eq!(chain.len(), 1);
    assert_eq!(chain[0].source, "deribit");
    assert_eq!(chain[0].market, "BTC");
    assert_eq!(chain[0].instrument, "BTC-29MAR24-50000-C");
    assert_eq!(chain[0].mark_iv.as_deref(), Some("0.8359"));
    assert_eq!(chain[0].mark_price, None);
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].instrument, "BTC-29MAR24-50000-C");

    let error = match client.option_tickers(query(Some(""))).await {
        Ok(_) => panic!("expected empty instrument error"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("instrument must be non-empty"));

    let root_path = root.path().to_owned();
    let server_url = server.uri();
    let blocking_rows = std::thread::spawn(move || {
        let client = blocking::PolarisClient::builder()
            .base_url(server_url)
            .dataset_root(root_path)
            .build()
            .expect("blocking client");
        client
            .option_tickers(query(Some("BTC-29MAR24-50000-C")))
            .expect("blocking option tickers")
            .collect::<Result<Vec<_>, _>>()
    })
    .join()
    .expect("blocking thread")
    .expect("blocking rows");
    assert_eq!(blocking_rows.len(), 1);
    assert_eq!(blocking_rows[0].instrument, "BTC-29MAR24-50000-C");
    let requests = server.received_requests().await.expect("requests");
    assert!(requests.iter().any(|request| {
        request
            .url
            .query_pairs()
            .any(|(key, value)| key == "instrument" && value == "BTC-29MAR24-50000-C")
    }));
}

#[tokio::test]
async fn direct_trades_and_funding_paginate_and_keep_nullable_fields() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = PolarisClient::builder()
        .base_url(server.uri())
        .api_key("secret")
        .dataset_root(root.path())
        .build()
        .expect("client");
    Mock::given(method("GET"))
        .and(path("/trades"))
        .and(query_param("start", "10"))
        .and(query_param("end", "10"))
        .and(query_param("limit", "1000"))
        .and(header("authorization", "Bearer secret"))
        .respond_with(|request: &wiremock::Request| {
            let second = request
                .url
                .query_pairs()
                .any(|(key, value)| key == "cursor" && value == "next");
            ResponseTemplate::new(200).set_body_json(json!({
                "items": [{
                    "event_id": if second { "t2" } else { "t1" },
                    "source": "binance", "market": "BTC-USDT",
                    "collector_timestamp": 10, "source_capture_id": "capture",
                    "schema_version": 1, "price": 100.0, "quantity": 2.0,
                    "side": null
                }],
                "has_more": !second,
                "next_cursor": if second { None } else { Some("next") }
            }))
        })
        .mount(&server)
        .await;
    let funding_page = json!({
        "items": [{
            "event_id": "f1", "source": "binance", "market": "BTC-USDT",
            "collector_timestamp": 10, "source_capture_id": "capture",
            "schema_version": 1, "funding_rate": null, "mark_price": "100"
        }, {
            "event_id": "f2", "source": "binance", "market": "BTC-USDT",
            "collector_timestamp": 11, "source_capture_id": "capture",
            "schema_version": 1, "funding_rate": "0.0001", "mark_price": null
        }],
        "has_more": false, "next_cursor": null
    });
    for route in ["/funding-rates", "/perpetual-ticker"] {
        Mock::given(method("GET"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(200).set_body_json(funding_page.clone()))
            .mount(&server)
            .await;
    }

    let trades = collect_stream(
        client
            .trades(HistoricalRowsQuery {
                source: Some("binance".into()),
                market: Some("BTC-USDT".into()),
                start: Some(10),
                end: Some(10),
            })
            .await
            .expect("trades"),
    )
    .await
    .expect("trade rows");
    assert_eq!(
        trades
            .iter()
            .map(|row| row.event_id.as_str())
            .collect::<Vec<_>>(),
        ["t1", "t2"]
    );
    assert_eq!(trades[0].side, None);
    let funding = collect_stream(
        client
            .funding_rates(HistoricalRowsQuery::default())
            .await
            .expect("funding"),
    )
    .await
    .expect("funding rows");
    assert_eq!(funding[0].funding_rate, None);
    assert_eq!(funding[0].mark_price.as_deref(), Some("100"));
    let tickers = collect_stream(
        client
            .perpetual_tickers(HistoricalRowsQuery::default())
            .await
            .expect("tickers"),
    )
    .await
    .expect("ticker rows");
    assert_eq!(tickers, funding);
    let requests = server.received_requests().await.expect("requests");
    assert!(
        requests
            .iter()
            .any(|request| request.url.path() == "/perpetual-ticker")
    );
    let funding_request = requests
        .iter()
        .find(|request| request.url.path() == "/funding-rates")
        .unwrap();
    assert!(
        !funding_request
            .url
            .query_pairs()
            .any(|(key, _)| key == "start" || key == "end")
    );
}

#[tokio::test]
async fn raw_channel_paginates_exact_text_through_source_channel_filters() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = PolarisClient::builder()
        .base_url(server.uri())
        .api_key("secret")
        .dataset_root(root.path())
        .build()
        .expect("client");
    Mock::given(method("GET"))
        .and(path("/raw"))
        .and(query_param("source", "binance"))
        .and(query_param("channel", "trades"))
        .and(query_param("start", "1970-01-01T00:00:00.010Z"))
        .and(query_param("end", "1970-01-01T00:00:00.010Z"))
        .and(query_param("limit", "1000"))
        .and(header("authorization", "Bearer secret"))
        .respond_with(|request: &wiremock::Request| {
            let second = request
                .url
                .query_pairs()
                .any(|(key, value)| key == "cursor" && value == "next");
            ResponseTemplate::new(200).set_body_json(json!({
                "data": [{
                    "raw_table": "raw.binance_trades",
                    "capture_id": if second { "c2" } else { "c1" },
                    "collector_timestamp": 10,
                    "recorder_version": "v1",
                    "ingested_at": 11,
                    "additional_context": {"channel": "trades"},
                    "original_json": "{ \"price\": 1.0 }"
                }],
                "has_more": !second,
                "next_cursor": if second { None } else { Some("next") }
            }))
        })
        .mount(&server)
        .await;

    let query = RawChannelQuery {
        exchange: "binance".into(),
        event: "trades".into(),
        start: 10,
        end: 10,
    };
    let rows = collect_stream(
        client
            .raw_channel(query.clone())
            .await
            .expect("raw channel"),
    )
    .await
    .expect("rows");
    assert_eq!(
        rows.iter()
            .map(|row| row.capture_id.as_str())
            .collect::<Vec<_>>(),
        ["c1", "c2"]
    );
    assert_eq!(rows[0].original_json, "{ \"price\": 1.0 }");
    assert_eq!(rows[0].raw_table, "raw.binance_trades");
    assert_eq!(rows[0].additional_context["channel"], "trades");
    assert!(
        client
            .raw_channel(RawChannelQuery { start: 11, ..query })
            .await
            .is_err()
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn raw_queries_source_and_market_as_paged_json_without_api_key() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    Mock::given(method("GET"))
        .and(path("/raw"))
        .and(query_param("source", "binance"))
        .and(query_param("market", "BTC-USDT"))
        .and(query_param("start", "2024-01-01T00:00:00Z"))
        .and(query_param("end", "2024-01-01T01:00:00Z"))
        .and(query_param("limit", "1"))
        .respond_with(|request: &wiremock::Request| {
            assert!(request.headers.get("authorization").is_none());
            assert!(!request.url.query_pairs().any(|(key, _)| key == "format"));
            let second = request.url.query_pairs().any(|(key, _)| key == "cursor");
            ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"raw_table": "raw.binance_trades", "capture_id": if second { "c2" } else { "c1" }}],
                "has_more": !second,
                "next_cursor": if second { None } else { Some("next") }
            }))
        })
        .mount(&server)
        .await;
    let rows = client
        .raw(RawQuery {
            source: "binance".into(),
            market: "BTC-USDT".into(),
            from: Some(TimeInput::Iso8601("2024-01-01T00:00:00Z".into())),
            to: Some(TimeInput::Iso8601("2024-01-01T01:00:00Z".into())),
            limit: 1,
        })
        .await
        .expect("raw rows");
    assert_eq!(
        rows.iter()
            .map(|row| row["capture_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["c1", "c2"]
    );
}

#[tokio::test]
async fn intents_queries_direct_route_with_exact_filter() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    Mock::given(method("GET"))
        .and(path("/intents"))
        .and(query_param("intent_id", "intent-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"event_id": "i1", "source": "uniswapx", "market": "intents",
                "collector_timestamp": 10, "source_capture_id": "capture", "schema_version": 1,
                "intent_id": "intent-1", "input_asset_id": null}],
            "has_more": false, "next_cursor": null
        })))
        .mount(&server)
        .await;

    let rows = collect_stream(
        client
            .intents(IntentRowsQuery {
                intent_id: Some("intent-1".into()),
                ..Default::default()
            })
            .await
            .expect("intents"),
    )
    .await
    .expect("intent rows");
    assert_eq!(rows[0].input_asset_id, None);
    assert!(
        client
            .intents(IntentRowsQuery {
                intent_id: Some(" ".into()),
                ..Default::default()
            })
            .await
            .is_err()
    );
}

fn l2_row(snapshot: bool) -> serde_json::Value {
    let mut row = json!({
        "event_id": if snapshot { "snapshot" } else { "delta" },
        "source": "hyperliquid", "market": "0G", "instrument": null,
        "collector_timestamp": 10, "exchange_timestamp": null,
        "source_capture_id": "capture", "schema_version": 1,
        "source_event_is_snapshot": snapshot,
    });
    let object = row.as_object_mut().expect("row object");
    for index in 0..25 {
        for side in ["bid", "ask"] {
            for field in ["px", "sz"] {
                object.insert(
                    format!("{side}_{field}_{index:02}"),
                    serde_json::Value::Null,
                );
            }
        }
    }
    object.insert("bid_px_00".to_owned(), json!(100.0));
    object.insert("bid_sz_00".to_owned(), json!(2.0));
    row
}

#[tokio::test]
async fn l2_direct_rows_page_and_accept_variable_ranges() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    Mock::given(method("GET"))
        .and(path("/l2-updates"))
        .respond_with(|request: &wiremock::Request| {
            let second = request.url.query_pairs().any(|(key, _)| key == "cursor");
            ResponseTemplate::new(200).set_body_json(json!({
                "items": [l2_row(!second)], "has_more": !second,
                "next_cursor": if second { None } else { Some("next") }
            }))
        })
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/l2-orderbooks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [l2_row(false)], "has_more": false, "next_cursor": null
        })))
        .mount(&server)
        .await;
    let updates = collect_stream(
        client
            .l2_updates(L2UpdatesQuery {
                source: Some("hyperliquid".to_owned()),
                market: Some("0G".to_owned()),
                instrument: Some("0G".to_owned()),
                start: Some(10),
                end: Some(10),
            })
            .await
            .expect("updates"),
    )
    .await
    .expect("rows");
    assert_eq!(updates.len(), 2);
    assert!(updates[0].source_event_is_snapshot);
    assert!(!updates[1].source_event_is_snapshot);
    assert_eq!(updates[0].bid_px_00, Some(100.0));
    assert_eq!(updates[0].ask_px_24, None);
    let books = collect_stream(
        client
            .l2_snapshots(L2OrderbooksQuery {
                source: "hyperliquid".to_owned(),
                market: "0G".to_owned(),
                instrument: None,
                start: 10,
                end: 300_010,
            })
            .await
            .expect("books"),
    )
    .await
    .expect("rows");
    assert_eq!(books.len(), 1);
    assert!(!books[0].source_event_is_snapshot);
    let long = collect_stream(
        client
            .l2_snapshots(L2OrderbooksQuery {
                source: "hyperliquid".to_owned(),
                market: "0G".to_owned(),
                instrument: None,
                start: 10,
                end: 600_010,
            })
            .await
            .expect("long books"),
    )
    .await
    .expect("long rows");
    assert_eq!(long.len(), 1);
    let requests = server.received_requests().await.expect("requests");
    assert_eq!(requests.len(), 4);
    assert!(
        requests[0]
            .url
            .query_pairs()
            .any(|(key, value)| key == "instrument" && value == "0G")
    );
    assert!(
        requests[2]
            .url
            .query_pairs()
            .any(|(key, value)| key == "end" && value == "300010")
    );
    assert!(
        requests[3]
            .url
            .query_pairs()
            .any(|(key, value)| key == "end" && value == "600010")
    );
}

#[tokio::test]
async fn events_pages_typed_rows_with_required_auth_and_filters() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = PolarisClient::builder()
        .base_url(server.uri())
        .api_key("secret")
        .dataset_root(root.path())
        .build()
        .expect("client");
    Mock::given(method("GET")).and(path("/events"))
        .and(header("authorization", "Bearer secret"))
        .and(query_param("start", "10")).and(query_param("end", "10"))
        .and(query_param("types", "trade,funding_rate"))
        .and(query_param("source", "binance"))
        .and(query_param("market", "BTC-USDT"))
        .and(query_param("instrument", "BTCUSDT"))
        .respond_with(|request: &wiremock::Request| {
            let second = request.url.query_pairs().any(|(key, value)| key == "cursor" && value == "next");
            let item = if second {
                json!({"type": "funding_rate", "data": {"event_id": "f1", "source": "binance", "market": "BTC-USDT",
                    "collector_timestamp": 10, "source_capture_id": "capture", "schema_version": 1,
                    "funding_rate": null, "mark_price": "100"}})
            } else {
                json!({"type": "trade", "data": {"event_id": "t1", "source": "binance", "market": "BTC-USDT",
                    "collector_timestamp": 10, "source_capture_id": "capture", "schema_version": 1,
                    "price": 100.0, "quantity": 2.0}})
            };
            ResponseTemplate::new(200).set_body_json(json!({"items": [item], "has_more": !second,
                "next_cursor": if second { None } else { Some("next") }}))
        }).mount(&server).await;
    let query = EventsQuery {
        start: 10,
        end: 10,
        types: Some(vec![MixedEventType::Trade, MixedEventType::FundingRate]),
        source: Some("binance".into()),
        market: Some("BTC-USDT".into()),
        instrument: Some("BTCUSDT".into()),
    };
    let rows = collect_stream(client.events(query.clone()).await.expect("events"))
        .await
        .expect("rows");
    assert_eq!(rows.len(), 2);
    assert!(matches!(&rows[0], MixedEventRow::Trade(row) if row.event_id == "t1"));
    assert!(
        matches!(&rows[1], MixedEventRow::FundingRate(row) if row.funding_rate.is_none() && row.mark_price.as_deref() == Some("100"))
    );

    let anonymous = build_client(&server, &TempDir::new().expect("anonymous root"));
    let error = collect_stream(anonymous.events(query).await.expect("stream"))
        .await
        .expect_err("auth required");
    assert!(matches!(error, PolarisError::Unauthorized { .. }));
}

#[tokio::test]
async fn ohlcv_returns_every_direct_candle_revision() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    Mock::given(method("GET"))
        .and(path("/ohlcv"))
        .and(query_param("interval", "1m"))
        .and(query_param("instrument", "BTCUSDT"))
        .and(query_param("start", "1704067200000"))
        .and(query_param("end", "1704070800000"))
        .respond_with(|request: &wiremock::Request| {
            let second = request.url.query_pairs().any(|(key, value)| key == "cursor" && value == "next");
            ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"event_id": if second { "c2" } else { "c1" }, "source": "binance", "market": "BTC-USDT",
                "collector_timestamp": 1_704_067_240_000_i64, "source_capture_id": "capture",
                "schema_version": 1, "interval": "1m", "open_timestamp": 1_704_067_200_000_i64,
                "open": 100.0, "high": 101.0, "low": 100.0, "close": 101.0,
                "base_volume": 2.0, "trade_count": 2}],
            "has_more": !second, "next_cursor": if second { None } else { Some("next") }
        }))})
        .mount(&server)
        .await;

    let output = collect_stream(
        client
            .ohlcv(OhlcvRowsQuery {
                source: Some("binance".to_owned()),
                market: Some("BTC-USDT".to_owned()),
                instrument: Some("BTCUSDT".to_owned()),
                start: Some(1_704_067_200_000),
                end: Some(1_704_070_800_000),
                interval: Some("1m".to_owned()),
            })
            .await
            .expect("ohlcv"),
    )
    .await
    .expect("rows");
    assert_eq!(output.len(), 2);
    assert_eq!(output[0].event_id, "c1");
    assert_eq!(output[1].event_id, "c2");
    assert_eq!(output[0].base_volume, Some(2.0));
}

#[tokio::test]
async fn http_errors_are_mapped() {
    for (status, matcher) in [
        (401, "Unauthorized"),
        (402, "AccessDenied"),
        (404, "NotFound"),
        (429, "RateLimited"),
    ] {
        let server = MockServer::start().await;
        let root = TempDir::new().expect("tempdir");
        let client = build_client(&server, &root);

        Mock::given(method("GET"))
            .and(path("/catalog"))
            .respond_with(ResponseTemplate::new(status).set_body_json(json!({
                "error": "boom",
                "reset_at": "2024-01-01T00:00:00Z"
            })))
            .mount(&server)
            .await;

        let error = client
            .catalog(CatalogQuery::default())
            .await
            .expect_err("error");
        match (status, error) {
            (401, PolarisError::Unauthorized { .. }) => {}
            (402, PolarisError::AccessDenied { .. }) => {}
            (404, PolarisError::NotFound { .. }) => {}
            (429, PolarisError::RateLimited { .. }) => {}
            (_, other) => panic!("unexpected error for {matcher}: {other:?}"),
        }
    }
}

#[tokio::test]
async fn invalid_json_health_response_returns_invalid_response() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);

    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;

    let error = client.health().await.expect_err("invalid json");
    assert!(matches!(error, PolarisError::InvalidResponse(_)));
}

#[tokio::test]
async fn auth_header_is_sent_when_api_key_is_available() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = PolarisClient::builder()
        .base_url(server.uri())
        .dataset_root(root.path())
        .api_key("secret")
        .build()
        .expect("client");

    Mock::given(method("GET"))
        .and(path("/catalog"))
        .and(header("authorization", "Bearer secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "updatedAt": "2024-01-15T00:00:00Z",
            "markets": []
        })))
        .mount(&server)
        .await;

    let response = client
        .catalog(CatalogQuery::default())
        .await
        .expect("catalog");
    assert!(response.markets.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_client_matches_async_and_rejects_active_runtime_calls() {
    let server = MockServer::start().await;
    let async_root = TempDir::new().expect("async root");
    let blocking_root = TempDir::new().expect("blocking root");
    let async_client = build_client(&server, &async_root);
    let blocking_client = blocking::PolarisClient::builder()
        .base_url(server.uri())
        .dataset_root(blocking_root.path())
        .build()
        .expect("blocking client");

    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;

    let error = blocking_client
        .health()
        .expect_err("active runtime must be rejected");
    assert!(matches!(error, PolarisError::BlockingInAsyncRuntime));

    let async_value = async_client.health().await.expect("async health");
    let blocking_value = std::thread::spawn(move || {
        let value = blocking_client.health();
        drop(blocking_client);
        value
    })
    .join()
    .expect("blocking thread")
    .expect("blocking health");
    assert_eq!(async_value, blocking_value);
}
