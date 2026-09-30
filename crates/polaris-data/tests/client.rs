use std::time::Duration;

use futures_util::StreamExt;
use log::Level;
use logtest::Logger;
use polaris_data::{
    AmountKind, CatalogQuery, HistoricalQuery, HistoricalRowsQuery, HistoricalStream,
    IntentRowsQuery, IntentStatus, L2OrderbooksQuery, L2UpdatesQuery, OhlcvFormat, OhlcvInterval,
    OhlcvOutput, OhlcvQuery, OhlcvRowsQuery, OptionTickerRowsQuery, PolarisClient, PolarisError,
    QuoteRowsQuery, RawChannelQuery, ReplayQuery, blocking,
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

fn write_propamm_fixture(root: &TempDir, source: &str, fixture: &str) {
    let key = format!("standard-{source}-ethereum-2024-01-01-000000");
    let path = root.path().join(format!(
        "data/standard/{source}/ethereum/2024-01-01/{key}.jsonl.zst"
    ));
    std::fs::create_dir_all(path.parent().expect("fixture parent")).expect("fixture directory");
    std::fs::write(
        path,
        zstd::stream::encode_all(fixture.as_bytes(), 0).expect("compressed fixture"),
    )
    .expect("fixture");
}

fn write_intent_fixture(root: &TempDir, fixture: &str) {
    let key = "standard-uniswapx-intents-2024-01-01-000000";
    let path = root.path().join(format!(
        "data/standard/uniswapx/intents/2024-01-01/{key}.jsonl.zst"
    ));
    std::fs::create_dir_all(path.parent().expect("fixture parent")).expect("fixture directory");
    std::fs::write(
        path,
        zstd::stream::encode_all(fixture.as_bytes(), 0).expect("compressed fixture"),
    )
    .expect("fixture");
}

fn write_new_event_fixture(root: &TempDir, fixture: &str) {
    let key = "standard-hyperliquid-BTC-2024-01-01-000000";
    let path = root.path().join(format!(
        "data/standard/hyperliquid/BTC/2024-01-01/{key}.jsonl.zst"
    ));
    std::fs::create_dir_all(path.parent().expect("fixture parent")).expect("fixture directory");
    std::fs::write(
        path,
        zstd::stream::encode_all(fixture.as_bytes(), 0).expect("compressed fixture"),
    )
    .expect("fixture");
}

fn zstd_ndjson(lines: &[serde_json::Value]) -> Vec<u8> {
    let body = lines
        .iter()
        .map(|line| serde_json::to_string(line).expect("json"))
        .collect::<Vec<_>>()
        .join("\n");
    zstd::stream::encode_all(body.as_bytes(), 0).expect("zstd body")
}

fn manifest_snapshot(date: &str, timestamp: &str, key: &str, url: String) -> serde_json::Value {
    json!({
        "date": date,
        "timestamp": timestamp,
        "key": key,
        "url": url,
        "expires_in_seconds": 86_400
    })
}

fn download_manifest(
    source: &str,
    market: &str,
    date: &str,
    snapshots: Vec<serde_json::Value>,
) -> serde_json::Value {
    json!({
        "source": source,
        "market": market,
        "date": date,
        "total": snapshots.len(),
        "total_bytes": 1234,
        "snapshots": snapshots
    })
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
async fn list_snapshots_paginates_across_data_and_snapshots_fields() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);

    Mock::given(method("GET"))
        .and(path("/snapshots"))
        .respond_with(|request: &wiremock::Request| {
            let has_cursor = request.url.query_pairs().any(|(key, _)| key == "cursor");
            if has_cursor {
                ResponseTemplate::new(200).set_body_json(json!({
                    "snapshots": [{"key": "standard-binance-BTC-USDT-2024-01-02", "date": "2024-01-02"}],
                    "has_more": false,
                    "next_cursor": null
                }))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({
                    "data": [{"key": "standard-binance-BTC-USDT-2024-01-01", "date": "2024-01-01"}],
                    "has_more": true,
                    "next_cursor": "abc"
                }))
            }
        })
        .mount(&server)
        .await;

    let snapshots = client
        .list_snapshots(polaris_data::ListSnapshotsQuery {
            source: "binance".to_owned(),
            market: "BTC-USDT".to_owned(),
            from: Some("2024-01-01T00:00:00Z".into()),
            to: Some("2024-01-03T00:00:00Z".into()),
            limit: Some(100),
        })
        .await
        .expect("snapshots");

    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0].key, "standard-binance-BTC-USDT-2024-01-01");
    assert_eq!(snapshots[1].key, "standard-binance-BTC-USDT-2024-01-02");
}

#[tokio::test]
async fn events_download_snapshots_and_reuse_local_cache() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    let key = "standard-binance-BTC-USDT-2024-01-01-000000";
    Mock::given(method("GET"))
        .and(path("/snapshots"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "snapshots": [{
                "key": key,
                "date": "2024-01-01",
                "start": "2024-01-01T00:00:00Z",
                "end": "2024-01-01T01:00:00Z"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/download"))
        .and(query_param("source", "binance"))
        .and(query_param("market", "BTC-USDT"))
        .and(query_param("date", "2024-01-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(download_manifest(
            "binance",
            "BTC-USDT",
            "2024-01-01",
            vec![manifest_snapshot(
                "2024-01-01",
                "000000",
                key,
                format!("{}/objects/day-1", server.uri()),
            )],
        )))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/objects/day-1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(zstd_ndjson(&[
            json!({"timestamp": 1_704_067_200_000_000_i64, "source": "binance", "market": "BTC-USDT", "type": "trade", "data": {"price": 100.0, "quantity": 2.0, "side": "buy"}}),
        ])))
        .expect(1)
        .mount(&server)
        .await;

    let query = HistoricalQuery {
        source: "binance".to_owned(),
        market: "BTC-USDT".to_owned(),
        from: Some("2024-01-01T00:00:00Z".into()),
        to: Some("2024-01-01T01:00:00Z".into()),
        allow_gaps: false,
        materialize_orderbooks: true,
    };

    let first = collect_stream(client.events(query.clone()).await.expect("first events"))
        .await
        .expect("first rows");
    let second = collect_stream(client.events(query).await.expect("second events"))
        .await
        .expect("second rows");

    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_eq!(first[0].event_type(), "trade");
    let sidecar = root
        .path()
        .join("data/standard/binance/BTC-USDT/2024-01-01")
        .join(format!("{key}.jsonl.zst.coverage.json"));
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(sidecar).expect("coverage sidecar"))
            .expect("valid sidecar");
    assert_eq!(metadata["start_us"], 1_704_067_200_000_000_i64);
    assert_eq!(metadata["end_us"], 1_704_070_800_000_000_i64);
}

#[tokio::test]
async fn propamm_quote_ladders_are_typed_and_inherit_metadata_market() {
    const MAX_UINT256: &str =
        "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    write_propamm_fixture(
        &root,
        "fermiswap",
        include_str!("../../../tests/fixtures/events/propamm-fermiswap-v2.jsonl"),
    );
    write_propamm_fixture(
        &root,
        "metric",
        include_str!("../../../tests/fixtures/events/propamm-metric-v2.jsonl"),
    );
    let client = build_client(&server, &root);
    let query = |source: &str| HistoricalQuery {
        source: source.to_owned(),
        market: "ethereum".to_owned(),
        from: Some("2024-01-01T00:00:00Z".into()),
        to: Some("2024-01-01T01:00:00Z".into()),
        allow_gaps: false,
        materialize_orderbooks: true,
    };

    let fermi = collect_stream(
        client
            .propamm_quote_ladders(query("fermiswap"))
            .await
            .expect("fermiswap stream"),
    )
    .await
    .expect("fermiswap rows");
    let metric = collect_stream(
        client
            .propamm_quote_ladders(query("metric"))
            .await
            .expect("metric stream"),
    )
    .await
    .expect("metric rows");

    assert_eq!(fermi.len(), 1);
    assert_eq!(fermi[0].market(), "ethereum");
    assert_eq!(fermi[0].data().values.quotes[0].amount_in, MAX_UINT256);
    assert_eq!(fermi[0].data().values.oracle, None);
    assert_eq!(fermi[0].data().values.pool, None);
    assert_eq!(metric.len(), 1);
    assert_eq!(metric[0].data().values.pool.as_deref(), Some("0xpool"));

    let root_path = root.path().to_owned();
    let blocking_rows = std::thread::spawn(move || {
        let client = blocking::PolarisClient::builder()
            .base_url("http://127.0.0.1:1")
            .dataset_root(root_path)
            .build()
            .expect("blocking client");
        client
            .propamm_quote_ladders(query("fermiswap"))
            .expect("blocking ladders")
            .collect::<Result<Vec<_>, _>>()
    })
    .join()
    .expect("blocking thread")
    .expect("blocking rows");
    assert_eq!(blocking_rows.len(), 1);
    assert_eq!(blocking_rows[0].market(), "ethereum");
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
        .and(path("/historical/options-ticker"))
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
        .and(path("/historical/trades"))
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
    Mock::given(method("GET"))
        .and(path("/historical/funding-rates"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
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
        })))
        .mount(&server)
        .await;

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
    let marks = collect_stream(
        client
            .mark_prices(HistoricalRowsQuery::default())
            .await
            .expect("marks"),
    )
    .await
    .expect("mark rows");
    assert_eq!(tickers, funding);
    assert_eq!(marks, vec![funding[0].clone()]);
    let requests = server.received_requests().await.expect("requests");
    let funding_request = requests
        .iter()
        .find(|request| request.url.path() == "/historical/funding-rates")
        .unwrap();
    assert!(
        !funding_request
            .url
            .query_pairs()
            .any(|(key, _)| key == "start" || key == "end")
    );
}

#[tokio::test]
async fn raw_channel_paginates_exact_text_and_keeps_legacy_raw_separate() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = PolarisClient::builder()
        .base_url(server.uri())
        .api_key("secret")
        .dataset_root(root.path())
        .build()
        .expect("client");
    Mock::given(method("GET"))
        .and(path("/raw/binance/trades"))
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
    assert_eq!(rows[0].additional_context["channel"], "trades");
    assert!(
        client
            .raw_channel(RawChannelQuery { start: 11, ..query })
            .await
            .is_err()
    );
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/raw")
    );
}

#[tokio::test]
async fn direct_ohlcv_intent_and_quote_rows_keep_published_observations() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = PolarisClient::builder()
        .base_url(server.uri())
        .api_key("secret")
        .dataset_root(root.path())
        .build()
        .expect("client");
    Mock::given(method("GET")).and(path("/historical/ohlcv"))
        .and(query_param("interval", "1m")).and(query_param("start", "10"))
        .and(query_param("end", "10")).and(header("authorization", "Bearer secret"))
        .respond_with(|request: &wiremock::Request| {
            let second = request.url.query_pairs().any(|(key, value)| key == "cursor" && value == "next");
            ResponseTemplate::new(200).set_body_json(json!({
                "items": [{"event_id": if second { "c2" } else { "c1" }, "source": "binance", "market": "BTC-USDT",
                    "collector_timestamp": 10, "source_capture_id": "capture", "schema_version": 1,
                    "interval": "1m", "open_timestamp": 10, "open": 100.0, "high": 102.0,
                    "low": 99.0, "close": if second { 101.0 } else { 100.0 }, "is_closed": second}],
                "has_more": !second, "next_cursor": if second { None } else { Some("next") }
            }))
        }).mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/historical/intents"))
        .and(query_param("intent_id", "intent-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"event_id": "i1", "source": "uniswapx", "market": "intents",
                "collector_timestamp": 10, "source_capture_id": "capture", "schema_version": 1,
                "intent_id": "intent-1", "input_asset_id": null}],
            "has_more": false, "next_cursor": null
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET")).and(path("/historical/quotes"))
        .and(query_param("observation_id", "obs-1"))
        .and(query_param("instrument", "pool-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"event_id": "q1", "source": "propamm", "market": "ethereum", "instrument": "pool-1",
                "collector_timestamp": 10, "source_capture_id": "capture", "schema_version": 1,
                "observation_id": "obs-1", "input_asset_id": "ETH", "input_chain_id": "1", "input_amount": "1000000000000000000",
                "input_decimals": 18, "output_asset_id": "USDC", "output_chain_id": "1", "output_amount": "2000000",
                "output_decimals": 6, "amount_kind": "exact_input", "block_number": 100, "block_hash": "0xblock",
                "transaction_hash": "0xtx", "transaction_index": 0, "router": "0xrouter", "pool": null}],
            "has_more": false, "next_cursor": null
        }))).mount(&server).await;

    let candles = collect_stream(
        client
            .ohlcv_rows(OhlcvRowsQuery {
                interval: Some("1m".into()),
                start: Some(10),
                end: Some(10),
                ..Default::default()
            })
            .await
            .expect("ohlcv rows"),
    )
    .await
    .expect("candles");
    assert_eq!(candles.len(), 2);
    assert_eq!(candles[0].open_timestamp, candles[1].open_timestamp);
    assert_eq!(candles[0].is_closed, Some(false));
    let intents = collect_stream(
        client
            .intent_rows(IntentRowsQuery {
                intent_id: Some("intent-1".into()),
                ..Default::default()
            })
            .await
            .expect("intent rows"),
    )
    .await
    .expect("intents");
    assert_eq!(intents[0].input_asset_id, None);
    let alias = collect_stream(
        client
            .intents(IntentRowsQuery {
                intent_id: Some("intent-1".into()),
                ..Default::default()
            })
            .await
            .expect("intents alias"),
    )
    .await
    .expect("intent rows");
    assert_eq!(alias, intents);
    let quotes = collect_stream(
        client
            .quote_rows(QuoteRowsQuery {
                observation_id: Some("obs-1".into()),
                instrument: Some("pool-1".into()),
                ..Default::default()
            })
            .await
            .expect("quote rows"),
    )
    .await
    .expect("quotes");
    assert_eq!(quotes[0].input_amount, "1000000000000000000");
    assert_eq!(quotes[0].pool, None);
    assert!(
        client
            .intent_rows(IntentRowsQuery {
                intent_id: Some(" ".into()),
                ..Default::default()
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn new_event_shapes_are_typed_and_supported_by_default() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    write_new_event_fixture(
        &root,
        include_str!("../../../tests/fixtures/events/new-event-shapes-v2.jsonl"),
    );
    let client = build_client(&server, &root);
    let query = HistoricalQuery {
        source: "hyperliquid".to_owned(),
        market: "BTC".to_owned(),
        from: Some("2024-01-01T00:00:00Z".into()),
        to: Some("2024-01-01T01:00:00Z".into()),
        allow_gaps: false,
        materialize_orderbooks: true,
    };

    let events = collect_stream(client.events(query.clone()).await.expect("events"))
        .await
        .expect("event rows");
    assert_eq!(events.len(), 6);
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type())
            .collect::<Vec<_>>(),
        [
            "perpetual_ticker",
            "perpetual_ticker",
            "option_ticker",
            "trade",
            "trade",
            "intent",
        ]
    );

    let trade_events = events
        .iter()
        .filter(|event| event.event_type() == "trade")
        .collect::<Vec<_>>();
    assert_eq!(trade_events.len(), 2);
    assert_eq!(trade_events[0].data()["maker"], "0xmaker");
    assert_eq!(trade_events[0].data()["taker"], "0xtaker");

    let root_path = root.path().to_owned();
    let prepared = std::thread::spawn(move || {
        let client = blocking::PolarisClient::builder()
            .base_url("http://127.0.0.1:1")
            .dataset_root(root_path)
            .build()
            .expect("blocking client");
        let prepared = client
            .prepare_historical(query.clone())?
            .perpetual_tickers()
            .collect::<Result<Vec<_>, _>>()?;
        Ok::<_, PolarisError>(prepared)
    })
    .join()
    .expect("blocking thread")
    .expect("blocking rows");
    assert_eq!(prepared.len(), 2);
}

#[tokio::test]
async fn malformed_perpetual_tickers_are_rejected() {
    let server = MockServer::start().await;
    for (field, value) in [("data", json!({})), ("data", json!({"funding_rate": 0.1}))] {
        let root = TempDir::new().expect("tempdir");
        let mut rows = include_str!("../../../tests/fixtures/events/new-event-shapes-v2.jsonl")
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("fixture row"))
            .collect::<Vec<_>>();
        rows[1][field] = value;
        let fixture = rows
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        write_new_event_fixture(&root, &fixture);
        let client = build_client(&server, &root);
        let error = collect_stream(
            client
                .events(HistoricalQuery {
                    source: "hyperliquid".to_owned(),
                    market: "BTC".to_owned(),
                    from: Some("2024-01-01T00:00:00Z".into()),
                    to: Some("2024-01-01T01:00:00Z".into()),
                    allow_gaps: false,
                    materialize_orderbooks: false,
                })
                .await
                .expect("event stream"),
        )
        .await
        .expect_err("invalid perpetual ticker must fail");
        assert!(
            error.to_string().contains("perpetual"),
            "unexpected error: {error}"
        );
    }
}

#[tokio::test]
async fn intents_are_typed_filtered_and_available_from_prepared_replay() {
    let root = TempDir::new().expect("tempdir");
    write_intent_fixture(
        &root,
        include_str!("../../../tests/fixtures/events/intents-v2.jsonl"),
    );
    let query = HistoricalQuery {
        source: "uniswapx".to_owned(),
        market: "intents".to_owned(),
        from: Some("2024-01-01T00:00:00Z".into()),
        to: Some("2024-01-01T01:00:00Z".into()),
        allow_gaps: false,
        materialize_orderbooks: true,
    };

    let root_path = root.path().to_owned();
    let prepared_rows = std::thread::spawn(move || {
        let client = blocking::PolarisClient::builder()
            .base_url("http://127.0.0.1:1")
            .dataset_root(root_path)
            .build()
            .expect("blocking client");
        let prepared_rows = client
            .prepare_historical(query.clone())
            .expect("prepared replay")
            .intents()
            .collect::<Result<Vec<_>, _>>()?;
        Ok::<_, PolarisError>(prepared_rows)
    })
    .join()
    .expect("blocking thread")
    .expect("blocking rows");
    assert_eq!(prepared_rows.len(), 4);
    assert_eq!(prepared_rows[0].source(), "uniswapx");
    assert_eq!(prepared_rows[0].data().rfq_id.as_deref(), Some("rfq-1"));
    assert_eq!(
        prepared_rows[0].data().amount_kind,
        Some(AmountKind::ExactInput)
    );
    assert_eq!(prepared_rows[2].data().status, Some(IntentStatus::Settled));

    let malformed_root = TempDir::new().expect("tempdir");
    let mut fixture = include_str!("../../../tests/fixtures/events/intents-v2.jsonl")
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("fixture row"))
        .collect::<Vec<_>>();
    fixture[1]["data"]["inputs"][0]
        .as_object_mut()
        .expect("asset")
        .remove("asset_id");
    write_intent_fixture(
        &malformed_root,
        &fixture
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let malformed_path = malformed_root.path().to_owned();
    let error = std::thread::spawn(move || {
        let client = blocking::PolarisClient::builder()
            .base_url("http://127.0.0.1:1")
            .dataset_root(malformed_path)
            .build()
            .expect("blocking client");
        client
            .prepare_historical(HistoricalQuery {
                source: "uniswapx".to_owned(),
                market: "intents".to_owned(),
                from: Some("2024-01-01T00:00:00Z".into()),
                to: Some("2024-01-01T01:00:00Z".into()),
                allow_gaps: false,
                materialize_orderbooks: false,
            })
            .expect("prepared replay")
            .intents()
            .collect::<Result<Vec<_>, _>>()
    })
    .join()
    .expect("blocking thread")
    .expect_err("missing asset_id must fail");
    assert!(error.to_string().contains("invalid v2 intent payload"));
}

#[tokio::test]
async fn legacy_midnight_shard_replays_without_remote_coverage_lookup() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    let key = "standard-binance-BTC-USDT-2024-01-01-000003";
    let path = root
        .path()
        .join("data/standard/binance/BTC-USDT/2024-01-01")
        .join(format!("{key}.jsonl.zst"));
    std::fs::create_dir_all(path.parent().expect("parent")).expect("data directory");
    std::fs::write(
        &path,
        zstd_ndjson(&[json!({
            "timestamp": 1_704_067_200_012_i64,
            "source": "binance",
            "market": "BTC-USDT",
            "type": "trade",
            "data": {"price": 100.0, "quantity": 1.0}
        })]),
    )
    .expect("snapshot");

    let rows = collect_stream(
        client
            .events(HistoricalQuery {
                source: "binance".to_owned(),
                market: "BTC-USDT".to_owned(),
                from: Some("2024-01-01T00:00:00Z".into()),
                to: Some("2024-01-01T01:00:00Z".into()),
                allow_gaps: false,
                materialize_orderbooks: false,
            })
            .await
            .expect("local stream"),
    )
    .await
    .expect("local rows");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].timestamp(), 1_704_067_200_012_i64);
    assert!(
        server
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
    );
    let diagnostics = client.take_diagnostics();
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].code, "estimated_snapshot_coverage");
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
async fn l2_direct_rows_page_and_validate_bounded_books() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    Mock::given(method("GET"))
        .and(path("/historical/l2-updates"))
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
        .and(path("/historical/l2-orderbooks"))
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
    assert!(
        client
            .l2_snapshots(L2OrderbooksQuery {
                source: "hyperliquid".to_owned(),
                market: "0G".to_owned(),
                instrument: None,
                start: 10,
                end: 300_011,
            })
            .await
            .is_err()
    );
    let requests = server.received_requests().await.expect("requests");
    assert_eq!(requests.len(), 3);
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
}

#[tokio::test]
async fn replay_allow_gaps_returns_partial_rows_and_logs_warning() {
    let mut logger = Logger::start();
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    let key_0 = "standard-binance-BTC-USDT-2024-01-01-000000";
    let key_2 = "standard-binance-BTC-USDT-2024-01-01-020000";
    Mock::given(method("GET"))
        .and(path("/snapshots"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "snapshots": [
                {
                    "key": key_0,
                    "date": "2024-01-01",
                    "start": "2024-01-01T00:00:00Z",
                    "end": "2024-01-01T01:00:00Z"
                },
                {
                    "key": key_2,
                    "date": "2024-01-01",
                    "start": "2024-01-01T02:00:00Z",
                    "end": "2024-01-01T03:00:00Z"
                }
            ]
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/download"))
        .and(query_param("source", "binance"))
        .and(query_param("market", "BTC-USDT"))
        .and(query_param("date", "2024-01-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(download_manifest(
            "binance",
            "BTC-USDT",
            "2024-01-01",
            vec![
                manifest_snapshot(
                    "2024-01-01",
                    "000000",
                    key_0,
                    format!("{}/objects/hour-0", server.uri()),
                ),
                manifest_snapshot(
                    "2024-01-01",
                    "020000",
                    key_2,
                    format!("{}/objects/hour-2", server.uri()),
                ),
            ],
        )))
        .mount(&server)
        .await;
    for (object_path, ts) in [
        ("/objects/hour-0", 1_704_067_200_000_000_i64),
        ("/objects/hour-2", 1_704_074_400_000_000_i64),
    ] {
        Mock::given(method("GET"))
            .and(path(object_path))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(zstd_ndjson(&[
                json!({"timestamp": ts, "source": "binance", "market": "BTC-USDT", "type": "trade", "data": {"price": 100.0, "quantity": 1.0, "side": "buy"}}),
            ])))
            .mount(&server)
            .await;
    }

    let mut replay = client
        .replay(ReplayQuery {
            source: "binance".to_owned(),
            market: "BTC-USDT".to_owned(),
            from: Some("2024-01-01T00:00:00Z".into()),
            to: Some("2024-01-01T03:00:00Z".into()),
            allow_gaps: true,
            materialize_orderbooks: true,
        })
        .await
        .expect("replay");

    let rows = replay.by_ref().collect::<Vec<_>>().await;
    let rows: Vec<_> = rows.into_iter().collect::<Result<_, _>>().expect("rows");

    assert_eq!(rows.len(), 2);
    let mut saw_warning = false;
    while let Some(record) = logger.pop() {
        if record.level() == Level::Warn && record.args().contains("has gaps") {
            saw_warning = true;
            break;
        }
    }
    assert!(saw_warning, "expected gap warning log");
}

#[tokio::test]
async fn replay_strict_gap_handling_returns_coverage_error() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    Mock::given(method("GET"))
        .and(path("/snapshots"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "snapshots": [{
                "key": "standard-binance-BTC-USDT-2024-01-01-000000",
                "date": "2024-01-01",
                "start": "2024-01-01T00:00:00Z",
                "end": "2024-01-01T01:00:00Z"
            }]
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/download"))
        .and(query_param("source", "binance"))
        .and(query_param("market", "BTC-USDT"))
        .and(query_param("date", "2024-01-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(download_manifest(
            "binance",
            "BTC-USDT",
            "2024-01-01",
            vec![manifest_snapshot(
                "2024-01-01",
                "000000",
                "standard-binance-BTC-USDT-2024-01-01-000000",
                format!("{}/objects/strict-gap-hour-0", server.uri()),
            )],
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/objects/strict-gap-hour-0"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(zstd_ndjson(&[])))
        .mount(&server)
        .await;

    let error = match client
        .replay(ReplayQuery {
            source: "binance".to_owned(),
            market: "BTC-USDT".to_owned(),
            from: Some("2024-01-01T00:00:00Z".into()),
            to: Some("2024-01-01T03:00:00Z".into()),
            allow_gaps: false,
            materialize_orderbooks: true,
        })
        .await
    {
        Ok(_) => panic!("expected coverage gap"),
        Err(error) => error,
    };

    match error {
        PolarisError::CoverageGap { intervals, .. } => {
            assert!(
                intervals
                    .iter()
                    .any(|gap| gap.contains("2024-01-01T01:00:00Z"))
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn ohlcv_returns_tradingview_output() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    Mock::given(method("GET"))
        .and(path("/historical/ohlcv"))
        .and(query_param("interval", "1m"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"event_id": "c1", "source": "binance", "market": "BTC-USDT",
                "collector_timestamp": 1_704_067_240_000_i64, "source_capture_id": "capture",
                "schema_version": 1, "interval": "1m", "open_timestamp": 1_704_067_200_000_i64,
                "open": 100.0, "high": 101.0, "low": 100.0, "close": 101.0,
                "base_volume": 2.0, "trade_count": 2}],
            "has_more": false, "next_cursor": null
        })))
        .mount(&server)
        .await;

    let output = client
        .ohlcv(OhlcvQuery {
            source: "binance".to_owned(),
            market: "BTC-USDT".to_owned(),
            from: Some("2024-01-01T00:00:00Z".into()),
            to: Some("2024-01-01T01:00:00Z".into()),
            interval: OhlcvInterval::M1,
            format: OhlcvFormat::TradingView,
            allow_gaps: false,
        })
        .await
        .expect("ohlcv");

    match output {
        OhlcvOutput::TradingView(view) => {
            assert_eq!(view.candles.len(), 1);
            assert_eq!(view.candles[0].open, 100.0);
            assert_eq!(view.candles[0].close, 101.0);
            assert_eq!(view.volumes[0].value, 2.0);
        }
        other => panic!("unexpected output: {other:?}"),
    }
    let query = OhlcvQuery {
        source: "binance".to_owned(),
        market: "BTC-USDT".to_owned(),
        from: Some("2024-01-01T00:00:00Z".into()),
        to: Some("2024-01-01T01:00:00Z".into()),
        interval: OhlcvInterval::M1,
        format: OhlcvFormat::Bars,
        allow_gaps: false,
    };
    assert_eq!(
        client.volume(query.clone()).await.expect("volume")[0].volume,
        2.0
    );
    let vwap = client.vwap(query).await.expect("vwap");
    assert_eq!(vwap[0].vwap, Some(101.0));
    assert_eq!(vwap[0].trades, 2);
}

#[tokio::test]
async fn preview_catalog_without_api_key_infers_public_cutoff_window() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);

    Mock::given(method("GET"))
        .and(path("/catalog"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "updatedAt": "2024-01-15T12:00:00Z",
            "markets": [{
                "source": "binance",
                "market": "BTC-USDT",
                "start": "2024-01-15T00:00:00Z",
                "end": "2024-01-20T00:00:00Z",
                "access": {"status": "preview", "public_cutoff_date": "2024-01-15"}
            }]
        })))
        .mount(&server)
        .await;

    let manifest_snapshots = (0..24)
        .map(|hour| {
            let timestamp = format!("{hour:02}0000");
            let key = format!("standard-binance-BTC-USDT-2024-01-15-{timestamp}");
            manifest_snapshot(
                "2024-01-15",
                &timestamp,
                &key,
                format!("{}/objects/preview-{hour:02}", server.uri()),
            )
        })
        .collect::<Vec<_>>();
    let snapshot_entries = manifest_snapshots
        .iter()
        .map(|entry| {
            json!({
                "key": entry["key"],
                "date": entry["date"],
                "timestamp": entry["timestamp"]
            })
        })
        .collect::<Vec<_>>();
    Mock::given(method("GET"))
        .and(path("/snapshots"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "snapshots": snapshot_entries
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/download"))
        .and(query_param("source", "binance"))
        .and(query_param("market", "BTC-USDT"))
        .and(query_param("date", "2024-01-15"))
        .respond_with(ResponseTemplate::new(200).set_body_json(download_manifest(
            "binance",
            "BTC-USDT",
            "2024-01-15",
            manifest_snapshots,
        )))
        .mount(&server)
        .await;
    for hour in 0..24 {
        let object_path = format!("/objects/preview-{hour:02}");
        let body = if hour == 0 {
            zstd_ndjson(&[
                json!({"timestamp": 1_705_276_800_000_000_i64, "source": "binance", "market": "BTC-USDT", "type": "trade", "data": {"price": 100.0, "quantity": 1.0, "side": "buy"}}),
            ])
        } else {
            zstd_ndjson(&[])
        };
        Mock::given(method("GET"))
            .and(path(object_path))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;
    }

    let rows = collect_stream(
        client
            .events(HistoricalQuery {
                source: "binance".to_owned(),
                market: "BTC-USDT".to_owned(),
                from: None,
                to: None,
                allow_gaps: false,
                materialize_orderbooks: true,
            })
            .await
            .expect("rows"),
    )
    .await
    .expect("stream rows");

    assert_eq!(rows.len(), 1);
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
async fn invalid_ndjson_and_invalid_zstd_are_mapped_to_decode_errors() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    let key = "standard-binance-BTC-USDT-2024-01-01-000000";
    Mock::given(method("GET"))
        .and(path("/snapshots"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "snapshots": [{"key": key, "date": "2024-01-01"}]
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/download"))
        .and(query_param("source", "binance"))
        .and(query_param("market", "BTC-USDT"))
        .and(query_param("date", "2024-01-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(download_manifest(
            "binance",
            "BTC-USDT",
            "2024-01-01",
            vec![manifest_snapshot(
                "2024-01-01",
                "000000",
                key,
                format!("{}/objects/bad-zstd", server.uri()),
            )],
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/objects/bad-zstd"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![1, 2, 3, 4]))
        .mount(&server)
        .await;

    let stream = client
        .events(HistoricalQuery {
            source: "binance".to_owned(),
            market: "BTC-USDT".to_owned(),
            from: Some("2024-01-01T00:00:00Z".into()),
            to: Some("2024-01-01T01:00:00Z".into()),
            allow_gaps: false,
            materialize_orderbooks: true,
        })
        .await
        .expect("stream setup");
    let error = collect_stream(stream).await.expect_err("invalid zstd");
    assert!(matches!(error, PolarisError::Decode(_)));

    let second_root = TempDir::new().expect("tempdir");
    let second_client = build_client(&server, &second_root);
    let bad_ndjson = zstd::stream::encode_all(&b"{broken-json"[..], 0).expect("zstd");

    Mock::given(method("GET"))
        .and(path("/download"))
        .and(query_param("source", "binance"))
        .and(query_param("market", "BTC-USDT"))
        .and(query_param("date", "2024-01-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(download_manifest(
            "binance",
            "BTC-USDT",
            "2024-01-01",
            vec![manifest_snapshot(
                "2024-01-01",
                "000000",
                key,
                format!("{}/objects/bad-ndjson", server.uri()),
            )],
        )))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/objects/bad-ndjson"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(bad_ndjson))
        .mount(&server)
        .await;

    let stream = second_client
        .events(HistoricalQuery {
            source: "binance".to_owned(),
            market: "BTC-USDT".to_owned(),
            from: Some("2024-01-01T00:00:00Z".into()),
            to: Some("2024-01-01T01:00:00Z".into()),
            allow_gaps: false,
            materialize_orderbooks: true,
        })
        .await
        .expect("stream setup");
    let error = collect_stream(stream).await.expect_err("invalid ndjson");
    assert!(matches!(error, PolarisError::Decode(_)));
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

#[tokio::test]
async fn concurrent_clients_atomically_share_one_snapshot_download() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let first = build_client(&server, &root);
    let second = build_client(&server, &root);
    let key = "standard-binance-BTC-USDT-2024-01-01";

    Mock::given(method("GET"))
        .and(path("/snapshots"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "snapshots": [{"key": key, "date": "2024-01-01"}]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/download"))
        .respond_with(ResponseTemplate::new(200).set_body_json(download_manifest(
            "binance",
            "BTC-USDT",
            "2024-01-01",
            vec![manifest_snapshot(
                "2024-01-01",
                "000000",
                key,
                format!("{}/objects/shared", server.uri()),
            )],
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/objects/shared"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(zstd_ndjson(&[json!({"timestamp": 1_704_067_200_000_i64})])),
        )
        .expect(1)
        .mount(&server)
        .await;

    let query = HistoricalQuery {
        source: "binance".to_owned(),
        market: "BTC-USDT".to_owned(),
        from: Some("2024-01-01T00:00:00Z".into()),
        to: Some("2024-01-02T00:00:00Z".into()),
        allow_gaps: false,
        materialize_orderbooks: true,
    };
    let (left, right) = tokio::join!(first.events(query.clone()), second.events(query));

    let left = collect_stream(left.expect("first"))
        .await
        .expect("first rows");
    let right = collect_stream(right.expect("second"))
        .await
        .expect("second rows");
    assert_eq!(left.len(), 1);
    assert_eq!(right.len(), 1);
    assert!(
        root.path()
            .join("tmp")
            .read_dir()
            .expect("tmp")
            .next()
            .is_none()
    );
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
