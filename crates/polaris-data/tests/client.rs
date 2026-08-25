use std::time::Duration;

use futures_util::StreamExt;
use log::Level;
use logtest::Logger;
use polaris_data::{
    AmountKind, CatalogQuery, HistoricalQuery, HistoricalStream, IntentStatus, OhlcvFormat,
    OhlcvInterval, OhlcvOutput, OhlcvQuery, OptionTickerQuery, PolarisClient, PolarisError,
    ReplayQuery, blocking,
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

fn write_option_fixture(root: &TempDir, fixture: &str) {
    let key = "standard-deribit-BTC-2024-01-01-000000";
    let path = root.path().join(format!(
        "data/standard/deribit/BTC/2024-01-01/{key}.jsonl.zst"
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
    write_option_fixture(
        &root,
        include_str!("../../../tests/fixtures/events/options-v2.jsonl"),
    );
    let client = build_client(&server, &root);
    let query = |instrument: Option<&str>| OptionTickerQuery {
        source: "deribit".to_owned(),
        market: "BTC".to_owned(),
        instrument: instrument.map(ToOwned::to_owned),
        from: Some("2024-01-01T00:00:00Z".into()),
        to: Some("2024-01-01T01:00:00Z".into()),
        allow_gaps: false,
    };

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

    assert_eq!(chain.len(), 2);
    assert_eq!(chain[0].source(), "deribit");
    assert_eq!(chain[0].market(), "BTC");
    assert_eq!(chain[0].instrument(), "BTC-29MAR24-50000-C");
    assert_eq!(chain[0].data().mark_iv.as_deref(), Some("0.8359"));
    assert_eq!(chain[0].data().greeks.delta.as_deref(), Some("0.431"));
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].instrument(), "BTC-29MAR24-50000-C");

    let error = match client.option_tickers(query(Some(""))).await {
        Ok(_) => panic!("expected empty instrument error"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("instrument must be non-empty"));

    let root_path = root.path().to_owned();
    let blocking_rows = std::thread::spawn(move || {
        let client = blocking::PolarisClient::builder()
            .base_url("http://127.0.0.1:1")
            .dataset_root(root_path)
            .build()
            .expect("blocking client");
        client
            .option_tickers(query(Some("BTC-29MAR24-45000-P")))
            .expect("blocking option tickers")
            .collect::<Result<Vec<_>, _>>()
    })
    .join()
    .expect("blocking thread")
    .expect("blocking rows");
    assert_eq!(blocking_rows.len(), 1);
    assert_eq!(blocking_rows[0].instrument(), "BTC-29MAR24-45000-P");
}

#[tokio::test]
async fn intents_are_typed_filtered_and_available_from_prepared_replay() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    write_intent_fixture(
        &root,
        include_str!("../../../tests/fixtures/events/intents-v2.jsonl"),
    );
    let client = build_client(&server, &root);
    let query = HistoricalQuery {
        source: "uniswapx".to_owned(),
        market: "intents".to_owned(),
        from: Some("2024-01-01T00:00:00Z".into()),
        to: Some("2024-01-01T01:00:00Z".into()),
        allow_gaps: false,
        materialize_orderbooks: true,
    };

    let rows = collect_stream(client.intents(query.clone()).await.expect("intents"))
        .await
        .expect("intent rows");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].source(), "uniswapx");
    assert_eq!(rows[0].market(), "intents");
    assert_eq!(rows[0].data().rfq_id.as_deref(), Some("rfq-1"));
    assert_eq!(rows[0].data().amount_kind, Some(AmountKind::ExactInput));
    assert_eq!(rows[0].data().quote.as_ref().unwrap().quote_id, "quote-1");
    assert_eq!(rows[0].data().inputs[0].extra["token_symbol"], "AAA");
    assert_eq!(rows[0].data().extra["venue_context"], "rfq");
    assert_eq!(rows[0].raw().unwrap()["requestId"], "rfq-1");
    assert_eq!(rows[0].raw().unwrap()["venuePayload"]["token"], "0xaaa");
    assert!(rows[1].raw().is_none());
    assert_eq!(rows[1].data().amount_kind, Some(AmountKind::ExactOutput));
    assert_eq!(rows[2].data().status, Some(IntentStatus::Settled));
    assert_eq!(
        rows[2].data().transactions[0].transaction_hash,
        "0xsettlement"
    );
    assert_eq!(rows[2].data().transactions[0].extra["block_number"], "123");

    let root_path = root.path().to_owned();
    let (prepared_rows, blocking_rows) = std::thread::spawn(move || {
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
        let blocking_rows = client
            .intents(query)
            .expect("blocking intents")
            .collect::<Result<Vec<_>, _>>()?;
        Ok::<_, PolarisError>((prepared_rows, blocking_rows))
    })
    .join()
    .expect("blocking thread")
    .expect("blocking rows");
    assert_eq!(prepared_rows.len(), 4);
    assert_eq!(blocking_rows.len(), 4);

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
    let malformed_client = build_client(&server, &malformed_root);
    let error = collect_stream(
        malformed_client
            .intents(HistoricalQuery {
                source: "uniswapx".to_owned(),
                market: "intents".to_owned(),
                from: Some("2024-01-01T00:00:00Z".into()),
                to: Some("2024-01-01T01:00:00Z".into()),
                allow_gaps: false,
                materialize_orderbooks: false,
            })
            .await
            .expect("malformed stream"),
    )
    .await
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

#[tokio::test]
async fn l2_updates_returns_raw_snapshots_and_deltas() {
    let server = MockServer::start().await;
    let root = TempDir::new().expect("tempdir");
    let client = build_client(&server, &root);
    let path = root
        .path()
        .join("daily/binance/BTC-USDT/2024-01-01.jsonl.zst");
    std::fs::create_dir_all(path.parent().expect("parent")).expect("data directory");
    std::fs::write(
        &path,
        zstd_ndjson(&[
            json!({
                "timestamp": 1_704_067_200_000_i64,
                "source": "binance",
                "market": "BTC-USDT",
                "type": "orderbook",
                "data": {"bids": [[100.0, 2.0]], "asks": [[101.0, 3.0]]}
            }),
            json!({
                "timestamp": 1_704_067_201_000_i64,
                "source": "binance",
                "market": "BTC-USDT",
                "type": "orderbook_delta",
                "data": {"bids": [[100.0, 4.0]]}
            }),
        ]),
    )
    .expect("snapshot");

    let query = HistoricalQuery {
        source: "binance".to_owned(),
        market: "BTC-USDT".to_owned(),
        from: Some("2024-01-01T00:00:00Z".into()),
        to: Some("2024-01-01T01:00:00Z".into()),
        allow_gaps: false,
        materialize_orderbooks: true,
    };
    let rows = collect_stream(client.l2_updates(query).await.expect("l2 updates"))
        .await
        .expect("rows");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].event_type(), "orderbook");
    assert_eq!(rows[0].data()["bids"], json!([[100.0, 2.0]]));
    assert_eq!(rows[0].data()["asks"], json!([[101.0, 3.0]]));
    assert_eq!(rows[1].event_type(), "orderbook_delta");
    assert_eq!(rows[1].data()["bids"], json!([[100.0, 4.0]]));
    assert!(rows[1].data().get("asks").is_none());

    let mut books = polaris_data::OrderbookBuilder::new();
    assert_eq!(
        books
            .apply(rows[0].clone())
            .expect("snapshot")
            .expect("complete snapshot")
            .event_type(),
        "orderbook"
    );
    assert_eq!(
        books
            .apply(rows[1].clone())
            .expect("delta")
            .expect("complete update")
            .event_type(),
        "orderbook"
    );
    assert!(
        server
            .received_requests()
            .await
            .expect("requests")
            .is_empty()
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
                format!("{}/objects/ohlcv", server.uri()),
            )],
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/objects/ohlcv"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(zstd_ndjson(&[
            json!({"timestamp": 1_704_067_200_000_000_i64, "source": "binance", "market": "BTC-USDT", "type": "trade", "data": {"price": 100.0, "quantity": 1.5, "side": "buy"}}),
            json!({"timestamp": 1_704_067_230_000_000_i64, "source": "binance", "market": "BTC-USDT", "type": "trade", "data": {"price": 101.0, "quantity": 0.5, "side": "sell"}}),
        ])))
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
