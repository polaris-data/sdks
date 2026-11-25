use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    time::Duration,
};

use futures_util::StreamExt;
use polaris_data::{
    CandlesQuery, CoverageQuery, DataQuery, PolarisClient, PolarisError, SqlQuery, StreamQuery,
    SubscriptionRequest, blocking,
};

const TABLE: &str = "standard.trades";
const EXACT: &str = "0.12345678901234567890123456789012345678";

struct Fixture {
    process: Child,
    endpoint: String,
}

impl Fixture {
    fn start() -> Self {
        let mut process = Command::new("python3")
            .arg("../../tests/fixture_server.py")
            .stdout(Stdio::piped())
            .spawn()
            .expect("start the shared fixture server");
        let mut endpoint = String::new();
        BufReader::new(process.stdout.take().unwrap())
            .read_line(&mut endpoint)
            .unwrap();
        assert!(endpoint.starts_with("http://"));
        Self {
            process,
            endpoint: endpoint.trim().to_owned(),
        }
    }

    fn client(&self) -> PolarisClient {
        PolarisClient::builder()
            .base_url(&self.endpoint)
            .api_key("fixture-key")
            .build()
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn data() -> DataQuery {
    DataQuery {
        table: TABLE.to_owned(),
        ..Default::default()
    }
}

fn stream() -> StreamQuery {
    StreamQuery {
        subscriptions: vec![SubscriptionRequest {
            table: TABLE.to_owned(),
            ..Default::default()
        }],
    }
}

#[tokio::test]
async fn production_routes_and_pagination() {
    let fixture = Fixture::start();
    let client = fixture.client();
    assert!(client.health().await.unwrap().ok);
    assert_eq!(client.openapi().await.unwrap()["openapi"], "3.1.0");
    let coverage = client.coverage(CoverageQuery::default()).await.unwrap();
    assert_eq!(coverage.tables[0].row_count, Some(9_007_199_254_740_993));
    assert_eq!(
        client
            .sql(SqlQuery {
                query: "SELECT 1".into()
            })
            .await
            .unwrap()
            .data[0][0],
        1
    );
    let candles = client
        .candles(CandlesQuery {
            source: "uniswap-v3".into(),
            chain_id: 1,
            pool: "pool".into(),
            interval: "1m".into(),
            start: 0,
            end: 60_000,
            manager: None,
        })
        .await
        .unwrap();
    assert_eq!(candles.data[0]["close"], EXACT);
    let mut query = data();
    query.filters = Some([("market.eq".to_owned(), "BTC/USDC + test".to_owned())].into());
    let rows: Vec<_> = client.data_rows(query).collect().await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].as_ref().unwrap()["price"], EXACT);
    assert!(rows[1].as_ref().unwrap()["price"].is_null());
}

#[tokio::test]
async fn realtime_replays_with_cursors() {
    let fixture = Fixture::start();
    let client = fixture.client();
    let mut pages = client.stream(stream()).await.unwrap();
    let page = |result: Option<Result<polaris_data::TableDataMessage, PolarisError>>| {
        result.unwrap().unwrap().cursor
    };
    assert_eq!(page(pages.next().await), "stream:1");
    assert_eq!(page(pages.next().await), "stream:2");
    assert!(pages.next().await.is_none());
    assert!(matches!(
        blocking::PolarisClient::from_async(client),
        Err(PolarisError::BlockingInAsyncRuntime)
    ));
}

#[test]
fn blocking_client_maps_errors_and_cancels_a_read() {
    let fixture = Fixture::start();
    let client = blocking::PolarisClient::from_async(fixture.client()).unwrap();
    let err = client
        .coverage(CoverageQuery {
            table: Some("rate-limited".into()),
            database: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, PolarisError::RateLimited(fault) if fault.reset_at.as_deref() == Some("60"))
    );
    let mut options = stream();
    options.subscriptions[0].filters = Some([("scenario".to_owned(), "quiet".to_owned())].into());
    let subscriber = client.stream(options).unwrap();
    std::thread::scope(|scope| {
        let reader = scope.spawn(|| subscriber.read());
        std::thread::sleep(Duration::from_millis(150));
        subscriber.close();
        assert!(reader.join().unwrap().unwrap().is_none());
    });
}
