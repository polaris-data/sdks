use std::{collections::HashSet, time::Duration};

use async_stream::stream;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{net::TcpStream, time::Instant};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, protocol::frame::coding::CloseCode},
};
use url::Url;

use crate::{PolarisError, RealtimeStream, StandardEvent, StreamQuery};

const MAX_SUBSCRIPTIONS: usize = 1_000;
const PING_INTERVAL: Duration = Duration::from_secs(30);
const PONG_TIMEOUT: Duration = Duration::from_secs(10);
const HEALTHY_CONNECTION: Duration = Duration::from_secs(30);
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const SUBSCRIBE_REQUEST_ID: &str = "polaris-sdk-subscribe";

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

enum ConnectFailure {
    Retryable(String),
    Terminal(PolarisError),
}

enum ServerMessage {
    Data(StandardEvent),
    Pong(Option<String>),
    Control,
}

pub(crate) fn resolve_stream_url(
    base_url: &str,
    explicit: Option<String>,
) -> Result<Url, PolarisError> {
    let mut url = Url::parse(explicit.as_deref().unwrap_or(base_url)).map_err(|error| {
        PolarisError::InvalidResponse(format!("invalid realtime stream URL: {error}"))
    })?;
    if explicit.is_none() {
        let scheme = match url.scheme() {
            "https" => "wss",
            "http" => "ws",
            "wss" => "wss",
            "ws" => "ws",
            other => {
                return Err(PolarisError::InvalidResponse(format!(
                    "cannot derive realtime stream URL from scheme '{other}'"
                )));
            }
        };
        url.set_scheme(scheme).map_err(|_| {
            PolarisError::InvalidResponse("failed to set realtime stream URL scheme".to_owned())
        })?;
        url.set_path("/stream");
        url.set_query(None);
        url.set_fragment(None);
    } else if !matches!(url.scheme(), "ws" | "wss") {
        return Err(PolarisError::InvalidResponse(
            "realtime stream URL must use ws:// or wss://".to_owned(),
        ));
    }
    Ok(url)
}

pub(crate) async fn open_stream(
    stream_url: Url,
    api_key: Option<String>,
    mut query: StreamQuery,
) -> Result<RealtimeStream, PolarisError> {
    query.source = query.source.trim().to_owned();
    if query.source.is_empty() {
        return Err(PolarisError::InvalidResponse(
            "stream source must not be empty".to_owned(),
        ));
    }

    let mut seen = HashSet::new();
    query.markets = query
        .markets
        .into_iter()
        .map(|market| market.trim().to_owned())
        .filter(|market| !market.is_empty() && seen.insert(market.clone()))
        .collect();
    if query.markets.is_empty() {
        return Err(PolarisError::InvalidResponse(
            "stream markets must contain at least one non-empty market".to_owned(),
        ));
    }
    if query.markets.len() > MAX_SUBSCRIPTIONS {
        return Err(PolarisError::InvalidResponse(format!(
            "stream markets must contain at most {MAX_SUBSCRIPTIONS} unique markets"
        )));
    }

    let first_socket = connect_and_subscribe(&stream_url, api_key.as_deref(), &query)
        .await
        .map_err(|failure| match failure {
            ConnectFailure::Retryable(message) => PolarisError::StreamConnection(message),
            ConnectFailure::Terminal(error) => error,
        })?;

    Ok(Box::pin(stream! {
        let mut socket = first_socket;
        let mut backoff = INITIAL_BACKOFF;
        let mut ping_counter = 0_u64;

        'connections: loop {
            let connected_at = Instant::now();
            let mut last_ping = Instant::now();
            let mut awaiting_pong: Option<(String, Instant)> = None;
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;

            let reconnect_reason = loop {
                tokio::select! {
                    incoming = socket.next() => {
                        match incoming {
                            Some(Ok(Message::Text(text))) => match parse_server_message(text.as_ref()) {
                                Ok(ServerMessage::Data(event)) => yield Ok(event),
                                Ok(ServerMessage::Pong(request_id)) => {
                                    if awaiting_pong.as_ref().is_some_and(|(expected, _)| request_id.as_deref() == Some(expected)) {
                                        awaiting_pong = None;
                                    }
                                }
                                Ok(ServerMessage::Control) => {}
                                Err(error) => {
                                    yield Err(error);
                                    break 'connections;
                                }
                            },
                            Some(Ok(Message::Close(frame))) => {
                                if frame.as_ref().is_some_and(|frame| matches!(frame.code, CloseCode::Normal | CloseCode::Away)) {
                                    break 'connections;
                                }
                                break frame
                                    .map(|frame| format!("server closed the realtime stream ({}): {}", frame.code, frame.reason))
                                    .unwrap_or_else(|| "realtime stream closed without a close frame".to_owned());
                            }
                            Some(Ok(Message::Binary(_))) => {
                                yield Err(protocol_error(None, "server sent an unexpected binary WebSocket message"));
                                break 'connections;
                            }
                            Some(Ok(Message::Ping(payload))) => {
                                if let Err(error) = socket.send(Message::Pong(payload)).await {
                                    break format!("failed to answer WebSocket ping: {error}");
                                }
                            }
                            Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                            Some(Err(error)) => break format!("WebSocket receive failed: {error}"),
                            None => break "realtime WebSocket ended unexpectedly".to_owned(),
                        }
                    }
                    _ = tick.tick() => {
                        let now = Instant::now();
                        if awaiting_pong.as_ref().is_some_and(|(_, sent_at)| now.duration_since(*sent_at) >= PONG_TIMEOUT) {
                            break "realtime keepalive pong timed out".to_owned();
                        }
                        if awaiting_pong.is_none() && now.duration_since(last_ping) >= PING_INTERVAL {
                            ping_counter = ping_counter.wrapping_add(1);
                            let request_id = format!("polaris-sdk-ping-{ping_counter}");
                            let command = json!({"action": "ping", "request_id": request_id});
                            if let Err(error) = socket.send(Message::Text(command.to_string().into())).await {
                                break format!("failed to send realtime keepalive ping: {error}");
                            }
                            awaiting_pong = Some((request_id, now));
                            last_ping = now;
                        }
                    }
                }
            };

            if connected_at.elapsed() >= HEALTHY_CONNECTION {
                backoff = INITIAL_BACKOFF;
            }
            log::warn!("{reconnect_reason}; reconnecting realtime stream");

            loop {
                tokio::time::sleep(jitter(backoff)).await;
                match connect_and_subscribe(&stream_url, api_key.as_deref(), &query).await {
                    Ok(new_socket) => {
                        socket = new_socket;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue 'connections;
                    }
                    Err(ConnectFailure::Retryable(message)) => {
                        log::warn!("realtime stream reconnect failed: {message}");
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    }
                    Err(ConnectFailure::Terminal(error)) => {
                        yield Err(error);
                        break 'connections;
                    }
                }
            }
        }
    }))
}

async fn connect_and_subscribe(
    stream_url: &Url,
    api_key: Option<&str>,
    query: &StreamQuery,
) -> Result<Socket, ConnectFailure> {
    let (mut socket, _) = connect_async(stream_url.as_str())
        .await
        .map_err(|error| ConnectFailure::Retryable(error.to_string()))?;
    let subscriptions = query
        .markets
        .iter()
        .map(|market| json!({"source": query.source, "market": market, "stream": "standard"}))
        .collect::<Vec<_>>();
    let mut command = json!({
        "action": "subscribe",
        "request_id": SUBSCRIBE_REQUEST_ID,
        "include_buffer": query.include_buffer,
        "subscriptions": subscriptions,
    });
    if let Some(api_key) = api_key {
        command["token"] = Value::String(api_key.to_owned());
    }
    socket
        .send(Message::Text(command.to_string().into()))
        .await
        .map_err(|error| ConnectFailure::Retryable(error.to_string()))?;

    loop {
        let message = socket.next().await.ok_or_else(|| {
            ConnectFailure::Retryable(
                "WebSocket ended before subscription acknowledgement".to_owned(),
            )
        })?;
        match message {
            Ok(Message::Text(text)) => {
                let value: Value = serde_json::from_str(text.as_ref()).map_err(|error| {
                    ConnectFailure::Terminal(protocol_error(
                        None,
                        format!("invalid JSON before subscription acknowledgement: {error}"),
                    ))
                })?;
                match value.get("type").and_then(Value::as_str) {
                    Some("ack") => {
                        let request_id = value.get("request_id").and_then(Value::as_str);
                        let action = value.get("action").and_then(Value::as_str);
                        let changed = value.get("changed").and_then(Value::as_u64);
                        let active = value.get("active_subscriptions").and_then(Value::as_u64);
                        let expected = query.markets.len() as u64;
                        if request_id != Some(SUBSCRIBE_REQUEST_ID)
                            || action != Some("subscribe")
                            || changed != Some(expected)
                            || active != Some(expected)
                        {
                            return Err(ConnectFailure::Terminal(protocol_error(
                                None,
                                "invalid subscription acknowledgement",
                            )));
                        }
                        return Ok(socket);
                    }
                    Some("error") => {
                        return Err(ConnectFailure::Terminal(error_message(&value)));
                    }
                    Some("pong") => {}
                    _ => {
                        return Err(ConnectFailure::Terminal(protocol_error(
                            None,
                            "unexpected server message before subscription acknowledgement",
                        )));
                    }
                }
            }
            Ok(Message::Ping(payload)) => socket
                .send(Message::Pong(payload))
                .await
                .map_err(|error| ConnectFailure::Retryable(error.to_string()))?,
            Ok(Message::Close(frame)) => {
                return Err(ConnectFailure::Retryable(
                    frame
                        .map(|frame| {
                            format!("server closed before acknowledgement: {}", frame.reason)
                        })
                        .unwrap_or_else(|| "server closed before acknowledgement".to_owned()),
                ));
            }
            Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
            Ok(Message::Binary(_)) => {
                return Err(ConnectFailure::Terminal(protocol_error(
                    None,
                    "unexpected binary message before subscription acknowledgement",
                )));
            }
            Err(error) => return Err(ConnectFailure::Retryable(error.to_string())),
        }
    }
}

fn parse_server_message(text: &str) -> Result<ServerMessage, PolarisError> {
    let value: Value = serde_json::from_str(text)
        .map_err(|error| protocol_error(None, format!("invalid realtime JSON: {error}")))?;
    if let Some(message_type) = value.get("type").and_then(Value::as_str) {
        return match message_type {
            "pong" => Ok(ServerMessage::Pong(
                value
                    .get("request_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            )),
            "ack" => Ok(ServerMessage::Control),
            "error" => Err(error_message(&value)),
            other => Err(protocol_error(
                None,
                format!("unexpected realtime server message type '{other}'"),
            )),
        };
    }

    let kind = value
        .get("kind")
        .and_then(Value::as_object)
        .ok_or_else(|| protocol_error(None, "realtime message did not include a kind object"))?;
    match kind.get("type").and_then(Value::as_str) {
        Some("persistence_checkpoint") => Ok(ServerMessage::Control),
        Some("data") => {
            if kind.get("stream").and_then(Value::as_str) != Some("standard") {
                return Err(protocol_error(
                    None,
                    "standard realtime stream received a non-standard data message",
                ));
            }
            let event_value = kind
                .get("event")
                .cloned()
                .ok_or_else(|| protocol_error(None, "standard data message omitted its event"))?;
            let mut event: StandardEvent =
                serde_json::from_value(event_value).map_err(|error| {
                    protocol_error(None, format!("invalid standard realtime event: {error}"))
                })?;
            if event.source.is_empty() {
                event.source = value
                    .get("source")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
            }
            if event.market.is_empty() {
                event.market = value
                    .get("market")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
            }
            Ok(ServerMessage::Data(event))
        }
        Some(other) => Err(protocol_error(
            None,
            format!("unexpected realtime message kind '{other}'"),
        )),
        None => Err(protocol_error(
            None,
            "realtime message kind omitted its type",
        )),
    }
}

fn error_message(value: &Value) -> PolarisError {
    protocol_error(
        value
            .get("code")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("realtime server returned an error"),
    )
}

fn protocol_error(code: Option<String>, message: impl Into<String>) -> PolarisError {
    PolarisError::StreamProtocol {
        code,
        message: message.into(),
    }
}

fn jitter(duration: Duration) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let percent = 80 + (nanos % 41) as u64;
    duration.mul_f64(percent as f64 / 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{
        accept_async,
        tungstenite::protocol::{CloseFrame, frame::coding::CloseCode},
    };

    #[test]
    fn derives_stream_url_from_http_base() {
        assert_eq!(
            resolve_stream_url("https://api.polaris.supply/api", None)
                .unwrap()
                .as_str(),
            "wss://api.polaris.supply/stream"
        );
        assert_eq!(
            resolve_stream_url("http://127.0.0.1:8000/api", None)
                .unwrap()
                .as_str(),
            "ws://127.0.0.1:8000/stream"
        );
    }

    #[test]
    fn parses_standard_event_and_fills_envelope_identity() {
        let message = r#"{
            "source":"afx","market":"AAPLUSDC","timestamp":"2026-08-06T12:00:00Z",
            "kind":{"type":"data","stream":"standard","event":{
                "timestamp":1786017600000,"type":"trade","data":{"price":1.0,"quantity":2.0,"side":"buy"}
            }}
        }"#;
        let ServerMessage::Data(event) = parse_server_message(message).unwrap() else {
            panic!("expected data");
        };
        assert_eq!(event.source, "afx");
        assert_eq!(event.market, "AAPLUSDC");
        assert_eq!(event.event_type, "trade");
    }

    #[tokio::test]
    async fn subscribes_with_token_and_yields_only_standard_events() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("ws://{}/stream", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            let Message::Text(command) = socket.next().await.unwrap().unwrap() else {
                panic!("expected subscribe command");
            };
            let command: Value = serde_json::from_str(command.as_ref()).unwrap();
            assert_eq!(command["action"], "subscribe");
            assert_eq!(command["token"], "secret");
            assert_eq!(command["include_buffer"], true);
            assert_eq!(command["subscriptions"].as_array().unwrap().len(), 2);
            assert_eq!(command["subscriptions"][0]["market"], "BTC-USDT");
            assert_eq!(command["subscriptions"][1]["market"], "ETH-USDT");
            socket
                .send(Message::Text(
                    json!({
                        "type":"ack","request_id":SUBSCRIBE_REQUEST_ID,"action":"subscribe",
                        "changed":2,"active_subscriptions":2
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            socket.send(Message::Text(json!({
                "source":"binance","market":"BTC-USDT","timestamp":"2026-08-06T12:00:00Z",
                "kind":{"type":"persistence_checkpoint","stream":"standard","reason":"manual","persisted_through_timestamp":"2026-08-06T12:00:00Z"}
            }).to_string().into())).await.unwrap();
            socket.send(Message::Text(json!({
                "source":"binance","market":"BTC-USDT","timestamp":"2026-08-06T12:00:01Z",
                "kind":{"type":"data","stream":"standard","event":{
                    "timestamp":1786017601000_i64,"type":"trade","data":{"price":1.0,"quantity":2.0,"side":"buy"}
                }}
            }).to_string().into())).await.unwrap();
            socket.close(None).await.unwrap();
        });

        let mut events = open_stream(
            url,
            Some("secret".to_owned()),
            StreamQuery {
                source: " binance ".to_owned(),
                markets: vec![
                    "BTC-USDT".to_owned(),
                    "BTC-USDT".to_owned(),
                    "ETH-USDT".to_owned(),
                ],
                include_buffer: true,
            },
        )
        .await
        .unwrap();
        let event = events.next().await.unwrap().unwrap();
        assert_eq!(event.source, "binance");
        assert_eq!(event.market, "BTC-USDT");
        assert_eq!(event.event_type, "trade");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reconnects_and_resubscribes_after_abnormal_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("ws://{}/stream", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(tcp).await.unwrap();
                let _command = socket.next().await.unwrap().unwrap();
                socket
                    .send(Message::Text(
                        json!({
                            "type":"ack","request_id":SUBSCRIBE_REQUEST_ID,"action":"subscribe",
                            "changed":1,"active_subscriptions":1
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .unwrap();
                if attempt == 0 {
                    socket
                        .send(Message::Close(Some(CloseFrame {
                            code: CloseCode::Again,
                            reason: "retry".into(),
                        })))
                        .await
                        .unwrap();
                } else {
                    socket.send(Message::Text(json!({
                        "source":"afx","market":"AAPLUSDC","timestamp":"2026-08-06T12:00:01Z",
                        "kind":{"type":"data","stream":"standard","event":{
                            "timestamp":1786017601000_i64,"source":"afx","market":"AAPLUSDC",
                            "type":"point","data":{"series":"mark_price","value":"1.0"}
                        }}
                    }).to_string().into())).await.unwrap();
                }
            }
        });

        let mut events = open_stream(
            url,
            None,
            StreamQuery {
                source: "afx".to_owned(),
                markets: vec!["AAPLUSDC".to_owned()],
                include_buffer: false,
            },
        )
        .await
        .unwrap();
        let event = tokio::time::timeout(Duration::from_secs(3), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(event.event_type, "point");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn subscription_error_is_terminal() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("ws://{}/stream", listener.local_addr().unwrap())).unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            let _command = socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    json!({
                        "type":"error","code":"forbidden","message":"stream access denied"
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        });
        let error = match open_stream(
            url,
            None,
            StreamQuery {
                source: "afx".to_owned(),
                markets: vec!["AAPLUSDC".to_owned()],
                include_buffer: false,
            },
        )
        .await
        {
            Ok(_) => panic!("expected error"),
            Err(error) => error,
        };
        assert!(
            matches!(error, PolarisError::StreamProtocol { code: Some(code), .. } if code == "forbidden")
        );
    }

    #[tokio::test]
    async fn validates_query_before_connecting() {
        let url = Url::parse("ws://127.0.0.1:1/stream").unwrap();
        let error = match open_stream(
            url,
            None,
            StreamQuery {
                source: " ".to_owned(),
                markets: vec!["BTC-USDT".to_owned()],
                include_buffer: false,
            },
        )
        .await
        {
            Ok(_) => panic!("expected error"),
            Err(error) => error,
        };
        assert!(matches!(error, PolarisError::InvalidResponse(_)));
    }
}
