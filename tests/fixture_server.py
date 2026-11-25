"""Local production-protocol fixture shared by the four SDKs."""

import argparse
import base64
import hashlib
import json
import socket
import struct
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

KEY = "fixture-key"
TABLE = "standard.trades"
EXACT = "0.12345678901234567890123456789012345678"


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def reply(self, status, value, headers=()):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Expose-Headers", "X-RateLimit-Reset")
        for name, header in headers:
            self.send_header(name, header)
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def fault(self, status, code, message):
        headers = [("X-RateLimit-Reset", "60")] if status == 429 else []
        self.reply(status, {"error": {"code": code, "message": message, "resolution": "Test resolution"}}, headers)

    def do_OPTIONS(self):
        self.reply(200, {}, [("Access-Control-Allow-Headers", "authorization,user-agent"),
                             ("Access-Control-Allow-Methods", "GET")])

    def do_GET(self):
        url = urlsplit(self.path)
        query = parse_qs(url.query, keep_blank_values=True)
        if url.path == "/health":
            if self.headers.get("Authorization"):
                return self.fault(400, "unexpected_key", "health must be public")
            if self.headers.get("User-Agent") == "slow":
                time.sleep(0.3)
            return self.reply(200, {"ok": True})
        if url.path == "/openapi.json":
            return self.reply(200, {"openapi": "3.1.0"})
        if url.path == "/coverage":
            if query.get("table") == ["rate-limited"]:
                return self.fault(429, "request_limited", "Try again later")
            return self.reply(200, {"updatedAt": "2026-11-25T00:00:00Z", "total": 1, "tables": [{
                "database": "standard", "table": "trades", "row_count": 9_007_199_254_740_993,
                "start": None, "end": None, "columns": [{"name": "price", "type": "Decimal(38, 20)"}],
            }]})
        if url.path == "/sql":
            if query.get("query") != ["SELECT 1"]:
                return self.fault(400, "invalid_query", "Expected SELECT 1")
            return self.reply(200, {"query_id": "qid", "rows": 1, "columns": [{"name": "1", "type": "UInt8"}], "data": [[1]]})
        if url.path in ("/data/" + TABLE, "/candles/uniswap-v3"):
            if self.headers.get("Authorization") != "Bearer " + KEY:
                return self.fault(401, "authentication_required", "An API key is required")
            if url.path.startswith("/candles/"):
                if query.get("start") != ["0"] or query.get("end") != ["60000"]:
                    return self.fault(400, "invalid_bounds", "Expected exclusive upper bound")
                return self.reply(200, {"data": [{"close": EXACT}], "has_more": False})
            if query.get("scenario") == ["malformed"]:
                return self.reply(200, {"data": [], "has_more": True})
            if query.get("scenario") == ["forbidden"]:
                return self.fault(403, "forbidden", "Table access denied")
            if query.get("scenario") == ["slow"]:
                time.sleep(0.3)
            if query.get("market.eq") not in (None, ["BTC/USDC + test"]):
                return self.fault(400, "invalid_filter", "Filter did not survive URL encoding")
            cursor = query.get("cursor")
            if cursor == ["page:1+/"]:
                more = query.get("scenario") == ["repeat"]
                return self.reply(200, {"data": [{"id": "18446744073709551615", "price": None}],
                                        "has_more": more, "next_cursor": "page:1+/" if more else None})
            if cursor is not None:
                return self.fault(400, "invalid_cursor", "Unexpected cursor")
            return self.reply(200, {"data": [{"id": "18446744073709551614", "price": EXACT}],
                                    "has_more": True, "next_cursor": "page:1+/"})
        if url.path == "/stream":
            return self.websocket()
        self.fault(404, "route_not_found", "Unknown fixture route")

    def websocket(self):
        key = self.headers.get("Sec-WebSocket-Key")
        if key is None:
            return self.fault(400, "invalid_upgrade", "A WebSocket upgrade is required")
        accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest())
        self.send_response_only(101)
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept.decode())
        self.end_headers()
        self.close_connection = True
        self.connection.settimeout(6)
        try:
            command = json.loads(self.read_frame())
            if command.get("action") != "subscribe" or command.get("token") != KEY:
                return self.send_frame({"type": "error", "code": "unauthorized", "message": "Invalid stream token"})
            subscription, = command["subscriptions"]
            filters = subscription.get("filters") or {}
            self.send_frame({"type": "ack", "action": "subscribe", "changed": 1,
                             "active_subscriptions": 1, "request_id": command["request_id"]})
            if filters.get("scenario") == "quiet":
                self.rfile.read(1)
                return
            if filters.get("scenario") == "malformed":
                self.send_frame({"type": "data", "table": TABLE})
                return
            cursor = subscription.get("cursor")
            if cursor not in (None, "stream:1"):
                return self.send_frame({"type": "error", "code": "invalid_cursor", "message": "Invalid resume cursor"})
            page = 1 if cursor is None else 2
            self.send_frame({"type": "data", "table": TABLE, "filters": filters,
                             "cursor": f"stream:{page}", "caught_up": page == 2,
                             "data": [{"page": page, "price": EXACT}]})
            if page == 2:
                self.send_frame(struct.pack("!H", 1000), opcode=8)
                self.rfile.read(1)
            else:
                self.connection.shutdown(socket.SHUT_RDWR)
        except (OSError, EOFError, ValueError, KeyError):
            pass

    def read_frame(self):
        head = self.rfile.read(2)
        if len(head) != 2:
            raise EOFError
        size = head[1] & 127
        if size == 126:
            size = struct.unpack("!H", self.rfile.read(2))[0]
        elif size == 127:
            size = struct.unpack("!Q", self.rfile.read(8))[0]
        mask = self.rfile.read(4)
        return bytes(value ^ mask[i % 4] for i, value in enumerate(self.rfile.read(size)))

    def send_frame(self, value, opcode=1):
        body = json.dumps(value).encode() if opcode == 1 else value
        head = bytes([128 | opcode])
        if len(body) < 126:
            head += bytes([len(body)])
        else:
            head += bytes([126]) + struct.pack("!H", len(body))
        self.wfile.write(head + body)
        self.wfile.flush()


def server():
    return ThreadingHTTPServer(("127.0.0.1", 0), Handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ready-file", type=Path)
    args = parser.parse_args()
    with server() as fixture:
        url = f"http://127.0.0.1:{fixture.server_port}"
        if args.ready_file:
            args.ready_file.write_text(url)
        else:
            print(url, flush=True)
        fixture.serve_forever()
