const std = @import("std");
const routes = @import("generated/routes.zig");

pub const A = std.mem.Allocator;
pub const Value = std.json.Value;
pub const Operation = routes.Operation;
pub const max_body = 16 * 1024 * 1024 + 4096;

pub const DataTime = union(enum) {
    text: []const u8,
    millis: i64,

    pub fn jsonStringify(self: DataTime, writer: *std.json.Stringify) !void {
        switch (self) {
            .text => |value| try writer.write(value),
            .millis => |value| try writer.write(value),
        }
    }
};

pub const Fault = struct {
    kind: []const u8,
    message: []const u8,
    status_code: ?u16 = null,
    code: ?[]const u8 = null,
    resolution: ?[]const u8 = null,
    body: ?[]const u8 = null,
    reset_at: ?[]const u8 = null,
};

pub const Result = union(enum) {
    ok: Value,
    fault: Fault,

    pub fn encode(self: Result, a: A) ![]u8 {
        return switch (self) {
            .ok => |value| std.json.Stringify.valueAlloc(a, .{ .ok = value }, .{}),
            .fault => |detail| std.json.Stringify.valueAlloc(a, .{ .@"error" = detail }, .{ .emit_null_optional_fields = false }),
        };
    }
};

pub fn fault(kind: []const u8, message: []const u8) Result {
    return .{ .fault = .{ .kind = kind, .message = message } };
}

pub fn json(a: A, bytes: []const u8) !Value {
    return std.json.parseFromSliceLeaky(Value, a, bytes, .{ .parse_numbers = false });
}

pub fn from(a: A, value: anytype) !Value {
    return json(a, try std.json.Stringify.valueAlloc(a, value, .{ .emit_null_optional_fields = false }));
}

pub fn get(value: Value, key: []const u8) Value {
    return if (value == .object) value.object.get(key) orelse .null else .null;
}

pub fn string(value: Value) ?[]const u8 {
    return if (value == .string) value.string else null;
}

pub fn field(value: Value, key: []const u8) ?[]const u8 {
    return string(get(value, key));
}

fn scalar(value: Value) ?[]const u8 {
    return switch (value) {
        .string => value.string,
        .number_string => value.number_string,
        .bool => if (value.bool) "true" else "false",
        else => null,
    };
}

pub const eq = std.ascii.eqlIgnoreCase;

fn escaped(a: A, out: *std.ArrayList(u8), bytes: []const u8) !void {
    const hex = "0123456789ABCDEF";
    for (bytes) |byte| {
        if (std.ascii.isAlphanumeric(byte) or std.mem.indexOfScalar(u8, "-._~", byte) != null) {
            try out.append(a, byte);
        } else {
            try out.appendSlice(a, &.{ '%', hex[byte >> 4], hex[byte & 15] });
        }
    }
}

fn queryParam(a: A, out: *std.ArrayList(u8), first: *bool, key: []const u8, value: []const u8) !void {
    try out.append(a, if (first.*) '?' else '&');
    first.* = false;
    try escaped(a, out, key);
    try out.append(a, '=');
    try escaped(a, out, value);
}

pub fn validTable(table: []const u8) bool {
    var parts = std.mem.splitScalar(u8, table, '.');
    for (0..2) |_| {
        const part = parts.next() orelse return false;
        if (part.len == 0) return false;
        for (part) |byte| {
            if (!std.ascii.isAlphanumeric(byte) and byte != '_') return false;
        }
    }
    return parts.next() == null;
}

pub fn endpoint(raw: []const u8, ws: bool, allow_query: bool) !std.Uri {
    for (raw) |ch| if (ch <= 32 or ch == 127 or ch == '\\') return error.InvalidEndpoint;
    const uri = try std.Uri.parse(raw);
    const accepts = if (ws) eq(uri.scheme, "ws") or eq(uri.scheme, "wss") else eq(uri.scheme, "http") or eq(uri.scheme, "https");
    if (!accepts or uri.host == null or uri.user != null or uri.password != null or
        (!allow_query and uri.query != null) or uri.fragment != null) return error.InvalidEndpoint;
    return uri;
}

pub fn prepare(a: A, operation: Operation, base_url: []const u8, key: ?[]const u8, query: Value) !Result {
    const route = routes.route(operation);
    const credential = if (key) |value| if (value.len > 0) value else null else null;
    if (route.auth == .required and credential == null)
        return fault("unauthorized", "API key is required for this endpoint");
    if (route.auth != .none and credential != null) {
        for (credential.?) |ch| if (ch < 33 or ch > 126) return fault("request", "Invalid API key");
    }

    _ = endpoint(base_url, false, false) catch return fault("request", "Invalid API base URL");

    var url: std.ArrayList(u8) = .empty;
    try url.appendSlice(a, std.mem.trimEnd(u8, base_url, "/"));
    if (route.path_key) |path_key| {
        const value = field(query, path_key) orelse return fault("request", "Missing endpoint path parameter");
        if (operation == .data and !validTable(value)) return fault("request", "Expected a database.table identifier");
        if (operation == .candles and !eq(value, "uniswap-v2") and !eq(value, "uniswap-v3") and !eq(value, "uniswap-v4"))
            return fault("request", "Expected a published candle source");
        try url.appendSlice(a, route.path[0..std.mem.indexOfScalar(u8, route.path, '{').?]);
        try escaped(a, &url, value);
    } else {
        try url.appendSlice(a, route.path);
    }

    if (operation == .data) {
        if (scalar(get(query, "limit"))) |value| {
            const limit = std.fmt.parseInt(u16, value, 10) catch 0;
            if (limit < 1 or limit > 1000) return fault("request", "Data page limit must be between 1 and 1000");
        }
    }
    if (operation == .sql) {
        const statement = field(query, "query") orelse return fault("request", "SQL query is required");
        if (statement.len == 0 or statement.len > 4096) return fault("request", "SQL query must be 1 to 4096 UTF-8 bytes");
    }
    if (operation == .candles) {
        const interval = field(query, "interval") orelse return fault("request", "Candle interval is required");
        const intervals = [_][]const u8{ "1s", "1m", "5m", "15m", "1h", "1d" };
        const durations = [_]i64{ 1000, 60_000, 300_000, 900_000, 3_600_000, 86_400_000 };
        const index = for (intervals, 0..) |value, i| {
            if (std.mem.eql(u8, interval, value)) break i;
        } else return fault("request", "Unsupported candle interval");
        const duration = durations[index];
        const start = std.fmt.parseInt(i64, scalar(get(query, "start")) orelse "", 10) catch -1;
        const end = std.fmt.parseInt(i64, scalar(get(query, "end")) orelse "", 10) catch -1;
        const chain_id = std.fmt.parseInt(i64, scalar(get(query, "chain_id")) orelse "", 10) catch -1;
        if (chain_id < 0 or (field(query, "pool") orelse @as([]const u8, "")).len == 0 or
            (eq(field(query, "source").?, "uniswap-v4") and (field(query, "manager") orelse @as([]const u8, "")).len == 0))
            return fault("request", "Candle pool and chain are required, with a manager for Uniswap V4");
        if (start < 0 or end <= start or end - start > 86_400_000 or
            @mod(start, duration) != 0 or @mod(end, duration) != 0 or @divTrunc(end - start, duration) > 1000)
            return fault("request", "Expected aligned candle bounds with an exclusive end and at most 1000 windows in one day");
    }

    var first = true;
    for (route.query_keys) |name| {
        const value = get(query, name);
        if (value != .null) {
            try queryParam(a, &url, &first, name, scalar(value) orelse return fault("request", "Invalid query parameter"));
        }
    }
    if (operation == .data) {
        const filters = get(query, "filters");
        if (filters == .object) {
            var entries = filters.object.iterator();
            while (entries.next()) |entry| {
                for (route.query_keys) |name| {
                    if (std.mem.eql(u8, entry.key_ptr.*, name))
                        return fault("request", "Use column.eq for filters on reserved control names");
                }
                const value = scalar(entry.value_ptr.*) orelse return fault("request", "Table filters must be scalar");
                try queryParam(a, &url, &first, entry.key_ptr.*, value);
            }
        } else if (filters != .null) return fault("request", "Expected a table filter map");
    }
    if (operation == .sql) {
        const authority = std.mem.indexOf(u8, url.items, "://").? + 3;
        const target = std.mem.indexOfScalarPos(u8, url.items, authority, '/') orelse url.items.len;
        if (url.items.len - target > 8192) return fault("request", "Encoded SQL target exceeds 8192 bytes");
    }
    return .{ .ok = try from(a, .{
        .url = url.items,
        .authorization = if (route.auth != .none and credential != null)
            try std.fmt.allocPrint(a, "Bearer {s}", .{credential.?})
        else
            null,
    }) };
}

fn validRows(rows: Value) bool {
    if (rows != .array) return false;
    for (rows.array.items) |row| if (row != .object) return false;
    return true;
}

fn validPage(value: Value) bool {
    if (!validRows(get(value, "data"))) return false;
    const more = get(value, "has_more");
    const cursor = get(value, "next_cursor");
    return more == .bool and (cursor == .null or cursor == .string) and
        (!more.bool or (cursor == .string and cursor.string.len > 0));
}

fn unsigned(value: Value) bool {
    if (value != .number_string) return false;
    _ = std.fmt.parseInt(u64, value.number_string, 10) catch return false;
    return true;
}

fn validColumns(value: Value) bool {
    if (value != .array) return false;
    for (value.array.items) |column| {
        if (field(column, "name") == null or field(column, "type") == null) return false;
    }
    return true;
}

fn validCoverage(value: Value) bool {
    const tables = get(value, "tables");
    if (tables != .array or field(value, "updatedAt") == null or !unsigned(get(value, "total"))) return false;
    for (tables.array.items) |table| {
        if (field(table, "database") == null or field(table, "table") == null or !validColumns(get(table, "columns")))
            return false;
        const start = table.object.get("start") orelse return false;
        const end = table.object.get("end") orelse return false;
        const count = table.object.get("row_count") orelse return false;
        if ((start != .null and start != .string) or (end != .null and end != .string) or
            (count != .null and !unsigned(count))) return false;
    }
    return true;
}

fn validSql(value: Value) bool {
    const columns = get(value, "columns");
    const data = get(value, "data");
    if (field(value, "query_id") == null or !validColumns(columns) or data != .array or !unsigned(get(value, "rows")))
        return false;
    for (data.array.items) |row| {
        if (row != .array or row.array.items.len != columns.array.items.len) return false;
    }
    return true;
}

pub fn finish(a: A, operation: Operation, status: u16, body: []const u8, reset_at: ?[]const u8) Result {
    if (status < 200 or status > 299) {
        const payload = json(a, body) catch Value.null;
        const detail = get(payload, "error");
        return .{ .fault = .{
            .kind = switch (status) {
                401 => "unauthorized",
                402, 403 => "access_denied",
                404 => "not_found",
                429 => "rate_limited",
                else => "request",
            },
            .message = field(detail, "message") orelse field(payload, "message") orelse "Polaris request failed",
            .status_code = status,
            .body = if (std.unicode.utf8ValidateSlice(body)) body else null,
            .code = field(detail, "code"),
            .resolution = field(detail, "resolution"),
            .reset_at = reset_at,
        } };
    }
    if (body.len > max_body) return fault("invalid_response", "Polaris response exceeds the maximum body size");
    if (!std.unicode.utf8ValidateSlice(body)) return fault("invalid_response", "Polaris response is not UTF-8");
    const value = json(a, body) catch return fault("invalid_response", "Invalid JSON response");
    if (value != .object) return fault("invalid_response", "Expected a JSON object response");
    const valid = switch (operation) {
        .data, .candles => validPage(value),
        .health => get(value, "ok") == .bool,
        .coverage => validCoverage(value),
        .sql => validSql(value),
        .openapi => true,
    };
    return if (valid) .{ .ok = value } else fault("invalid_response", "Invalid Polaris response envelope");
}

pub const CursorSet = struct {
    allocator: A,
    seen: std.StringHashMapUnmanaged(void) = .empty,

    pub fn remember(self: *CursorSet, cursor: []const u8) !bool {
        if (self.seen.contains(cursor)) return false;
        const owned = try self.allocator.dupe(u8, cursor);
        errdefer self.allocator.free(owned);
        try self.seen.put(self.allocator, owned, {});
        return true;
    }

    pub fn deinit(self: *CursorSet) void {
        var keys = self.seen.keyIterator();
        while (keys.next()) |key| self.allocator.free(key.*);
        self.seen.deinit(self.allocator);
    }
};

pub const Session = struct {
    arena: std.heap.ArenaAllocator,
    subscriptions: []Subscription,
    key: ?[]const u8,
    pending: ?usize = null,
    acknowledged: bool = false,

    const Subscription = struct {
        table: []const u8,
        filters: Value,
        committed: ?[]const u8,
        delivered: ?[]const u8 = null,
    };

    pub fn init(a: A, query_bytes: []const u8, api_key: ?[]const u8) !Session {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const alloc = arena.allocator();
        const query = try json(alloc, try alloc.dupe(u8, query_bytes));
        const list = get(query, "subscriptions");
        if (list != .array or list.array.items.len < 1 or list.array.items.len > 1000) return error.InvalidSubscriptions;
        const subscriptions = try alloc.alloc(Subscription, list.array.items.len);
        var initialized: usize = 0;
        errdefer for (subscriptions[0..initialized]) |sub| {
            if (sub.committed) |owned| a.free(owned);
        };
        for (list.array.items, 0..) |entry, i| {
            const table = field(entry, "table") orelse return error.InvalidSubscriptions;
            if (!validTable(table)) return error.InvalidSubscriptions;
            const filters = get(entry, "filters");
            if (filters != .null and filters != .object) return error.InvalidSubscriptions;
            if (filters == .object) {
                for (filters.object.values()) |value| if (value != .string) return error.InvalidSubscriptions;
            }
            const cursor = get(entry, "cursor");
            if (cursor != .null and (cursor != .string or cursor.string.len == 0)) return error.InvalidSubscriptions;
            for (subscriptions[0..i]) |sub| {
                if (std.mem.eql(u8, table, sub.table) and sameFilters(filters, sub.filters)) return error.InvalidSubscriptions;
            }
            subscriptions[i] = .{
                .table = table,
                .filters = filters,
                .committed = if (cursor == .string) try a.dupe(u8, cursor.string) else null,
            };
            initialized += 1;
        }
        const key = if (api_key) |token| try alloc.dupe(u8, token) else null;
        return .{ .arena = arena, .subscriptions = subscriptions, .key = key };
    }

    pub fn deinit(self: *Session) void {
        const a = self.arena.child_allocator;
        for (self.subscriptions) |sub| {
            if (sub.committed) |cursor| a.free(cursor);
            if (sub.delivered) |cursor| a.free(cursor);
        }
        self.arena.deinit();
    }

    pub fn subscribe(self: *Session, a: A) ![]u8 {
        self.acknowledged = false;
        const Wire = struct { table: []const u8, filters: ?Value, cursor: ?[]const u8 };
        const list = try a.alloc(Wire, self.subscriptions.len);
        defer a.free(list);
        for (self.subscriptions, list) |sub, *entry| entry.* = .{
            .table = sub.table,
            .filters = if (sub.filters == .null) null else sub.filters,
            .cursor = sub.committed,
        };
        return std.json.Stringify.valueAlloc(a, .{
            .action = "subscribe",
            .request_id = "polaris-sdk-subscribe",
            .token = if (self.key) |key| if (key.len != 0) key else null else null,
            .subscriptions = list,
        }, .{ .emit_null_optional_fields = false });
    }

    pub fn advance(self: *Session) void {
        const index = self.pending orelse return;
        const sub = &self.subscriptions[index];
        if (sub.committed) |cursor| self.arena.child_allocator.free(cursor);
        sub.committed = sub.delivered;
        sub.delivered = null;
        self.pending = null;
    }

    pub fn discard(self: *Session) void {
        if (self.pending) |index| {
            const sub = &self.subscriptions[index];
            self.arena.child_allocator.free(sub.delivered.?);
            sub.delivered = null;
            self.pending = null;
        }
    }

    pub fn receive(self: *Session, a: A, bytes: []const u8) !Result {
        if (bytes.len > max_body or !std.unicode.utf8ValidateSlice(bytes))
            return fault("stream_decode", "Invalid WebSocket text message");
        const value = json(a, bytes) catch return fault("stream_decode", "Invalid WebSocket JSON");
        const kind = field(value, "type") orelse return fault("stream_protocol", "Missing WebSocket message type");
        if (std.mem.eql(u8, kind, "error")) return .{ .fault = .{
            .kind = "stream_protocol",
            .message = field(value, "message") orelse "Stream request failed",
            .code = field(value, "code"),
        } };
        if (std.mem.eql(u8, kind, "pong") and self.acknowledged) return .{ .ok = value };
        if (std.mem.eql(u8, kind, "ack") and !self.acknowledged and
            std.mem.eql(u8, field(value, "action") orelse "", "subscribe") and
            std.mem.eql(u8, field(value, "request_id") orelse "", "polaris-sdk-subscribe"))
        {
            self.acknowledged = true;
            return .{ .ok = value };
        }
        if (!self.acknowledged or !std.mem.eql(u8, kind, "data"))
            return fault("stream_protocol", "Unexpected WebSocket message");
        const table = field(value, "table") orelse return fault("stream_protocol", "Missing table in stream page");
        const cursor = field(value, "cursor") orelse return fault("stream_protocol", "Missing stream cursor");
        const filters = get(value, "filters");
        if (cursor.len == 0 or filters != .object or get(value, "caught_up") != .bool or !validRows(get(value, "data")))
            return fault("stream_protocol", "Invalid WebSocket data page");
        for (self.subscriptions, 0..) |*sub, i| {
            if (std.mem.eql(u8, table, sub.table) and sameFilters(filters, sub.filters)) {
                if (self.pending != null) return fault("stream_protocol", "Previous stream page is still pending");
                sub.delivered = try self.arena.child_allocator.dupe(u8, cursor);
                self.pending = i;
                return .{ .ok = value };
            }
        }
        return fault("stream_protocol", "WebSocket data did not match a subscription");
    }
};

fn sameFilters(left: Value, right: Value) bool {
    const left_len = if (left == .object) left.object.count() else 0;
    const right_len = if (right == .object) right.object.count() else 0;
    if (left_len != right_len) return false;
    if (left_len == 0) return true;
    var iterator = left.object.iterator();
    while (iterator.next()) |entry| {
        const l = string(entry.value_ptr.*) orelse return false;
        const r = field(right, entry.key_ptr.*) orelse return false;
        if (!std.mem.eql(u8, l, r)) return false;
    }
    return true;
}

test "retain exact tokens and reject invalid pages" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const body = "{\"data\":[{\"decimal\":\"0.123456789123456789\",\"id\":18446744073709551615}],\"has_more\":false}";
    const value = try finish(a, .data, 200, body, null).encode(a);
    try std.testing.expectEqualStrings("{\"ok\":" ++ body ++ "}", value);
    try std.testing.expect(finish(a, .data, 200, "{\"data\":[],\"has_more\":true}", null) == .fault);
}

test "commit a stream cursor only on advance" {
    var session = try Session.init(std.testing.allocator, "{\"subscriptions\":[{\"table\":\"standard.trades\",\"cursor\":\"first\"}]}", null);
    defer session.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    _ = try session.subscribe(a);
    const ack = "{\"type\":\"ack\",\"action\":\"subscribe\",\"request_id\":\"polaris-sdk-subscribe\"}";
    try std.testing.expect((try session.receive(a, ack)) == .ok);
    try std.testing.expect((try session.receive(a, "{\"type\":\"data\",\"table\":\"standard.trades\",\"filters\":{},\"cursor\":\"second\",\"caught_up\":true,\"data\":[]}")) == .ok);
    try std.testing.expect(std.mem.indexOf(u8, try session.subscribe(a), "first") != null);
    session.advance();
    try std.testing.expect(std.mem.indexOf(u8, try session.subscribe(a), "second") != null);
}

test "resume only consumed pages for each subscription" {
    const input =
        \\{"subscriptions":[
        \\  {"table":"standard.trades","filters":{"market":"A"}},
        \\  {"table":"standard.trades","filters":{"market":"B"},"cursor":"prior"}
        \\]}
    ;
    var session = try Session.init(std.testing.allocator, input, "token");
    defer session.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const temp = arena.allocator();
    _ = try session.subscribe(temp);
    const ack = "{\"type\":\"ack\",\"action\":\"subscribe\",\"request_id\":\"polaris-sdk-subscribe\"}";
    try std.testing.expect((try session.receive(temp, ack)) == .ok);
    const first = "{\"type\":\"data\",\"table\":\"standard.trades\",\"filters\":{\"market\":\"A\"},\"cursor\":\"consumed\",\"caught_up\":true,\"data\":[]}";
    const second = "{\"type\":\"data\",\"table\":\"standard.trades\",\"filters\":{\"market\":\"B\"},\"cursor\":\"not-delivered\",\"caught_up\":false,\"data\":[]}";
    try std.testing.expect((try session.receive(temp, first)) == .ok);
    session.advance();
    try std.testing.expect((try session.receive(temp, second)) == .ok);
    session.discard();
    const resumed = get(try json(temp, try session.subscribe(temp)), "subscriptions").array.items;
    try std.testing.expectEqualStrings("consumed", field(resumed[0], "cursor").?);
    try std.testing.expectEqualStrings("prior", field(resumed[1], "cursor").?);
}
