const std = @import("std");
const builtin = @import("builtin");
const p = @import("protocol.zig");
pub const models = @import("generated/models.zig");
pub const DataTime = p.DataTime;
const Io = std.Io;
const a = std.heap.c_allocator;

var threaded: ?Io.Threaded = null;
var startup: Io.Mutex = .init;

fn runtime() Io {
    const init_io = Io.Threaded.global_single_threaded.io();
    startup.lockUncancelable(init_io);
    defer startup.unlock(init_io);
    if (threaded == null) threaded = .init(a, .{});
    return threaded.?.io();
}

pub const Options = struct {
    base_url: []const u8 = "https://api.polaris.supply",
    stream_url: ?[]const u8 = null,
    api_key: ?[]const u8 = null,
    timeout_ms: u32 = 30_000,
};

pub fn Reply(T: type) type {
    return struct {
        arena: std.heap.ArenaAllocator,
        result: union(enum) { ok: T, fault: p.Fault },

        pub fn deinit(self: *@This()) void {
            self.arena.deinit();
        }
    };
}

fn convert(T: type, arena: *std.heap.ArenaAllocator, result: p.Result) !Reply(T) {
    const alloc = arena.allocator();
    const converted: Reply(T) = .{
        .arena = undefined,
        .result = switch (result) {
            .ok => |value| .{ .ok = if (T == p.Value) value else try std.json.parseFromValueLeaky(T, alloc, value, .{ .ignore_unknown_fields = true }) },
            .fault => |detail| .{ .fault = detail },
        },
    };
    var reply = converted;
    reply.arena = arena.*;
    return reply;
}

pub fn requestAs(T: type, allocator: p.A, options: Options, operation: p.Operation, query: anytype) !Reply(T) {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const temp = arena.allocator();
    const result = try request(temp, options, operation, try p.from(temp, query));
    return convert(T, &arena, result);
}

fn duration(ms: u32) Io.Timeout {
    return .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(ms) } };
}

fn websocketRandom(io: Io, data: []u8) !void {
    io.randomSecure(data) catch |err| {
        if (builtin.os.tag != .linux or err != error.EntropyUnavailable) return err;
        // The Linux archive targeting glibc 2.17 can fail Zig's entropy probe.
        // Read from the kernel's random device through regular file I/O.
        const random = try Io.Dir.openFileAbsolute(io, "/dev/urandom", .{});
        defer random.close(io);
        var reader = random.readerStreaming(io, &.{});
        try reader.interface.readSliceAll(data);
    };
}

fn within(io: Io, comptime work: anytype, args: std.meta.ArgsTuple(@TypeOf(work)), timeout_ms: u32, closed: ?*Io.Event) @typeInfo(@TypeOf(work)).@"fn".return_type.? {
    const T = @typeInfo(@TypeOf(work)).@"fn".return_type.?;
    const Choice = union(enum) { work: T, timeout: Io.Cancelable!void, close: Io.Cancelable!void };
    var queue: [3]Choice = undefined;
    var select = Io.Select(Choice).init(io, &queue);
    defer select.cancelDiscard();
    try select.concurrent(.work, work, args);
    if (timeout_ms != 0) try select.concurrent(.timeout, Io.Timeout.sleep, .{ duration(timeout_ms), io });
    if (closed) |event| try select.concurrent(.close, Io.Event.wait, .{ event, io });
    return switch (try select.await()) {
        .work => |value| value,
        .timeout => error.Timeout,
        .close => error.Closed,
    };
}

pub fn request(temp: p.A, options: Options, operation: p.Operation, query: p.Value) !p.Result {
    const prepared = try p.prepare(temp, operation, options.base_url, options.api_key, query);
    if (prepared == .fault) return prepared;
    const uri = try std.Uri.parse(p.field(prepared.ok, "url").?);
    const token = p.field(prepared.ok, "authorization");
    const io = runtime();
    return within(io, execute, .{ io, temp, operation, uri, token }, options.timeout_ms, null) catch |err| {
        return if (err == error.Timeout)
            p.fault("request", "Request timed out")
        else if (err == error.StreamTooLong)
            p.fault("invalid_response", "Polaris response exceeds the maximum body size")
        else
            p.fault("request", try std.fmt.allocPrint(temp, "HTTP request failed: {s}", .{@errorName(err)}));
    };
}

fn execute(io: Io, temp: p.A, operation: p.Operation, initial: std.Uri, credential: ?[]const u8) anyerror!p.Result {
    var http: std.http.Client = .{ .io = io, .allocator = a, .read_buffer_size = 32 * 1024 };
    defer http.deinit();
    var uri = initial;
    var token = credential;
    for (0..6) |i| {
        var req = try http.request(.GET, uri, .{
            .keep_alive = false,
            .redirect_behavior = .unhandled,
            .headers = .{ .authorization = if (token) |value| .{ .override = value } else .omit },
            .extra_headers = &.{.{ .name = "Accept", .value = "application/json" }},
        });
        defer {
            if (req.connection) |conn| conn.closing = true;
            req.deinit();
        }
        try req.sendBodiless();
        var response = try req.receiveHead(&.{});
        const status = @backingInt(response.head.status);
        if (status == 301 or status == 302 or status == 303 or status == 307 or status == 308) {
            if (response.head.location) |location| {
                if (i == 5 or location.len >= 8192) return error.InvalidHttpRedirect;
                var buffer = try temp.alloc(u8, 8192);
                @memcpy(buffer[0..location.len], location);
                const next = try uri.resolveInPlace(location.len, &buffer);
                if (next.user != null or next.password != null or next.host == null or
                    (!p.eq(next.scheme, "http") and !p.eq(next.scheme, "https"))) return error.InvalidHttpRedirect;
                if (!try sameOrigin(uri, next)) token = null;
                uri = next;
                continue;
            }
        }
        var reset: ?[]const u8 = null;
        var headers = response.head.iterateHeaders();
        while (headers.next()) |header| {
            if (p.eq(header.name, "x-ratelimit-reset") or (reset == null and p.eq(header.name, "retry-after")))
                reset = try temp.dupe(u8, header.value);
        }
        const size: usize = switch (response.head.content_encoding) {
            .identity => 0,
            .gzip, .deflate => std.compress.flate.max_window_len,
            .zstd => std.compress.zstd.default_window_len + std.compress.zstd.block_size_max,
            else => return error.UnsupportedCompression,
        };
        var buffer: [8192]u8 = undefined;
        var decompressor: std.http.Decompress = undefined;
        const reader = response.readerDecompressing(&buffer, &decompressor, try temp.alloc(u8, size));
        const body = try reader.allocRemaining(temp, .limited(p.max_body));
        return p.finish(temp, operation, status, body, reset);
    }
    unreachable;
}

fn sameOrigin(left: std.Uri, right: std.Uri) !bool {
    if (!p.eq(left.scheme, right.scheme)) return false;
    const port: u16 = if (p.eq(left.scheme, "https")) 443 else 80;
    if ((left.port orelse port) != (right.port orelse port)) return false;
    const HostName = Io.net.HostName;
    var l: [HostName.max_len]u8 = undefined;
    var r: [HostName.max_len]u8 = undefined;
    return p.eq((try HostName.fromUri(left, &l)).bytes, (try HostName.fromUri(right, &r)).bytes);
}

pub const StreamQuery = struct {
    subscriptions: []const models.SubscriptionRequest,
};

pub fn openStream(client: anytype, query: StreamQuery) !*Stream {
    const input = try std.json.Stringify.valueAlloc(a, query, .{ .emit_null_optional_fields = false });
    defer a.free(input);
    return Stream.init(client.options, input);
}

pub const DataRows = struct {
    options: Options,
    allocator: p.A,
    cursors: p.CursorSet,
    query: models.DataQuery,
    current: ?Reply(models.DataResponse) = null,
    index: usize = 0,
    done: bool = false,

    /// Rows are borrowed until the next page is loaded or the iterator is closed.
    pub fn next(self: *DataRows) !?p.Value {
        if (self.done) return null;
        errdefer self.done = true;
        if (self.current) |*current| {
            const page = current.result.ok;
            if (self.index < page.data.len) {
                defer self.index += 1;
                return page.data[self.index];
            }
            if (!page.has_more) {
                self.done = true;
                return null;
            }
        }
        while (true) {
            var response = try requestAs(models.DataResponse, self.allocator, self.options, .data, self.query);
            if (response.result == .fault) {
                self.done = true;
                if (self.current) |*current| current.deinit();
                self.current = response;
                return error.PolarisRequest;
            }
            errdefer response.deinit();
            const page = response.result.ok;
            if (page.has_more and !try self.cursors.remember(page.next_cursor.?)) return error.RepeatedCursor;
            self.query.cursor = page.next_cursor;
            if (self.current) |*current| current.deinit();
            self.current = response;
            self.index = 0;
            if (page.data.len != 0) {
                self.index = 1;
                return page.data[0];
            }
            if (!page.has_more) {
                self.done = true;
                return null;
            }
        }
    }

    pub fn diagnostic(self: DataRows) ?p.Fault {
        const current = self.current orelse return null;
        return if (current.result == .fault) current.result.fault else null;
    }

    pub fn deinit(self: *DataRows) void {
        if (self.current) |*current| current.deinit();
        self.cursors.deinit();
    }
};

pub fn dataRows(client: anytype, allocator: p.A, query: models.DataQuery) !DataRows {
    var cursor_set: p.CursorSet = .{ .allocator = allocator };
    errdefer cursor_set.deinit();
    if (query.cursor) |cursor| _ = try cursor_set.remember(cursor);
    return .{ .options = client.options, .allocator = allocator, .cursors = cursor_set, .query = query };
}

pub const Stream = struct {
    io: Io,
    session: p.Session,
    endpoint: []const u8,
    http: std.http.Client,
    timeout_ms: u32,
    stopped: Io.Event = .unset,
    connection: ?*std.http.Client.Connection = null,
    connected_once: bool = false,
    backoff_ms: u32 = 500,
    connected_ms: i64 = 0,
    write_lock: Io.Mutex = .init,
    pong: std.atomic.Value(bool) = .init(false),

    pub fn init(options: Options, input: []const u8) !*Stream {
        const self = try a.create(Stream);
        errdefer a.destroy(self);
        const io = runtime();
        const uri = try p.endpoint(options.stream_url orelse options.base_url, options.stream_url != null, options.stream_url != null);
        var url = uri;
        const allow_http = options.stream_url == null;
        url.scheme = if (p.eq(uri.scheme, "wss") or (allow_http and p.eq(uri.scheme, "https"))) "https" else if (p.eq(uri.scheme, "ws") or (allow_http and p.eq(uri.scheme, "http"))) "http" else return error.InvalidStreamUrl;
        if (allow_http) {
            url.path = .{ .raw = "/stream" };
            url.query = null;
        }
        const endpoint = try std.fmt.allocPrint(a, "{f}", .{url});
        errdefer a.free(endpoint);
        self.* = .{
            .endpoint = endpoint,
            .io = io,
            .session = try .init(a, input, options.api_key),
            .http = .{ .allocator = a, .io = io },
            .timeout_ms = options.timeout_ms,
        };
        return self;
    }

    pub fn cancel(self: *Stream) void {
        self.stopped.set(self.io);
    }

    pub fn deinit(self: *Stream) void {
        self.cancel();
        self.disconnect();
        self.http.deinit();
        self.session.deinit();
        a.free(self.endpoint);
        a.destroy(self);
    }

    fn disconnect(self: *Stream) void {
        if (self.connection) |conn| {
            conn.closing = true;
            self.http.connection_pool.release(conn, self.io);
            self.connection = null;
        }
    }

    fn connect(self: *Stream) anyerror!void {
        var random: [16]u8 = undefined;
        try websocketRandom(self.io, &random);
        var buf: [24]u8 = undefined;
        const key = std.base64.standard.Encoder.encode(&buf, &random);
        const uri = try std.Uri.parse(self.endpoint);
        var req = try self.http.request(.GET, uri, .{
            .redirect_behavior = .unhandled,
            .headers = .{ .connection = .{ .override = "Upgrade" } },
            .extra_headers = &.{
                .{ .name = "Upgrade", .value = "websocket" },
                .{ .name = "Sec-WebSocket-Version", .value = "13" },
                .{ .name = "Sec-WebSocket-Key", .value = key },
            },
        });
        defer {
            if (req.connection) |conn| conn.closing = true;
            req.deinit();
        }
        try req.sendBodiless();
        const response = try req.receiveHead(&.{});
        if (response.head.status != .switching_protocols) return error.WebSocketUpgradeRejected;
        var upgrade = false;
        var connection = false;
        var accept: ?[]const u8 = null;
        var headers = response.head.iterateHeaders();
        while (headers.next()) |header| {
            if (p.eq(header.name, "upgrade")) upgrade = p.eq(header.value, "websocket");
            if (p.eq(header.name, "connection")) {
                var tokens = std.mem.tokenizeAny(u8, header.value, ", \t");
                while (tokens.next()) |value| if (p.eq(value, "upgrade")) {
                    connection = true;
                    break;
                };
            }
            if (p.eq(header.name, "sec-websocket-accept")) accept = header.value;
            if (p.eq(header.name, "sec-websocket-extensions")) return error.InvalidWebSocketHandshake;
        }
        var digest: [20]u8 = undefined;
        var sha = std.crypto.hash.Sha1.init(.{});
        sha.update(key);
        sha.update("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        sha.final(&digest);
        var expected: [28]u8 = undefined;
        const value = std.base64.standard.Encoder.encode(&expected, &digest);
        if (!upgrade or !connection or !std.mem.eql(u8, accept orelse "", value)) return error.InvalidWebSocketHandshake;
        const command = try self.session.subscribe(a);
        defer a.free(command);
        try self.write(req.connection.?, 1, command);
        self.connection = req.connection.?;
        req.connection = null;
    }

    fn write(self: *Stream, conn: *std.http.Client.Connection, opcode: u8, payload: []const u8) !void {
        try self.write_lock.lock(self.io);
        defer self.write_lock.unlock(self.io);
        const writer = conn.writer();
        try writer.writeByte(0x80 | opcode);
        if (payload.len < 126) {
            try writer.writeByte(0x80 | @as(u8, @intCast(payload.len)));
        } else if (payload.len <= 65535) {
            try writer.writeByte(0xfe);
            try writer.writeInt(u16, @intCast(payload.len), .big);
        } else {
            try writer.writeByte(0xff);
            try writer.writeInt(u64, payload.len, .big);
        }
        var mask: [4]u8 = undefined;
        try websocketRandom(self.io, &mask);
        try writer.writeAll(&mask);
        var scratch: [1024]u8 = undefined;
        var i: usize = 0;
        while (i < payload.len) {
            const count = @min(scratch.len, payload.len - i);
            for (scratch[0..count], payload[i..][0..count], 0..) |*out, ch, j| out.* = ch ^ mask[(i + j) % 4];
            try writer.writeAll(scratch[0..count]);
            i += count;
        }
        try conn.flush();
    }

    fn frame(self: *Stream, temp: p.A) !?[]const u8 {
        const conn = self.connection.?;
        const reader = conn.reader();
        var fragments: std.ArrayList(u8) = .empty;
        var fragmented = false;
        while (true) {
            const head = try reader.takeByte();
            const length_byte = try reader.takeByte();
            const fin = head & 0x80 != 0;
            const op = head & 0x0f;
            if (head & 0x70 != 0 or length_byte & 0x80 != 0) return error.InvalidWebSocketFrame;
            const n: u64 = switch (length_byte) {
                126 => try reader.takeInt(u16, .big),
                127 => try reader.takeInt(u64, .big),
                else => length_byte,
            };
            if ((length_byte == 126 and n < 126) or (length_byte == 127 and n < 65536) or n > p.max_body)
                return error.InvalidWebSocketFrame;
            if (op >= 8) {
                if (!fin or n > 125) return error.InvalidWebSocketFrame;
                var control: [125]u8 = undefined;
                const payload = control[0..@intCast(n)];
                try reader.readSliceAll(payload);
                switch (op) {
                    8 => {
                        if (n == 1) return error.InvalidWebSocketFrame;
                        const code = if (n >= 2) std.mem.readInt(u16, payload[0..2], .big) else 1000;
                        self.write(conn, 8, payload) catch {};
                        if (code == 1000 or code == 1001) return null;
                        if (code == 1008) return error.WebSocketPolicyViolation;
                        return error.WebSocketDisconnected;
                    },
                    9 => try self.write(conn, 10, payload),
                    10 => {},
                    else => return error.InvalidWebSocketFrame,
                }
                continue;
            }
            if (op != @as(u8, if (fragmented) 0 else 1) or n > p.max_body - fragments.items.len)
                return error.InvalidWebSocketFrame;
            const target = try fragments.addManyAsSlice(temp, @intCast(n));
            try reader.readSliceAll(target);
            if (fin) return fragments.items;
            fragmented = true;
        }
    }

    fn acknowledge(self: *Stream, temp: p.A) anyerror!p.Result {
        const bytes = try self.frame(temp) orelse return error.WebSocketDisconnected;
        const value = try self.session.receive(temp, bytes);
        if (value == .ok) {
            self.connected_once = true;
            self.connected_ms = std.Io.Timestamp.now(self.io, .awake).toMilliseconds();
        }
        return value;
    }

    fn readPage(self: *Stream, temp: p.A) anyerror!p.Result {
        while (true) {
            var arena = std.heap.ArenaAllocator.init(temp);
            defer arena.deinit();
            const scratch = arena.allocator();
            const bytes = try self.frame(scratch) orelse return .{ .ok = .null };
            const value = try self.session.receive(scratch, bytes);
            if (value == .fault) return .{ .fault = .{
                .kind = value.fault.kind,
                .message = try temp.dupe(u8, value.fault.message),
                .code = if (value.fault.code) |code| try temp.dupe(u8, code) else null,
            } };
            const tag = p.field(value.ok, "type").?;
            if (std.mem.eql(u8, tag, "data")) return .{ .ok = try p.json(temp, try temp.dupe(u8, bytes)) };
            if (std.mem.eql(u8, tag, "pong") and std.mem.eql(u8, p.field(value.ok, "request_id") orelse "", "polaris-sdk-ping"))
                self.pong.store(true, .release);
        }
    }

    fn heartbeat(self: *Stream) anyerror!void {
        while (true) {
            try duration(30_000).sleep(self.io);
            self.pong.store(false, .release);
            try self.write(self.connection.?, 1, "{\"action\":\"ping\",\"request_id\":\"polaris-sdk-ping\"}");
            try duration(10_000).sleep(self.io);
            if (!self.pong.load(.acquire)) return error.WebSocketDisconnected;
        }
    }

    fn readWithHeartbeat(self: *Stream, temp: p.A) !p.Result {
        const Choice = union(enum) { page: anyerror!p.Result, heartbeat: anyerror!void, close: Io.Cancelable!void };
        var queue: [3]Choice = undefined;
        var select = Io.Select(Choice).init(self.io, &queue);
        defer select.cancelDiscard();
        try select.concurrent(.page, readPage, .{ self, temp });
        try select.concurrent(.heartbeat, heartbeat, .{self});
        try select.concurrent(.close, Io.Event.wait, .{ &self.stopped, self.io });
        return switch (try select.await()) {
            .page => |page| page,
            .heartbeat => error.WebSocketDisconnected,
            .close => error.Closed,
        };
    }

    fn reconnect(self: *Stream) bool {
        if (self.connection != null and self.session.acknowledged and
            std.Io.Timestamp.now(self.io, .awake).toMilliseconds() - self.connected_ms >= 30_000) self.backoff_ms = 500;
        self.disconnect();
        self.session.discard();
        const wait = self.backoff_ms;
        self.backoff_ms = @min(30_000, self.backoff_ms * 2);
        self.stopped.waitTimeout(self.io, duration(wait)) catch |err| return err == error.Timeout;
        return false;
    }

    pub fn nextResult(self: *Stream, temp: p.A) !p.Result {
        self.session.advance();
        while (!self.stopped.isSet()) {
            if (self.connection == null) {
                within(self.io, connect, .{self}, self.timeout_ms, &self.stopped) catch |err| {
                    if (self.stopped.isSet()) break;
                    if (self.connected_once and err != error.WebSocketUpgradeRejected and err != error.InvalidWebSocketHandshake) {
                        if (self.reconnect()) continue;
                        break;
                    }
                    self.cancel();
                    return p.fault("stream_connection", try std.fmt.allocPrint(temp, "WebSocket connection failed: {s}", .{@errorName(err)}));
                };
                const ack = within(self.io, acknowledge, .{ self, temp }, self.timeout_ms, &self.stopped) catch |err| {
                    if (self.stopped.isSet()) break;
                    if (self.connected_once and err != error.InvalidWebSocketFrame and err != error.WebSocketPolicyViolation) {
                        if (self.reconnect()) continue;
                        break;
                    }
                    self.cancel();
                    self.disconnect();
                    return p.fault("stream_connection", try std.fmt.allocPrint(temp, "WebSocket subscription failed: {s}", .{@errorName(err)}));
                };
                if (ack == .fault) {
                    self.cancel();
                    self.disconnect();
                    return ack;
                }
            }
            const value = self.readWithHeartbeat(temp) catch |err| {
                if (self.stopped.isSet()) break;
                if (err == error.InvalidWebSocketFrame or err == error.WebSocketPolicyViolation) {
                    self.cancel();
                    self.disconnect();
                    return p.fault("stream_protocol", "Invalid or rejected WebSocket frame");
                }
                if (self.connected_once) {
                    if (self.reconnect()) continue;
                    break;
                }
                self.cancel();
                self.disconnect();
                return p.fault("stream_connection", "WebSocket closed before subscription acknowledgement");
            };
            if (value == .fault or value.ok == .null) {
                self.cancel();
                self.disconnect();
            }
            return value;
        }
        self.disconnect();
        return .{ .ok = .null };
    }

    pub fn next(self: *Stream, allocator: p.A) !Reply(?models.TableDataMessage) {
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const result = try self.nextResult(arena.allocator());
        return convert(?models.TableDataMessage, &arena, result);
    }
};
