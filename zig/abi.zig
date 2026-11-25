const std = @import("std");
const p = @import("protocol.zig");
const native = @import("native.zig");
const a = std.heap.c_allocator;

const Buffer = extern struct {
    ptr: ?[*]u8,
    len: usize,
};

fn encode(result: p.Result) Buffer {
    const bytes = result.encode(a) catch return .{ .ptr = null, .len = 0 };
    return .{ .ptr = bytes.ptr, .len = bytes.len };
}

export fn polaris_v1_free(response: Buffer) void {
    if (response.ptr) |ptr| a.free(ptr[0..response.len]);
}

fn options(msg: p.Value) native.Options {
    const value = p.get(msg, "timeout_ms");
    return .{
        .base_url = p.field(msg, "base_url") orelse "https://api.polaris.supply",
        .stream_url = p.field(msg, "stream_url"),
        .api_key = p.field(msg, "api_key"),
        .timeout_ms = if (value == .number_string) std.fmt.parseInt(u32, value.number_string, 10) catch 30_000 else 30_000,
    };
}

export fn polaris_v1_validate(ptr: [*]const u8, len: usize) Buffer {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const msg = p.json(arena.allocator(), ptr[0..len]) catch return encode(p.fault("request", "Invalid SDK bridge input"));
    const config = options(msg);
    _ = p.endpoint(config.base_url, false, false) catch return encode(p.fault("request", "Invalid API base URL"));
    if (config.stream_url) |url| {
        _ = p.endpoint(url, true, true) catch return encode(p.fault("request", "Invalid WebSocket URL"));
    }
    return encode(.{ .ok = .null });
}

export fn polaris_v1_request(ptr: [*]const u8, len: usize) Buffer {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const msg = p.json(temp, ptr[0..len]) catch return encode(p.fault("request", "Invalid SDK bridge input"));
    const operation = std.meta.stringToEnum(p.Operation, p.field(msg, "operation") orelse "") orelse return encode(p.fault("request", "Unknown SDK operation"));
    const result = native.request(temp, options(msg), operation, p.get(msg, "query")) catch p.fault("request", "Native request failed");
    return encode(result);
}

export fn polaris_v1_cursor_create() ?*p.CursorSet {
    const cursor = a.create(p.CursorSet) catch return null;
    cursor.* = .{ .allocator = a };
    return cursor;
}

export fn polaris_v1_cursor_remember(cursor: *p.CursorSet, ptr: [*]const u8, len: usize) bool {
    return cursor.remember(ptr[0..len]) catch false;
}

export fn polaris_v1_cursor_destroy(cursor: *p.CursorSet) void {
    cursor.deinit();
    a.destroy(cursor);
}

export fn polaris_v1_stream_create(ptr: [*]const u8, len: usize, out: *?*native.Stream) Buffer {
    out.* = null;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const msg = p.json(temp, ptr[0..len]) catch return encode(p.fault("stream_protocol", "Invalid stream options"));
    const query = std.json.Stringify.valueAlloc(temp, p.get(msg, "query"), .{}) catch return encode(p.fault("request", "Could not encode stream options"));
    out.* = native.Stream.init(options(msg), query) catch |err| return encode(p.fault("stream_protocol", @errorName(err)));
    return encode(.{ .ok = .null });
}

export fn polaris_v1_stream_next(stream: *native.Stream) Buffer {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    return encode(stream.nextResult(arena.allocator()) catch p.fault("stream_connection", "Native stream failed"));
}

export fn polaris_v1_stream_cancel(stream: *native.Stream) void {
    stream.cancel();
}

export fn polaris_v1_stream_destroy(stream: *native.Stream) void {
    stream.deinit();
}

test {
    _ = p;
}
