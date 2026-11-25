const std = @import("std");
const p = @import("protocol.zig");
const a = std.heap.wasm_allocator;
var result_len: usize = 0;

export fn polaris_v1_alloc(len: usize) [*]u8 {
    return (a.alloc(u8, len) catch @panic("out of memory")).ptr;
}

export fn polaris_v1_free(ptr: [*]u8, len: usize) void {
    a.free(ptr[0..len]);
}

export fn polaris_v1_result_len() usize {
    return result_len;
}

fn output(result: p.Result) [*]u8 {
    const bytes = result.encode(a) catch @panic("out of memory");
    result_len = bytes.len;
    return bytes.ptr;
}

fn fail() p.Result {
    return p.fault("request", "Invalid SDK bridge input");
}

export fn polaris_v1_request(ptr: [*]const u8, len: usize) [*]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const msg = p.json(temp, ptr[0..len]) catch return output(fail());
    const operation = std.meta.stringToEnum(p.Operation, p.field(msg, "operation") orelse "") orelse return output(fail());
    if (p.field(msg, "base_url")) |url| {
        return output(p.prepare(temp, operation, url, p.field(msg, "api_key"), p.get(msg, "query")) catch fail());
    }
    const status_value = p.get(msg, "status");
    if (status_value != .number_string) return output(fail());
    const status = std.fmt.parseInt(u16, status_value.number_string, 10) catch return output(fail());
    return output(p.finish(temp, operation, status, p.field(msg, "body") orelse "", p.field(msg, "reset_at")));
}

export fn polaris_v1_cursor_create() ?*p.CursorSet {
    const tracker = a.create(p.CursorSet) catch return null;
    tracker.* = .{ .allocator = a };
    return tracker;
}

export fn polaris_v1_cursor_remember(tracker: *p.CursorSet, ptr: [*]const u8, len: usize) bool {
    return tracker.remember(ptr[0..len]) catch false;
}

export fn polaris_v1_cursor_destroy(tracker: *p.CursorSet) void {
    tracker.deinit();
    a.destroy(tracker);
}

export fn polaris_v1_session_create(ptr: [*]const u8, len: usize) ?*p.Session {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const msg = p.json(arena.allocator(), ptr[0..len]) catch return null;
    const query = p.get(msg, "query");
    const encoded = std.json.Stringify.valueAlloc(arena.allocator(), query, .{}) catch return null;
    const session = a.create(p.Session) catch return null;
    session.* = p.Session.init(a, encoded, p.field(msg, "api_key")) catch {
        a.destroy(session);
        return null;
    };
    return session;
}

export fn polaris_v1_session_subscribe(session: *p.Session) [*]u8 {
    const bytes = session.subscribe(a) catch @panic("out of memory");
    result_len = bytes.len;
    return bytes.ptr;
}

export fn polaris_v1_session_receive(session: *p.Session, ptr: [*]const u8, len: usize) [*]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    return output(session.receive(arena.allocator(), ptr[0..len]) catch fail());
}

export fn polaris_v1_session_advance(session: *p.Session) void {
    session.advance();
}

export fn polaris_v1_session_destroy(session: *p.Session) void {
    session.deinit();
    a.destroy(session);
}

export fn polaris_v1_ohlcv_finish(ptr: [*]const u8, len: usize) [*]u8 {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const bars = @import("ohlcv.zig").finish(arena.allocator(), ptr[0..len]) catch return output(fail());
    const bytes = std.json.Stringify.valueAlloc(a, .{ .ok = bars }, .{}) catch @panic("out of memory");
    result_len = bytes.len;
    return bytes.ptr;
}
