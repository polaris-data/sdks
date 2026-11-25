const std = @import("std");
const p = @import("protocol.zig");

const Side = std.AutoHashMapUnmanaged(u64, f64);
const Level = struct { price: f64, quantity: f64 };
const Key = struct { source: []const u8, market: []const u8 };
const KeyContext = struct {
    pub fn hash(_: KeyContext, key: Key) u64 {
        return std.hash.Wyhash.hash(std.hash.Wyhash.hash(0, key.source), key.market);
    }

    pub fn eql(_: KeyContext, a: Key, b: Key) bool {
        return equal(a.source, b.source) and equal(a.market, b.market);
    }
};

const Book = struct {
    bids: Side = .empty,
    asks: Side = .empty,

    fn deinit(self: *Book, a: p.A) void {
        self.bids.deinit(a);
        self.asks.deinit(a);
    }

    fn update(self: *Book, a: p.A, bids: []const Level, asks: []const Level) !void {
        try self.bids.ensureUnusedCapacity(a, @intCast(bids.len));
        try self.asks.ensureUnusedCapacity(a, @intCast(asks.len));
        applyLevels(&self.bids, bids);
        applyLevels(&self.asks, asks);
    }
};

pub const Orderbooks = struct {
    allocator: p.A,
    books: std.HashMapUnmanaged(Key, Book, KeyContext, std.hash_map.default_max_load_percentage) = .empty,

    pub fn create(a: p.A) !*Orderbooks {
        const self = try a.create(Orderbooks);
        self.* = .{ .allocator = a };
        return self;
    }

    pub fn destroy(self: *Orderbooks) void {
        self.clear();
        self.books.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn clone(self: *const Orderbooks) !*Orderbooks {
        const copy = try create(self.allocator);
        errdefer copy.destroy();
        var it = self.books.iterator();
        while (it.next()) |entry| {
            var book: Book = .{};
            errdefer book.deinit(self.allocator);
            book.bids = try entry.value_ptr.bids.clone(self.allocator);
            book.asks = try entry.value_ptr.asks.clone(self.allocator);
            try copy.put(entry.key_ptr.*, book);
        }
        return copy;
    }

    fn put(self: *Orderbooks, key: Key, book: Book) !void {
        const a = self.allocator;
        if (self.books.getPtr(key)) |existing| {
            existing.deinit(a);
            existing.* = book;
            return;
        }
        const source = try a.dupe(u8, key.source);
        errdefer a.free(source);
        const market = try a.dupe(u8, key.market);
        errdefer a.free(market);
        try self.books.put(a, .{ .source = source, .market = market }, book);
    }

    fn clear(self: *Orderbooks) void {
        var it = self.books.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.source);
            self.allocator.free(entry.key_ptr.market);
            entry.value_ptr.deinit(self.allocator);
        }
        self.books.clearRetainingCapacity();
    }

    pub fn call(self: *Orderbooks, temp: p.A, input: []const u8) !p.Result {
        const request = p.json(temp, input) catch return p.fault("orderbook", "invalid orderbook input");
        const operation = p.field(request, "operation") orelse "";
        if (equal(operation, "clear")) {
            self.clear();
            return .{ .ok = .null };
        }
        const key: Key = .{
            .source = p.field(request, "source") orelse "",
            .market = p.field(request, "market") orelse "",
        };
        if (equal(operation, "clear_book")) {
            if (self.books.fetchRemove(key)) |removed| {
                var book = removed.value;
                self.allocator.free(removed.key.source);
                self.allocator.free(removed.key.market);
                book.deinit(self.allocator);
            }
            return .{ .ok = .null };
        }
        if (equal(operation, "snapshot")) {
            const book = self.books.get(key) orelse return .{ .ok = .null };
            return .{ .ok = try p.from(temp, .{
                .bids = try levels(temp, book.bids, .desc),
                .asks = try levels(temp, book.asks, .asc),
            }) };
        }
        if (equal(operation, "update")) return self.update(temp, key, request);
        return p.fault("orderbook", "unknown orderbook operation");
    }

    fn update(self: *Orderbooks, temp: p.A, key: Key, request: p.Value) !p.Result {
        const kind = p.field(request, "kind") orelse "";
        const data = p.get(request, "data");
        const is_snapshot = if (p.get(request, "v2") == .bool and p.get(request, "v2").bool) blk: {
            if (!equal(kind, "orderbook")) return status("ignored");
            const flag = p.get(data, "is_snapshot");
            if (flag != .bool) return p.fault("orderbook", "invalid v2 orderbook payload: data.is_snapshot must be a boolean");
            break :blk flag.bool;
        } else blk: {
            if (equal(kind, "orderbook_delta")) break :blk false;
            if (equal(kind, "orderbook") or equal(kind, "orderbook_snapshot") or equal(kind, "l2_snapshot")) break :blk true;
            return status("ignored");
        };

        const top_bids = request.object.get("bids");
        const top_asks = request.object.get("asks");
        if (data != .object and top_bids == null and top_asks == null)
            return p.fault("orderbook", "invalid orderbook payload: data must be an object");
        const bids = parseSide(temp, if (data == .object) data.object.get("bids") orelse top_bids else top_bids, is_snapshot) catch |err|
            return sideFault(temp, "bids", err);
        const asks = parseSide(temp, if (data == .object) data.object.get("asks") orelse top_asks else top_asks, is_snapshot) catch |err|
            return sideFault(temp, "asks", err);
        if (is_snapshot) {
            var book: Book = .{};
            errdefer book.deinit(self.allocator);
            try book.update(self.allocator, bids, asks);
            try self.put(key, book);
        } else {
            const book = self.books.getPtr(key) orelse return status("suppressed");
            try book.update(self.allocator, bids, asks);
        }
        return status("applied");
    }
};

fn equal(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

fn status(value: []const u8) p.Result {
    return .{ .ok = .{ .string = value } };
}

const ParseError = error{ MissingSide, ExpectedArray, InvalidPrice, InvalidQuantity, InvalidLevel, OutOfMemory };

fn sideFault(a: p.A, side: []const u8, err: ParseError) error{OutOfMemory}!p.Result {
    return p.fault("orderbook", switch (err) {
        error.MissingSide => try std.fmt.allocPrint(a, "invalid orderbook snapshot: data.{s} is required", .{side}),
        error.ExpectedArray => try std.fmt.allocPrint(a, "invalid orderbook payload: data.{s} must be an array", .{side}),
        error.InvalidPrice => "invalid orderbook level: price must be numeric",
        error.InvalidQuantity => "invalid orderbook level: quantity must be numeric",
        error.InvalidLevel => "invalid orderbook level: price must be positive and quantity non-negative",
        error.OutOfMemory => return error.OutOfMemory,
    });
}

fn number(value: p.Value) ?f64 {
    const text = switch (value) {
        .number_string => value.number_string,
        .string => value.string,
        else => return null,
    };
    // Zig permits underscores and hexadecimal floats in source literals.
    if (std.mem.indexOfAny(u8, text, "_xX") != null) return null;
    return std.fmt.parseFloat(f64, text) catch null;
}

fn parseSide(a: p.A, value: ?p.Value, required: bool) ParseError![]const Level {
    const side = value orelse return if (required) error.MissingSide else &.{};
    if (side != .array) return error.ExpectedArray;
    const parsed = try a.alloc(Level, side.array.items.len);
    for (side.array.items, parsed) |row, *level| {
        const price, const quantity = switch (row) {
            .array => .{
                if (row.array.items.len > 0) row.array.items[0] else .null,
                if (row.array.items.len > 1) row.array.items[1] else .null,
            },
            .object => .{
                p.get(row, "price"),
                row.object.get("quantity") orelse row.object.get("size") orelse p.get(row, "amount"),
            },
            else => .{ p.Value.null, p.Value.null },
        };
        const px = number(price) orelse return error.InvalidPrice;
        const qty = number(quantity) orelse return error.InvalidQuantity;
        if (!std.math.isFinite(px) or px <= 0 or !std.math.isFinite(qty) or qty < 0) return error.InvalidLevel;
        level.* = .{ .price = px, .quantity = qty };
    }
    return parsed;
}

fn applyLevels(side: *Side, changes: []const Level) void {
    for (changes) |level| {
        const bits: u64 = @bitCast(level.price);
        if (level.quantity == 0) {
            _ = side.remove(bits);
        } else {
            side.putAssumeCapacity(bits, level.quantity);
        }
    }
}

fn levels(a: p.A, side: Side, comptime order: enum { asc, desc }) ![]Level {
    const output = try a.alloc(Level, side.count());
    var it = side.iterator();
    for (output) |*level| {
        const entry = it.next().?;
        level.* = .{ .price = @bitCast(entry.key_ptr.*), .quantity = entry.value_ptr.* };
    }
    std.mem.sort(Level, output, {}, struct {
        fn compare(_: void, lhs: Level, rhs: Level) bool {
            return if (order == .asc) lhs.price < rhs.price else lhs.price > rhs.price;
        }
    }.compare);
    return output;
}
