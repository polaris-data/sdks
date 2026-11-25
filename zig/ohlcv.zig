const std = @import("std");

const Bucket = struct {
    ts: f64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    vol_scaled: f64 = 0,
    trades: u64 = 0,
};

const Bar = struct {
    timestamp: f64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    trades: u64,
};

pub const Ohlcv = struct {
    allocator: std.mem.Allocator,
    width: f64,
    buckets: std.AutoHashMapUnmanaged(u64, Bucket) = .empty,

    pub fn create(a: std.mem.Allocator, interval: []const u8) !*Ohlcv {
        const width = try intervalWidth(interval);
        const self = try a.create(Ohlcv);
        self.* = .{ .allocator = a, .width = width };
        return self;
    }

    pub fn destroy(self: *Ohlcv) void {
        self.buckets.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn add(self: *Ohlcv, timestamp: f64, price: f64, quantity: f64) !void {
        if (!std.math.isFinite(timestamp) or !std.math.isFinite(price) or !std.math.isFinite(quantity))
            return error.NonFiniteObservation;

        const start = @floor(timestamp / self.width) * self.width;
        const entry = try self.buckets.getOrPut(self.allocator, @bitCast(canonicalZero(start)));
        const bucket = entry.value_ptr;
        if (!entry.found_existing) bucket.* = .{ .ts = start, .open = price, .high = price, .low = price, .close = price };
        bucket.high = @max(bucket.high, price);
        bucket.low = @min(bucket.low, price);
        bucket.close = price;

        // Match JavaScript Math.round, including ties for negative quantities.
        const scaled = quantity * 1e12;
        const integral = @floor(scaled);
        bucket.vol_scaled += if (scaled - integral >= 0.5) integral + 1 else integral;
        bucket.trades += 1;
    }

    pub fn finish(self: *const Ohlcv, temp: std.mem.Allocator) ![]Bar {
        const buckets = try temp.alloc(Bucket, self.buckets.count());
        var it = self.buckets.valueIterator();
        for (buckets) |*bucket| {
            bucket.* = it.next().?.*;
            if (!std.math.isFinite(bucket.ts) or !std.math.isFinite(bucket.vol_scaled))
                return error.NonFiniteBucket;
        }
        std.mem.sort(Bucket, buckets, {}, earlier);
        const bars = try temp.alloc(Bar, buckets.len);
        for (buckets, bars, 0..) |bucket, *bar, i| {
            bar.* = .{
                .timestamp = canonicalZero(bucket.ts),
                .open = canonicalZero(if (i == 0) bucket.open else buckets[i - 1].close),
                .high = canonicalZero(bucket.high),
                .low = canonicalZero(bucket.low),
                .close = canonicalZero(bucket.close),
                .volume = bucket.vol_scaled / 1e12,
                .trades = bucket.trades,
            };
        }
        return bars;
    }
};

fn canonicalZero(value: f64) f64 {
    return if (value == 0) 0 else value;
}

fn intervalWidth(interval: []const u8) !f64 {
    const suffix = std.mem.indexOfNone(u8, interval, "0123456789") orelse return error.InvalidInterval;
    if (suffix == 0) return error.InvalidInterval;
    const unit = interval[suffix..];
    const scale: f64 = if (std.mem.eql(u8, unit, "ms")) 1 else if (std.mem.eql(u8, unit, "s")) 1_000 else if (std.mem.eql(u8, unit, "m")) 60_000 else if (std.mem.eql(u8, unit, "h")) 3_600_000 else return error.InvalidInterval;
    return (try std.fmt.parseFloat(f64, interval[0..suffix])) * scale;
}

fn earlier(_: void, lhs: Bucket, rhs: Bucket) bool {
    return lhs.ts < rhs.ts;
}
