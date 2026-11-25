const std = @import("std");

const Bucket = struct {
    ts: f64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volScaled: f64,
    trades: u64,
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

fn earlier(_: void, lhs: Bucket, rhs: Bucket) bool {
    return lhs.ts < rhs.ts;
}

pub fn finish(allocator: std.mem.Allocator, input: []const u8) ![]const Bar {
    const buckets = try std.json.parseFromSliceLeaky([]Bucket, allocator, input, .{});
    std.mem.sort(Bucket, buckets, {}, earlier);
    const bars = try allocator.alloc(Bar, buckets.len);
    for (buckets, bars, 0..) |bucket, *bar, i| {
        bar.* = .{
            .timestamp = bucket.ts,
            .open = if (i == 0) bucket.open else buckets[i - 1].close,
            .high = bucket.high,
            .low = bucket.low,
            .close = bucket.close,
            .volume = bucket.volScaled / 1e12,
            .trades = bucket.trades,
        };
    }
    return bars;
}
