const std = @import("std");
const polaris = @import("polaris_data");

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const a = std.heap.page_allocator;
    const client = polaris.Client.init(.{ .base_url = args[1], .api_key = "fixture-key" });

    var health = try client.health(a);
    defer health.deinit();
    try std.testing.expect(health.result.ok.ok);

    var page = try client.data(a, .{ .table = "standard.trades" });
    defer page.deinit();
    try std.testing.expectEqualStrings("page:1+/", page.result.ok.next_cursor.?);

    var rows = try client.dataRows(a, .{ .table = "standard.trades" });
    defer rows.deinit();
    try std.testing.expectEqualStrings("18446744073709551614", (try rows.next()).?.object.get("id").?.string);
    try std.testing.expectEqualStrings("18446744073709551615", (try rows.next()).?.object.get("id").?.string);
    try std.testing.expect((try rows.next()) == null);

    const stream = try client.stream(.{ .subscriptions = &.{.{ .table = "standard.trades" }} });
    defer stream.deinit();
    var first = try stream.next(a);
    defer first.deinit();
    try std.testing.expectEqualStrings("stream:1", first.result.ok.?.cursor);
    var second = try stream.next(a);
    defer second.deinit();
    try std.testing.expectEqualStrings("stream:2", second.result.ok.?.cursor);
    var terminal = try stream.next(a);
    defer terminal.deinit();
    try std.testing.expect(terminal.result.ok == null);
}
