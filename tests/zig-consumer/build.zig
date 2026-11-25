const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const polaris = b.dependency("polaris_data", .{ .target = target, .optimize = optimize });
    const app = b.addExecutable(.{
        .name = "polaris-consumer",
        .root_module = b.createModule(.{
            .root_source_file = b.path("main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "polaris_data", .module = polaris.module("polaris_data") }},
        }),
    });
    b.getInstallStep().dependOn(&b.addInstallArtifact(app, .{}).step);
}
