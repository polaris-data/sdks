const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const core = b.createModule(.{
        .root_source_file = b.path("zig/abi.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .pic = true,
        .strip = optimize != .debug,
    });
    const lib = b.addLibrary(.{ .name = "polaris_zig_core", .linkage = .static, .root_module = core });
    lib.bundle_compiler_rt = true;
    lib.installHeadersDirectory(b.path("include"), "", .{});
    b.getInstallStep().dependOn(&b.addInstallArtifact(lib, .{}).step);
    _ = b.addModule("polaris_data", .{
        .root_source_file = b.path("zig/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    const tests = b.addRunArtifact(b.addTest(.{ .root_module = core }));
    b.step("test", "Run the Zig core tests").dependOn(&tests.step);

    const wasm = b.addExecutable(.{
        .name = "polaris",
        .root_module = b.createModule(.{
            .root_source_file = b.path("zig/wasm.zig"),
            .target = b.resolveTargetQuery(.{
                .cpu_arch = .wasm32,
                .os_tag = .freestanding,
                .cpu_model = .baseline,
            }),
            .optimize = .small,
        }),
    });
    wasm.entry = .disabled;
    wasm.rdynamic = true;
    const install_wasm = b.addInstallArtifact(wasm, .{ .dest_dir = .{ .override = .bin } });
    b.step("wasm", "Build the portable Wasm core").dependOn(&install_wasm.step);
}
