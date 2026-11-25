use std::{env, path::PathBuf, process::Command};

fn main() {
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let target = env::var("TARGET").unwrap();
    let prebuilt = manifest.join("prebuilt").join(&target);
    let library = if target.contains("msvc") {
        "polaris_zig_core.lib"
    } else {
        "libpolaris_zig_core.a"
    };
    println!("cargo:rerun-if-env-changed=ZIG");
    println!("cargo:rerun-if-changed={}", prebuilt.display());

    let native_lib = if prebuilt.join(library).is_file() {
        prebuilt
    } else {
        let workspace = manifest.join("../..");
        let source = if workspace.join("zig/abi.zig").is_file() {
            workspace
        } else {
            manifest.join("zig-source")
        };
        let output = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("zig");
        for file in [
            "build.zig",
            "build.zig.zon",
            "zig",
            "include",
            ".zigversion",
        ] {
            println!("cargo:rerun-if-changed={}", source.join(file).display());
        }
        let zig = env::var("ZIG").unwrap_or_else(|_| "zig".to_owned());
        let version = Command::new(&zig)
            .arg("version")
            .output()
            .expect("Zig 0.17.1 is required when a bundled Polaris archive is unavailable");
        assert_eq!(String::from_utf8_lossy(&version.stdout).trim(), "0.17.1");
        let mut args = target.split('-');
        let arch = args.next().unwrap();
        let triple = if target.ends_with("apple-darwin") {
            format!("{arch}-macos")
        } else if target.contains("-linux-") {
            let abi = args.next_back().unwrap();
            format!("{arch}-linux-{abi}")
        } else if target.contains("-windows-") {
            format!("{arch}-windows-{}", args.next_back().unwrap())
        } else {
            let os = env::var("CARGO_CFG_TARGET_OS").unwrap();
            format!("{arch}-{os}")
        };
        let status = Command::new(zig)
            .current_dir(source)
            .args(["build", "-Doptimize=ReleaseSafe", "--prefix"])
            .arg(&output)
            .arg(format!("-Dtarget={triple}"))
            .status()
            .expect("run Zig build");
        assert!(status.success(), "build Polaris Zig core for {target}");
        output.join("lib")
    };
    println!("cargo:rustc-link-search=native={}", native_lib.display());
    println!("cargo:rustc-link-lib=static=polaris_zig_core");
    if target.contains("apple-darwin") {
        for framework in ["Security", "CoreFoundation"] {
            println!("cargo:rustc-link-lib=framework={framework}");
        }
    }
    if target.contains("windows") {
        for library in [
            "ws2_32", "crypt32", "bcrypt", "ntdll", "advapi32", "iphlpapi",
        ] {
            println!("cargo:rustc-link-lib={library}");
        }
    }
}
