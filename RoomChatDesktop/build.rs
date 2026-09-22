use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    slint_build::compile("ui/app.slint").expect("Slint 编译失败");
    embed_macos_info_plist();
    link_macos_compiler_rt();
}

/// 把 `macos/Info.plist` 嵌入可执行文件的 `__TEXT,__info_plist` 段。
///
/// 命令行程序 (非 .app bundle) 访问摄像头/麦克风时, macOS TCC 需要从
/// Info.plist 读取 `NSCameraUsageDescription` / `NSMicrophoneUsageDescription`,
/// 否则会直接终止进程。用链接器 `-sectcreate` 把 plist 嵌进去即可。
fn embed_macos_info_plist() {
    if std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() != "macos" {
        return;
    }
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let plist = manifest_dir.join("macos/Info.plist");
    println!("cargo:rerun-if-changed={}", plist.display());
    if plist.exists() {
        println!(
            "cargo:rustc-link-arg=-Wl,-sectcreate,__TEXT,__info_plist,{}",
            plist.display()
        );
    }
}

/// reactor-webrtc 的 macOS 预编译 `libwebrtc.a` 引用了 compiler-rt 提供的
/// `___isPlatformVersionAtLeast` (由 `__builtin_available` 生成), 而 rustc 的
/// 链接命令默认不包含 compiler-rt, 于是出现:
///
/// ```text
/// Undefined symbols for architecture arm64:
///   "___isPlatformVersionAtLeast", referenced from ... screen_capturer_sck.o
/// ```
///
/// 这里定位 clang 的 resource dir 并显式静态链接 `libclang_rt.osx.a`。
/// (配合 `.cargo/config.toml` 里把 `MACOSX_DEPLOYMENT_TARGET` 对齐到 13.0)
fn link_macos_compiler_rt() {
    if std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() != "macos" {
        return;
    }

    let resource_dir = ["clang", "cc"].iter().find_map(|cc| {
        Command::new(cc)
            .arg("--print-resource-dir")
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    });

    let Some(resource_dir) = resource_dir else {
        println!("cargo:warning=未找到 clang, 跳过 compiler-rt 链接 (可能链接失败)");
        return;
    };

    let lib_dir = Path::new(&resource_dir).join("lib/darwin");
    if lib_dir.join("libclang_rt.osx.a").exists() {
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
        println!("cargo:rustc-link-lib=static=clang_rt.osx");
    } else {
        println!(
            "cargo:warning=未找到 {} , 可能链接失败",
            lib_dir.join("libclang_rt.osx.a").display()
        );
    }
}
