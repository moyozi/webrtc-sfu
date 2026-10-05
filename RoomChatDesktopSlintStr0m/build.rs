//! 构建脚本:
//!   1. 编译 Slint UI (`ui/app.slint`);
//!   2. (macOS) 把 `macos/Info.plist` 嵌入可执行文件, 供 TCC 读取摄像头/麦克风用途说明。
//!
//! 与 `RoomChatDesktopSlint` 不同, 本 crate 不再链接 libwebrtc, 因此不需要
//! compiler-rt / MSVC STL 等专用链接参数。媒体采集与编解码全部交给外部
//! `ffmpeg` 可执行文件 (运行时依赖)。

use std::path::PathBuf;

fn main() {
    slint_build::compile("ui/app.slint").expect("Slint 编译失败");
    embed_macos_info_plist();
}

/// 把 `macos/Info.plist` 嵌入可执行文件的 `__TEXT,__info_plist` 段。
///
/// 摄像头 / 麦克风采集由子进程 ffmpeg 完成, 但 TCC 判定的是父进程 (本程序);
/// 命令行程序没有 `.app` bundle 时, 需要该段才能弹出系统授权框。
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
