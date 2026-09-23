use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    embed_macos_info_plist();
    link_macos_compiler_rt();
    link_msvc_cpp_stdlib();
    link_linux_xkbcommon_x11_shim();
}

/// GPUI 的 Linux 后端会无条件链接 `-lxkbcommon-x11`。部分发行版 (如 Fedora)
/// 只安装运行时的 `libxkbcommon-x11.so.0`, 缺少开发用的无版本号软链
/// `libxkbcommon-x11.so`, 于是链接报错:
///
/// ```text
/// rust-lld: error: unable to find library -lxkbcommon-x11
/// ```
///
/// 这里在 OUT_DIR 里生成一个仅用于链接期的软链并加入搜索路径, 无需 root,
/// 也不影响运行时 (动态加载器按 SONAME 找到系统里的 `.so.0`)。
/// 若系统已安装 dev 包 (`libxkbcommon-x11-devel`), 则本函数为空操作。
fn link_linux_xkbcommon_x11_shim() {
    if std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() != "linux" {
        return;
    }

    let dev_symlinks = [
        "/usr/lib64/libxkbcommon-x11.so",
        "/usr/lib/libxkbcommon-x11.so",
        "/usr/lib/x86_64-linux-gnu/libxkbcommon-x11.so",
        "/usr/lib/aarch64-linux-gnu/libxkbcommon-x11.so",
    ];
    if dev_symlinks.iter().any(|p| Path::new(p).exists()) {
        return;
    }

    let runtime_libs = [
        "/usr/lib64/libxkbcommon-x11.so.0",
        "/usr/lib/libxkbcommon-x11.so.0",
        "/usr/lib/x86_64-linux-gnu/libxkbcommon-x11.so.0",
        "/usr/lib/aarch64-linux-gnu/libxkbcommon-x11.so.0",
    ];
    let Some(runtime_lib) = runtime_libs.iter().find(|p| Path::new(p).exists()) else {
        println!(
            "cargo:warning=未找到 libxkbcommon-x11.so.0; 请安装 libxkbcommon-x11 \
             (Fedora: libxkbcommon-x11-devel, Debian: libxkbcommon-x11-dev)"
        );
        return;
    };

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR 未设置"));
    let link_path = out_dir.join("libxkbcommon-x11.so");
    let _ = std::fs::remove_file(&link_path);
    #[cfg(unix)]
    match std::os::unix::fs::symlink(runtime_lib, &link_path) {
        Ok(()) => {
            println!("cargo:rustc-link-search=native={}", out_dir.display());
        }
        Err(e) => {
            println!("cargo:warning=创建 libxkbcommon-x11 软链失败: {e}");
        }
    }
}

/// reactor-webrtc 预编译的 `libwebrtc.lib` 以 `/MT` (静态 CRT + 静态 STL) 构建,
/// 引用了新版 MSVC STL 的 out-of-line 符号 (`__std_rotate` / `__std_min_element_*`
/// / `__std_find_*` 等)。rustc 的最终链接不会自动带上 C++ 标准库, 因此会出现:
///
/// ```text
/// libwebrtc.lib(...) : error LNK2019: unresolved external symbol __std_rotate
/// ```
///
/// 这里显式补上匹配 `/MT` 的静态 C++ 标准库 `libcpmt.lib` (与 libwebrtc 一致),
/// 同时消除 `LNK4098: libcmt.lib conflicts` 的 CRT 混用告警来源。
fn link_msvc_cpp_stdlib() {
    if std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() != "windows" {
        return;
    }
    if std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default() != "msvc" {
        return;
    }
    // 注意: 必须用 /DEFAULTLIB 而不是 rustc-link-lib。后者会被 rustc 放到
    // 链接命令最前面, 早于引入 __std_* 引用的 /WHOLEARCHIVE:libwebrtc.lib,
    // 链接器不会再回头重扫, 符号依旧未解析。 /DEFAULTLIB 会在所有显式库之后
    // 补搜, 能正确解析。
    // libcpmt = /MT 静态 STL; msvcprt = /MD 动态 STL (msvcp140.dll)。
    // 不同 MSVC 版本把 __std_* 放在不同库里, 两个都挂上最稳。
    println!("cargo:rustc-link-arg=/DEFAULTLIB:libcpmt.lib");
    println!("cargo:rustc-link-arg=/DEFAULTLIB:msvcprt.lib");
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
