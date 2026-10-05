#!/usr/bin/env bash
# 免 root 安装 GStreamer WebRTC 开发文件 (供 gstreamer-webrtc crate 构建) 到用户级目录。
#
# `gstreamer-webrtc` / `gstreamer-webrtc-sys` 通过 pkg-config 查找
# `gstreamer-webrtc-1.0.pc` 以及 `gst/webrtc/*.h` 头文件 (由
# `gstreamer1-plugins-bad-free-devel` 提供)。很多系统只装了运行时插件
# (`gstreamer1-plugins-bad-free`), 并没有装对应的 devel 包。
#
#   Fedora : sudo dnf install gstreamer1-plugins-bad-free-devel
#   Debian : sudo apt install libgstreamer-plugins-bad1.0-dev
#   macOS  : brew install gstreamer
#
# 本脚本面向**没有 sudo 权限**的 Fedora 系统: 用 `dnf download` 拉取
# `gstreamer1-plugins-bad-free-devel` RPM, 用 `rpm2cpio | cpio` 解压到
# `$HOME/.local/share/gst-webrtc-dev`, 并改写 `.pc` 的 prefix。
# 之后 `.cargo/config.toml` 中的 `PKG_CONFIG_PATH` 即可直接使用。
#
# 用法:
#   ./scripts/setup-gstreamer-webrtc-dev.sh

set -euo pipefail

PREFIX="${GST_WEBRTC_DEV_PREFIX:-$HOME/.local/share/gst-webrtc-dev}"
# 开发文件 (头文件 + .pc + 链接库)
PKG="gstreamer1-plugins-bad-free-devel"
# 运行时插件: webrtcbin 依赖 libnice 提供的 nicesrc/nicesink (nice 插件)。
# Fedora 把它拆在单独的 libnice-gstreamer1 包里, 默认可能没装。
PKG_RUNTIME="libnice-gstreamer1"

command -v dnf >/dev/null 2>&1 || {
    echo "本脚本仅支持 Fedora (需要 dnf)。其他系统请用包管理器安装 GStreamer WebRTC 开发包。" >&2
    exit 1
}
command -v rpm2cpio >/dev/null 2>&1 && command -v cpio >/dev/null 2>&1 || {
    echo "缺少 rpm2cpio / cpio, 请先安装 (sudo dnf install rpm cpio)。" >&2
    exit 1
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo ">> 下载 ${PKG} / ${PKG_RUNTIME} RPM ..."
# 限制为本机架构, 避免混入 i686 插件 (GStreamer 加载时会报 wrong ELF class)。
ARCH="$(uname -m)"
dnf download --arch "$ARCH" --destdir "$TMP" "$PKG" "$PKG_RUNTIME"

echo ">> 解压到 $PREFIX ..."
rm -rf "$PREFIX"
mkdir -p "$PREFIX"
for rpm in "$TMP"/*.rpm; do
    (cd "$PREFIX" && rpm2cpio "$rpm" | cpio -idm --quiet)
done

echo ">> 改写 pkg-config prefix ..."
PCDIR="$PREFIX/usr/lib64/pkgconfig"
[ -d "$PCDIR" ] || PCDIR="$PREFIX/usr/lib/pkgconfig"
for pc in "$PCDIR"/gstreamer-webrtc*.pc; do
    [ -f "$pc" ] || continue
    sed -i \
        -e "s|^prefix=/usr$|prefix=$PREFIX/usr|" \
        -e "s|^includedir=\${prefix}/include$|includedir=\${prefix}/include/gstreamer-1.0|" \
        -e "s|^includedir=\${prefix}/include/gstreamer-1.0$|includedir=\${prefix}/include/gstreamer-1.0|" \
        "$pc"
done

echo ">> 修正未版本化的 .so 链接 (指向系统运行库的 SONAME) ..."
# RPM 里的 `libgstwebrtc-1.0.so` 是指向同目录 `libgstwebrtc-1.0.so.0` 的相对
# 符号链接, 但该 SONAME 实际由运行时包安装在系统目录, 解压目录里并没有它,
# 于是链接时 `-lgstwebrtc-1.0` 失败。这里改指系统里的真实库文件。
cd "$PREFIX/usr/lib64" 2>/dev/null || cd "$PREFIX/usr/lib"
SYSLIBDIR=/usr/lib64
[ -d "$SYSLIBDIR" ] || SYSLIBDIR=/usr/lib
for so in libgstwebrtc-1.0 libgstwebrtcnice-1.0; do
    if [ -L "${so}.so" ]; then
        sys="$(ls "$SYSLIBDIR/${so}".so.0 2>/dev/null | head -1 || true)"
        if [ -n "$sys" ]; then
            ln -sf "$(readlink -f "$sys")" "${so}.so"
        else
            echo "警告: 未找到系统 ${so}.so.0, 链接可能失败" >&2
        fi
    fi
done

echo
echo "完成。pkg-config 路径:"
echo "  $PCDIR"
echo
echo "请确认 crate 的 .cargo/config.toml 中 PKG_CONFIG_PATH 指向该目录,"
echo "或直接导出环境变量:"
echo "  export PKG_CONFIG_PATH=$PCDIR"
echo
echo "运行时 nice 插件目录 (GStreamer 会自动从 cargo config / 程序中加入):"
for d in "$PREFIX/usr/lib64/gstreamer-1.0" "$PREFIX/usr/lib/gstreamer-1.0"; do
    [ -d "$d" ] && echo "  $d"
done
