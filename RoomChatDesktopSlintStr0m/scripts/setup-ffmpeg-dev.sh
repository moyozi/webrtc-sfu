#!/usr/bin/env bash
# 免 root 安装 FFmpeg 开发文件 (供 ffmpeg-next 构建) 到用户级目录。
#
# ffmpeg-next 通过 pkg-config 寻找 libavcodec / libavformat / libavutil /
# libavfilter / libavdevice / libswscale / libswresample 的开发文件 (头文件 +
# `.pc` + 未版本化的 `.so` 链接)。大多数系统直接安装对应的 `-dev` / `-devel`
# 包即可:
#
#   Fedora : sudo dnf install ffmpeg-free-devel
#   Debian : sudo apt install libavcodec-dev libavformat-dev libavutil-dev \
#                            libavfilter-dev libavdevice-dev libswscale-dev \
#                            libswresample-dev
#   macOS  : brew install ffmpeg          (自带 pkg-config 文件)
#
# 本脚本面向**没有 sudo 权限**的 Fedora 系统: 用 `dnf download` 拉取
# `*-free-devel` RPM, 用 `rpm2cpio | cpio` 解压到
# `$HOME/.local/share/ffmpeg-dev`, 并把 `.pc` 的 prefix 改写到该目录。
# 之后 `.cargo/config.toml` 中的 `PKG_CONFIG_PATH` 即可直接使用。
#
# 用法:
#   ./scripts/setup-ffmpeg-dev.sh

set -euo pipefail

PREFIX="${FFMPEG_DEV_PREFIX:-$HOME/.local/share/ffmpeg-dev}"
PKGS=(
    libavcodec-free-devel.x86_64
    libavformat-free-devel.x86_64
    libavutil-free-devel.x86_64
    libavfilter-free-devel.x86_64
    libavdevice-free-devel.x86_64
    libswscale-free-devel.x86_64
    libswresample-free-devel.x86_64
)

command -v dnf >/dev/null 2>&1 || {
    echo "本脚本仅支持 Fedora (需要 dnf)。其他系统请用包管理器安装 FFmpeg 开发包。" >&2
    exit 1
}
command -v rpm2cpio >/dev/null 2>&1 && command -v cpio >/dev/null 2>&1 || {
    echo "缺少 rpm2cpio / cpio, 请先安装 (sudo dnf install rpm cpio)。" >&2
    exit 1
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo ">> 下载 FFmpeg 开发 RPM ..."
dnf download --destdir "$TMP" "${PKGS[@]}"

echo ">> 解压到 $PREFIX ..."
rm -rf "$PREFIX"
mkdir -p "$PREFIX"
for rpm in "$TMP"/*.rpm; do
    (cd "$PREFIX" && rpm2cpio "$rpm" | cpio -idm --quiet)
done

echo ">> 改写 pkg-config prefix ..."
for pc in "$PREFIX"/usr/lib64/pkgconfig/libav*.pc \
          "$PREFIX"/usr/lib64/pkgconfig/libsw*.pc; do
    [ -f "$pc" ] || continue
    sed -i \
        -e "s|^prefix=/usr$|prefix=$PREFIX|" \
        -e "s|^libdir=/usr/lib64$|libdir=\${prefix}/usr/lib64|" \
        -e "s|^includedir=/usr/include/ffmpeg$|includedir=\${prefix}/usr/include/ffmpeg|" \
        "$pc"
done

echo ">> 修正未版本化的 .so 链接 (指向系统运行库的 SONAME) ..."
cd "$PREFIX/usr/lib64"
for so in libavcodec libavdevice libavfilter libavformat libavutil \
          libswresample libswscale; do
    if [ -L "$so.so" ] && [ ! -e "$(readlink "$so.so")" ]; then
        target="$(ls /usr/lib64/"$so".so.* 2>/dev/null | head -1 || true)"
        [ -n "$target" ] && ln -sf "$target" "$so.so"
    fi
done

echo
echo "完成。pkg-config 路径:"
echo "  $PREFIX/usr/lib64/pkgconfig"
echo
echo "请确认 crate 的 .cargo/config.toml 中 PKG_CONFIG_PATH 指向该目录,"
echo "或直接导出环境变量:"
echo "  export PKG_CONFIG_PATH=$PREFIX/usr/lib64/pkgconfig"
