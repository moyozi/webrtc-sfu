#!/usr/bin/env bash
# 把 roomchat-desktop-gstreamer 打包成 macOS .app bundle。
#
# 对 TCC (摄像头/麦克风权限) 而言, 带 Contents/Info.plist 的 .app 是最可靠的
# 形式; `cargo run` 依赖嵌入到可执行文件 __TEXT,__info_plist 段的 plist, 通常
# 也能弹出授权框, 但不同 macOS 版本行为略有差异。
#
# 用法:
#   ./macos/bundle.sh        # 构建并生成 target/release/RoomChat Desktop GStreamer.app
#   open "target/release/RoomChat Desktop GStreamer.app"
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release/roomchat-desktop-gstreamer"
APP="$ROOT/target/release/RoomChat Desktop GStreamer.app"

cargo build --release --manifest-path "$ROOT/Cargo.toml"

rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp "$BIN" "$APP/Contents/MacOS/roomchat-desktop-gstreamer"
cp "$ROOT/macos/Info.plist" "$APP/Contents/Info.plist"

# ad-hoc 签名 (本地运行足够; App Store 分发需开发者证书)
codesign --force --sign - "$APP" >/dev/null 2>&1 || true

echo "已生成: $APP"
echo "运行:   open \"$APP\""
