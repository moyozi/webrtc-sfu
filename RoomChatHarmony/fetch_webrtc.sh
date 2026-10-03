#!/usr/bin/env bash
# fetch_webrtc.sh — 获取预编译 WebRTC 依赖（libwebrtc.a + include 头文件）
#
# 该依赖不入库（见 ohos_webrtc/.gitignore），首次构建前运行一次即可恢复：
#
#   ./RoomChatHarmony/fetch_webrtc.sh
#
# 数据来源：hezhubo/OhosWebrtc（上游用 Git LFS 托管 libwebrtc.a）。
# 此处固定到某一上游提交，保证可复现；如需升级，同步更新 UPSTREAM_REF 与
# EXPECTED_SHA256。

set -euo pipefail

UPSTREAM_REPO="https://github.com/hezhubo/OhosWebrtc"
UPSTREAM_REF="762168516c67a546955263c04d66014258992c8d"  # add mediasoup 3.4.3
UPSTREAM_WEBRTC="ohos_webrtc/src/main/cpp/deps/webrtc"

# libwebrtc.a 直链（GitHub LFS），SHA-256 对应上游 .gitattributes 中的 oid
LIBWEBRTC_URL="https://media.githubusercontent.com/media/hezhubo/OhosWebrtc/${UPSTREAM_REF}/${UPSTREAM_WEBRTC}/libs/arm64-v8a/libwebrtc.a"
EXPECTED_SHA256="15db40eceb9c55174d9ad38b2e88607748636d698ca1bfb570fc717921cd90e2"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST_DIR="${SCRIPT_DIR}/ohos_webrtc/src/main/cpp/deps/webrtc"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

mkdir -p "${DEST_DIR}"

# 1) 头文件：从上游源码 tarball 解出（LFS 文件在 tarball 里只是指针，不会带出 388MB）
echo ">> [1/2] 拉取 WebRTC include 头文件 ..."
curl -fL --retry 3 -o "${TMP_DIR}/upstream.tar.gz" \
  "${UPSTREAM_REPO}/archive/${UPSTREAM_REF}.tar.gz"
tar -xzf "${TMP_DIR}/upstream.tar.gz" -C "${TMP_DIR}"
cp -R "${TMP_DIR}/OhosWebrtc-${UPSTREAM_REF}/${UPSTREAM_WEBRTC}/include" "${DEST_DIR}/"

# 2) 预编译静态库：走 GitHub LFS 直链，无需安装 git-lfs
echo ">> [2/2] 下载 libwebrtc.a (arm64-v8a, 约 388MB) ..."
mkdir -p "${DEST_DIR}/libs/arm64-v8a"
curl -fL --retry 3 -o "${DEST_DIR}/libs/arm64-v8a/libwebrtc.a" "${LIBWEBRTC_URL}"

echo ">> 校验 SHA-256 ..."
echo "${EXPECTED_SHA256}  ${DEST_DIR}/libs/arm64-v8a/libwebrtc.a" | shasum -a 256 -c -

echo "✅ 完成。依赖已恢复到：${DEST_DIR}"
