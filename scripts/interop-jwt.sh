#!/usr/bin/env bash
# 跨项目 JWT 联调: 用 Go auth 服务的非对称私钥签发 token, 由 Rust SFU 公钥验签。
#
#   ./scripts/interop-jwt.sh
#
# 流程:
#   1. 用 `go/src/auth/cmd/jwtgen` 生成非对称密钥对 (或用 PRIVATE_KEY 复用)
#   2. 用私钥签发一个带 room/role 的 SFU token, 并导出对应公钥
#   3. 把公钥文件与 token 通过环境变量注入 Rust 侧, 运行真实验签测试
#
# 可覆盖的环境变量:
#   GO_AUTH_DIR   Go auth 仓库路径    (默认: $HOME/go/src/auth)
#   KEY_TYPE      rsa | ec            (默认: rsa, 对应 RS256 / ES256)
#   ALG           签名算法            (默认: 由 KEY_TYPE 推断; 如 RS256 / ES256)
#   KID           密钥 ID             (默认: auth-1)
#   SUB           用户标识            (默认: alice)
#   ROOM          限定房间            (默认: room-1)
#   ROLE          角色                (默认: publisher)
#   AUD           受众                (默认: authenticated)
#   TTL           有效期秒数          (默认: 3600)
#   PRIVATE_KEY   复用已有私钥文件    (默认: 临时生成)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_AUTH_DIR="${GO_AUTH_DIR:-$HOME/go/src/auth}"

KEY_TYPE="${KEY_TYPE:-rsa}"
case "$KEY_TYPE" in
  ec|EC|es) DEFAULT_ALG="ES256"; GEN_EC=1 ;;
  *)        DEFAULT_ALG="RS256"; GEN_EC=0 ;;
esac
ALG="${ALG:-$DEFAULT_ALG}"
KID="${KID:-auth-1}"
SUB="${SUB:-alice}"
ROOM="${ROOM:-room-1}"
ROLE="${ROLE:-publisher}"
AUD="${AUD:-authenticated}"
TTL="${TTL:-3600}"
PRIVATE_KEY="${PRIVATE_KEY:-}"

if [[ ! -d "$GO_AUTH_DIR" ]]; then
  echo "错误: 找不到 Go auth 仓库: $GO_AUTH_DIR (可用 GO_AUTH_DIR=... 覆盖)" >&2
  exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# 1. 生成或复用私钥。
if [[ -n "$PRIVATE_KEY" ]]; then
  [[ -f "$PRIVATE_KEY" ]] || { echo "错误: 私钥不存在: $PRIVATE_KEY" >&2; exit 1; }
  PRIV="$PRIVATE_KEY"
else
  echo "==> [$GO_AUTH_DIR] 生成 $ALG 密钥对"
  GEN_ARGS=(-keygen -keydir "$WORK/secrets" -alg "$ALG")
  [[ "$GEN_EC" == "1" ]] && GEN_ARGS+=(-ec)
  go -C "$GO_AUTH_DIR" run ./cmd/jwtgen "${GEN_ARGS[@]}" >/dev/null
  PRIV="$WORK/secrets/jwt-private.pem"
fi

# 2. 用 Go auth 的私钥签发 SFU token, 并导出公钥。
echo "==> 用 Go auth 私钥签发 token (sub=$SUB room=$ROOM role=$ROLE aud=$AUD)"
TOKEN="$(go -C "$GO_AUTH_DIR" run ./cmd/jwtgen \
  -private-key "$PRIV" -alg "$ALG" -kid "$KID" \
  -sub "$SUB" -room "$ROOM" -role "$ROLE" -aud "$AUD" -ttl "$TTL" \
  -public-out "$WORK/jwt-public.pem")"

if [[ -z "$TOKEN" ]]; then
  echo "错误: 未生成 token" >&2
  exit 1
fi
echo "    token: ${TOKEN:0:48}... (${#TOKEN} 字节)"

# 供人工核对 payload (不校验签名)。
if command -v python3 >/dev/null 2>&1; then
  echo "==> payload:"
  python3 - "$TOKEN" <<'PY'
import base64, json, sys
p = sys.argv[1].split(".")[1]
p += "=" * (-len(p) % 4)
print("    " + json.dumps(json.loads(base64.urlsafe_b64decode(p)), ensure_ascii=False))
PY
fi

# 3. 注入环境变量, 在 Rust SFU 中真实验签。
echo "==> Rust SFU 验签 (cargo test signaling::auth::tests::verifies_go_auth_token_from_env)"
cd "$ROOT"
JWT_INTEROP_PUBLIC_KEY_FILE="$WORK/jwt-public.pem" \
JWT_INTEROP_TOKEN="$TOKEN" \
JWT_INTEROP_ALGORITHM="$ALG" \
JWT_INTEROP_AUDIENCE="$AUD" \
  cargo test --quiet --bin sfu-server \
    signaling::auth::tests::verifies_go_auth_token_from_env -- --nocapture

echo "==> 联调通过: Go auth ($ALG) 签发的 token 已被 Rust SFU 公钥验签接受 ✅"
