#!/usr/bin/env bash
# 生成用于 SFU 信令鉴权的 HS256 JWT (测试/联调用)
#
#   ./scripts/gen-jwt.sh <sub> [room] [role] [ttl_seconds]
#
# 例:
#   SFU_JWT_SECRET=dev-secret ./scripts/gen-jwt.sh alice room-1 publisher 3600
#
# 服务端启动:
#   SFU_JWT_SECRET=dev-secret ./target/release/sfu-server
#
# 仅依赖 python3, 无需第三方库。
set -euo pipefail

SUB="${1:-alice}"
ROOM="${2:-}"
ROLE="${3:-}"
TTL="${4:-3600}"
SECRET="${SFU_JWT_SECRET:-}"
ALG="${SFU_JWT_ALGORITHM:-HS256}"

if [[ -z "$SECRET" ]]; then
  echo "错误: 需设置 SFU_JWT_SECRET 环境变量 (与服务端一致)" >&2
  exit 1
fi

SUB="$SUB" ROOM="$ROOM" ROLE="$ROLE" TTL="$TTL" SECRET="$SECRET" ALG="$ALG" \
python3 - <<'PY'
import base64, hashlib, hmac, json, os, time

def b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()

alg = os.environ["ALG"].upper()
hash_by_alg = {"HS256": hashlib.sha256, "HS384": hashlib.sha384, "HS512": hashlib.sha512}
if alg not in hash_by_alg:
    raise SystemExit(f"gen-jwt.sh 仅支持 HS256/HS384/HS512, 收到 {alg}")

now = int(time.time())
payload = {"sub": os.environ["SUB"], "exp": now + int(os.environ["TTL"])}
room = os.environ.get("ROOM")
role = os.environ.get("ROLE")
if room:
    payload["room"] = room
if role:
    payload["role"] = role

header = {"alg": alg, "typ": "JWT"}
signing_input = f"{b64(json.dumps(header, separators=(',', ':')).encode())}." \
                f"{b64(json.dumps(payload, separators=(',', ':')).encode())}"
sig = hmac.new(os.environ["SECRET"].encode(), signing_input.encode(), hash_by_alg[alg]).digest()
print(f"{signing_input}.{b64(sig)}")
PY
