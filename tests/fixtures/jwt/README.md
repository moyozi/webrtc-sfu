# JWT interop fixtures (Go auth → Rust SFU)

这些文件用于验证**跨项目 JWT 联调**：由 Go auth 仓库
(`go/src/auth`) 使用非对称私钥签发，本 Rust SFU 仅用公钥验签。

| 文件 | 说明 |
|------|------|
| `go_auth_rs256_pub.pem` | Go `cmd/jwtgen` 生成的 RS256 公钥 (PKIX/PEM) |
| `go_auth_rs256_token.txt` | 对应私钥签发的 token，`exp` 为 2100 年，便于长期作为 fixture |

生成命令（在 `go/src/auth` 中执行，再复制回本仓库）：

```bash
cd ~/go/src/auth
go run ./cmd/jwtgen -keygen -keydir /tmp/jwt -alg RS256 -kid auth-1
TTL=$((4102444800 - $(date +%s)))   # 到 2100 年
go run ./cmd/jwtgen -private-key /tmp/jwt/jwt-private.pem -alg RS256 -kid auth-1 \
  -sub alice -room room-1 -role publisher -aud authenticated -ttl "$TTL" \
  > /tmp/jwt/token.txt

SFU_REPO=~/rust/webrtc-sfu   # 改成实际路径
cp /tmp/jwt/jwt-public.pem "$SFU_REPO/tests/fixtures/jwt/go_auth_rs256_pub.pem"
cp /tmp/jwt/token.txt       "$SFU_REPO/tests/fixtures/jwt/go_auth_rs256_token.txt"
```

- 测试：`cargo test --bin sfu-server signaling::auth::tests::verifies_go_auth_rs256_token`
- 动态联调（临时密钥，不落盘）：`./scripts/interop-jwt.sh`

> `exp` 为 2100 年，仅为让固定 fixture 长期有效；生产环境请使用正常有效期。
> 私钥**不**随仓库提交。
