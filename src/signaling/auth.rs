//! JWT 用户鉴权 (传输无关)
//!
//! WebSocket / QUIC / WHIP 三种接入在创建参与者 (`join`) 之前都必须携带并
//! 通过 JWT 校验，否则无法进入房间。模块本身不关心 token 从哪里来:
//!
//! - WebSocket: 握手时 `Authorization: Bearer <jwt>` 头，或 `?token=<jwt>` 查询参数;
//! - QUIC: `join` 消息里的 `token` 字段 (QUIC 无 HTTP 头);
//! - WHIP: `Authorization: Bearer <jwt>` 头，或 `?token=<jwt>` 查询参数。
//!
//! ## 配置 (环境变量)
//!
//! ```text
//! SFU_JWT_SECRET=<HMAC 密钥>          # HS256/384/512 对称密钥
//! SFU_JWT_PUBLIC_KEY=<PEM 公钥>       # RS/ES/EdDSA 非对称公钥
//! SFU_JWT_PUBLIC_KEY_FILE=<公钥文件>  # 与上者二选一
//! SFU_JWT_ALGORITHM=HS256             # 可选, 缺省按密钥类型推断
//! SFU_JWT_ISSUER=https://idp         # 可选, 校验 iss
//! SFU_JWT_AUDIENCE=webrtc-sfu        # 可选, 校验 aud
//! SFU_JWT_REQUIRED=on                 # 可选, 配置密钥后默认 on; 设 off 则"有则校验"
//! ```
//!
//! 未配置任何密钥时鉴权整体关闭，保持对既有 Demo 客户端的兼容。
//!
//! ## 声明 (claims)
//!
//! ```json
//! { "sub": "alice", "exp": 1735689600, "room": "room-1", "role": "publisher" }
//! ```
//!
//! - `sub` (必填): 用户唯一标识。发布者 `participant_id` 必须等于 `sub`;
//!   订阅者必须为 `<sub>-recv-<publisherId>`，从而杜绝冒充他人身份。
//! - `exp` (必填): 过期时间 (Unix 秒)，由 jsonwebtoken 校验。
//! - `room` (可选): 限定只能加入该房间。
//! - `role` (可选): `subscriber` 表示只能订阅; `publisher` 或省略则既可发布也可订阅。

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use tracing::warn;

/// JWT 相关配置 (来自环境变量，可 Clone)
///
/// 注意: 自定义 `Debug` 会屏蔽密钥，避免通过 `info!(?config)` 泄漏到日志。
#[derive(Clone, Default)]
pub struct JwtSettings {
    /// HMAC 对称密钥 (HS256/384/512)
    pub secret: Option<String>,
    /// 非对称公钥 PEM (RS/ES/EdDSA)
    pub public_key: Option<String>,
    /// 显式指定算法; 缺省按密钥类型推断
    pub algorithm: Option<String>,
    /// 校验 issuer
    pub issuer: Option<String>,
    /// 校验 audience
    pub audience: Option<String>,
    /// 是否强制要求鉴权 (配置密钥后默认 true)
    pub required: bool,
}

impl std::fmt::Debug for JwtSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtSettings")
            .field("secret", &self.secret.as_ref().map(|_| "<redacted>"))
            .field(
                "public_key",
                &self.public_key.as_ref().map(|k| format!("<{} bytes>", k.len())),
            )
            .field("algorithm", &self.algorithm)
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("required", &self.required)
            .finish()
    }
}

impl JwtSettings {
    /// 从环境变量读取 JWT 配置。
    pub fn from_env() -> Self {
        let secret = non_empty(std::env::var("SFU_JWT_SECRET").ok());
        let public_key_raw = std::env::var("SFU_JWT_PUBLIC_KEY")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let public_key_file = std::env::var("SFU_JWT_PUBLIC_KEY_FILE")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let attempted_key = public_key_raw.is_some() || public_key_file.is_some();
        let public_key = public_key_raw.or_else(|| {
            public_key_file.as_ref().and_then(|path| {
                match std::fs::read_to_string(path) {
                    Ok(pem) => Some(pem),
                    Err(e) => {
                        warn!("读取 SFU_JWT_PUBLIC_KEY_FILE ({}) 失败: {}", path, e);
                        None
                    }
                }
            })
        });
        let algorithm = non_empty(std::env::var("SFU_JWT_ALGORITHM").ok());
        let issuer = non_empty(std::env::var("SFU_JWT_ISSUER").ok());
        let audience = non_empty(std::env::var("SFU_JWT_AUDIENCE").ok());
        let configured = secret.is_some() || public_key.is_some();
        // “尝试配置” 任一密钥 (即使读取失败) 也默认强制鉴权, 避免因文件名写错
        // 导致鉴权静默关闭。
        let attempted = configured || attempted_key;
        let required = match std::env::var("SFU_JWT_REQUIRED") {
            Ok(v) => !is_falsey(&v),
            Err(_) => attempted,
        };
        Self {
            secret,
            public_key,
            algorithm,
            issuer,
            audience,
            required,
        }
    }

    /// 是否配置了任一校验密钥。
    pub fn is_configured(&self) -> bool {
        self.secret.is_some() || self.public_key.is_some()
    }
}

fn non_empty(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.trim().is_empty())
}

fn is_falsey(v: &str) -> bool {
    v.is_empty()
        || v == "0"
        || v.eq_ignore_ascii_case("off")
        || v.eq_ignore_ascii_case("false")
        || v.eq_ignore_ascii_case("no")
}

/// JWT 负载 (claims)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// 用户唯一标识
    pub sub: String,
    /// 过期时间 (Unix 秒), jsonwebtoken 会强制要求并校验
    #[serde(default)]
    pub exp: Option<u64>,
    /// 可选: 限定房间
    #[serde(default)]
    pub room: Option<String>,
    /// 可选: 限定角色 "publisher" / "subscriber"
    #[serde(default)]
    pub role: Option<String>,
}

/// JWT 校验器。`required=false` 且未配置密钥时整体禁用。
pub struct Authenticator {
    enabled: bool,
    required: bool,
    decoding_key: Option<DecodingKey>,
    validation: Validation,
}

impl Authenticator {
    /// 未配置任何密钥的"关闭"实例。
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            required: false,
            decoding_key: None,
            validation: Validation::new(Algorithm::HS256),
        }
    }

    /// 由配置构造校验器。配置了密钥但参数非法时返回错误 (应阻止启动)。
    pub fn from_settings(settings: &JwtSettings) -> Result<Self, String> {
        if !settings.is_configured() {
            if settings.required {
                return Err(
                    "SFU_JWT_REQUIRED 已开启, 但未配置 SFU_JWT_SECRET / SFU_JWT_PUBLIC_KEY".into(),
                );
            }
            return Ok(Self::disabled());
        }

        let algorithm = resolve_algorithm(settings)?;
        let decoding_key = build_decoding_key(settings, algorithm)?;

        let mut validation = Validation::new(algorithm);
        validation.validate_exp = true;
        // 未配置 aud 时关闭 audience 校验，否则 jsonwebtoken 默认会拒绝无 aud 的 token。
        validation.validate_aud = settings.audience.is_some();
        if let Some(aud) = &settings.audience {
            validation.set_audience(&[aud]);
        }
        if let Some(iss) = &settings.issuer {
            validation.set_issuer(&[iss]);
        }

        Ok(Self {
            enabled: true,
            required: settings.required,
            decoding_key: Some(decoding_key),
            validation,
        })
    }

    /// 鉴权是否启用 (配置了密钥)。
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// 是否强制要求 token。
    pub fn is_required(&self) -> bool {
        self.required
    }

    /// 校验一个 JWT，返回其 claims。
    pub fn verify(&self, token: &str) -> Result<Claims, String> {
        let key = self
            .decoding_key
            .as_ref()
            .ok_or_else(|| "JWT 鉴权未启用".to_string())?;
        decode::<Claims>(token, key, &self.validation)
            .map(|data| data.claims)
            .map_err(|e| format!("JWT 校验失败: {}", e))
    }

    /// 校验 claims 是否允许该参与者加入该房间。
    ///
    /// 关键约束: `participant_id` 必须与 `sub` 绑定，防止用户伪造他人身份:
    /// - 发布者: `participant_id == sub`
    /// - 订阅者: `participant_id == "<sub>-recv-<publisher>"`
    pub fn authorize_join(
        &self,
        claims: &Claims,
        room_id: &str,
        participant_id: &str,
        role: &str,
    ) -> Result<(), String> {
        if let Some(room) = &claims.room {
            if room != room_id {
                return Err(format!(
                    "token 仅允许加入房间 {}, 实际请求 {}",
                    room, room_id
                ));
            }
        }

        let is_publisher = role != "subscriber";
        // `role=subscriber` 的 token 只能订阅; `publisher` 或省略时既可发布也可订阅。
        if let Some(want) = &claims.role {
            if want == "subscriber" && is_publisher {
                return Err("token 仅允许订阅 (subscriber)".to_string());
            }
        }

        if is_publisher {
            if participant_id != claims.sub {
                return Err(format!(
                    "participant_id 必须为 token sub ({})",
                    claims.sub
                ));
            }
        } else {
            let prefix = format!("{}-recv-", claims.sub);
            if !participant_id.starts_with(&prefix) {
                return Err(format!(
                    "订阅 participant_id 必须为 {}<publisherId>",
                    prefix
                ));
            }
        }
        Ok(())
    }
}

fn resolve_algorithm(settings: &JwtSettings) -> Result<Algorithm, String> {
    if let Some(name) = &settings.algorithm {
        return parse_algorithm(name).ok_or_else(|| format!("未知 JWT 算法: {}", name));
    }
    // 未显式指定: 密钥类型推断
    if settings.secret.is_some() {
        Ok(Algorithm::HS256)
    } else {
        Ok(Algorithm::RS256)
    }
}

fn parse_algorithm(name: &str) -> Option<Algorithm> {
    Some(match name.to_ascii_uppercase().as_str() {
        "HS256" => Algorithm::HS256,
        "HS384" => Algorithm::HS384,
        "HS512" => Algorithm::HS512,
        "RS256" => Algorithm::RS256,
        "RS384" => Algorithm::RS384,
        "RS512" => Algorithm::RS512,
        "PS256" => Algorithm::PS256,
        "PS384" => Algorithm::PS384,
        "PS512" => Algorithm::PS512,
        "ES256" => Algorithm::ES256,
        "ES384" => Algorithm::ES384,
        "EDDSA" => Algorithm::EdDSA,
        _ => return None,
    })
}

fn build_decoding_key(settings: &JwtSettings, algorithm: Algorithm) -> Result<DecodingKey, String> {
    if let Some(secret) = &settings.secret {
        // HMAC 密钥只能用于 HMAC 算法，避免算法降级攻击。
        if !matches!(
            algorithm,
            Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512
        ) {
            return Err(format!(
                "SFU_JWT_SECRET (HMAC) 与算法 {:?} 不匹配, 请配置 SFU_JWT_PUBLIC_KEY",
                algorithm
            ));
        }
        return Ok(DecodingKey::from_secret(secret.as_bytes()));
    }

    let pem = settings
        .public_key
        .as_ref()
        .ok_or_else(|| "缺少 JWT 公钥".to_string())?;
    let bytes = pem.as_bytes();
    let key = match algorithm {
        Algorithm::RS256
        | Algorithm::RS384
        | Algorithm::RS512
        | Algorithm::PS256
        | Algorithm::PS384
        | Algorithm::PS512 => DecodingKey::from_rsa_pem(bytes),
        Algorithm::ES256 | Algorithm::ES384 => DecodingKey::from_ec_pem(bytes),
        Algorithm::EdDSA => DecodingKey::from_ed_pem(bytes),
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
            return Err("HMAC 算法需要配置 SFU_JWT_SECRET".into())
        }
    };
    key.map_err(|e| format!("解析 JWT 公钥失败: {}", e))
}

/// 从 HTTP `Authorization` 头 / URL 查询串中提取 token。
///
/// 供 WebSocket 握手与 WHIP 复用。查询参数 `token` 会做百分号解码。
pub fn extract_token(authorization: Option<&str>, query: Option<&str>) -> Option<String> {
    if let Some(header) = authorization {
        let header = header.trim();
        if let Some(rest) = header
            .strip_prefix("Bearer ")
            .or_else(|| header.strip_prefix("bearer "))
        {
            let token = rest.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }
    let query = query?;
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == "token" {
                let token = percent_decode(v);
                if !token.is_empty() {
                    return Some(token);
                }
            }
        }
    }
    None
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};

    fn hs_settings() -> JwtSettings {
        JwtSettings {
            secret: Some("test-secret".into()),
            required: true,
            ..Default::default()
        }
    }

    fn sign(claims: &Claims) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            claims,
            &EncodingKey::from_secret(b"test-secret"),
        )
        .unwrap()
    }

    fn exp_in(secs: i64) -> u64 {
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + secs) as u64
    }

    #[test]
    fn roundtrip_and_authorize() {
        let auth = Authenticator::from_settings(&hs_settings()).unwrap();
        assert!(auth.is_required());
        let claims = Claims {
            sub: "alice".into(),
            exp: Some(exp_in(60)),
            room: Some("room-1".into()),
            role: None,
        };
        let token = sign(&claims);
        let got = auth.verify(&token).unwrap();
        assert_eq!(got.sub, "alice");
        assert!(auth
            .authorize_join(&got, "room-1", "alice", "publisher")
            .is_ok());
        assert!(auth
            .authorize_join(&got, "room-1", "alice-recv-bob", "subscriber")
            .is_ok());
        // 冒充他人
        assert!(auth
            .authorize_join(&got, "room-1", "mallory", "publisher")
            .is_err());
        // 房间不符
        assert!(auth
            .authorize_join(&got, "room-2", "alice", "publisher")
            .is_err());
    }

    #[test]
    fn subscriber_role_token_can_only_subscribe() {
        let auth = Authenticator::from_settings(&hs_settings()).unwrap();
        let claims = Claims {
            sub: "carol".into(),
            exp: Some(exp_in(60)),
            room: None,
            role: Some("subscriber".into()),
        };
        // 订阅 (subscriber) 允许
        assert!(auth
            .authorize_join(&claims, "room-1", "carol-recv-bob", "subscriber")
            .is_ok());
        // 发布被拒
        assert!(auth
            .authorize_join(&claims, "room-1", "carol", "publisher")
            .is_err());
    }

    #[test]
    fn rejects_bad_signature_and_expiry() {
        let auth = Authenticator::from_settings(&hs_settings()).unwrap();
        let claims = Claims {
            sub: "alice".into(),
            exp: Some(exp_in(60)),
            room: None,
            role: None,
        };
        let token = sign(&claims);
        assert!(auth.verify(&format!("{}x", token)).is_err());

        let expired = Claims {
            sub: "alice".into(),
            exp: Some(exp_in(-120)),
            room: None,
            role: None,
        };
        let token = sign(&expired);
        assert!(auth.verify(&token).is_err());
    }

    #[test]
    fn required_without_key_is_error() {
        let settings = JwtSettings {
            required: true,
            ..Default::default()
        };
        assert!(Authenticator::from_settings(&settings).is_err());
    }

    #[test]
    fn extract_token_variants() {
        assert_eq!(
            extract_token(Some("Bearer abc.def.ghi"), None).as_deref(),
            Some("abc.def.ghi")
        );
        assert_eq!(
            extract_token(None, Some("room=r1&token=a%2Eb")).as_deref(),
            Some("a.b")
        );
        assert_eq!(extract_token(Some("Basic xyz"), None), None);
    }

    /// 校验脚本 `scripts/gen-jwt.sh` 生成的 token 可被服务端接受 (算法/编码互通)。
    #[test]
    fn verifies_script_generated_token() {
        let settings = JwtSettings {
            secret: Some("dev-secret".into()),
            required: true,
            ..Default::default()
        };
        let auth = Authenticator::from_settings(&settings).unwrap();
        // 由 scripts/gen-jwt.sh 等效生成 (exp=4102444800, 2100 年)。
        let token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\
                     .eyJzdWIiOiJib2IiLCJleHAiOjQxMDI0NDQ4MDB9\
                     .pjoErtsIwBl2AAnPvbJv9WRxgbRyF4bEapHrqz6WqiY";
        let claims = auth.verify(token).unwrap();
        assert_eq!(claims.sub, "bob");
    }

    /// 跨项目联调: 校验由 Go auth 服务 (`golang-jwt/jwt/v4`, RS256 非对称私钥)
    /// 签发的 token。fixture 由 `go/src/auth/cmd/jwtgen` 生成 (见 scripts/interop-jwt.sh)。
    #[test]
    fn verifies_go_auth_rs256_token() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/jwt");
        let public_key = std::fs::read_to_string(dir.join("go_auth_rs256_pub.pem"))
            .expect("缺少 Go auth 公钥 fixture");
        let token = std::fs::read_to_string(dir.join("go_auth_rs256_token.txt"))
            .expect("缺少 Go auth token fixture");

        let settings = JwtSettings {
            public_key: Some(public_key),
            algorithm: Some("RS256".into()),
            audience: Some("authenticated".into()),
            required: true,
            ..Default::default()
        };
        let auth = Authenticator::from_settings(&settings).unwrap();
        assert!(auth.is_enabled());

        let claims = auth
            .verify(token.trim())
            .expect("Go 签发的 RS256 token 应通过校验");
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.room.as_deref(), Some("room-1"));
        assert_eq!(claims.role.as_deref(), Some("publisher"));

        assert!(auth
            .authorize_join(&claims, "room-1", "alice", "publisher")
            .is_ok());
        // room 声明限定: 其它房间应被拒绝
        assert!(auth
            .authorize_join(&claims, "room-2", "alice", "publisher")
            .is_err());
    }

    /// 由 `scripts/interop-jwt.sh` 注入环境变量, 对"当场生成"的 Go token 做联调校验。
    /// 未设置环境变量时跳过, 因此可安全地常驻测试套件。
    #[test]
    fn verifies_go_auth_token_from_env() {
        let (Ok(pub_path), Ok(token)) = (
            std::env::var("JWT_INTEROP_PUBLIC_KEY_FILE"),
            std::env::var("JWT_INTEROP_TOKEN"),
        ) else {
            return; // 未设置则跳过 (常规 `cargo test` 路径)
        };
        let public_key = std::fs::read_to_string(&pub_path)
            .unwrap_or_else(|e| panic!("读取 {pub_path} 失败: {e}"));
        let algorithm = std::env::var("JWT_INTEROP_ALGORITHM").unwrap_or_else(|_| "RS256".into());
        let audience = std::env::var("JWT_INTEROP_AUDIENCE").unwrap_or_else(|_| "authenticated".into());

        let settings = JwtSettings {
            public_key: Some(public_key),
            algorithm: Some(algorithm),
            audience: Some(audience),
            required: true,
            ..Default::default()
        };
        let auth = Authenticator::from_settings(&settings).unwrap();
        let claims = auth.verify(token.trim()).expect("live Go token 应通过校验");
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.room.as_deref(), Some("room-1"));
    }
}
