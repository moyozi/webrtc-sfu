//! 信令消息协议 — 与传输层 (WebSocket / QUIC) 无关
//!
//! 两种传输承载完全相同的 JSON 消息, 因此协议定义集中放在这里,
//! 由 `session::SignalingSession` 统一解析与分发。
//!
//! ## 连接生命周期
//!
//! ```text
//!   client                                   server
//!     │── hello (可选, 协商版本) ────────────►│
//!     │◄────────────── welcome ───────────────│
//!     │── join (SDP offer) ──────────────────►│
//!     │◄────────────── sdp_answer ────────────│
//!     │◄────────────── joined (房间快照) ──────│
//!     │◄────────────── participant_joined* ───│  (房间内既有成员, 兼容旧客户端)
//!     │── ice_candidate ─────────────────────►│
//!     │── subscribe / unsubscribe ───────────►│
//!     │◄────────────── subscribed/unsubscribed│
//!     │── update_media (静音/关摄像头) ───────►│
//!     │◄────────────── participant_updated ───│  (广播给房间内其他人)
//!     │── ping ──────────────────────────────►│
//!     │◄────────────── pong ──────────────────│
//!     │── get_room_state ────────────────────►│
//!     │◄────────────── room_state ────────────│
//!     │── leave ─────────────────────────────►│
//!     │◄────────────── left ──────────────────│
//! ```
//!
//! ## 兼容性
//!
//! 协议为**纯增量演进**: 旧客户端只识别 `sdp_answer` / `participant_joined` /
//! `participant_left` / `subscribed` / `error`, 新增消息 (joined/welcome/pong/...)
//! 会被它们安全忽略 (JSON 反序列化按 `type` 匹配)。新客户端应优先使用
//! `joined.participants` 作为初始成员快照, 并对 `participant_joined` 去重。

use serde::{Deserialize, Serialize};

/// 当前协议版本。新增不兼容字段时递增。
pub const PROTOCOL_VERSION: u32 = 1;

/// 服务端标识 (写入 `welcome`)。
pub const SERVER_NAME: &str = concat!("webrtc-sfu/", env!("CARGO_PKG_VERSION"));

/// 发布者的一条媒体轨道。
///
/// 一个参与者可发布多条轨道 (如 音频 + 摄像头视频 + 屏幕共享视频)。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrackInfo {
    /// 该轨道在发布者 SDP 中的 mid (订阅者按此 mid 订阅)。
    pub mid: String,
    /// "audio" / "video"
    pub kind: String,
}

/// 房间成员信息 (成员快照 / 广播共用)。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParticipantInfo {
    pub participant_id: String,
    /// 是否开启麦克风 (true = 有音频)
    #[serde(default = "default_true")]
    pub audio_enabled: bool,
    /// 是否开启摄像头 (true = 有视频)
    #[serde(default = "default_true")]
    pub video_enabled: bool,
    /// 该成员发布的轨道列表 (mid + kind)。订阅者据此逐 track 订阅:
    /// 一个发布者可能有 音频 + 摄像头视频 + 屏幕共享视频 等多条轨道。
    #[serde(default)]
    pub tracks: Vec<TrackInfo>,
}

fn default_true() -> bool {
    true
}

/// 错误码 — 客户端可据此做程序化处理 (比解析中文错误串可靠)。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// 消息格式 / 参数非法
    BadRequest,
    /// JWT 鉴权失败
    Unauthorized,
    /// 尚未 join 就发送会话内消息
    NotJoined,
    /// 同一 pc_id 重复 join
    AlreadyJoined,
    /// pc_id / 会话不存在
    UnknownSession,
    /// Worker 返回错误 (如 SDP 解析失败)
    WorkerError,
    /// Worker 无响应 / 不可用
    WorkerUnavailable,
    /// 协议版本不受支持
    UnsupportedProtocol,
    /// 服务端内部错误
    Internal,
}

/// 客户端 → 服务器消息
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum SignalingMessage {
    /// 可选握手: 协商协议版本, 服务端回 `welcome`。
    #[serde(rename = "hello")]
    Hello {
        #[serde(default)]
        protocol_version: Option<u32>,
        #[serde(default)]
        client: Option<String>,
    },

    /// 应用层心跳: 服务端原样回 `pong` (带 `t`)。
    /// 移动端 NAT 空闲超时 / 中间设备断长连接时保持信令通道活跃。
    #[serde(rename = "ping")]
    Ping {
        #[serde(default)]
        t: Option<u64>,
    },

    /// 发布 / 订阅连接: 携带 SDP offer 创建参与者。
    #[serde(rename = "join")]
    Join {
        /// 会话标识: "pub" 或 "sub-<publisherId>"
        pc_id: String,
        room_id: String,
        participant_id: String,
        sdp: String,
        /// 连接角色: "publisher" (默认) 或 "subscriber" (recvonly)
        #[serde(default)]
        role: String,
        /// 用户鉴权 JWT。WebSocket/WHIP 也可在握手头/查询参数中携带;
        /// QUIC 无 HTTP 头, 必须通过该字段携带。启用鉴权时必填。
        #[serde(default)]
        token: Option<String>,
    },

    /// 对**已存在**的 pc_id 重新协商: 典型场景是 ICE restart。
    ///
    /// 与 `join` 不同, 它复用服务器上已有的参与者 (Rtc) 实例, 只应用新的
    /// SDP offer (新 ICE 凭证 → str0m 自动做 ICE restart), 服务端回 `sdp_answer`。
    #[serde(rename = "renegotiate")]
    Renegotiate {
        pc_id: String,
        sdp: String,
        /// 用户鉴权 JWT (启用了鉴权时必填, 与 `join` 一致)。
        #[serde(default)]
        token: Option<String>,
    },

    #[serde(rename = "ice_candidate")]
    IceCandidate {
        pc_id: String,
        candidate: String,
        sdp_mid: Option<String>,
        /// 兼容浏览器 RTCPeerConnection.onicecandidate 的字段, 当前服务端忽略。
        #[serde(default)]
        #[allow(dead_code)]
        sdp_mline_index: Option<u32>,
    },

    #[serde(rename = "subscribe")]
    Subscribe {
        pc_id: String,
        publisher_id: String,
        track_mid: String,
        /// 订阅者自身对应的 mid (可选)。
        ///
        /// 发布者与订阅者的 SDP mid 未必相同 (str0m 会随机分配 mid)。
        /// 缺省时退化为与 `track_mid` 相同, 兼容浏览器 "0"/"1" 约定。
        #[serde(default)]
        subscriber_mid: Option<String>,
    },

    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        pc_id: String,
        publisher_id: String,
        track_mid: String,
    },

    /// 更新本连接的媒体状态 (静音 / 关摄像头), 服务端会广播给房间内其他人。
    #[serde(rename = "update_media")]
    UpdateMedia {
        pc_id: String,
        #[serde(default)]
        audio_enabled: Option<bool>,
        #[serde(default)]
        video_enabled: Option<bool>,
    },

    /// 请求房间成员快照 (断线重连 / UI 刷新)。`room_id` 省略时取本连接已加入的房间。
    #[serde(rename = "get_room_state")]
    GetRoomState {
        #[serde(default)]
        room_id: Option<String>,
    },

    #[serde(rename = "leave")]
    Leave { pc_id: String },
}

/// 服务器 → 客户端消息
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum SignalingResponse {
    /// 对 `hello` 的回复: 服务端协议版本与能力集。
    #[serde(rename = "welcome")]
    Welcome {
        protocol_version: u32,
        server: String,
        features: Vec<String>,
    },

    /// 对 `ping` 的回复, 原样带回 `t`。
    #[serde(rename = "pong")]
    Pong {
        #[serde(skip_serializing_if = "Option::is_none")]
        t: Option<u64>,
    },

    #[serde(rename = "sdp_answer")]
    SdpAnswer {
        pc_id: String,
        sdp: String,
        ice_candidates: Vec<String>,
    },

    /// `join` 成功确认, 携带房间成员快照 (不含自己)。
    #[serde(rename = "joined")]
    Joined {
        pc_id: String,
        participant_id: String,
        room_id: String,
        role: String,
        participants: Vec<ParticipantInfo>,
    },

    /// `leave` 成功确认。
    #[serde(rename = "left")]
    Left { pc_id: String },

    #[serde(rename = "participant_joined")]
    ParticipantJoined {
        participant_id: String,
        /// 新成员的发布轨道列表 (可能为空: 刚加入尚未发布)。
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tracks: Vec<TrackInfo>,
    },

    #[serde(rename = "participant_left")]
    ParticipantLeft { participant_id: String },

    /// 某成员发布的轨道列表发生变化 (重协商新增/删除 track, 如开始/停止屏幕共享)。
    #[serde(rename = "participant_tracks")]
    ParticipantTracks {
        participant_id: String,
        tracks: Vec<TrackInfo>,
    },

    /// 房间内某成员的媒体状态变化 (静音 / 关摄像头)。
    #[serde(rename = "participant_updated")]
    ParticipantUpdated {
        participant_id: String,
        audio_enabled: bool,
        video_enabled: bool,
    },

    #[serde(rename = "subscribed")]
    Subscribed {
        pc_id: String,
        publisher_id: String,
        track_mid: String,
    },

    #[serde(rename = "unsubscribed")]
    Unsubscribed {
        pc_id: String,
        publisher_id: String,
        track_mid: String,
    },

    /// 房间成员快照 (响应 `get_room_state`)。
    #[serde(rename = "room_state")]
    RoomState {
        room_id: String,
        participants: Vec<ParticipantInfo>,
    },

    #[serde(rename = "error")]
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        pc_id: Option<String>,
        code: ErrorCode,
        error: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_client_messages() {
        let hello: SignalingMessage =
            serde_json::from_str(r#"{"type":"hello","protocol_version":1}"#).unwrap();
        assert!(matches!(hello, SignalingMessage::Hello { .. }));

        let ping: SignalingMessage = serde_json::from_str(r#"{"type":"ping"}"#).unwrap();
        assert!(matches!(ping, SignalingMessage::Ping { t: None }));
        let ping: SignalingMessage = serde_json::from_str(r#"{"type":"ping","t":42}"#).unwrap();
        assert!(matches!(ping, SignalingMessage::Ping { t: Some(42) }));

        // 兼容浏览器 ice_candidate 的额外字段
        let ice: SignalingMessage = serde_json::from_str(
            r#"{"type":"ice_candidate","pc_id":"pub","candidate":"c","sdp_mid":"0","sdp_mline_index":0}"#,
        )
        .unwrap();
        assert!(matches!(ice, SignalingMessage::IceCandidate { .. }));

        let media: SignalingMessage = serde_json::from_str(
            r#"{"type":"update_media","pc_id":"pub","video_enabled":false}"#,
        )
        .unwrap();
        match media {
            SignalingMessage::UpdateMedia {
                video_enabled: Some(false),
                audio_enabled: None,
                ..
            } => {}
            other => panic!("unexpected: {other:?}"),
        }

        let state: SignalingMessage =
            serde_json::from_str(r#"{"type":"get_room_state"}"#).unwrap();
        assert!(matches!(
            state,
            SignalingMessage::GetRoomState { room_id: None }
        ));
    }

    #[test]
    fn serialize_server_messages() {
        let joined = SignalingResponse::Joined {
            pc_id: "pub".into(),
            participant_id: "alice".into(),
            room_id: "room-1".into(),
            role: "publisher".into(),
            participants: vec![ParticipantInfo {
                participant_id: "bob".into(),
                audio_enabled: true,
                video_enabled: false,
                tracks: vec![TrackInfo {
                    mid: "0".into(),
                    kind: "audio".into(),
                }],
            }],
        };
        let v: serde_json::Value = serde_json::to_value(&joined).unwrap();
        assert_eq!(v["type"], "joined");
        assert_eq!(v["participants"][0]["participant_id"], "bob");
        assert_eq!(v["participants"][0]["video_enabled"], false);

        let err = SignalingResponse::Error {
            pc_id: Some("pub".into()),
            code: ErrorCode::UnknownSession,
            error: "x".into(),
        };
        let v: serde_json::Value = serde_json::to_value(&err).unwrap();
        assert_eq!(v["code"], "unknown_session");

        // pong 省略 t 时不输出该字段
        let pong = SignalingResponse::Pong { t: None };
        let v: serde_json::Value = serde_json::to_value(&pong).unwrap();
        assert!(v.get("t").is_none());
    }

    #[test]
    fn participant_info_defaults_enabled() {
        let p: ParticipantInfo =
            serde_json::from_str(r#"{"participant_id":"x"}"#).unwrap();
        assert!(p.audio_enabled && p.video_enabled);
    }
}
