//! 信令消息协议 — 与传输层 (WebSocket / QUIC) 无关
//!
//! 两种传输承载完全相同的 JSON 消息, 因此协议定义集中放在这里,
//! 由 `session::SignalingSession` 统一解析与分发。

use serde::{Deserialize, Serialize};

/// 客户端 → 服务器消息
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum SignalingMessage {
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
    },

    #[serde(rename = "ice_candidate")]
    IceCandidate {
        pc_id: String,
        candidate: String,
        sdp_mid: Option<String>,
        sdp_mline_index: Option<u16>,
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

    #[serde(rename = "leave")]
    Leave { pc_id: String },
}

/// 服务器 → 客户端消息
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum SignalingResponse {
    #[serde(rename = "sdp_answer")]
    SdpAnswer {
        pc_id: String,
        sdp: String,
        ice_candidates: Vec<String>,
    },

    #[serde(rename = "ice_candidate")]
    IceCandidate {
        pc_id: String,
        candidate: String,
        sdp_mid: Option<String>,
    },

    #[serde(rename = "participant_joined")]
    ParticipantJoined { participant_id: String },

    #[serde(rename = "participant_left")]
    ParticipantLeft { participant_id: String },

    #[serde(rename = "subscribed")]
    Subscribed {
        pc_id: String,
        publisher_id: String,
    },

    #[serde(rename = "error")]
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        pc_id: Option<String>,
        error: String,
    },
}
