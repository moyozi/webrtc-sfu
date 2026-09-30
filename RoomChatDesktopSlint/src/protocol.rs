//! 信令 JSON 协议 — 与 SFU 服务端 `src/signaling/protocol.rs` 完全对应。
//!
//! 一条 WebSocket 连接可承载多个 RTCPeerConnection, 用 `pc_id` 区分:
//!   - `"pub"`            发布连接 (sendonly), 加入房间
//!   - `"sub-<发布者id>"`  订阅连接 (recvonly), 不加入房间

use serde::{Deserialize, Serialize};

/// 客户端 → 服务器
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    #[serde(rename = "join")]
    Join {
        pc_id: String,
        room_id: String,
        participant_id: String,
        sdp: String,
        /// "publisher" (发布并加入房间) 或 "subscriber" (recvonly 订阅)
        role: String,
    },

    #[serde(rename = "ice_candidate")]
    IceCandidate {
        pc_id: String,
        candidate: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        sdp_mid: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        sdp_mline_index: Option<u16>,
    },

    #[serde(rename = "subscribe")]
    Subscribe {
        pc_id: String,
        publisher_id: String,
        /// 发布者该轨道的 mid
        track_mid: String,
        /// 订阅者自身该轨道的 mid (缺省 = track_mid)
        #[serde(skip_serializing_if = "Option::is_none")]
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

impl ClientMessage {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// 服务器 → 客户端
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "sdp_answer")]
    SdpAnswer {
        pc_id: String,
        sdp: String,
        #[serde(default)]
        ice_candidates: Vec<String>,
    },

    #[serde(rename = "ice_candidate")]
    IceCandidate {
        pc_id: String,
        candidate: String,
        #[serde(default)]
        sdp_mid: Option<String>,
    },

    #[serde(rename = "participant_joined")]
    ParticipantJoined { participant_id: String },

    #[serde(rename = "participant_left")]
    ParticipantLeft { participant_id: String },

    #[serde(rename = "subscribed")]
    Subscribed { pc_id: String, publisher_id: String },

    #[serde(rename = "error")]
    Error {
        #[serde(default)]
        pc_id: Option<String>,
        error: String,
    },
}
