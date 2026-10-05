//! 信令 JSON 协议 — 与 SFU 服务端 `src/signaling/protocol.rs` 完全对应。
//!
//! 一条 WebSocket 连接可承载多个 RTCPeerConnection, 用 `pc_id` 区分:
//!   - `"pub"`            发布连接 (sendonly), 加入房间
//!   - `"sub-<发布者id>"`  订阅连接 (recvonly), 不加入房间
//!
//! 协议版本 v2: `subscribe` 必须携带 `subscriber_mid` (订阅者自身该轨道的
//! mid), 且发布者的每个 mid 由 `joined.participants[].tracks` /
//! `participant_joined.tracks` / `participant_tracks.tracks` 给出。

#![allow(dead_code)] // 协议镜像: 部分字段当前 UI 未使用, 但为与服务端保持一致而保留。

use serde::{Deserialize, Serialize};

/// 当前协议版本 (与服务端 `PROTOCOL_VERSION` 对应)。
pub const PROTOCOL_VERSION: u32 = 2;

/// 发布者的一条媒体轨道 (mid + kind)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackInfo {
    /// 该轨道在发布者 SDP 中的 mid (订阅者按此 mid 订阅)。
    pub mid: String,
    /// "audio" / "video"
    pub kind: String,
}

/// 房间成员信息 (成员快照 / 广播共用)。
#[derive(Debug, Clone, Deserialize)]
pub struct ParticipantInfo {
    pub participant_id: String,
    #[serde(default = "default_true")]
    pub audio_enabled: bool,
    #[serde(default = "default_true")]
    pub video_enabled: bool,
    /// 该成员发布的轨道列表。
    #[serde(default)]
    pub tracks: Vec<TrackInfo>,
}

fn default_true() -> bool {
    true
}

/// 客户端 → 服务器
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    /// 可选握手: 协商协议版本, 服务端回 `welcome`。
    #[serde(rename = "hello")]
    Hello {
        protocol_version: u32,
        client: String,
    },

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
    },

    #[serde(rename = "subscribe")]
    Subscribe {
        pc_id: String,
        publisher_id: String,
        /// 发布者该轨道的 mid (来自 `participant_joined.tracks` 等)。
        track_mid: String,
        /// 订阅者自身接收该轨道的 mid (必填)。
        subscriber_mid: String,
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
    #[serde(rename = "welcome")]
    Welcome {
        protocol_version: u32,
        #[serde(default)]
        server: String,
    },

    #[serde(rename = "pong")]
    Pong {
        #[serde(default)]
        t: Option<u64>,
    },

    #[serde(rename = "sdp_answer")]
    SdpAnswer {
        pc_id: String,
        sdp: String,
        #[serde(default)]
        ice_candidates: Vec<String>,
    },

    /// `join` 成功确认, 携带房间成员快照 (不含自己)。
    #[serde(rename = "joined")]
    Joined {
        pc_id: String,
        participant_id: String,
        room_id: String,
        #[serde(default)]
        role: String,
        #[serde(default)]
        participants: Vec<ParticipantInfo>,
    },

    #[serde(rename = "left")]
    Left { pc_id: String },

    #[serde(rename = "ice_candidate")]
    IceCandidate {
        pc_id: String,
        candidate: String,
        #[serde(default)]
        sdp_mid: Option<String>,
    },

    #[serde(rename = "participant_joined")]
    ParticipantJoined {
        participant_id: String,
        /// 新成员的发布轨道列表 (可能为空: 刚加入尚未发布)。
        #[serde(default)]
        tracks: Vec<TrackInfo>,
    },

    #[serde(rename = "participant_left")]
    ParticipantLeft { participant_id: String },

    /// 某成员发布的轨道列表发生变化 (重协商新增/删除 track)。
    #[serde(rename = "participant_tracks")]
    ParticipantTracks {
        participant_id: String,
        #[serde(default)]
        tracks: Vec<TrackInfo>,
    },

    /// 房间内某成员的媒体状态变化 (静音 / 关摄像头)。
    #[serde(rename = "participant_updated")]
    ParticipantUpdated {
        participant_id: String,
        #[serde(default = "default_true")]
        audio_enabled: bool,
        #[serde(default = "default_true")]
        video_enabled: bool,
    },

    #[serde(rename = "subscribed")]
    Subscribed { pc_id: String, publisher_id: String },

    #[serde(rename = "unsubscribed")]
    Unsubscribed { pc_id: String, publisher_id: String },

    /// 房间成员快照 (响应 `get_room_state`)。
    #[serde(rename = "room_state")]
    RoomState {
        #[serde(default)]
        room_id: String,
        #[serde(default)]
        participants: Vec<ParticipantInfo>,
    },

    #[serde(rename = "error")]
    Error {
        #[serde(default)]
        pc_id: Option<String>,
        #[serde(default)]
        error: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_serializes_required_subscriber_mid() {
        let msg = ClientMessage::Subscribe {
            pc_id: "sub-bob".into(),
            publisher_id: "bob".into(),
            track_mid: "JOy".into(),
            subscriber_mid: "0".into(),
        };
        let v: serde_json::Value = serde_json::from_str(&msg.to_json()).unwrap();
        assert_eq!(v["type"], "subscribe");
        assert_eq!(v["track_mid"], "JOy");
        assert_eq!(v["subscriber_mid"], "0");
    }

    #[test]
    fn ice_candidate_omits_mline_index() {
        let msg = ClientMessage::IceCandidate {
            pc_id: "pub".into(),
            candidate: "c".into(),
            sdp_mid: Some("0".into()),
        };
        let v: serde_json::Value = serde_json::from_str(&msg.to_json()).unwrap();
        assert!(v.get("sdp_mline_index").is_none());
    }

    #[test]
    fn parses_joined_snapshot_with_tracks() {
        let json = r#"{"type":"joined","pc_id":"pub","participant_id":"me","room_id":"r","role":"publisher","participants":[{"participant_id":"bob","audio_enabled":true,"video_enabled":true,"tracks":[{"mid":"a1","kind":"audio"},{"mid":"v1","kind":"video"}]}]}"#;
        match serde_json::from_str::<ServerMessage>(json).unwrap() {
            ServerMessage::Joined {
                participants,
                room_id,
                ..
            } => {
                assert_eq!(room_id, "r");
                assert_eq!(participants.len(), 1);
                assert_eq!(participants[0].tracks.len(), 2);
                assert_eq!(participants[0].tracks[1].mid, "v1");
                assert_eq!(participants[0].tracks[1].kind, "video");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_participant_tracks_update() {
        let json = r#"{"type":"participant_tracks","participant_id":"bob","tracks":[{"mid":"s1","kind":"video"}]}"#;
        match serde_json::from_str::<ServerMessage>(json).unwrap() {
            ServerMessage::ParticipantTracks {
                participant_id,
                tracks,
            } => {
                assert_eq!(participant_id, "bob");
                assert_eq!(tracks[0].mid, "s1");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
