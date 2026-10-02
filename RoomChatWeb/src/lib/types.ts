/** 信令消息类型定义 — 与 Rust 服务端 `SignalingMessage` / `SignalingResponse` 对应 */

// 客户端 → 服务器
// 每条消息带 pc_id, 用于在同一条 WebSocket 上区分不同的 RTCPeerConnection:
//   - "pub"            发布连接 (sendonly)
//   - "sub-<remoteId>" 订阅某个远端参与者的连接 (recvonly)
export type ClientMessage =
  | { type: "hello"; protocol_version?: number; client?: string }
  | { type: "ping"; t?: number }
  | { type: "join"; pc_id: string; room_id: string; participant_id: string; sdp: string; role: "publisher" | "subscriber"; token?: string }
  | { type: "renegotiate"; pc_id: string; sdp: string; token?: string }
  | { type: "ice_candidate"; pc_id: string; candidate: string; sdp_mid: string | null }
  | { type: "subscribe"; pc_id: string; publisher_id: string; track_mid: string; subscriber_mid: string }
  | { type: "unsubscribe"; pc_id: string; publisher_id: string; track_mid: string }
  | { type: "update_media"; pc_id: string; audio_enabled?: boolean; video_enabled?: boolean }
  | { type: "get_room_state"; room_id?: string }
  | { type: "leave"; pc_id: string };

/** 发布者的一条媒体轨道 (与 Rust `TrackInfo` 对应) */
export interface TrackInfo {
  mid: string;
  kind: string; // "audio" | "video"
}

/** 房间成员信息 (与 Rust `ParticipantInfo` 对应) */
export interface ParticipantInfo {
  participant_id: string;
  audio_enabled: boolean;
  video_enabled: boolean;
  /** 该成员发布的轨道列表 (一个参与者可有 音频 + 摄像头 + 屏幕共享 多路) */
  tracks?: TrackInfo[];
}

// 服务器 → 客户端
export type ServerMessage =
  | { type: "welcome"; protocol_version: number; server: string; features: string[] }
  | { type: "pong"; t?: number }
  | { type: "sdp_answer"; pc_id: string; sdp: string; ice_candidates: string[] }
  | { type: "joined"; pc_id: string; participant_id: string; room_id: string; role: string; participants: ParticipantInfo[] }
  | { type: "left"; pc_id: string }
  | { type: "ice_candidate"; pc_id: string; candidate: string; sdp_mid: string | null }
  | { type: "participant_joined"; participant_id: string; tracks?: TrackInfo[] }
  | { type: "participant_left"; participant_id: string }
  | { type: "participant_tracks"; participant_id: string; tracks: TrackInfo[] }
  | { type: "participant_updated"; participant_id: string; audio_enabled: boolean; video_enabled: boolean }
  | { type: "subscribed"; pc_id: string; publisher_id: string; track_mid?: string }
  | { type: "unsubscribed"; pc_id: string; publisher_id: string; track_mid?: string }
  | { type: "room_state"; room_id: string; participants: ParticipantInfo[] }
  | { type: "error"; pc_id?: string; code?: string; error: string };

/** 订阅状态 */
export type SubStatus = "idle" | "connecting" | "subscribed" | "error";

/** 日志条目 */
export interface LogEntry {
  time: string;
  msg: string;
  level: "info" | "success" | "warn" | "error";
}

/** 远端订阅信息 */
export interface RemoteParticipant {
  id: string;
  status: SubStatus;
  /** 主画面 (音频 + 第一路视频 = 摄像头) */
  stream: MediaStream | null;
  /** 第二路视频 (屏幕共享), 无则为 null */
  screenStream?: MediaStream | null;
  /** 该成员发布的轨道列表 (用于 UI 标注) */
  tracks?: TrackInfo[];
  /** 麦克风是否开启 (来自 participant_updated / joined 快照) */
  audioEnabled?: boolean;
  /** 摄像头是否开启 */
  videoEnabled?: boolean;
}
