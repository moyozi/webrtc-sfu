/** 信令消息类型定义 — 与 Rust 服务端 `SignalingMessage` / `SignalingResponse` 对应 */

// 客户端 → 服务器
// 每条消息带 pc_id, 用于在同一条 WebSocket 上区分不同的 RTCPeerConnection:
//   - "pub"            发布连接 (sendonly)
//   - "sub-<remoteId>" 订阅某个远端参与者的连接 (recvonly)
export type ClientMessage =
  | { type: "join"; pc_id: string; room_id: string; participant_id: string; sdp: string; role: "publisher" | "subscriber" }
  | { type: "ice_candidate"; pc_id: string; candidate: string; sdp_mid: string | null; sdp_mline_index: number | null }
  | { type: "subscribe"; pc_id: string; publisher_id: string; track_mid: string }
  | { type: "unsubscribe"; pc_id: string; publisher_id: string; track_mid: string }
  | { type: "leave"; pc_id: string };

// 服务器 → 客户端
export type ServerMessage =
  | { type: "sdp_answer"; pc_id: string; sdp: string; ice_candidates: string[] }
  | { type: "ice_candidate"; pc_id: string; candidate: string; sdp_mid: string | null }
  | { type: "participant_joined"; participant_id: string }
  | { type: "participant_left"; participant_id: string }
  | { type: "subscribed"; pc_id: string; publisher_id: string }
  | { type: "error"; pc_id?: string; error: string };

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
  stream: MediaStream | null;
}
