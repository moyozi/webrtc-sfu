/** 信令消息类型定义 — 与 Rust 服务端 `SignalingMessage` / `SignalingResponse` 对应 */

// 客户端 → 服务器
export type ClientMessage =
  | { type: "join"; room_id: string; participant_id: string; sdp: string }
  | { type: "ice_candidate"; candidate: string; sdp_mid: string | null; sdp_mline_index: number | null }
  | { type: "subscribe"; publisher_id: string; track_mid: string }
  | { type: "unsubscribe"; publisher_id: string; track_mid: string }
  | { type: "leave" };

// 服务器 → 客户端
export type ServerMessage =
  | { type: "sdp_answer"; sdp: string; ice_candidates: string[] }
  | { type: "ice_candidate"; candidate: string; sdp_mid: string | null }
  | { type: "participant_joined"; participant_id: string }
  | { type: "participant_left"; participant_id: string }
  | { type: "subscribed"; publisher_id: string }
  | { type: "error"; error: string };

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
