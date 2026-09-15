//! 信令层 — 同时支持 WebSocket 与 QUIC 两种传输
//!
//! ```text
//!                    ┌──────────────────────────────┐
//!   客户端 ──ws────► │  server.rs  (WebSocket 传输)  │ ─┐
//!                    └──────────────────────────────┘  │
//!                                                       ├─► session.rs ──► Worker
//!                    ┌──────────────────────────────┐  │   (协议/房间/订阅)
//!   客户端 ──quic──► │  quic.rs    (QUIC 传输)       │ ─┘
//!                    └──────────────────────────────┘
//! ```
//!
//! 两条传输承载相同的 JSON 协议 (`protocol.rs`), 复用同一套会话状态机
//! (`session.rs`)。QUIC 天然支持连接迁移: 移动端从 WiFi 切到蜂窝时 IP 变化,
//! 连接仍由 Connection ID 标识而不断开, 信令无需重连即可恢复。

pub mod protocol;
pub mod quic;
pub mod server;
pub mod session;

use crate::core::WorkerCommand;
use ahash::AHashMap;
use crossbeam_channel::Sender;
use parking_lot::Mutex;
use tokio::sync::mpsc;

/// 信令服务器全局状态
pub struct SignalingState {
    /// Worker 命令发送器 (每个 worker 一个)
    /// 同一房间的所有参与者 (发布者 + 订阅连接) 必须落在同一个 worker,
    /// 因为每个 worker 拥有独立的 participants/router, 跨 worker 无法转发媒体。
    pub worker_txs: Vec<Sender<WorkerCommand>>,
    /// 房间表: room_id → 房间信息
    pub rooms: Mutex<AHashMap<String, RoomInfo>>,
    /// 信令消息发送器: participant_id → channel sender
    /// 用于向特定参与者推送通知 (participant_joined / participant_left)。
    /// 与传输无关: WebSocket 与 QUIC 连接注册的 sender 语义完全一致。
    pub senders: Mutex<AHashMap<String, mpsc::UnboundedSender<String>>>,
}

/// 房间信息 (信令层)
#[derive(Debug)]
pub struct RoomInfo {
    pub id: String,
    pub participants: Vec<String>,
}

impl SignalingState {
    pub fn new(worker_txs: Vec<Sender<WorkerCommand>>) -> Self {
        Self {
            worker_txs,
            rooms: Mutex::new(AHashMap::new()),
            senders: Mutex::new(AHashMap::new()),
        }
    }

    /// 按房间选择 worker (稳定哈希)
    ///
    /// 同一房间的发布者和订阅连接必须落在同一个 worker 上,
    /// 否则订阅关系与媒体转发会落到不同的 participants/router 中而失败。
    /// 不同房间则尽量均匀分布到不同 worker。
    pub fn select_worker(&self, room_id: &str) -> usize {
        let n = self.worker_txs.len();
        if n <= 1 {
            return 0;
        }
        // FNV-1a 64 位稳定哈希
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for b in room_id.as_bytes() {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        (hash as usize) % n
    }

    /// 加入房间
    pub fn join_room(&self, room_id: &str, participant_id: &str) {
        let mut rooms = self.rooms.lock();
        let room = rooms
            .entry(room_id.to_string())
            .or_insert_with(|| RoomInfo {
                id: room_id.to_string(),
                participants: Vec::new(),
            });
        let pid = participant_id.to_string();
        if !room.participants.contains(&pid) {
            room.participants.push(pid);
        }
    }

    /// 离开房间
    pub fn leave_room(&self, room_id: &str, participant_id: &str) {
        let mut rooms = self.rooms.lock();
        if let Some(room) = rooms.get_mut(room_id) {
            room.participants.retain(|p| p != participant_id);
            if room.participants.is_empty() {
                rooms.remove(room_id);
            }
        }
    }

    /// 获取房间内其他参与者
    pub fn room_participants(&self, room_id: &str, exclude: &str) -> Vec<String> {
        let rooms = self.rooms.lock();
        rooms
            .get(room_id)
            .map(|r| {
                r.participants
                    .iter()
                    .filter(|p| p.as_str() != exclude)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 注册信令消息发送器 (WebSocket / QUIC 通用)
    pub fn register_sender(&self, participant_id: &str, tx: mpsc::UnboundedSender<String>) {
        self.senders.lock().insert(participant_id.to_string(), tx);
    }

    /// 注销信令消息发送器
    pub fn unregister_sender(&self, participant_id: &str) {
        self.senders.lock().remove(participant_id);
    }

    /// 向特定参与者发送消息
    pub fn send_to(&self, participant_id: &str, msg: &str) {
        if let Some(tx) = self.senders.lock().get(participant_id) {
            let _ = tx.send(msg.to_string());
        }
    }

    /// 通知房间内所有参与者 (排除指定参与者)
    pub fn notify_room(&self, room_id: &str, exclude: &str, msg: &str) {
        let participants: Vec<String> = {
            let rooms = self.rooms.lock();
            rooms
                .get(room_id)
                .map(|r| {
                    r.participants
                        .iter()
                        .filter(|p| p.as_str() != exclude)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default()
        };
        let senders = self.senders.lock();
        for pid in &participants {
            if let Some(tx) = senders.get(pid) {
                let _ = tx.send(msg.to_string());
            }
        }
    }
}
