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
pub mod whip;

use crate::cluster::WorkerRouter;
use crate::core::{WorkerCommand, WorkerReply};
use ahash::AHashMap;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

/// 信令服务器全局状态
pub struct SignalingState {
    /// 统一的 worker 命令路由 (本地 channel 或远端 TCP 控制面)。
    pub router: Arc<WorkerRouter>,
    /// 整集群全局 worker 总数 (0..total_workers)。
    pub total_workers: u32,
    /// 全局 round-robin 计数器 (房间跨 worker/主机分配)。
    assign_counter: AtomicUsize,
    /// 房间表: room_id → 房间信息
    pub rooms: Mutex<AHashMap<String, RoomInfo>>,
    /// 信令消息发送器: participant_id → channel sender
    /// 用于向特定参与者推送通知 (participant_joined / participant_left)。
    /// 与传输无关: WebSocket 与 QUIC 连接注册的 sender 语义完全一致。
    pub senders: Mutex<AHashMap<String, mpsc::UnboundedSender<String>>>,
    /// participant_id → worker_idx (用于订阅时定位发布者所在 worker)
    pub participant_worker: Mutex<AHashMap<String, usize>>,
}

/// 房间信息 (信令层)
#[derive(Debug)]
pub struct RoomInfo {
    pub id: String,
    pub participants: Vec<String>,
}

impl SignalingState {
    pub fn new(router: Arc<WorkerRouter>, total_workers: u32) -> Self {
        Self {
            router,
            total_workers: total_workers.max(1),
            assign_counter: AtomicUsize::new(0),
            rooms: Mutex::new(AHashMap::new()),
            senders: Mutex::new(AHashMap::new()),
            participant_worker: Mutex::new(AHashMap::new()),
        }
    }

    /// 为参与者分配**全局** worker: round-robin 覆盖整集群, 使一个房间
    /// 可以跨 worker / 跨主机分布。
    pub fn assign_worker(&self, _room_id: &str, participant_id: &str) -> usize {
        let total = self.total_workers as usize;
        let idx = self.assign_counter.fetch_add(1, Ordering::Relaxed) % total;
        self.participant_worker
            .lock()
            .insert(participant_id.to_string(), idx);
        idx
    }

    /// 创建参与者 (可能需要远端 worker 往返)。
    pub fn create_participant(
        &self,
        global: usize,
        room_id: String,
        participant_id: String,
        sdp_offer: String,
    ) -> Option<WorkerReply> {
        self.router
            .create_participant(global, room_id, participant_id, sdp_offer)
    }

    /// 发送无需回复的 worker 命令。
    pub fn send_worker(&self, global: usize, cmd: WorkerCommand) {
        self.router.send(global, cmd);
    }

    /// 查询参与者所在 worker (未知默认 0)。
    pub fn worker_of(&self, participant_id: &str) -> usize {
        self.participant_worker
            .lock()
            .get(participant_id)
            .copied()
            .unwrap_or(0)
    }

    /// 参与者离开时清除其 worker 绑定。
    pub fn release_worker(&self, _room_id: &str, participant_id: &str) {
        self.participant_worker.lock().remove(participant_id);
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
