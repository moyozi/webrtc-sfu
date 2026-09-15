//! 信令层 — WebSocket 信令服务器
//!
//! 负责:
//! - WebSocket 连接管理
//! - SDP offer/answer 交换
//! - Trickle ICE 候选交换
//! - Room 创建/加入/离开
//! - 订阅/取消订阅控制
//! - 与 Worker 线程通过无锁 channel 通信

pub mod server;

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
    /// WebSocket 消息发送器: participant_id → channel sender
    /// 用于向特定参与者推送通知 (participant_joined / participant_left)
    pub ws_senders: Mutex<AHashMap<String, mpsc::UnboundedSender<String>>>,
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
            ws_senders: Mutex::new(AHashMap::new()),
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

    /// 注册 WebSocket 消息发送器
    pub fn register_ws_sender(&self, participant_id: &str, tx: mpsc::UnboundedSender<String>) {
        self.ws_senders.lock().insert(participant_id.to_string(), tx);
    }

    /// 注销 WebSocket 消息发送器
    pub fn unregister_ws_sender(&self, participant_id: &str) {
        self.ws_senders.lock().remove(participant_id);
    }

    /// 向特定参与者发送消息
    pub fn send_to(&self, participant_id: &str, msg: &str) {
        if let Some(tx) = self.ws_senders.lock().get(participant_id) {
            let _ = tx.send(msg.to_string());
        }
    }

    /// 通知房间内所有参与者 (排除指定参与者)
    pub fn notify_room(&self, room_id: &str, exclude: &str, msg: &str) {
        let participants: Vec<String> = {
            let rooms = self.rooms.lock();
            rooms
                .get(room_id)
                .map(|r| r.participants.iter().filter(|p| p.as_str() != exclude).cloned().collect())
                .unwrap_or_default()
        };
        let senders = self.ws_senders.lock();
        for pid in &participants {
            if let Some(tx) = senders.get(pid) {
                let _ = tx.send(msg.to_string());
            }
        }
    }
}
