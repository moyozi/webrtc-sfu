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

pub mod auth;
pub mod http;
pub mod protocol;
pub mod quic;
pub mod server;
pub mod session;
pub mod whep;
pub mod whip;

use crate::cluster::WorkerRouter;
use crate::signaling::protocol::ParticipantInfo;
use crate::worker::{WorkerCommand, WorkerReply};
use ahash::AHashMap;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

/// 每条信令连接的发送队列容量。
///
/// 控制消息量小，容量用于吸收突发 (如房间内大量 participant_joined 广播)。
/// 写满说明客户端读取过慢，丢弃并断开推送，避免服务端内存被拖垮。
pub const SIGNALING_CHANNEL_CAP: usize = 256;

/// 信令服务器全局状态
pub struct SignalingState {
    /// 统一的 worker 命令路由 (本地 channel 或远端 TCP 控制面)。
    pub router: Arc<WorkerRouter>,
    /// 整集群全局 worker 总数 (0..total_workers)。
    pub total_workers: u32,
    /// 房间 → 允许的全局 worker 列表。
    /// 长度为 1 时房间钉定到该 worker; 长度 >1 时房间内参与者轮转分布。
    /// 未匹配的房间按房间 id 哈希固定 (或 `default_worker` 覆盖)。
    room_workers: AHashMap<String, Vec<usize>>,
    /// 未配置房间的默认全局 worker 覆盖 (None = 按房间哈希)。
    default_worker: Option<usize>,
    /// 房间 → round-robin 游标 (仅房间配置了多个 worker 时使用)。
    room_cursor: Mutex<AHashMap<String, usize>>,
    /// 房间表: room_id → 房间信息
    pub rooms: Mutex<AHashMap<String, RoomInfo>>,
    /// 信令消息发送器: participant_id → channel sender
    /// 用于向特定参与者推送通知 (participant_joined / participant_left)。
    /// 与传输无关: WebSocket 与 QUIC 连接注册的 sender 语义完全一致。
    pub senders: Mutex<AHashMap<String, mpsc::Sender<String>>>,
    /// participant_id → worker_idx (用于订阅时定位发布者所在 worker)
    pub participant_worker: Mutex<AHashMap<String, usize>>,
    /// JWT 用户鉴权 (未配置密钥时整体禁用)。
    pub auth: Arc<auth::Authenticator>,
}

/// 房间信息 (信令层)
#[derive(Debug)]
pub struct RoomInfo {
    pub participants: Vec<ParticipantInfo>,
}

impl SignalingState {
    pub fn new(
        router: Arc<WorkerRouter>,
        total_workers: u32,
        room_workers: HashMap<String, Vec<u32>>,
        default_worker: Option<u32>,
        auth: Arc<auth::Authenticator>,
    ) -> Self {
        let total = total_workers.max(1);
        let room_workers = room_workers
            .into_iter()
            .map(|(room, workers)| {
                let workers = workers
                    .into_iter()
                    .map(|w| w as usize)
                    .filter(|w| *w < total as usize)
                    .collect();
                (room, workers)
            })
            .collect();
        Self {
            router,
            total_workers: total,
            room_workers,
            default_worker: default_worker.map(|w| (w as usize).min(total as usize - 1)),
            room_cursor: Mutex::new(AHashMap::new()),
            rooms: Mutex::new(AHashMap::new()),
            senders: Mutex::new(AHashMap::new()),
            participant_worker: Mutex::new(AHashMap::new()),
            auth,
        }
    }

    /// 为参与者分配**全局** worker。
    ///
    /// 默认每个房间按其 id 哈希**固定到一个 worker**（同房间同 worker,
    /// 不同房间自动分散）; 也可用 `SFU_DEFAULT_WORKER` 强制未配置房间落同一 worker。
    /// 只有通过 `SFU_ROOM_WORKERS` 显式配置的房间才会:
    /// - 配置单个 worker → 房间钉定到该 worker;
    /// - 配置多个 worker → 房间内参与者轮转, 使一个房间跨 worker / 跨主机
    ///   (单机多 worker / 多主机多 worker 均需显式配置)。
    pub fn assign_worker(&self, room_id: &str, participant_id: &str) -> usize {
        let idx = pick_worker(
            &self.room_workers,
            self.default_worker,
            self.total_workers as usize,
            &self.room_cursor,
            room_id,
        );
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

    /// 查询发布者已发布的轨道 (mid, kind)。
    pub fn publisher_tracks(&self, global: usize, publisher_id: String) -> Vec<(String, String)> {
        self.router
            .publisher_tracks(global, publisher_id)
            .unwrap_or_default()
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

    /// 加入房间 (发布者; 默认音频/视频均开启)。
    pub fn join_room(&self, room_id: &str, participant_id: &str) {
        let mut rooms = self.rooms.lock();
        let room = rooms.entry(room_id.to_string()).or_insert_with(|| RoomInfo {
            participants: Vec::new(),
        });
        if !room
            .participants
            .iter()
            .any(|p| p.participant_id == participant_id)
        {
            room.participants.push(ParticipantInfo {
                participant_id: participant_id.to_string(),
                audio_enabled: true,
                video_enabled: true,
            });
        }
    }

    /// 离开房间
    pub fn leave_room(&self, room_id: &str, participant_id: &str) {
        let empty = {
            let mut rooms = self.rooms.lock();
            match rooms.get_mut(room_id) {
                Some(room) => {
                    room.participants
                        .retain(|p| p.participant_id != participant_id);
                    if room.participants.is_empty() {
                        rooms.remove(room_id);
                        true
                    } else {
                        false
                    }
                }
                None => false,
            }
        };
        // 房间已销毁 → 清理其分配游标, 避免内存泄漏。
        if empty {
            self.room_cursor.lock().remove(room_id);
        }
    }

    /// 获取房间内其他参与者的 id (排除指定参与者)
    #[allow(dead_code)]
    pub fn room_participants(&self, room_id: &str, exclude: &str) -> Vec<String> {
        self.room_roster(room_id)
            .into_iter()
            .filter(|p| p.participant_id != exclude)
            .map(|p| p.participant_id)
            .collect()
    }

    /// 获取房间成员快照 (含自己, 若在房间内)。
    pub fn room_roster(&self, room_id: &str) -> Vec<ParticipantInfo> {
        self.rooms
            .lock()
            .get(room_id)
            .map(|r| r.participants.clone())
            .unwrap_or_default()
    }

    /// 更新成员媒体状态, 返回更新后的快照 (不在房间内则返回 None)。
    pub fn update_media(
        &self,
        room_id: &str,
        participant_id: &str,
        audio_enabled: Option<bool>,
        video_enabled: Option<bool>,
    ) -> Option<ParticipantInfo> {
        let mut rooms = self.rooms.lock();
        let participant = rooms
            .get_mut(room_id)?
            .participants
            .iter_mut()
            .find(|p| p.participant_id == participant_id)?;
        if let Some(a) = audio_enabled {
            participant.audio_enabled = a;
        }
        if let Some(v) = video_enabled {
            participant.video_enabled = v;
        }
        Some(participant.clone())
    }

    /// 注册信令消息发送器 (WebSocket / QUIC 通用)
    pub fn register_sender(&self, participant_id: &str, tx: mpsc::Sender<String>) {
        self.senders.lock().insert(participant_id.to_string(), tx);
    }

    /// 注销信令消息发送器
    pub fn unregister_sender(&self, participant_id: &str) {
        self.senders.lock().remove(participant_id);
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
                        .filter(|p| p.participant_id != exclude)
                        .map(|p| p.participant_id.clone())
                        .collect()
                })
                .unwrap_or_default()
        };
        let mut senders = self.senders.lock();
        let mut failed: Vec<String> = Vec::new();
        for pid in &participants {
            if let Some(tx) = senders.get(pid) {
                if tx.try_send(msg.to_string()).is_err() {
                    failed.push(pid.clone());
                }
            }
        }
        for pid in failed {
            warn!("信令发送队列已满, 断开参与者 {} 的推送", pid);
            senders.remove(&pid);
        }
    }
}

/// 依据房间配置挑选全局 worker (不修改参与者状态, 便于单测)。
///
/// 优先级: 精确房间规则 > `*` 兜底规则 > `default_worker` 覆盖 > 房间 id 哈希。
/// 候选只有一个时固定; 多个时按房间内游标轮转。
fn pick_worker(
    room_workers: &AHashMap<String, Vec<usize>>,
    default_worker: Option<usize>,
    total: usize,
    room_cursor: &Mutex<AHashMap<String, usize>>,
    room_id: &str,
) -> usize {
    let idx = match room_workers.get(room_id).or_else(|| room_workers.get("*")) {
        Some(candidates) if candidates.len() == 1 => candidates[0],
        Some(candidates) if candidates.len() > 1 => {
            let mut cursors = room_cursor.lock();
            let cursor = cursors.entry(room_id.to_string()).or_insert(0);
            let idx = candidates[*cursor % candidates.len()];
            *cursor = cursor.wrapping_add(1);
            idx
        }
        // 未配置 → 按房间 id 哈希固定 (同房间的所有参与者落同一 worker)。
        // `default_worker` 显式设置时覆盖哈希。
        _ => match default_worker {
            Some(w) => w,
            None => (room_hash(room_id) % total.max(1) as u64) as usize,
        },
    };
    idx.min(total.saturating_sub(1))
}

/// FNV-1a 64-bit — 稳定、无随机种子, 同一 room_id 始终映射到同一 worker。
fn room_hash(room_id: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in room_id.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    fn room_workers(rules: &[(&str, &[usize])]) -> AHashMap<String, Vec<usize>> {
        rules
            .iter()
            .map(|(r, ws)| (r.to_string(), ws.to_vec()))
            .collect()
    }

    #[test]
    fn default_hashes_rooms_stickily() {
        let cfg = AHashMap::new();
        let cursor = Mutex::new(AHashMap::new());
        let rooms = ["a", "b", "c", "d", "e", "f", "g", "h"];
        // 同一房间始终同一 worker。
        for room in rooms {
            let first = pick_worker(&cfg, None, 8, &cursor, room);
            assert!(first < 8);
            assert_eq!(pick_worker(&cfg, None, 8, &cursor, room), first);
        }
        // 不同房间应分散到不同 worker。
        let distinct: std::collections::HashSet<usize> = rooms
            .iter()
            .map(|r| pick_worker(&cfg, None, 8, &cursor, r))
            .collect();
        assert!(distinct.len() > 1, "房间哈希应使不同房间分散");
    }

    #[test]
    fn default_worker_override() {
        let cfg = AHashMap::new();
        let cursor = Mutex::new(AHashMap::new());
        assert_eq!(pick_worker(&cfg, Some(3), 8, &cursor, "room"), 3);
    }

    #[test]
    fn single_worker_pins_room() {
        let cfg = room_workers(&[("room", &[5])]);
        let cursor = Mutex::new(AHashMap::new());
        for _ in 0..4 {
            assert_eq!(pick_worker(&cfg, None, 8, &cursor, "room"), 5);
        }
        // 其它房间仍按哈希默认。
        assert_eq!(
            pick_worker(&cfg, None, 8, &cursor, "other"),
            (room_hash("other") % 8) as usize
        );
    }

    #[test]
    fn multiple_workers_spread_within_room() {
        let cfg = room_workers(&[("room", &[1, 2, 3])]);
        let cursor = Mutex::new(AHashMap::new());
        let got: Vec<usize> = (0..6)
            .map(|_| pick_worker(&cfg, None, 8, &cursor, "room"))
            .collect();
        assert_eq!(got, vec![1, 2, 3, 1, 2, 3]);
    }

    #[test]
    fn wildcard_rule_is_fallback() {
        let cfg = room_workers(&[("special", &[7]), ("*", &[4, 5])]);
        let cursor = Mutex::new(AHashMap::new());
        assert_eq!(pick_worker(&cfg, None, 8, &cursor, "special"), 7);
        // 未命中精确规则的房间回退到 `*`, 游标按房间独立。
        assert_eq!(pick_worker(&cfg, None, 8, &cursor, "a"), 4);
        assert_eq!(pick_worker(&cfg, None, 8, &cursor, "a"), 5);
        assert_eq!(pick_worker(&cfg, None, 8, &cursor, "b"), 4);
    }
}
