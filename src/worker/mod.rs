//! Worker 层 — per-core 事件循环、跨 worker 中继与定时器
//!
//! 每个 worker 线程独占一个 CPU 核心与独立 UDP 端口，运行一个完全同步的
//! 事件循环 (媒体路径不经过 async 运行时)。本模块按职责拆分:
//!
//! - [`event_loop`] — 线程主循环 (收包 → 定时器 → 命令 → 发包 → epoll)
//! - [`packet`] —     UDP 包 → str0m 实例的 O(1) 路由与命令处理
//! - [`forward`] —    RTP 包级转发、simulcast/SVC 选层、跨 worker 中继投递
//! - [`output`] —     str0m 输出的 drain 与定时器调度
//! - [`relay`] —      跨 worker 中继总线 ([`RelayHub`])
//! - [`timer`] —      基于 deadline 的最小堆定时器
//!
//! 跨 worker / 跨主机中继设计见 `docs/multi-worker-relay.md`。

mod event_loop;
mod forward;
mod output;
mod packet;
pub mod relay;
pub mod timer;

pub use event_loop::run_worker;
pub use relay::{dispatch_relay_control, RelayHub, RelayedRtp};

use crate::net::uring::IoUringMode;
use crossbeam_channel::{Receiver, Sender};
use std::net::SocketAddr;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use str0m::media::Rid;

/// 每个 worker 的信令命令队列容量。
///
/// 控制命令 (create/subscribe/leave) 低频且不允许丢弃，容量取大以减少
/// 背压；真的写满 (worker 卡死) 时由 `try_send` 丢弃并计数。
pub const CMD_CHANNEL_CAP: usize = 16_384;

/// 每个 worker 的中继媒体队列容量。
///
/// 媒体包可以丢弃: 消费端跟不上时，丢弃新包比无界堆积到 OOM 更安全。
pub const RELAY_CHANNEL_CAP: usize = 4_096;

/// Worker 线程配置
pub struct WorkerConfig {
    pub cpu_id: usize,
    /// UDP 实际绑定地址 (通常 0.0.0.0:base_port)
    pub listen_addr: SocketAddr,
    /// 对外通告地址 (announce_ip:base_port), 用于 ICE host 候选与 Receive destination
    pub announce_addr: SocketAddr,
    pub batch_size: usize,
    pub send_batch_size: usize,
    /// 发送队列积压上限 (超过时丢弃最旧的包)
    pub send_backlog: usize,
    pub busy_poll_us: u32,
    /// BWE 初始带宽估计 (bit/s)
    pub bwe_initial_bps: u64,
    /// 收包/等待 I/O 后端选择 (io_uring / epoll)
    pub io_uring: IoUringMode,
    /// UDP socket 收发缓冲区目标大小 (字节)
    pub sock_buf_bytes: usize,
    /// 本 worker 在当前进程内的编号 (0-based)
    pub self_idx: usize,
    /// 跨 worker 中继总线
    pub relay: Arc<RelayHub>,
    /// 本 worker 的中继接收端
    pub relay_rx: Receiver<RelayedRtp>,
    /// 中继唤醒读端 (注册进 epoll)
    pub wake_rx: UnixStream,
}

/// 信令层 → Worker 的命令 (通过无锁 channel 传递)
#[derive(Debug)]
pub enum WorkerCommand {
    /// 创建新参与者 (SDP offer → answer)
    CreateParticipant {
        room_id: String,
        participant_id: String,
        sdp_offer: String,
        reply_tx: Sender<WorkerReply>,
    },

    /// 对已存在的参与者重新协商 (ICE restart / 新增/删除轨道), SDP offer → answer
    Renegotiate {
        participant_id: String,
        sdp_offer: String,
        reply_tx: Sender<WorkerReply>,
    },

    /// 添加远程 ICE 候选
    AddRemoteCandidate {
        participant_id: String,
        candidate: String,
        sdp_mid: Option<String>,
    },

    /// 移除参与者
    RemoveParticipant { participant_id: String },

    /// 查询发布者已发布的轨道 (WHEP 等“按发布者订阅”用)
    GetPublisherTracks {
        publisher_id: String,
        reply_tx: Sender<WorkerReply>,
    },

    /// 订阅发布者的轨道
    Subscribe {
        room_id: String,
        subscriber_id: String,
        publisher_id: String,
        publisher_mid: String,
        /// 订阅者自身该轨道的 mid (None = 与 publisher_mid 相同)
        subscriber_mid: Option<String>,
        /// 发布者所在 worker (用于判断是否需要跨 worker 中继)
        publisher_worker: usize,
    },

    /// 取消订阅
    Unsubscribe {
        subscriber_id: String,
        publisher_id: String,
        publisher_mid: String,
        publisher_worker: usize,
    },

    /// 在发布者 worker 上登记一个远端订阅者 (用于中继)
    RegisterRelaySubscriber {
        publisher_id: String,
        publisher_mid: String,
        /// 订阅者所在 worker
        subscriber_worker: usize,
        subscriber_id: String,
        subscriber_mid: Option<String>,
    },

    /// 注销发布者 worker 上的远端订阅者
    UnregisterRelaySubscriber {
        publisher_id: String,
        publisher_mid: String,
        subscriber_id: String,
    },

    /// 请求发布者重发关键帧 (跨 worker 的 PLI)
    RequestKeyframe {
        publisher_id: String,
        publisher_mid: String,
        rid: Option<Rid>,
    },

    /// 关闭 Worker
    Shutdown,
}

/// Worker → 信令层的回复
#[derive(Debug)]
pub enum WorkerReply {
    /// SDP Answer (成功创建参与者)
    SdpAnswer {
        participant_id: String,
        sdp_answer: String,
        local_candidates: Vec<String>,
    },

    /// 发布者的已发布轨道: (mid, kind) 列表; kind 为 "audio"/"video"
    PublisherTracks { tracks: Vec<(String, String)> },

    /// 错误
    Error {
        participant_id: String,
        error: String,
    },
}

#[cfg(test)]
mod tests;
