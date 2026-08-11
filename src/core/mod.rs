//! 核心层 — Worker 事件循环与定时器

pub mod worker;
pub mod timer;

use std::net::SocketAddr;
use crossbeam_channel::Sender;

/// Worker 线程配置
pub struct WorkerConfig {
    pub cpu_id: usize,
    pub listen_addr: SocketAddr,
    pub batch_size: usize,
    pub send_batch_size: usize,
    pub busy_poll_us: u32,
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

    /// 添加远程 ICE 候选
    AddRemoteCandidate {
        participant_id: String,
        candidate: String,
        sdp_mid: Option<String>,
    },

    /// 移除参与者
    RemoveParticipant {
        participant_id: String,
    },

    /// 订阅发布者的轨道
    Subscribe {
        room_id: String,
        subscriber_id: String,
        publisher_id: String,
        publisher_mid: String,
    },

    /// 取消订阅
    Unsubscribe {
        subscriber_id: String,
        publisher_id: String,
        publisher_mid: String,
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

    /// 参与者已连接 (ICE + DTLS 完成)
    ParticipantConnected {
        participant_id: String,
    },

    /// 参与者已断开
    ParticipantDisconnected {
        participant_id: String,
    },

    /// 错误
    Error {
        participant_id: String,
        error: String,
    },
}
