//! 核心层 — Worker 事件循环与定时器
//!
//! 跨 worker 中继 (`RelayHub` / `RelayedRtp`) 让同一房间可以分布到多个
//! worker 线程: 发布者 worker 解密出 RTP 后, 通过进程内 channel 把明文
//! 载荷中继给订阅者所在 worker, 由后者完成 PT 映射与逐订阅者 SRTP。
//! 详见 `docs/multi-worker-relay.md`。

pub mod worker;
pub mod timer;

use crate::net::relay::{self, RelayEndpoint};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::Instant;
use crossbeam_channel::{Receiver, Sender};
use str0m::format::PayloadParams;
use str0m::media::{Mid, Rid};
use str0m::rtp::{ExtensionValues, SeqNo};

/// 跨 worker 中继的一个明文 RTP 包。
///
/// `payload` 为 `Arc<[u8]>`, 在同一进程内多个目标 worker 间共享, 零拷贝。
#[derive(Clone)]
pub struct RelayedRtp {
    pub publisher_id: Arc<str>,
    pub publisher_mid: Mid,
    pub params: PayloadParams,
    pub seq_no: SeqNo,
    pub timestamp: u32,
    pub marker: bool,
    pub ext_vals: ExtensionValues,
    pub rid: Option<Rid>,
    /// 发布者入站 SSRC (远端 worker 用它区分各 simulcast 层)
    pub ssrc: str0m::rtp::Ssrc,
    /// 入站到达时刻 (作为 wallclock)
    pub arrival: Instant,
    pub payload: Arc<[u8]>,
}

/// 跨 worker 的中继控制消息 (可进程内或跨主机)。
pub enum RelayControl {
    Register {
        publisher_id: String,
        publisher_mid: Mid,
        subscriber_worker: usize,
        subscriber_id: String,
        subscriber_mid: Option<Mid>,
    },
    Unregister {
        publisher_id: String,
        publisher_mid: Mid,
        subscriber_id: String,
    },
    RequestKeyframe {
        publisher_id: String,
        publisher_mid: Mid,
        rid: Option<Rid>,
    },
}

/// 跨 worker / 跨主机中继总线。
///
/// worker 使用**全局编号**: 本进程拥有 `[worker_offset, worker_offset + local_workers)`，
/// 其余编号在远端进程。同一个房间可跨进程分布。
///
/// - 本地目标: 进程内 channel + `UnixStream` 唤醒。
/// - 远端目标: 经 `RelayEndpoint` (UDP) 序列化发送；对端 relay 监听线程解码后
///   按 `target_worker` 重新注入本地 channel (见 `net::relay`)。
pub struct RelayHub {
    /// 本地 worker 的中继队列 (下标 = 本地编号)
    pub relay_txs: Vec<Sender<RelayedRtp>>,
    /// 本地 worker 的唤醒管道 (下标 = 本地编号)
    pub wake_txs: Vec<Arc<UnixStream>>,
    /// 本地 worker 的控制命令 (下标 = 本地编号)
    pub cmd_txs: Vec<Sender<WorkerCommand>>,
    /// 本进程 worker 全局编号起始
    pub worker_offset: u32,
    /// 本进程 worker 数
    pub local_workers: u32,
    /// 远端 worker 全局编号 → 其所属进程的 relay UDP 地址
    pub peer_addrs: HashMap<u32, SocketAddr>,
    /// 本进程 relay UDP 端点 (跨主机发送; None = 纯进程内)
    pub endpoint: Option<Arc<RelayEndpoint>>,
}

impl RelayHub {
    pub fn local_index(&self, global: u32) -> Option<usize> {
        if global >= self.worker_offset
            && global < self.worker_offset + self.local_workers
        {
            Some((global - self.worker_offset) as usize)
        } else {
            None
        }
    }

    /// 向全局编号 `target_global` 的 worker 发送一个中继 RTP 包。
    pub fn relay(&self, target_global: u32, pkt: RelayedRtp) {
        if let Some(li) = self.local_index(target_global) {
            if let Some(tx) = self.relay_txs.get(li) {
                if tx.send(pkt).is_ok() {
                    self.wake(li);
                }
            }
            return;
        }
        if let (Some(ep), Some(addr)) = (&self.endpoint, self.peer_addrs.get(&target_global)) {
            let buf = relay::encode_rtp(target_global, &pkt);
            let _ = ep.send_to(&buf, *addr);
        }
    }

    /// 向全局编号 `target_global` 的 worker 发送中继控制消息。
    pub fn send_control(&self, target_global: u32, ctrl: RelayControl) {
        if let Some(li) = self.local_index(target_global) {
            let cmd = match ctrl {
                RelayControl::Register {
                    publisher_id,
                    publisher_mid,
                    subscriber_worker,
                    subscriber_id,
                    subscriber_mid,
                } => WorkerCommand::RegisterRelaySubscriber {
                    publisher_id,
                    publisher_mid: publisher_mid.to_string(),
                    subscriber_worker,
                    subscriber_id,
                    subscriber_mid: subscriber_mid.map(|m| m.to_string()),
                },
                RelayControl::Unregister {
                    publisher_id,
                    publisher_mid,
                    subscriber_id,
                } => WorkerCommand::UnregisterRelaySubscriber {
                    publisher_id,
                    publisher_mid: publisher_mid.to_string(),
                    subscriber_id,
                },
                RelayControl::RequestKeyframe {
                    publisher_id,
                    publisher_mid,
                    rid,
                } => WorkerCommand::RequestKeyframe {
                    publisher_id,
                    publisher_mid: publisher_mid.to_string(),
                    rid,
                },
            };
            if let Some(tx) = self.cmd_txs.get(li) {
                if tx.send(cmd).is_ok() {
                    self.wake(li);
                }
            }
            return;
        }
        if let (Some(ep), Some(addr)) = (&self.endpoint, self.peer_addrs.get(&target_global)) {
            let buf = match ctrl {
                RelayControl::Register {
                    publisher_id,
                    publisher_mid,
                    subscriber_worker,
                    subscriber_id,
                    subscriber_mid,
                } => relay::encode_register(
                    target_global,
                    &publisher_id,
                    publisher_mid,
                    subscriber_worker,
                    &subscriber_id,
                    subscriber_mid,
                ),
                RelayControl::Unregister {
                    publisher_id,
                    publisher_mid,
                    subscriber_id,
                } => relay::encode_unregister(
                    target_global,
                    &publisher_id,
                    publisher_mid,
                    &subscriber_id,
                ),
                RelayControl::RequestKeyframe {
                    publisher_id,
                    publisher_mid,
                    rid,
                } => relay::encode_request_keyframe(
                    target_global,
                    &publisher_id,
                    publisher_mid,
                    rid,
                ),
            };
            let _ = ep.send_to(&buf, *addr);
        }
    }

    /// 唤醒本地 worker (drain 中继队列)。
    pub fn wake(&self, local: usize) {
        if let Some(w) = self.wake_txs.get(local) {
            use std::io::Write;
            // 非阻塞: EAGAIN 说明已有未读字节, 目标一定会被唤醒。
            let _ = (&**w).write(&[1u8]);
        }
    }
}

/// 把解码后的 relay 控制消息注入本地 worker 的命令通道。
pub fn dispatch_relay_control(hub: &RelayHub, decoded: relay::DecodedControl) {
    let (target_global, cmd) = match decoded {
        relay::DecodedControl::Register {
            target_worker,
            publisher_id,
            publisher_mid,
            subscriber_worker,
            subscriber_id,
            subscriber_mid,
        } => (
            target_worker,
            WorkerCommand::RegisterRelaySubscriber {
                publisher_id,
                publisher_mid: publisher_mid.to_string(),
                subscriber_worker,
                subscriber_id,
                subscriber_mid: subscriber_mid.map(|m| m.to_string()),
            },
        ),
        relay::DecodedControl::Unregister {
            target_worker,
            publisher_id,
            publisher_mid,
            subscriber_id,
        } => (
            target_worker,
            WorkerCommand::UnregisterRelaySubscriber {
                publisher_id,
                publisher_mid: publisher_mid.to_string(),
                subscriber_id,
            },
        ),
        relay::DecodedControl::RequestKeyframe {
            target_worker,
            publisher_id,
            publisher_mid,
            rid,
        } => (
            target_worker,
            WorkerCommand::RequestKeyframe {
                publisher_id,
                publisher_mid: publisher_mid.to_string(),
                rid,
            },
        ),
    };
    if let Some(li) = hub.local_index(target_global) {
        if let Some(tx) = hub.cmd_txs.get(li) {
            if tx.send(cmd).is_ok() {
                hub.wake(li);
            }
        }
    }
}

/// Worker 线程配置
pub struct WorkerConfig {
    pub cpu_id: usize,
    /// UDP 实际绑定地址 (通常 0.0.0.0:base_port)
    pub listen_addr: SocketAddr,
    /// 对外通告地址 (announce_ip:base_port), 用于 ICE host 候选与 Receive destination
    pub announce_addr: SocketAddr,
    pub batch_size: usize,
    pub send_batch_size: usize,
    pub busy_poll_us: u32,
    /// BWE 初始带宽估计 (bit/s)
    pub bwe_initial_bps: u64,
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
