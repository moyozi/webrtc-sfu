//! 跨 worker / 跨主机中继总线
//!
//! 同一房间可分布到多个 worker 线程甚至多台主机。发布者所在 worker 解密出
//! 明文 RTP 后，通过 `RelayHub` 把 `RelayedRtp` 投递给订阅者所在 worker，
//! 由后者完成 PT 映射与逐订阅者 SRTP。进程内使用有界 channel + `UnixStream`
//! 唤醒；跨主机复用同一送达路径，仅把数据经 UDP 序列化发送。
//!
//! 详见 `docs/multi-worker-relay.md`。

use crate::net::relay::{self, RelayEndpoint};
use crossbeam_channel::Sender;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use str0m::format::PayloadParams;
use str0m::media::{Mid, Rid};
use str0m::rtp::{ExtensionValues, SeqNo};

use super::WorkerCommand;

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
    /// 因本地中继队列满而丢弃的中继媒体包数 (诊断用)
    pub dropped_relay: AtomicU64,
    /// 因本地命令队列满而丢弃的控制命令数 (诊断用)
    pub dropped_cmd: AtomicU64,
}

impl RelayHub {
    pub fn local_index(&self, global: u32) -> Option<usize> {
        if global >= self.worker_offset && global < self.worker_offset + self.local_workers {
            Some((global - self.worker_offset) as usize)
        } else {
            None
        }
    }

    /// 向全局编号 `target_global` 的 worker 发送一个中继 RTP 包。
    ///
    /// `include_params` 仅在跨主机路径生效: 为 true 时把 `PayloadParams` 随包
    /// 携带，接收端缓存后即可让后续数据包省去这份编码 (见 `net::relay`)。
    /// 进程内中继直接传递结构体，无需该参数。
    pub fn relay(&self, target_global: u32, pkt: RelayedRtp, include_params: bool) {
        if let Some(li) = self.local_index(target_global) {
            if let Some(tx) = self.relay_txs.get(li) {
                // 有界队列, 满了丢弃新包 (实时媒体宁可丢帧也不要无界堆积)。
                if tx.try_send(pkt).is_ok() {
                    self.wake(li);
                } else {
                    let n = self.dropped_relay.fetch_add(1, Ordering::Relaxed) + 1;
                    if n == 1 || n.is_multiple_of(1024) {
                        tracing::warn!("本地中继队列已满, 累计丢弃 {} 个媒体包", n);
                    }
                }
            }
            return;
        }
        if let (Some(ep), Some(addr)) = (&self.endpoint, self.peer_addrs.get(&target_global)) {
            let buf = relay::encode_rtp(target_global, &pkt, include_params);
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
                if tx.try_send(cmd).is_ok() {
                    self.wake(li);
                } else {
                    let n = self.dropped_cmd.fetch_add(1, Ordering::Relaxed) + 1;
                    if n == 1 || n.is_multiple_of(1024) {
                        tracing::warn!("本地命令队列已满, 累计丢弃 {} 条控制命令", n);
                    }
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
                } => {
                    relay::encode_request_keyframe(target_global, &publisher_id, publisher_mid, rid)
                }
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
            if tx.try_send(cmd).is_ok() {
                hub.wake(li);
            } else {
                hub.dropped_cmd.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
