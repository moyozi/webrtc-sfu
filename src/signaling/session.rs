//! 传输无关的信令会话处理
//!
//! 无论信令承载在 WebSocket 还是 QUIC 之上, 消息语义完全一致, 因此把
//! 「收消息 → 解析 → 调用 Worker → 回消息 → 清理」这段逻辑集中在这里。
//!
//! 各传输层只需提供两样东西:
//!   - `inbound`:  已将底层字节解码为 JSON 字符串的输入流 (mpsc receiver)
//!   - `msg_tx`:   接收要发往该连接的消息的输出通道 (mpsc sender)
//!
//! 具体如何把 `msg_tx` 里的字符串写到 socket, 以及如何从 socket 读出字节
//! 并解码成字符串, 由各自的传输模块负责。
//!
//! 这样设计的直接好处: QUIC 分支复用同一套房间/订阅/Worker 交互逻辑,
//! 不会出现 WebSocket 与 QUIC 行为不一致的问题。
//!
//! 一条连接可承载多个 RTCPeerConnection:
//!   - role=publisher: 发布连接, 加入房间, 参与 participant_joined 广播
//!   - role=subscriber: recvonly 订阅连接, 不加入房间
//!
//! 所有消息通过 `pc_id` 路由到对应的参与者/Rtc 实例。

use crate::core::{WorkerCommand, WorkerReply};
use crate::signaling::protocol::{SignalingMessage, SignalingResponse};
use crate::signaling::SignalingState;
use ahash::AHashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// 一条信令连接上的一个 RTCPeerConnection 会话
pub struct PcSession {
    /// 服务端内部参与者 id (发布者 = 用户 id; 订阅 = `${user}-recv-${publisher}`)
    pub participant_id: String,
    pub room_id: String,
    /// 该参与者所在 worker, 后续 ICE/订阅/离开命令均发往此 worker
    pub worker_idx: usize,
    /// 是否为发布连接 (false = recvonly 订阅连接)
    pub is_publisher: bool,
}

/// 一个信令连接上的会话状态机 (传输无关)
pub struct SignalingSession {
    state: Arc<SignalingState>,
    /// 发往本连接的输出通道 (由传输层消费后写入 socket)
    msg_tx: mpsc::UnboundedSender<String>,
    /// pc_id → 会话
    sessions: AHashMap<String, PcSession>,
    /// 对端标识 (日志用)
    peer: String,
    /// 传输类型: "websocket" / "quic" (日志用)
    transport: &'static str,
}

impl SignalingSession {
    pub fn new(
        state: Arc<SignalingState>,
        msg_tx: mpsc::UnboundedSender<String>,
        peer: String,
        transport: &'static str,
    ) -> Self {
        Self {
            state,
            msg_tx,
            sessions: AHashMap::new(),
            peer,
            transport,
        }
    }

    /// 发送一条响应给客户端
    fn send(&self, resp: SignalingResponse) {
        match serde_json::to_string(&resp) {
            Ok(s) => {
                let _ = self.msg_tx.send(s);
            }
            Err(e) => warn!("信令响应序列化失败: {}", e),
        }
    }

    /// 处理一条来自客户端的原始 JSON 文本消息
    pub fn handle(&mut self, raw: &str) {
        let signal: SignalingMessage = match serde_json::from_str(raw) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    transport = self.transport,
                    peer = %self.peer,
                    "JSON 解析失败: {}",
                    e
                );
                self.send(SignalingResponse::Error {
                    pc_id: None,
                    error: format!("JSON 解析失败: {}", e),
                });
                return;
            }
        };

        match signal {
            SignalingMessage::Join {
                pc_id,
                room_id,
                participant_id,
                sdp,
                role,
            } => self.on_join(pc_id, room_id, participant_id, sdp, role),

            SignalingMessage::IceCandidate {
                pc_id,
                candidate,
                sdp_mid,
                ..
            } => {
                if let Some(sess) = self.sessions.get(&pc_id) {
                    let worker_tx = &self.state.worker_txs[sess.worker_idx];
                    let _ = worker_tx.send(WorkerCommand::AddRemoteCandidate {
                        participant_id: sess.participant_id.clone(),
                        candidate,
                        sdp_mid,
                    });
                }
            }

            SignalingMessage::Subscribe {
                pc_id,
                publisher_id,
                track_mid,
                subscriber_mid,
            } => {
                if let Some(sess) = self.sessions.get(&pc_id) {
                    let worker_tx = &self.state.worker_txs[sess.worker_idx];
                    let _ = worker_tx.send(WorkerCommand::Subscribe {
                        room_id: sess.room_id.clone(),
                        subscriber_id: sess.participant_id.clone(),
                        publisher_id: publisher_id.clone(),
                        publisher_mid: track_mid,
                        subscriber_mid,
                    });

                    self.send(SignalingResponse::Subscribed {
                        pc_id,
                        publisher_id,
                    });
                }
            }

            SignalingMessage::Unsubscribe {
                pc_id,
                publisher_id,
                track_mid,
            } => {
                if let Some(sess) = self.sessions.get(&pc_id) {
                    let worker_tx = &self.state.worker_txs[sess.worker_idx];
                    let _ = worker_tx.send(WorkerCommand::Unsubscribe {
                        subscriber_id: sess.participant_id.clone(),
                        publisher_id,
                        publisher_mid: track_mid,
                    });
                }
            }

            SignalingMessage::Leave { pc_id } => {
                if let Some(sess) = self.sessions.remove(&pc_id) {
                    cleanup_session(&self.state, &sess);
                }
            }
        }
    }

    fn on_join(&mut self, pc_id: String, room_id: String, participant_id: String, sdp: String, role: String) {
        // subscriber 是 recvonly 的订阅连接，不是房间参与者，
        // 不应触发 participant_joined 广播 (否则会形成无限订阅循环)
        let is_publisher = role != "subscriber";

        info!(
            transport = self.transport,
            peer = %self.peer,
            "参与者 {} 加入房间 {} (role={}, pc_id={})",
            participant_id, room_id, role, pc_id
        );

        // 选择 Worker — 同一房间的发布者/订阅连接必须落在同一 worker
        let worker_idx = self.state.select_worker(&room_id);
        let worker_tx = &self.state.worker_txs[worker_idx];

        // 创建 reply channel
        let (tx, rx) = crossbeam_channel::bounded(1);

        let cmd = WorkerCommand::CreateParticipant {
            room_id: room_id.clone(),
            participant_id: participant_id.clone(),
            sdp_offer: sdp,
            reply_tx: tx,
        };

        if worker_tx.send(cmd).is_err() {
            self.send(SignalingResponse::Error {
                pc_id: Some(pc_id),
                error: "Worker 不可用".to_string(),
            });
            return;
        }

        // 等待 Worker 的 SDP answer
        match rx.recv() {
            Ok(WorkerReply::SdpAnswer {
                sdp_answer,
                local_candidates,
                ..
            }) => {
                self.sessions.insert(
                    pc_id.clone(),
                    PcSession {
                        participant_id: participant_id.clone(),
                        room_id: room_id.clone(),
                        worker_idx,
                        is_publisher,
                    },
                );

                self.send(SignalingResponse::SdpAnswer {
                    pc_id: pc_id.clone(),
                    sdp: sdp_answer,
                    ice_candidates: local_candidates,
                });

                if is_publisher {
                    // 加入房间
                    self.state.join_room(&room_id, &participant_id);
                    // 注册发送器, 供其他参与者的通知推送到本连接
                    self.state.register_sender(&participant_id, self.msg_tx.clone());

                    // 告诉新加入者房间内已有的参与者 (使其能够订阅对方)
                    for existing in self.state.room_participants(&room_id, &participant_id) {
                        self.send(SignalingResponse::ParticipantJoined {
                            participant_id: existing,
                        });
                    }

                    // 通知房间内其他参与者有新人加入
                    let notify = serde_json::to_string(&SignalingResponse::ParticipantJoined {
                        participant_id: participant_id.clone(),
                    })
                    .unwrap();
                    self.state.notify_room(&room_id, &participant_id, &notify);

                    debug!("已通知房间 {} 内的其他参与者: {} 加入", room_id, participant_id);
                }
            }
            Ok(WorkerReply::Error { error, .. }) => {
                self.send(SignalingResponse::Error {
                    pc_id: Some(pc_id),
                    error,
                });
            }
            Err(_) => {
                self.send(SignalingResponse::Error {
                    pc_id: Some(pc_id),
                    error: "Worker 无响应".to_string(),
                });
            }
            _ => {}
        }
    }

    /// 连接关闭时清理该连接上所有会话
    pub fn finish(&mut self) {
        for (_, sess) in self.sessions.drain() {
            cleanup_session(&self.state, &sess);
        }
    }
}

/// 清理一个 PC 会话: 发布者需离开房间并广播, 订阅连接只移除 Rtc
fn cleanup_session(state: &SignalingState, sess: &PcSession) {
    if sess.is_publisher {
        state.leave_room(&sess.room_id, &sess.participant_id);
        state.unregister_sender(&sess.participant_id);

        let notify = serde_json::to_string(&SignalingResponse::ParticipantLeft {
            participant_id: sess.participant_id.clone(),
        })
        .unwrap();
        state.notify_room(&sess.room_id, &sess.participant_id, &notify);
    }

    // 通知 Worker 移除参与者 (释放对应的 Rtc)
    let _ = state.worker_txs[sess.worker_idx].send(WorkerCommand::RemoveParticipant {
        participant_id: sess.participant_id.clone(),
    });
}

/// 从 `inbound` 读取 JSON 字符串并驱动会话, 直到通道关闭 (连接断开)。
///
/// 传输层把解码后的文本消息投递到 `inbound`, 并消费 `msg_tx` 写回 socket。
pub async fn run_session(
    state: Arc<SignalingState>,
    msg_tx: mpsc::UnboundedSender<String>,
    mut inbound: mpsc::UnboundedReceiver<String>,
    peer: String,
    transport: &'static str,
) {
    let mut session = SignalingSession::new(state, msg_tx, peer, transport);
    while let Some(raw) = inbound.recv().await {
        session.handle(&raw);
    }
    session.finish();
}
