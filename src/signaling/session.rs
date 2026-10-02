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

use crate::signaling::auth::Claims;
use crate::signaling::protocol::{
    ErrorCode, ParticipantInfo, SignalingMessage, SignalingResponse, PROTOCOL_VERSION, SERVER_NAME,
};
use crate::signaling::SignalingState;
use crate::worker::{WorkerCommand, WorkerReply};
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
    msg_tx: mpsc::Sender<String>,
    /// pc_id → 会话
    sessions: AHashMap<String, PcSession>,
    /// 对端标识 (日志用)
    peer: String,
    /// 传输类型: "websocket" / "quic" (日志用)
    transport: &'static str,
    /// 已通过鉴权的 JWT claims (启用鉴权时非空)。
    /// 连接级别: 握手阶段 (WS/WHIP) 预校验的 claims 在此建立。
    claims: Option<Claims>,
    /// 发送队列是否已满 (客户端读取过慢) — 置位后由 `run_session` 结束连接。
    overflow: bool,
}

impl SignalingSession {
    pub fn new(
        state: Arc<SignalingState>,
        msg_tx: mpsc::Sender<String>,
        peer: String,
        transport: &'static str,
        claims: Option<Claims>,
    ) -> Self {
        Self {
            state,
            msg_tx,
            sessions: AHashMap::new(),
            peer,
            transport,
            claims,
            overflow: false,
        }
    }

    /// 发送一条响应给客户端。
    ///
    /// 发送队列满 (客户端读取过慢) 时标记会话溢出，由 `run_session` 结束连接，
    /// 避免每连接队列无界增长。
    fn send(&mut self, resp: SignalingResponse) {
        match serde_json::to_string(&resp) {
            Ok(s) => {
                if self.msg_tx.try_send(s).is_err() {
                    self.overflow = true;
                }
            }
            Err(e) => warn!("信令响应序列化失败: {}", e),
        }
    }

    /// 发送一个带错误码的 `error` 响应。
    fn send_error(&mut self, pc_id: Option<String>, code: ErrorCode, error: impl Into<String>) {
        self.send(SignalingResponse::Error {
            pc_id,
            code,
            error: error.into(),
        });
    }

    /// 该会话是否因发送队列溢出而应关闭。
    pub fn overflowed(&self) -> bool {
        self.overflow
    }

    /// 处理一条来自客户端的原始 JSON 文本消息
    pub async fn handle(&mut self, raw: &str) {
        let signal: SignalingMessage = match serde_json::from_str(raw) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    transport = self.transport,
                    peer = %self.peer,
                    "JSON 解析失败: {}",
                    e
                );
                self.send_error(
                    None,
                    ErrorCode::BadRequest,
                    format!("JSON 解析失败: {}", e),
                );
                return;
            }
        };

        match signal {
            SignalingMessage::Hello {
                protocol_version,
                client,
            } => self.on_hello(protocol_version, client),

            SignalingMessage::Ping { t } => self.send(SignalingResponse::Pong { t }),

            SignalingMessage::Join {
                pc_id,
                room_id,
                participant_id,
                sdp,
                role,
                token,
            } => {
                if self.sessions.contains_key(&pc_id) {
                    self.send_error(Some(pc_id), ErrorCode::AlreadyJoined, "该 pc_id 已加入");
                    return;
                }
                if let Err(error) =
                    self.authenticate_join(token.as_deref(), &room_id, &participant_id, &role)
                {
                    warn!(
                        transport = self.transport,
                        peer = %self.peer,
                        "鉴权失败: {}",
                        error
                    );
                    self.send_error(Some(pc_id), ErrorCode::Unauthorized, error);
                    return;
                }
                self.on_join(pc_id, room_id, participant_id, sdp, role)
                    .await
            }

            SignalingMessage::Renegotiate { pc_id, sdp, token } => {
                self.on_renegotiate(pc_id, sdp, token).await
            }

            SignalingMessage::IceCandidate {
                pc_id,
                candidate,
                sdp_mid,
                ..
            } => {
                let target = self
                    .sessions
                    .get(&pc_id)
                    .map(|s| (s.worker_idx, s.participant_id.clone()));
                match target {
                    Some((worker_idx, participant_id)) => self.state.send_worker(
                        worker_idx,
                        WorkerCommand::AddRemoteCandidate {
                            participant_id,
                            candidate,
                            sdp_mid,
                        },
                    ),
                    None => self.send_error(
                        Some(pc_id),
                        ErrorCode::UnknownSession,
                        "未知 pc_id, 请先 join",
                    ),
                }
            }

            SignalingMessage::Subscribe {
                pc_id,
                publisher_id,
                track_mid,
                subscriber_mid,
            } => {
                let target = self.sessions.get(&pc_id).map(|s| {
                    (
                        s.worker_idx,
                        s.room_id.clone(),
                        s.participant_id.clone(),
                    )
                });
                match target {
                    Some((worker_idx, room_id, subscriber_id)) => {
                        let publisher_worker = self.state.worker_of(&publisher_id);
                        self.state.send_worker(
                            worker_idx,
                            WorkerCommand::Subscribe {
                                room_id,
                                subscriber_id,
                                publisher_id: publisher_id.clone(),
                                publisher_mid: track_mid.clone(),
                                subscriber_mid,
                                publisher_worker,
                            },
                        );
                        self.send(SignalingResponse::Subscribed {
                            pc_id,
                            publisher_id,
                            track_mid,
                        });
                    }
                    None => self.send_error(
                        Some(pc_id),
                        ErrorCode::UnknownSession,
                        "未知 pc_id, 请先 join",
                    ),
                }
            }

            SignalingMessage::Unsubscribe {
                pc_id,
                publisher_id,
                track_mid,
            } => {
                let target = self
                    .sessions
                    .get(&pc_id)
                    .map(|s| (s.worker_idx, s.participant_id.clone()));
                match target {
                    Some((worker_idx, subscriber_id)) => {
                        let publisher_worker = self.state.worker_of(&publisher_id);
                        self.state.send_worker(
                            worker_idx,
                            WorkerCommand::Unsubscribe {
                                subscriber_id,
                                publisher_id: publisher_id.clone(),
                                publisher_mid: track_mid.clone(),
                                publisher_worker,
                            },
                        );
                        self.send(SignalingResponse::Unsubscribed {
                            pc_id,
                            publisher_id,
                            track_mid,
                        });
                    }
                    None => self.send_error(
                        Some(pc_id),
                        ErrorCode::UnknownSession,
                        "未知 pc_id, 请先 join",
                    ),
                }
            }

            SignalingMessage::UpdateMedia {
                pc_id,
                audio_enabled,
                video_enabled,
            } => self.on_update_media(pc_id, audio_enabled, video_enabled),

            SignalingMessage::GetRoomState { room_id } => self.on_get_room_state(room_id),

            SignalingMessage::Leave { pc_id } => {
                if let Some(sess) = self.sessions.remove(&pc_id) {
                    cleanup_session(&self.state, &sess);
                    self.send(SignalingResponse::Left { pc_id });
                } else {
                    self.send_error(Some(pc_id), ErrorCode::UnknownSession, "未知 pc_id");
                }
            }
        }
    }

    /// 处理 `hello`: 协商协议版本并返回服务端能力。
    fn on_hello(&mut self, protocol_version: Option<u32>, _client: Option<String>) {
        if let Some(v) = protocol_version {
            if v > PROTOCOL_VERSION {
                self.send_error(
                    None,
                    ErrorCode::UnsupportedProtocol,
                    format!("不支持的协议版本 {} (服务端 {})", v, PROTOCOL_VERSION),
                );
                return;
            }
        }
        let mut features: Vec<String> =
            ["heartbeat", "join_ack", "room_state", "media_state", "ice_restart"]
                .iter()
                .map(|s| s.to_string())
                .collect();
        if self.state.auth.is_enabled() {
            features.push("jwt".to_string());
        }
        self.send(SignalingResponse::Welcome {
            protocol_version: PROTOCOL_VERSION,
            server: SERVER_NAME.to_string(),
            features,
        });
    }

    /// 处理 `update_media`: 更新发布者媒体状态 (静音/关摄像头) 并广播给房间其他人。
    fn on_update_media(
        &mut self,
        pc_id: String,
        audio_enabled: Option<bool>,
        video_enabled: Option<bool>,
    ) {
        if audio_enabled.is_none() && video_enabled.is_none() {
            return;
        }
        let target = self.sessions.get(&pc_id).map(|s| {
            (
                s.room_id.clone(),
                s.participant_id.clone(),
                s.is_publisher,
            )
        });
        let Some((room_id, participant_id, is_publisher)) = target else {
            self.send_error(
                Some(pc_id),
                ErrorCode::UnknownSession,
                "未知 pc_id, 请先 join",
            );
            return;
        };
        if !is_publisher {
            return;
        }
        if let Some(info) =
            self.state
                .update_media(&room_id, &participant_id, audio_enabled, video_enabled)
        {
            let notify = serde_json::to_string(&SignalingResponse::ParticipantUpdated {
                participant_id: info.participant_id,
                audio_enabled: info.audio_enabled,
                video_enabled: info.video_enabled,
            })
            .unwrap();
            self.state.notify_room(&room_id, &participant_id, &notify);
        }
    }

    /// 处理 `get_room_state`: 返回房间成员快照。
    fn on_get_room_state(&mut self, room_id: Option<String>) {
        let room_id = room_id.or_else(|| {
            self.sessions
                .values()
                .find(|s| s.is_publisher)
                .map(|s| s.room_id.clone())
        });
        match room_id {
            Some(room_id) => {
                let participants = self.state.room_roster(&room_id);
                self.send(SignalingResponse::RoomState {
                    room_id,
                    participants,
                });
            }
            None => self.send_error(None, ErrorCode::BadRequest, "缺少 room_id"),
        }
    }

    /// 在 `join` 前完成 JWT 鉴权。
    ///
    /// - 连接级已预校验 (WS/WHIP 握手头) 或此前 join 已通过时复用;
    /// - 否则使用本次 join 携带的 `token` 校验;
    /// - 启用强制鉴权且无有效 token 时返回错误, 拒绝创建参与者。
    fn authenticate_join(
        &mut self,
        token: Option<&str>,
        room_id: &str,
        participant_id: &str,
        role: &str,
    ) -> Result<(), String> {
        let auth = self.state.auth.clone();
        if !auth.is_enabled() {
            return Ok(());
        }

        if self.claims.is_none() {
            if let Some(token) = token {
                self.claims = Some(auth.verify(token)?);
            }
        }

        match &self.claims {
            Some(claims) => auth.authorize_join(claims, room_id, participant_id, role),
            None if auth.is_required() => Err("缺少鉴权 token (JWT)".to_string()),
            None => Ok(()),
        }
    }

    async fn on_join(
        &mut self,
        pc_id: String,
        room_id: String,
        participant_id: String,
        sdp: String,
        role: String,
    ) {
        // subscriber 是 recvonly 的订阅连接，不是房间参与者，
        // 不应触发 participant_joined 广播 (否则会形成无限订阅循环)
        let is_publisher = role != "subscriber";

        info!(
            transport = self.transport,
            peer = %self.peer,
            "参与者 {} 加入房间 {} (role={}, pc_id={})",
            participant_id, room_id, role, pc_id
        );

        // 选择全局 Worker — 房间可跨 worker/主机分布 (订阅经中继平面)
        let worker_idx = self.state.assign_worker(&room_id, &participant_id);

        // 创建参与者 (本地 worker 走 channel, 远端经控制面往返)。
        // 该调用会阻塞等待 worker 回复 (最长 REPLY_TIMEOUT=10s)，必须放到 blocking
        // 线程池，否则会堵住 Tokio 的 async 工作线程。
        let state = self.state.clone();
        let room_for_task = room_id.clone();
        let pid_for_task = participant_id.clone();
        let reply = tokio::task::spawn_blocking(move || {
            state.create_participant(worker_idx, room_for_task, pid_for_task, sdp)
        })
        .await
        .unwrap_or(None);

        match reply {
            Some(WorkerReply::SdpAnswer {
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
                    self.state
                        .register_sender(&participant_id, self.msg_tx.clone());

                    // 既有成员快照 (不含自己)
                    let roster: Vec<ParticipantInfo> = self
                        .state
                        .room_roster(&room_id)
                        .into_iter()
                        .filter(|p| p.participant_id != participant_id)
                        .collect();

                    // 明确入会确认 + 成员快照 (新客户端据此初始化列表)
                    self.send(SignalingResponse::Joined {
                        pc_id: pc_id.clone(),
                        participant_id: participant_id.clone(),
                        room_id: room_id.clone(),
                        role: role.clone(),
                        participants: roster.clone(),
                    });

                    // 兼容旧客户端: 逐个推送既有成员
                    for p in &roster {
                        self.send(SignalingResponse::ParticipantJoined {
                            participant_id: p.participant_id.clone(),
                        });
                    }

                    // 通知房间内其他参与者有新人加入
                    let notify = serde_json::to_string(&SignalingResponse::ParticipantJoined {
                        participant_id: participant_id.clone(),
                    })
                    .unwrap();
                    self.state.notify_room(&room_id, &participant_id, &notify);

                    debug!(
                        "已通知房间 {} 内的其他参与者: {} 加入",
                        room_id, participant_id
                    );
                } else {
                    // 订阅连接: 仅回 joined 确认 (空成员表), 不加入房间
                    self.send(SignalingResponse::Joined {
                        pc_id: pc_id.clone(),
                        participant_id: participant_id.clone(),
                        room_id: room_id.clone(),
                        role: role.clone(),
                        participants: Vec::new(),
                    });
                }
            }
            Some(WorkerReply::Error { error, .. }) => {
                self.send_error(Some(pc_id), ErrorCode::WorkerError, error);
            }
            Some(_) => {
                self.send_error(Some(pc_id), ErrorCode::Internal, "worker 返回了非预期响应");
            }
            None => {
                self.send_error(
                    Some(pc_id),
                    ErrorCode::WorkerUnavailable,
                    "Worker 无响应",
                );
            }
        }
    }

    /// 对已存在的 pc_id 重新协商 (ICE restart / 重协商)。
    ///
    /// 复用已有的参与者 (Rtc) 实例, 只应用新的 SDP offer: 若 offer 携带新的
    /// ICE 凭证, str0m 在 `accept_offer` 内部自动执行 ICE restart, 服务端回
    /// 一个新的 `sdp_answer` (含重新生成的本地候选)。
    async fn on_renegotiate(&mut self, pc_id: String, sdp: String, token: Option<String>) {
        let target = self.sessions.get(&pc_id).map(|s| {
            (
                s.worker_idx,
                s.participant_id.clone(),
                s.room_id.clone(),
                s.is_publisher,
            )
        });
        let Some((worker_idx, participant_id, room_id, is_publisher)) = target else {
            self.send_error(
                Some(pc_id),
                ErrorCode::UnknownSession,
                "未知 pc_id, 请先 join",
            );
            return;
        };

        // 复用 join 的鉴权规则 (claims 已在 join 时校验过, 此处幂等重校验)。
        let role = if is_publisher { "publisher" } else { "subscriber" };
        if let Err(error) =
            self.authenticate_join(token.as_deref(), &room_id, &participant_id, role)
        {
            warn!(transport = self.transport, peer = %self.peer, "重协商鉴权失败: {}", error);
            self.send_error(Some(pc_id), ErrorCode::Unauthorized, error);
            return;
        }

        let state = self.state.clone();
        let reply = tokio::task::spawn_blocking(move || {
            state.renegotiate(worker_idx, participant_id, sdp)
        })
        .await
        .unwrap_or(None);

        match reply {
            Some(WorkerReply::SdpAnswer {
                sdp_answer,
                local_candidates,
                ..
            }) => {
                info!(
                    transport = self.transport,
                    peer = %self.peer,
                    "pc_id {} 重新协商完成 (ICE restart)",
                    pc_id
                );
                self.send(SignalingResponse::SdpAnswer {
                    pc_id,
                    sdp: sdp_answer,
                    ice_candidates: local_candidates,
                });
            }
            Some(WorkerReply::Error { error, .. }) => {
                self.send_error(Some(pc_id), ErrorCode::WorkerError, error)
            }
            Some(_) => {
                self.send_error(Some(pc_id), ErrorCode::Internal, "worker 返回了非预期响应")
            }
            None => self.send_error(
                Some(pc_id),
                ErrorCode::WorkerUnavailable,
                "Worker 无响应",
            ),
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
    // 释放 worker 计数
    state.release_worker(&sess.room_id, &sess.participant_id);
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
    state.send_worker(
        sess.worker_idx,
        WorkerCommand::RemoveParticipant {
            participant_id: sess.participant_id.clone(),
        },
    );
}

/// 从 `inbound` 读取 JSON 字符串并驱动会话, 直到通道关闭 (连接断开)。
///
/// 传输层把解码后的文本消息投递到 `inbound`, 并消费 `msg_tx` 写回 socket。
pub async fn run_session(
    state: Arc<SignalingState>,
    msg_tx: mpsc::Sender<String>,
    mut inbound: mpsc::Receiver<String>,
    peer: String,
    transport: &'static str,
    claims: Option<Claims>,
) {
    let mut session = SignalingSession::new(state, msg_tx, peer, transport, claims);
    while let Some(raw) = inbound.recv().await {
        session.handle(&raw).await;
        if session.overflowed() {
            warn!(
                transport = session.transport,
                peer = %session.peer,
                "信令发送队列已满, 关闭连接"
            );
            break;
        }
    }
    session.finish();
}
