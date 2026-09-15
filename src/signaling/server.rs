//! WebSocket 信令服务器
//!
//! 使用 tokio + tokio-tungstenite 实现 WebSocket 服务器。
//! 处理客户端的 JSON 消息，通过 channel 与 Worker 线程通信。
//!
//! 协议消息格式:
//! - join: 加入房间 + SDP offer (pc_id 标识一个 RTCPeerConnection)
//! - ice_candidate: Trickle ICE 候选
//! - subscribe: 订阅发布者轨道
//! - unsubscribe: 取消订阅
//! - leave: 关闭某个 pc_id 对应的会话
//!
//! 一条 WebSocket 连接可承载多个 RTCPeerConnection:
//!   - role=publisher: 发布连接, 加入房间, 参与 participant_joined 广播
//!   - role=subscriber: recvonly 订阅连接, 不加入房间
//! 所有消息通过 `pc_id` 路由到对应的参与者/Rtc 实例。

use crate::core::{WorkerCommand, WorkerReply};
use crate::signaling::SignalingState;
use ahash::AHashMap;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn, error, debug};

/// 一条 WebSocket 上的一个 RTCPeerConnection 会话
struct PcSession {
    /// 服务端内部参与者 id (发布者 = 用户 id; 订阅 = `${user}-recv-${publisher}`)
    participant_id: String,
    room_id: String,
    /// 该参与者所在 worker, 后续 ICE/订阅/离开命令均发往此 worker
    worker_idx: usize,
    /// 是否为发布连接 (false = recvonly 订阅连接)
    is_publisher: bool,
}

/// 运行 WebSocket 信令服务器
pub async fn run_server(
    addr: SocketAddr,
    state: Arc<SignalingState>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("信令服务器启动: ws://{}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, peer_addr, state).await {
                        warn!("WebSocket 连接错误: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("接受连接失败: {}", e);
            }
        }
    }
}

/// 处理单个 WebSocket 连接
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    state: Arc<SignalingState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let ws_stream = tokio_tungstenite::accept_async(stream).await?;
    let (ws_sender, mut ws_receiver) = ws_stream.split();

    info!("WebSocket 连接: {}", peer_addr);

    // 创建消息通道 — 所有发给此 WebSocket 的消息都通过此通道
    // 这样可以在其他任务中向此连接推送通知 (如 participant_joined)
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<String>();

    // 启动任务: 从通道读取消息并发送到 WebSocket
    let send_task = tokio::spawn(async move {
        let mut ws_sender = ws_sender;
        while let Some(msg) = msg_rx.recv().await {
            if ws_sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // 一条 WebSocket 上承载多个 RTCPeerConnection 会话, 以 pc_id 区分
    let mut sessions: AHashMap<String, PcSession> = AHashMap::new();

    while let Some(msg) = ws_receiver.next().await {
        let msg = match msg {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) => {
                info!("WebSocket 关闭: {}", peer_addr);
                break;
            }
            Ok(_) => continue,
            Err(e) => {
                warn!("WebSocket 错误: {}", e);
                break;
            }
        };

        // 解析 JSON 消息
        let signal: SignalingMessage = match serde_json::from_str(&msg) {
            Ok(s) => s,
            Err(e) => {
                warn!("JSON 解析失败: {}", e);
                let _ = msg_tx.send(serde_json::to_string(&SignalingResponse::Error {
                    pc_id: None,
                    error: format!("JSON 解析失败: {}", e),
                }).unwrap());
                continue;
            }
        };

        match signal {
            SignalingMessage::Join {
                pc_id,
                room_id,
                participant_id,
                sdp,
                role,
            } => {
                // subscriber 是 recvonly 的订阅连接，不是房间参与者，
                // 不应触发 participant_joined 广播 (否则会形成无限订阅循环)
                let is_publisher = role != "subscriber";

                info!(
                    "参与者 {} 加入房间 {} from {} (role={}, pc_id={})",
                    participant_id, room_id, peer_addr, role, pc_id
                );

                // 选择 Worker — 同一房间的发布者/订阅连接必须落在同一 worker
                let worker_idx = state.select_worker(&room_id);
                let worker_tx = &state.worker_txs[worker_idx];

                // 创建 reply channel
                let (tx, rx) = crossbeam_channel::bounded(1);

                // 发送创建参与者命令到 Worker
                let cmd = WorkerCommand::CreateParticipant {
                    room_id: room_id.clone(),
                    participant_id: participant_id.clone(),
                    sdp_offer: sdp,
                    reply_tx: tx,
                };

                if worker_tx.send(cmd).is_err() {
                    let _ = msg_tx.send(serde_json::to_string(&SignalingResponse::Error {
                        pc_id: Some(pc_id),
                        error: "Worker 不可用".to_string(),
                    }).unwrap());
                    continue;
                }

                // 等待 Worker 的 SDP answer
                match rx.recv() {
                    Ok(WorkerReply::SdpAnswer {
                        sdp_answer,
                        local_candidates,
                        ..
                    }) => {
                        // 记录该 pc_id 对应的会话
                        sessions.insert(
                            pc_id.clone(),
                            PcSession {
                                participant_id: participant_id.clone(),
                                room_id: room_id.clone(),
                                worker_idx,
                                is_publisher,
                            },
                        );

                        // 发送 SDP answer 给当前客户端 (带 pc_id, 客户端据此路由到对应 PC)
                        let response = SignalingResponse::SdpAnswer {
                            pc_id: pc_id.clone(),
                            sdp: sdp_answer,
                            ice_candidates: local_candidates,
                        };
                        let _ = msg_tx.send(serde_json::to_string(&response).unwrap());

                        // 只有发布者才算房间参与者
                        if is_publisher {
                            // 加入房间
                            state.join_room(&room_id, &participant_id);

                            // 注册 WebSocket 发送器 (用于接收其他参与者的通知)
                            state.register_ws_sender(&participant_id, msg_tx.clone());

                            // 告诉新加入者房间内已有的参与者 (使其能够订阅对方)
                            for existing in state.room_participants(&room_id, &participant_id) {
                                let notify = serde_json::to_string(
                                    &SignalingResponse::ParticipantJoined {
                                        participant_id: existing,
                                    }
                                ).unwrap();
                                let _ = msg_tx.send(notify);
                            }

                            // 通知房间内其他参与者有新人加入
                            let notify = serde_json::to_string(
                                &SignalingResponse::ParticipantJoined {
                                    participant_id: participant_id.clone(),
                                }
                            ).unwrap();
                            state.notify_room(&room_id, &participant_id, &notify);

                            debug!("已通知房间 {} 内的其他参与者: {} 加入", room_id, participant_id);
                        }
                    }
                    Ok(WorkerReply::Error { error, .. }) => {
                        let _ = msg_tx.send(serde_json::to_string(
                            &SignalingResponse::Error { pc_id: Some(pc_id), error }
                        ).unwrap());
                    }
                    Err(_) => {
                        let _ = msg_tx.send(serde_json::to_string(&SignalingResponse::Error {
                            pc_id: Some(pc_id),
                            error: "Worker 无响应".to_string(),
                        }).unwrap());
                    }
                    _ => {}
                }
            }

            SignalingMessage::IceCandidate {
                pc_id,
                candidate,
                sdp_mid,
                ..
            } => {
                if let Some(sess) = sessions.get(&pc_id) {
                    let worker_tx = &state.worker_txs[sess.worker_idx];
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
            } => {
                if let Some(sess) = sessions.get(&pc_id) {
                    let worker_tx = &state.worker_txs[sess.worker_idx];
                    let _ = worker_tx.send(WorkerCommand::Subscribe {
                        room_id: sess.room_id.clone(),
                        subscriber_id: sess.participant_id.clone(),
                        publisher_id: publisher_id.clone(),
                        publisher_mid: track_mid,
                    });

                    let _ = msg_tx.send(serde_json::to_string(&SignalingResponse::Subscribed {
                        pc_id,
                        publisher_id,
                    }).unwrap());
                }
            }

            SignalingMessage::Unsubscribe {
                pc_id,
                publisher_id,
                track_mid,
            } => {
                if let Some(sess) = sessions.get(&pc_id) {
                    let worker_tx = &state.worker_txs[sess.worker_idx];
                    let _ = worker_tx.send(WorkerCommand::Unsubscribe {
                        subscriber_id: sess.participant_id.clone(),
                        publisher_id,
                        publisher_mid: track_mid,
                    });
                }
            }

            SignalingMessage::Leave { pc_id } => {
                if let Some(sess) = sessions.remove(&pc_id) {
                    cleanup_session(&state, &sess);
                }
            }
        }
    }

    // 清理: 关闭该 WebSocket 上所有会话
    for (_, sess) in sessions.drain() {
        cleanup_session(&state, &sess);
    }

    send_task.abort();
    Ok(())
}

/// 清理一个 PC 会话: 发布者需离开房间并广播, 订阅连接只移除 Rtc
fn cleanup_session(state: &SignalingState, sess: &PcSession) {
    if sess.is_publisher {
        state.leave_room(&sess.room_id, &sess.participant_id);
        state.unregister_ws_sender(&sess.participant_id);

        // 通知房间内其他参与者
        let notify = serde_json::to_string(&SignalingResponse::ParticipantLeft {
            participant_id: sess.participant_id.clone(),
        }).unwrap();
        state.notify_room(&sess.room_id, &sess.participant_id, &notify);
    }

    // 通知 Worker 移除参与者 (释放对应的 Rtc)
    let _ = state.worker_txs[sess.worker_idx].send(WorkerCommand::RemoveParticipant {
        participant_id: sess.participant_id.clone(),
    });
}

// ──────────────────────────────────────────────────────────
// 消息协议定义
// ──────────────────────────────────────────────────────────

/// 客户端 → 服务器消息
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum SignalingMessage {
    #[serde(rename = "join")]
    Join {
        /// 会话标识: "pub" 或 "sub-<publisherId>"
        pc_id: String,
        room_id: String,
        participant_id: String,
        sdp: String,
        /// 连接角色: "publisher" (默认) 或 "subscriber" (recvonly)
        #[serde(default)]
        role: String,
    },

    #[serde(rename = "ice_candidate")]
    IceCandidate {
        pc_id: String,
        candidate: String,
        sdp_mid: Option<String>,
        sdp_mline_index: Option<u16>,
    },

    #[serde(rename = "subscribe")]
    Subscribe {
        pc_id: String,
        publisher_id: String,
        track_mid: String,
    },

    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        pc_id: String,
        publisher_id: String,
        track_mid: String,
    },

    #[serde(rename = "leave")]
    Leave {
        pc_id: String,
    },
}

/// 服务器 → 客户端消息
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum SignalingResponse {
    #[serde(rename = "sdp_answer")]
    SdpAnswer {
        pc_id: String,
        sdp: String,
        ice_candidates: Vec<String>,
    },

    #[serde(rename = "ice_candidate")]
    IceCandidate {
        pc_id: String,
        candidate: String,
        sdp_mid: Option<String>,
    },

    #[serde(rename = "participant_joined")]
    ParticipantJoined {
        participant_id: String,
    },

    #[serde(rename = "participant_left")]
    ParticipantLeft {
        participant_id: String,
    },

    #[serde(rename = "subscribed")]
    Subscribed {
        pc_id: String,
        publisher_id: String,
    },

    #[serde(rename = "error")]
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        pc_id: Option<String>,
        error: String,
    },
}
