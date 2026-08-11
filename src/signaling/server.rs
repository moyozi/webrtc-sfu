//! WebSocket 信令服务器
//!
//! 使用 tokio + tokio-tungstenite 实现 WebSocket 服务器。
//! 处理客户端的 JSON 消息，通过 channel 与 Worker 线程通信。
//!
//! 协议消息格式:
//! - join: 加入房间 + SDP offer
//! - ice_candidate: Trickle ICE 候选
//! - subscribe: 订阅发布者轨道
//! - unsubscribe: 取消订阅
//! - leave: 离开房间

use crate::core::{WorkerCommand, WorkerReply};
use crate::signaling::SignalingState;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn, error, debug};

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

    // 当前连接的参与者信息
    let mut current_participant_id: Option<String> = None;
    let mut current_room_id: Option<String> = None;

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
                    error: format!("JSON 解析失败: {}", e),
                }).unwrap());
                continue;
            }
        };

        match signal {
            SignalingMessage::Join {
                room_id,
                participant_id,
                sdp,
            } => {
                info!(
                    "参与者 {} 加入房间 {} from {}",
                    participant_id, room_id, peer_addr
                );

                // 选择 Worker (round-robin)
                let worker_idx = state.select_worker();
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
                        // 加入房间
                        state.join_room(&room_id, &participant_id);

                        // 注册 WebSocket 发送器 (用于接收其他参与者的通知)
                        state.register_ws_sender(&participant_id, msg_tx.clone());

                        current_participant_id = Some(participant_id.clone());
                        current_room_id = Some(room_id.clone());

                        // 发送 SDP answer 给当前客户端
                        let response = SignalingResponse::SdpAnswer {
                            sdp: sdp_answer,
                            ice_candidates: local_candidates,
                        };
                        let _ = msg_tx.send(serde_json::to_string(&response).unwrap());

                        // 通知房间内其他参与者有新人加入
                        let notify = serde_json::to_string(
                            &SignalingResponse::ParticipantJoined {
                                participant_id: participant_id.clone(),
                            }
                        ).unwrap();
                        state.notify_room(&room_id, &participant_id, &notify);

                        debug!("已通知房间 {} 内的其他参与者: {} 加入", room_id, participant_id);
                    }
                    Ok(WorkerReply::Error { error, .. }) => {
                        let _ = msg_tx.send(serde_json::to_string(
                            &SignalingResponse::Error { error }
                        ).unwrap());
                    }
                    Err(_) => {
                        let _ = msg_tx.send(serde_json::to_string(&SignalingResponse::Error {
                            error: "Worker 无响应".to_string(),
                        }).unwrap());
                    }
                    _ => {}
                }
            }

            SignalingMessage::IceCandidate {
                candidate,
                sdp_mid,
                ..
            } => {
                if let Some(ref pid) = current_participant_id {
                    let worker_idx = 0;
                    let worker_tx = &state.worker_txs[worker_idx];

                    let _ = worker_tx.send(WorkerCommand::AddRemoteCandidate {
                        participant_id: pid.clone(),
                        candidate,
                        sdp_mid,
                    });
                }
            }

            SignalingMessage::Subscribe {
                publisher_id,
                track_mid,
            } => {
                if let (Some(ref pid), Some(ref room_id)) = (&current_participant_id, &current_room_id) {
                    let worker_idx = 0;
                    let worker_tx = &state.worker_txs[worker_idx];

                    let _ = worker_tx.send(WorkerCommand::Subscribe {
                        room_id: room_id.clone(),
                        subscriber_id: pid.clone(),
                        publisher_id: publisher_id.clone(),
                        publisher_mid: track_mid.clone(),
                    });

                    let _ = msg_tx.send(serde_json::to_string(&SignalingResponse::Subscribed {
                        publisher_id,
                    }).unwrap());
                }
            }

            SignalingMessage::Unsubscribe {
                publisher_id,
                track_mid,
            } => {
                if let Some(ref pid) = current_participant_id {
                    let worker_idx = 0;
                    let worker_tx = &state.worker_txs[worker_idx];

                    let _ = worker_tx.send(WorkerCommand::Unsubscribe {
                        subscriber_id: pid.clone(),
                        publisher_id,
                        publisher_mid: track_mid,
                    });
                }
            }

            SignalingMessage::Leave => {
                if let (Some(pid), Some(room_id)) = (current_participant_id.take(), current_room_id.take()) {
                    // 从房间移除
                    state.leave_room(&room_id, &pid);
                    state.unregister_ws_sender(&pid);

                    // 通知房间内其他参与者
                    let notify = serde_json::to_string(&SignalingResponse::ParticipantLeft {
                        participant_id: pid.clone(),
                    }).unwrap();
                    state.notify_room(&room_id, &pid, &notify);

                    // 通知 Worker 移除参与者
                    let worker_idx = 0;
                    let worker_tx = &state.worker_txs[worker_idx];
                    let _ = worker_tx.send(WorkerCommand::RemoveParticipant {
                        participant_id: pid,
                    });
                }
            }
        }
    }

    // 清理: 离开房间
    if let (Some(pid), Some(room_id)) = (current_participant_id, current_room_id) {
        state.leave_room(&room_id, &pid);
        state.unregister_ws_sender(&pid);

        // 通知房间内其他参与者
        let notify = serde_json::to_string(&SignalingResponse::ParticipantLeft {
            participant_id: pid.clone(),
        }).unwrap();
        state.notify_room(&room_id, &pid, &notify);

        let worker_tx = &state.worker_txs[0];
        let _ = worker_tx.send(WorkerCommand::RemoveParticipant {
            participant_id: pid,
        });
    }

    send_task.abort();
    Ok(())
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
        room_id: String,
        participant_id: String,
        sdp: String,
    },

    #[serde(rename = "ice_candidate")]
    IceCandidate {
        candidate: String,
        sdp_mid: Option<String>,
        sdp_mline_index: Option<u16>,
    },

    #[serde(rename = "subscribe")]
    Subscribe {
        publisher_id: String,
        track_mid: String,
    },

    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        publisher_id: String,
        track_mid: String,
    },

    #[serde(rename = "leave")]
    Leave,
}

/// 服务器 → 客户端消息
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum SignalingResponse {
    #[serde(rename = "sdp_answer")]
    SdpAnswer {
        sdp: String,
        ice_candidates: Vec<String>,
    },

    #[serde(rename = "ice_candidate")]
    IceCandidate {
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
        publisher_id: String,
    },

    #[serde(rename = "error")]
    Error {
        error: String,
    },
}
