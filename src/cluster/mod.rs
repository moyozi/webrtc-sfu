//! 集群控制面 — 信令进程向**远端 worker 进程**下发命令。
//!
//! 部署模型: 一个进程运行信令 (WebSocket/QUIC) + 一段全局 worker 编号；
//! 其余进程为 worker-only (不跑信令)。信令通过本模块把 `WorkerCommand`
//! 路由到任意全局 worker:
//!
//! - 本地 worker: 直接走进程内 channel；
//! - 远端 worker: 经 TCP 控制连接发送，`CreateParticipant` 用请求/响应关联
//!   (SDP answer 经同一连接按 `req_id` 回传)。
//!
//! 媒体中继另走 UDP (`net::relay`)。详见 `docs/multi-worker-relay.md`。
//!
//! 消息格式: `u32 be 长度 + serde_json 负载`。

use crate::core::{WorkerCommand, WorkerReply};
use crossbeam_channel::Sender;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn};

/// 单个控制连接的读超时 (仅用于探测断开)。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// CreateParticipant 等待远端 worker 回复的超时。
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);
/// 单帧最大长度 (16 MiB, 防异常长度前缀)。
const MAX_FRAME: usize = 16 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
pub struct ControlRequest {
    /// 目标全局 worker 编号
    pub target_worker: u32,
    /// 0 = 无回复 (fire-and-forget); 非 0 = 需要回复并关联到该 id
    pub req_id: u64,
    pub body: ControlBody,
}

#[derive(Serialize, Deserialize)]
pub enum ControlBody {
    CreateParticipant {
        room_id: String,
        participant_id: String,
        sdp_offer: String,
    },
    AddRemoteCandidate {
        participant_id: String,
        candidate: String,
        sdp_mid: Option<String>,
    },
    RemoveParticipant {
        participant_id: String,
    },
    Subscribe {
        room_id: String,
        subscriber_id: String,
        publisher_id: String,
        publisher_mid: String,
        subscriber_mid: Option<String>,
        publisher_worker: usize,
    },
    Unsubscribe {
        subscriber_id: String,
        publisher_id: String,
        publisher_mid: String,
        publisher_worker: usize,
    },
}

#[derive(Serialize, Deserialize)]
pub struct ControlResponse {
    pub req_id: u64,
    pub reply: ControlReply,
}

#[derive(Serialize, Deserialize)]
pub enum ControlReply {
    SdpAnswer {
        participant_id: String,
        sdp_answer: String,
        local_candidates: Vec<String>,
    },
    Error {
        participant_id: String,
        error: String,
    },
}

fn write_frame(stream: &mut TcpStream, bytes: &[u8]) -> std::io::Result<()> {
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()
}

fn read_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let n = u32::from_be_bytes(len) as usize;
    if n > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "control frame too large",
        ));
    }
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf)?;
    Ok(buf)
}

// ──────────────────────────────────────────────────────────
// 客户端: 信令进程 → 远端 worker 进程
// ──────────────────────────────────────────────────────────

struct Conn {
    writer: Mutex<TcpStream>,
}

/// 集群控制客户端 (持有到各对端进程的连接与待回复请求表)。
pub struct ClusterControl {
    /// 全局 worker → 其所属进程的控制地址
    peer_addrs: HashMap<u32, SocketAddr>,
    conns: Mutex<HashMap<SocketAddr, Arc<Conn>>>,
    pending: Arc<Mutex<HashMap<u64, Sender<ControlReply>>>>,
    next_req: AtomicU64,
}

impl ClusterControl {
    pub fn new(peer_addrs: HashMap<u32, SocketAddr>) -> Self {
        Self {
            peer_addrs,
            conns: Mutex::new(HashMap::new()),
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_req: AtomicU64::new(1),
        }
    }

    fn conn_for(&self, target: u32) -> Option<Arc<Conn>> {
        let addr = *self.peer_addrs.get(&target)?;
        if let Some(c) = self.conns.lock().unwrap().get(&addr) {
            return Some(c.clone());
        }
        let stream = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT).ok()?;
        stream.set_nodelay(true).ok();
        let mut reader = stream.try_clone().ok()?;
        let conn = Arc::new(Conn {
            writer: Mutex::new(stream),
        });
        // 读取线程: 把回复按 req_id 派发到 pending
        let pending = self.pending.clone();
        std::thread::Builder::new()
            .name(format!("sfu-ctl-rx-{}", target))
            .spawn(move || while let Ok(frame) = read_frame(&mut reader) {
                let resp: ControlResponse = match serde_json::from_slice(&frame) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                if let Some(tx) = pending.lock().unwrap().remove(&resp.req_id) {
                    let _ = tx.send(resp.reply);
                }
            })
            .ok()?;
        self.conns.lock().unwrap().insert(addr, conn.clone());
        Some(conn)
    }

    fn send_request(&self, target: u32, req_id: u64, body: ControlBody) -> bool {
        let Some(conn) = self.conn_for(target) else {
            warn!("无法连接远端 worker {} 的控制地址", target);
            return false;
        };
        let req = ControlRequest {
            target_worker: target,
            req_id,
            body,
        };
        let Ok(bytes) = serde_json::to_vec(&req) else {
            return false;
        };
        let mut w = conn.writer.lock().unwrap();
        write_frame(&mut w, &bytes).is_ok()
    }

    /// 同步创建参与者: 发送请求并等待远端 worker 的 SDP answer。
    pub fn create_participant(
        &self,
        target: u32,
        room_id: String,
        participant_id: String,
        sdp_offer: String,
    ) -> Option<WorkerReply> {
        let req_id = self.next_req.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.pending.lock().unwrap().insert(req_id, tx);
        let body = ControlBody::CreateParticipant {
            room_id,
            participant_id,
            sdp_offer,
        };
        if !self.send_request(target, req_id, body) {
            self.pending.lock().unwrap().remove(&req_id);
            return None;
        }
        match rx.recv_timeout(REPLY_TIMEOUT) {
            Ok(ControlReply::SdpAnswer {
                participant_id,
                sdp_answer,
                local_candidates,
            }) => Some(WorkerReply::SdpAnswer {
                participant_id,
                sdp_answer,
                local_candidates,
            }),
            Ok(ControlReply::Error {
                participant_id,
                error,
            }) => Some(WorkerReply::Error {
                participant_id,
                error,
            }),
            Err(_) => {
                self.pending.lock().unwrap().remove(&req_id);
                warn!("远端 worker {} 创建参与者超时", target);
                None
            }
        }
    }

    /// 发送无需回复的命令。
    pub fn send(&self, target: u32, body: ControlBody) {
        let _ = self.send_request(target, 0, body);
    }
}

// ──────────────────────────────────────────────────────────
// 服务端: 接收远端信令下发的命令, 转发给本地 worker
// ──────────────────────────────────────────────────────────

/// 启动控制面监听 (仅在需要接收远端信令的进程上调用)。
pub fn start_listener(
    addr: SocketAddr,
    local_txs: Vec<Sender<WorkerCommand>>,
    worker_offset: u32,
    local_workers: u32,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    info!("集群控制面监听 {}", addr);
    let local_txs = Arc::new(local_txs);
    std::thread::Builder::new()
        .name("sfu-control".to_string())
        .spawn(move || {
            for stream in listener.incoming().flatten() {
                let txs = local_txs.clone();
                std::thread::Builder::new()
                    .name("sfu-control-conn".to_string())
                    .spawn(move || handle_conn(stream, txs, worker_offset, local_workers))
                    .ok();
            }
        })?;
    Ok(())
}

fn handle_conn(
    stream: TcpStream,
    local_txs: Arc<Vec<Sender<WorkerCommand>>>,
    worker_offset: u32,
    local_workers: u32,
) {
    let mut reader = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let writer = Arc::new(Mutex::new(stream));
    while let Ok(frame) = read_frame(&mut reader) {
        let req: ControlRequest = match serde_json::from_slice(&frame) {
            Ok(r) => r,
            Err(_) => continue,
        };
        // 全局编号 → 本地编号
        let li = req
            .target_worker
            .checked_sub(worker_offset)
            .filter(|l| *l < local_workers)
            .map(|l| l as usize);
        let Some(li) = li else {
            continue;
        };
        let Some(tx) = local_txs.get(li) else {
            continue;
        };

        match req.body {
            ControlBody::CreateParticipant {
                room_id,
                participant_id,
                sdp_offer,
            } => {
                let (rtx, rrx) = crossbeam_channel::bounded(1);
                let cmd = WorkerCommand::CreateParticipant {
                    room_id,
                    participant_id,
                    sdp_offer,
                    reply_tx: rtx,
                };
                if tx.send(cmd).is_err() {
                    continue;
                }
                // 等待 worker 回复后，用同一连接写回 (每请求一线程，避免头阻塞)
                let writer = writer.clone();
                let req_id = req.req_id;
                std::thread::Builder::new()
                    .name("sfu-control-reply".to_string())
                    .spawn(move || {
                        let reply = match rrx.recv_timeout(REPLY_TIMEOUT) {
                            Ok(WorkerReply::SdpAnswer {
                                participant_id,
                                sdp_answer,
                                local_candidates,
                            }) => ControlReply::SdpAnswer {
                                participant_id,
                                sdp_answer,
                                local_candidates,
                            },
                            Ok(WorkerReply::Error {
                                participant_id,
                                error,
                            }) => ControlReply::Error {
                                participant_id,
                                error,
                            },
                            _ => ControlReply::Error {
                                participant_id: String::new(),
                                error: "worker 未响应".to_string(),
                            },
                        };
                        let resp = ControlResponse { req_id, reply };
                        if let Ok(bytes) = serde_json::to_vec(&resp) {
                            if let Ok(mut w) = writer.lock() {
                                let _ = write_frame(&mut w, &bytes);
                            }
                        }
                    })
                    .ok();
            }
            ControlBody::AddRemoteCandidate {
                participant_id,
                candidate,
                sdp_mid,
            } => {
                let _ = tx.send(WorkerCommand::AddRemoteCandidate {
                    participant_id,
                    candidate,
                    sdp_mid,
                });
            }
            ControlBody::RemoveParticipant { participant_id } => {
                let _ = tx.send(WorkerCommand::RemoveParticipant { participant_id });
            }
            ControlBody::Subscribe {
                room_id,
                subscriber_id,
                publisher_id,
                publisher_mid,
                subscriber_mid,
                publisher_worker,
            } => {
                let _ = tx.send(WorkerCommand::Subscribe {
                    room_id,
                    subscriber_id,
                    publisher_id,
                    publisher_mid,
                    subscriber_mid,
                    publisher_worker,
                });
            }
            ControlBody::Unsubscribe {
                subscriber_id,
                publisher_id,
                publisher_mid,
                publisher_worker,
            } => {
                let _ = tx.send(WorkerCommand::Unsubscribe {
                    subscriber_id,
                    publisher_id,
                    publisher_mid,
                    publisher_worker,
                });
            }
        }
    }
}

// ──────────────────────────────────────────────────────────
// WorkerRouter: 信令侧统一的「全局 worker → 命令」路由
// ──────────────────────────────────────────────────────────

/// 把信令命令路由到本地或远端 worker。
pub struct WorkerRouter {
    local_txs: Vec<Sender<WorkerCommand>>,
    worker_offset: u32,
    local_workers: u32,
    control: Option<Arc<ClusterControl>>,
}

impl WorkerRouter {
    pub fn new(
        local_txs: Vec<Sender<WorkerCommand>>,
        worker_offset: u32,
        local_workers: u32,
        control: Option<Arc<ClusterControl>>,
    ) -> Self {
        Self {
            local_txs,
            worker_offset,
            local_workers,
            control,
        }
    }

    fn local_index(&self, global: usize) -> Option<usize> {
        let g = global as u32;
        if g >= self.worker_offset && g < self.worker_offset + self.local_workers {
            Some((g - self.worker_offset) as usize)
        } else {
            None
        }
    }

    /// 创建参与者 (可能需要远端往返，返回 worker 回复)。
    pub fn create_participant(
        &self,
        global: usize,
        room_id: String,
        participant_id: String,
        sdp_offer: String,
    ) -> Option<WorkerReply> {
        if let Some(li) = self.local_index(global) {
            let (tx, rx) = crossbeam_channel::bounded(1);
            let cmd = WorkerCommand::CreateParticipant {
                room_id,
                participant_id,
                sdp_offer,
                reply_tx: tx,
            };
            self.local_txs.get(li)?.send(cmd).ok()?;
            return rx.recv_timeout(REPLY_TIMEOUT).ok();
        }
        self.control
            .as_ref()?
            .create_participant(global as u32, room_id, participant_id, sdp_offer)
    }

    /// 发送无需回复的命令。
    pub fn send(&self, global: usize, cmd: WorkerCommand) {
        if let Some(li) = self.local_index(global) {
            if let Some(tx) = self.local_txs.get(li) {
                let _ = tx.send(cmd);
            }
            return;
        }
        if let Some(control) = &self.control {
            if let Some(body) = command_to_body(cmd) {
                control.send(global as u32, body);
            }
        }
    }
}

fn command_to_body(cmd: WorkerCommand) -> Option<ControlBody> {
    match cmd {
        WorkerCommand::AddRemoteCandidate {
            participant_id,
            candidate,
            sdp_mid,
        } => Some(ControlBody::AddRemoteCandidate {
            participant_id,
            candidate,
            sdp_mid,
        }),
        WorkerCommand::RemoveParticipant { participant_id } => {
            Some(ControlBody::RemoveParticipant { participant_id })
        }
        WorkerCommand::Subscribe {
            room_id,
            subscriber_id,
            publisher_id,
            publisher_mid,
            subscriber_mid,
            publisher_worker,
        } => Some(ControlBody::Subscribe {
            room_id,
            subscriber_id,
            publisher_id,
            publisher_mid,
            subscriber_mid,
            publisher_worker,
        }),
        WorkerCommand::Unsubscribe {
            subscriber_id,
            publisher_id,
            publisher_mid,
            publisher_worker,
        } => Some(ControlBody::Unsubscribe {
            subscriber_id,
            publisher_id,
            publisher_mid,
            publisher_worker,
        }),
        // Create/Register/Unregister/RequestKeyframe 不走此路径
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 编码/解码一帧控制消息。
    #[test]
    fn control_frame_roundtrip() {
        let req = ControlRequest {
            target_worker: 3,
            req_id: 7,
            body: ControlBody::CreateParticipant {
                room_id: "r".into(),
                participant_id: "p".into(),
                sdp_offer: "v=0".into(),
            },
        };
        let bytes = serde_json::to_vec(&req).unwrap();
        let req2: ControlRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(req2.target_worker, 3);
        assert_eq!(req2.req_id, 7);
    }

    /// 端到端: 控制监听 + 客户端 create_participant 请求/响应。
    #[test]
    fn control_request_response() {
        let (ctx, crx) = crossbeam_channel::unbounded::<WorkerCommand>();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // 简化的服务端: 直接用 handle_conn
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            // 该服务端只拥有全局 worker 1
            handle_conn(stream, Arc::new(vec![ctx]), 1, 1);
        });
        // 模拟本地 worker: 收到 CreateParticipant 后回 SDP answer
        std::thread::spawn(move || {
            if let Ok(WorkerCommand::CreateParticipant { participant_id, reply_tx, .. }) =
                crx.recv()
            {
                let _ = reply_tx.send(WorkerReply::SdpAnswer {
                    participant_id,
                    sdp_answer: "v=0 answer".into(),
                    local_candidates: vec!["candidate:x".into()],
                });
            }
        });

        let mut peers = HashMap::new();
        peers.insert(1u32, addr);
        let ctl = ClusterControl::new(peers);
        let reply = ctl
            .create_participant(1, "room".into(), "p1".into(), "v=0 offer".into())
            .expect("应收到回复");
        match reply {
            WorkerReply::SdpAnswer { sdp_answer, .. } => assert_eq!(sdp_answer, "v=0 answer"),
            _ => panic!("expected SdpAnswer"),
        }
    }
}
