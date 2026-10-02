//! gRPC 控制面**同步门面**。
//!
//! 信令与 worker 事件循环都是同步代码, 因此这里把 tonic 的异步客户端包装成
//! 同步 API: 内部持有一个专用 Tokio runtime, 同步方法通过 `std::sync::mpsc`
//! 一次性回复通道把异步结果取回。这样无论调用方是否处于 Tokio 上下文中
//! (例如 `spawn_blocking`), 都不会触发 "runtime within runtime"。

use crate::grpc::pb::media_control_client::MediaControlClient;
use crate::grpc::{convert, pb};
use crate::worker::{WorkerCommand, WorkerReply};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{mpsc, Arc};
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::warn;

/// 单次 RPC 超时。
const RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// 信令 → 媒体服务的 gRPC 控制客户端。
///
/// 跨主机媒体 RTP 不走这里, 仍由 `RelayHub` 点对点 UDP 直连。
pub struct GrpcControl {
    /// 全局 worker 编号 → 其所属媒体服务的 gRPC 地址
    peers: HashMap<u32, SocketAddr>,
    /// 专用 gRPC runtime。用 `Option` 包裹以便在 `Drop` 中后台关闭。
    runtime: Option<tokio::runtime::Runtime>,
    client_cache: Arc<Mutex<HashMap<SocketAddr, MediaControlClient<Channel>>>>,
    /// fire-and-forget 命令队列 (由 runtime 上的任务消费)
    tx: tokio::sync::mpsc::UnboundedSender<(u32, pb::Command)>,
}

impl GrpcControl {
    /// 构造并启动后台发送任务。`peers` 为空时所有远程调用直接失败。
    pub fn new(peers: HashMap<u32, SocketAddr>) -> Arc<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("sfu-grpc")
            .enable_all()
            .build()
            .expect("failed to build gRPC runtime");
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let this = Arc::new(Self {
            peers,
            runtime: Some(runtime),
            client_cache: Arc::new(Mutex::new(HashMap::new())),
            tx,
        });
        this.clone().spawn_drain(rx);
        this
    }

    /// 内层 runtime 句柄 (仅在 `Drop` 之后不可用)。
    fn runtime(&self) -> &tokio::runtime::Runtime {
        self.runtime.as_ref().expect("gRPC runtime 已关闭")
    }

    fn spawn_drain(self: Arc<Self>, mut rx: tokio::sync::mpsc::UnboundedReceiver<(u32, pb::Command)>) {
        let cache = self.client_cache.clone();
        let peers = self.peers.clone();
        self.runtime().spawn(async move {
            while let Some((target, command)) = rx.recv().await {
                let Some(addr) = peers.get(&target).copied() else {
                    warn!("gRPC 控制: 未知目标 worker {}", target);
                    continue;
                };
                match media_client(addr, &cache).await {
                    Ok(mut client) => {
                        let req = tonic::Request::new(pb::SendCommandRequest {
                            target_worker: target,
                            command: Some(command),
                        });
                        if let Err(e) = client.send_command(req).await {
                            warn!("gRPC SendCommand 失败 (worker {}): {}", target, e);
                        }
                    }
                    Err(e) => warn!("gRPC 连接媒体服务 {} 失败: {}", addr, e),
                }
            }
        });
    }

    /// 同步创建参与者 (远端媒体服务)。
    pub fn create_participant(
        &self,
        target: u32,
        room_id: String,
        participant_id: String,
        sdp_offer: String,
    ) -> Option<WorkerReply> {
        let addr = *self.peers.get(&target)?;
        let cache = self.client_cache.clone();
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.runtime().spawn(async move {
            let result = async {
                let mut client = media_client(addr, &cache).await.ok()?;
                let req = tonic::Request::new(pb::CreateParticipantRequest {
                    target_worker: target,
                    room_id,
                    participant_id,
                    sdp_offer,
                });
                let resp = tokio::time::timeout(RPC_TIMEOUT, client.create_participant(req))
                    .await
                    .ok()?
                    .ok()?
                    .into_inner();
                Some(if resp.ok {
                    WorkerReply::SdpAnswer {
                        participant_id: resp.participant_id,
                        sdp_answer: resp.sdp_answer,
                        local_candidates: resp.local_candidates,
                    }
                } else {
                    WorkerReply::Error {
                        participant_id: resp.participant_id,
                        error: resp.error,
                    }
                })
            }
            .await;
            let _ = reply_tx.send(result);
        });
        match reply_rx.recv_timeout(RPC_TIMEOUT + Duration::from_secs(1)) {
            Ok(reply) => reply,
            Err(_) => {
                warn!("gRPC CreateParticipant 超时 (worker {})", target);
                None
            }
        }
    }

    /// 发送无需回复的 worker 命令 (非阻塞, 入队后由后台任务发送)。
    pub fn send(&self, target: u32, cmd: WorkerCommand) {
        if let Some(command) = convert::command_to_proto(cmd) {
            if self.tx.send((target, command)).is_err() {
                warn!("gRPC 控制队列已关闭, 丢弃发往 worker {} 的命令", target);
            }
        }
    }

    /// 同步查询发布者已发布的轨道 (mid, kind)。
    pub fn publisher_tracks(
        &self,
        target: u32,
        publisher_id: String,
    ) -> Option<Vec<(String, String)>> {
        let addr = *self.peers.get(&target)?;
        let cache = self.client_cache.clone();
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.runtime().spawn(async move {
            let result = async {
                let mut client = media_client(addr, &cache).await.ok()?;
                let req = tonic::Request::new(pb::GetPublisherTracksRequest {
                    target_worker: target,
                    publisher_id,
                });
                let resp = tokio::time::timeout(RPC_TIMEOUT, client.get_publisher_tracks(req))
                    .await
                    .ok()?
                    .ok()?
                    .into_inner();
                if resp.ok {
                    Some(resp.tracks.into_iter().map(|t| (t.mid, t.kind)).collect())
                } else {
                    None
                }
            }
            .await;
            let _ = reply_tx.send(result);
        });
        reply_rx
            .recv_timeout(RPC_TIMEOUT + Duration::from_secs(1))
            .ok()
            .flatten()
    }
}

impl Drop for GrpcControl {
    fn drop(&mut self) {
        // GrpcControl 可能被持有 `SignalingState` 的 async 任务在 runtime 线程上
        // 释放。此时同步 drop 内层 runtime 会阻塞等待 blocking 线程池, 触发 tokio
        // 「Cannot drop a runtime in a context where blocking is not allowed」
        // panic。进程已在退出路径上, 后台关闭即可, 无需等待残留 RPC。
        if let Some(rt) = self.runtime.take() {
            rt.shutdown_background();
        }
    }
}

async fn media_client(
    addr: SocketAddr,
    cache: &Mutex<HashMap<SocketAddr, MediaControlClient<Channel>>>,
) -> Result<MediaControlClient<Channel>, tonic::transport::Error> {
    {
        let guard = cache.lock().await;
        if let Some(c) = guard.get(&addr) {
            return Ok(c.clone());
        }
    }
    let client = MediaControlClient::connect(format!("http://{}", addr)).await?;
    cache.lock().await.insert(addr, client.clone());
    Ok(client)
}
