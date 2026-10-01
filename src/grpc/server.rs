//! gRPC 控制面服务端 (媒体服务实现)。
//!
//! [`MediaControlSvc`] 接收信令服务的 `CreateParticipant` / `SendCommand`,
//! 转发给本地 worker 命令通道。媒体 RTP 不经这里。

use crate::grpc::pb::media_control_server::{MediaControl, MediaControlServer};
use crate::grpc::{convert, pb};
use crate::worker::{WorkerCommand, WorkerReply};
use crossbeam_channel::Sender;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

/// 向本地 worker 命令队列投递的超时。
const CMD_SEND_TIMEOUT: Duration = Duration::from_secs(5);
/// 等待 worker 回复的超时。
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// 媒体服务控制面实现。
pub struct MediaControlSvc {
    local_txs: Arc<Vec<Sender<WorkerCommand>>>,
    worker_offset: u32,
    local_workers: u32,
}

impl MediaControlSvc {
    pub fn new(
        local_txs: Arc<Vec<Sender<WorkerCommand>>>,
        worker_offset: u32,
        local_workers: u32,
    ) -> Self {
        Self {
            local_txs,
            worker_offset,
            local_workers,
        }
    }

    /// 全局 worker 编号 → 本地编号 (不属于本机则 None)。
    fn local_index(&self, global: u32) -> Option<usize> {
        global
            .checked_sub(self.worker_offset)
            .filter(|l| *l < self.local_workers)
            .map(|l| l as usize)
    }

    fn local_tx(&self, global: u32) -> Option<Sender<WorkerCommand>> {
        let li = self.local_index(global)?;
        self.local_txs.get(li).cloned()
    }
}

#[tonic::async_trait]
impl MediaControl for MediaControlSvc {
    async fn create_participant(
        &self,
        request: Request<pb::CreateParticipantRequest>,
    ) -> Result<Response<pb::CreateParticipantResponse>, Status> {
        let req = request.into_inner();
        let tx = self
            .local_tx(req.target_worker)
            .ok_or_else(|| Status::not_found(format!("worker {} 不在此媒体服务", req.target_worker)))?;
        // send + recv 都是阻塞操作, 放到 blocking 线程池。
        let reply = tokio::task::spawn_blocking(move || {
            let (rtx, rrx) = crossbeam_channel::bounded(1);
            let cmd = WorkerCommand::CreateParticipant {
                room_id: req.room_id,
                participant_id: req.participant_id,
                sdp_offer: req.sdp_offer,
                reply_tx: rtx,
            };
            if tx.send_timeout(cmd, CMD_SEND_TIMEOUT).is_err() {
                return None;
            }
            rrx.recv_timeout(REPLY_TIMEOUT).ok()
        })
        .await
        .map_err(|e| Status::internal(format!("blocking task join 失败: {}", e)))?;

        let resp = match reply {
            Some(WorkerReply::SdpAnswer {
                participant_id,
                sdp_answer,
                local_candidates,
            }) => pb::CreateParticipantResponse {
                ok: true,
                participant_id,
                sdp_answer,
                local_candidates,
                error: String::new(),
            },
            Some(WorkerReply::Error {
                participant_id,
                error,
            }) => pb::CreateParticipantResponse {
                ok: false,
                participant_id,
                error,
                ..Default::default()
            },
            Some(_) => pb::CreateParticipantResponse {
                ok: false,
                error: "非预期 worker 回复".to_string(),
                ..Default::default()
            },
            None => pb::CreateParticipantResponse {
                ok: false,
                error: "worker 未响应".to_string(),
                ..Default::default()
            },
        };
        Ok(Response::new(resp))
    }

    async fn get_publisher_tracks(
        &self,
        request: Request<pb::GetPublisherTracksRequest>,
    ) -> Result<Response<pb::GetPublisherTracksResponse>, Status> {
        let req = request.into_inner();
        let tx = self
            .local_tx(req.target_worker)
            .ok_or_else(|| Status::not_found(format!("worker {} 不在此媒体服务", req.target_worker)))?;
        let reply = tokio::task::spawn_blocking(move || {
            let (rtx, rrx) = crossbeam_channel::bounded(1);
            let cmd = WorkerCommand::GetPublisherTracks {
                publisher_id: req.publisher_id,
                reply_tx: rtx,
            };
            if tx.send_timeout(cmd, CMD_SEND_TIMEOUT).is_err() {
                return None;
            }
            rrx.recv_timeout(REPLY_TIMEOUT).ok()
        })
        .await
        .map_err(|e| Status::internal(format!("blocking task join 失败: {}", e)))?;

        let resp = match reply {
            Some(WorkerReply::PublisherTracks { tracks, .. }) => pb::GetPublisherTracksResponse {
                ok: true,
                tracks: tracks
                    .into_iter()
                    .map(|(mid, kind)| pb::PublisherTrack { mid, kind })
                    .collect(),
                error: String::new(),
            },
            Some(WorkerReply::Error { error, .. }) => pb::GetPublisherTracksResponse {
                ok: false,
                error,
                ..Default::default()
            },
            _ => pb::GetPublisherTracksResponse {
                ok: false,
                error: "worker 未响应".to_string(),
                ..Default::default()
            },
        };
        Ok(Response::new(resp))
    }

    async fn send_command(
        &self,
        request: Request<pb::SendCommandRequest>,
    ) -> Result<Response<pb::SendCommandResponse>, Status> {
        let req = request.into_inner();
        let tx = self
            .local_tx(req.target_worker)
            .ok_or_else(|| Status::not_found(format!("worker {} 不在此媒体服务", req.target_worker)))?;
        let command = req
            .command
            .ok_or_else(|| Status::invalid_argument("缺少 command"))?;
        let cmd = convert::proto_to_command(command)
            .ok_or_else(|| Status::invalid_argument("无法识别的 command"))?;
        let accepted = match tx.try_send(cmd) {
            Ok(()) => true,
            Err(_) => {
                warn!("worker {} 命令队列已满, 丢弃 gRPC 命令", req.target_worker);
                false
            }
        };
        Ok(Response::new(pb::SendCommandResponse { accepted }))
    }
}

/// 启动媒体控制 gRPC 服务。
pub async fn serve_media_control(
    addr: SocketAddr,
    local_txs: Arc<Vec<Sender<WorkerCommand>>>,
    worker_offset: u32,
    local_workers: u32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let svc = MediaControlSvc::new(local_txs, worker_offset, local_workers);
    info!("媒体控制 gRPC 监听 {}", addr);
    tonic::transport::Server::builder()
        .add_service(MediaControlServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}
