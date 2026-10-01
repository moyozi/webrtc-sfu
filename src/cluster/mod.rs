//! 控制面 — 把信令命令路由到本地或**远端 worker**。
//!
//! 部署模型: 信令进程与 worker/媒体进程可分离 (旧称 worker-only)。信令对
//! 远端 worker 的命令统一走 gRPC (见 `crate::grpc`), 取代早期的自研 TCP 控制面:
//!
//! - 本地 worker: 直接走进程内 channel;
//! - 远端 worker: `gRPC MediaControl` (CreateParticipant 同步返回 SDP answer,
//!   其余命令 fire-and-forget)。
//!
//! 媒体中继的 RTP 仍走 UDP (见 `net::relay`), 跨主机点对点直连。

use crate::grpc::GrpcControl;
use crate::worker::{WorkerCommand, WorkerReply};
use crossbeam_channel::Sender;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// 向本地 worker 命令队列投递 (有界队列可能暂满) 的超时。
const CMD_SEND_TIMEOUT: Duration = Duration::from_secs(5);
/// CreateParticipant 等待本地 worker 回复的超时。
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// 把信令命令路由到本地或远端 worker。
pub struct WorkerRouter {
    local_txs: Vec<Sender<WorkerCommand>>,
    worker_offset: u32,
    local_workers: u32,
    control: Option<Arc<GrpcControl>>,
}

impl WorkerRouter {
    pub fn new(
        local_txs: Vec<Sender<WorkerCommand>>,
        worker_offset: u32,
        local_workers: u32,
        control: Option<Arc<GrpcControl>>,
    ) -> Self {
        Self {
            local_txs,
            worker_offset,
            local_workers,
            control,
        }
    }

    /// 全局编号是否属于本进程, 是则返回本地下标。
    pub fn local_index(&self, global: usize) -> Option<usize> {
        let g = global as u32;
        if self.local_workers > 0 && g >= self.worker_offset && g < self.worker_offset + self.local_workers
        {
            Some((g - self.worker_offset) as usize)
        } else {
            None
        }
    }

    /// 创建参与者 (可能需要远端 gRPC 往返，返回 worker 回复)。
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
            self.local_txs
                .get(li)?
                .send_timeout(cmd, CMD_SEND_TIMEOUT)
                .ok()?;
            return rx.recv_timeout(REPLY_TIMEOUT).ok();
        }
        self.control
            .as_ref()?
            .create_participant(global as u32, room_id, participant_id, sdp_offer)
    }

    /// 查询发布者已发布的轨道 (mid, kind)，用于 WHEP 按发布者订阅。
    pub fn publisher_tracks(
        &self,
        global: usize,
        publisher_id: String,
    ) -> Option<Vec<(String, String)>> {
        if let Some(li) = self.local_index(global) {
            let (tx, rx) = crossbeam_channel::bounded(1);
            let cmd = WorkerCommand::GetPublisherTracks {
                publisher_id,
                reply_tx: tx,
            };
            self.local_txs
                .get(li)?
                .send_timeout(cmd, CMD_SEND_TIMEOUT)
                .ok()?;
            return match rx.recv_timeout(REPLY_TIMEOUT).ok()? {
                WorkerReply::PublisherTracks { tracks, .. } => Some(tracks),
                _ => None,
            };
        }
        self.control
            .as_ref()?
            .publisher_tracks(global as u32, publisher_id)
    }

    /// 发送无需回复的命令。
    pub fn send(&self, global: usize, cmd: WorkerCommand) {
        if let Some(li) = self.local_index(global) {
            if let Some(tx) = self.local_txs.get(li) {
                if tx.try_send(cmd).is_err() {
                    warn!("本地 worker {} 命令队列已满, 丢弃命令", global);
                }
            }
            return;
        }
        if let Some(control) = &self.control {
            control.send(global as u32, cmd);
        } else {
            warn!("worker {} 在远端但未配置媒体服务 (gRPC) 地址", global);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn router(local_workers: u32, offset: u32) -> WorkerRouter {
        let txs = (0..local_workers)
            .map(|_| crossbeam_channel::bounded(4).0)
            .collect();
        WorkerRouter::new(txs, offset, local_workers, None)
    }

    #[test]
    fn local_index_mapping() {
        let r = router(2, 2);
        assert_eq!(r.local_index(2), Some(0));
        assert_eq!(r.local_index(3), Some(1));
        assert_eq!(r.local_index(1), None);
        assert_eq!(r.local_index(4), None);
    }

    #[test]
    fn zero_local_workers_routes_nothing_local() {
        let r = router(0, 0);
        assert_eq!(r.local_index(0), None);
    }
}
