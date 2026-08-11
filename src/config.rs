//! SFU 配置定义

use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct SfuConfig {
    /// UDP 媒体端口 (所有 worker 通过 SO_REUSEPORT 共享)
    pub listen_addr: SocketAddr,
    /// WebSocket 信令端口
    pub ws_listen_addr: SocketAddr,
    /// Worker 线程数 (默认 = CPU 核心数)
    pub num_workers: usize,
    /// recvmmsg 批量大小
    pub batch_size: usize,
    /// sendmmsg 批量大小
    pub send_batch_size: usize,
    /// SO_BUSY_POLL 微秒数 (0 = 禁用)
    pub busy_poll_us: u32,
}

impl SfuConfig {
    pub fn from_args() -> Self {
        let num_workers = std::env::var("SFU_WORKERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| num_cpus());

        Self {
            listen_addr: "0.0.0.0:5175".parse().unwrap(),
            ws_listen_addr: "0.0.0.0:5176".parse().unwrap(),
            num_workers,
            batch_size: 64,
            send_batch_size: 128,
            busy_poll_us: 0,
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}
