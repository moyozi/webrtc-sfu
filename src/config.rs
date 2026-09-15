//! SFU 配置定义

use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct SfuConfig {
    /// UDP 媒体基础端口。worker i 实际监听 `listen_addr.port() + i`。
    /// 例如 base=5175, num_workers=4 → 占用 5175/5176/5177/5178 四个 UDP 端口。
    pub listen_addr: SocketAddr,
    /// WebSocket 信令端口
    pub ws_listen_addr: SocketAddr,
    /// Worker 线程数 (每个 worker 占用一个媒体端口, 需放行端口段)
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
        // 多 worker 采用「房间级分片 + 每 worker 独立媒体端口」模式:
        // 同一房间固定到同一 worker, 所有媒体转发在 worker 内部完成。
        // 需保证防火墙放行 [base_port, base_port + num_workers - 1] 的 UDP 端口。
        // 默认单 worker; 通过环境变量 SFU_WORKERS 开启更多 worker。
        let num_workers = std::env::var("SFU_WORKERS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(1);

        Self {
            listen_addr: "192.168.31.112:5175".parse().unwrap(),
            ws_listen_addr: "192.168.31.112:5174".parse().unwrap(),
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
