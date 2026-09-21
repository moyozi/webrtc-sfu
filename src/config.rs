//! SFU 配置定义

use std::net::{IpAddr, SocketAddr};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct SfuConfig {
    /// UDP 媒体绑定地址 (通常为 0.0.0.0)。worker i 实际监听 `listen_addr.port() + i`。
    /// 例如 base=5175, num_workers=4 → 占用 5175/5176/5177/5178 四个 UDP 端口。
    pub listen_addr: SocketAddr,
    /// WebSocket 信令绑定地址 (通常为 0.0.0.0)
    pub ws_listen_addr: SocketAddr,
    /// QUIC 信令绑定地址 (None = 禁用 QUIC)。默认启用, 移动端优先使用。
    pub quic_listen_addr: Option<SocketAddr>,
    /// 对外通告 IP — 写入 SDP/ICE host 候选的地址。
    /// 服务实际绑定 0.0.0.0, 客户端连接的却是该地址 (公网/内网可达 IP)。
    pub announce_ip: IpAddr,
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

        // 监听地址一律绑定 0.0.0.0 (所有网卡), 由防火墙控制暴露面。
        let ws_listen_addr: SocketAddr = std::env::var("SFU_WS_ADDR")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| "0.0.0.0:5174".parse().unwrap());

        let listen_addr: SocketAddr = std::env::var("SFU_MEDIA_ADDR")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| "0.0.0.0:5175".parse().unwrap());

        // QUIC 信令默认开启, IP 与 WebSocket 相同; 端口可用 SFU_QUIC_ADDR 覆盖,
        // 置空 (SFU_QUIC_ADDR="") 或设为 "0" 可禁用。
        let quic_listen_addr = match std::env::var("SFU_QUIC_ADDR") {
            Ok(v) if v.is_empty() || v == "0" || v.eq_ignore_ascii_case("off") => None,
            Ok(v) => v.parse().ok(),
            Err(_) => Some(SocketAddr::new(ws_listen_addr.ip(), 5173)),
        };

        // 对外通告 IP — 专门配置, 与监听地址解耦。
        // 优先 SFU_ANNOUNCE_IP; 未配置时尝试自动探测本机非环回 IP,
        // 探测失败则回退 127.0.0.1 (仅本机可用, 会打印告警)。
        let announce_ip = match std::env::var("SFU_ANNOUNCE_IP")
            .ok()
            .and_then(|v| v.parse::<IpAddr>().ok())
        {
            Some(ip) => ip,
            None => match detect_local_ip() {
                Some(ip) => {
                    warn!(
                        "未配置 SFU_ANNOUNCE_IP, 自动探测对外通告 IP 为 {}; \
                         生产环境请显式配置",
                        ip
                    );
                    ip
                }
                None => {
                    warn!(
                        "未配置 SFU_ANNOUNCE_IP 且自动探测失败, 回退 127.0.0.1 (仅本机可连)"
                    );
                    IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                }
            },
        };

        Self {
            listen_addr,
            ws_listen_addr,
            quic_listen_addr,
            announce_ip,
            num_workers,
            batch_size: 64,
            send_batch_size: 128,
            busy_poll_us: 0,
        }
    }
}

/// 探测本机默认出口 IP。
///
/// 通过 UDP `connect` 让内核按路由表选择源地址 (不会真正发包),
/// 适用于多网卡主机选择对外可达的地址。
fn detect_local_ip() -> Option<IpAddr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    // 任意公网地址即可, 不会产生实际流量。
    socket.connect("8.8.8.8:80").ok()?;
    socket.local_addr().ok().map(|addr| addr.ip())
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}
