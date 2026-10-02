//! SFU 配置定义

use crate::net::uring::IoUringMode;
use crate::signaling::auth::JwtSettings;
use std::collections::HashMap;
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
    /// Worker 线程 CPU 亲和性的起始编号 (worker i pin 到 `cpu_base + i`)。
    /// 默认 0; 同机跑多个 worker 进程时用它错开核心。
    pub cpu_base: usize,
    /// recvmmsg / io_uring 批量收包大小 (≤ [crate::net::udp::MAX_BATCH])
    pub batch_size: usize,
    /// sendmmsg 批量大小 (≤ [crate::net::udp::MAX_BATCH])
    pub send_batch_size: usize,
    /// 发送队列积压上限 (超过时丢弃最旧的包)
    pub send_backlog: usize,
    /// SO_BUSY_POLL 微秒数 (0 = 禁用)
    pub busy_poll_us: u32,
    /// BWE 初始带宽估计 (bit/s)。订阅者侧用于启动带宽估计。
    pub bwe_initial_bps: u64,
    /// 收包/等待 I/O 后端: io_uring (支持则用) / epoll 回退。
    pub io_uring: IoUringMode,
    /// UDP socket 收发缓冲区目标大小 (字节)。实际值受内核 rmem_max/wmem_max 限制。
    pub sock_buf_bytes: usize,
    /// 本进程 worker 的全局编号起始 (跨主机唯一)
    pub worker_offset: u32,
    /// 本进程 relay UDP 监听地址 (跨主机中继; None = 不启用跨主机)
    pub relay_listen: Option<SocketAddr>,
    /// 远端 worker 全局编号 → 其所属进程的 relay UDP 地址
    pub relay_peers: HashMap<u32, SocketAddr>,
    /// 整集群 worker 总数 (全局编号范围 0..total)
    pub total_workers: u32,
    /// 房间 → 允许的全局 worker 列表 (房间级分片配置)。
    /// 未配置 / 未匹配 `*` 的房间按房间 id 哈希固定 (或 `default_worker` 覆盖)。
    pub room_workers: HashMap<String, Vec<u32>>,
    /// 未显式配置房间时的默认全局 worker 覆盖。
    /// `None` = 默认按房间 id 哈希固定到一个 worker。
    pub default_worker: Option<u32>,
    /// 本进程控制面 TCP 监听地址 (接收远端信令命令; None = 不监听)
    pub control_listen: Option<SocketAddr>,
    /// 远端 worker 全局编号 → 其所属进程的控制地址
    pub control_peers: HashMap<u32, SocketAddr>,
    /// 本进程是否运行信令服务器 (worker-only 进程设 false)
    pub signaling: bool,
    /// WHIP 推流接入 HTTP 监听地址 (None = 禁用)
    pub whip_listen: Option<SocketAddr>,
    /// WHEP 拉流接入 HTTP 监听地址 (None = 禁用)
    pub whep_listen: Option<SocketAddr>,
    /// JWT 用户鉴权配置 (未配置密钥时禁用)
    pub jwt: JwtSettings,
}

impl SfuConfig {
    pub fn from_args() -> Self {
        // 多 worker 采用「房间级分片 + 每 worker 独立媒体端口」模式:
        // 同一房间固定到同一 worker, 所有媒体转发在 worker 内部完成。
        // 需保证防火墙放行 [base_port, base_port + num_workers - 1] 的 UDP 端口。
        // 默认单 worker; 通过环境变量 SFU_WORKERS 开启更多 worker。
        // SFU_WORKERS=0 为特殊值: 纯信令节点, 本进程不跑任何 worker,
        // 媒体全部在远端媒体机 (需配合 SFU_TOTAL_WORKERS / SFU_CONTROL_PEERS)。
        let num_workers = std::env::var("SFU_WORKERS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1);

        // Worker 线程 pin 的起始 CPU (worker i → cpu_base + i)。默认 0。
        let cpu_base = std::env::var("SFU_CPU_BASE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);

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
        // 纯信令节点 (num_workers=0) 不产生 ICE 候选, 此处静默占位、不告警。
        let announce_ip = match std::env::var("SFU_ANNOUNCE_IP")
            .ok()
            .and_then(|v| v.parse::<IpAddr>().ok())
        {
            Some(ip) => ip,
            None if num_workers == 0 => IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
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
                    warn!("未配置 SFU_ANNOUNCE_IP 且自动探测失败, 回退 127.0.0.1 (仅本机可连)");
                    IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                }
            },
        };

        // BWE 初始带宽估计 (bit/s)。
        let bwe_initial_bps = std::env::var("SFU_BWE_INITIAL_BPS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v >= 50_000)
            .unwrap_or(1_000_000);

        // UDP socket 缓冲区 (MB)，默认 16MB；受内核 rmem_max/wmem_max 截断。
        let sock_buf_bytes = std::env::var("SFU_SOCK_BUF_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(16)
            * 1024
            * 1024;

        // 批量参数 (可配)。收/发批量受 MAX_BATCH=256 约束 (栈上 iov 数组);
        // 发送积压上限用于高 fan-out 时减少丢包 (代价是内存)。
        let max_batch = crate::net::udp::MAX_BATCH;
        let batch_size = std::env::var("SFU_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.clamp(1, max_batch))
            .unwrap_or(64);
        let send_batch_size = std::env::var("SFU_SEND_BATCH")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.clamp(1, max_batch))
            .unwrap_or(128);
        let send_backlog = std::env::var("SFU_SEND_BACKLOG")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.clamp(send_batch_size, crate::net::udp::MAX_SEND_BACKLOG_LIMIT))
            .unwrap_or(crate::net::udp::MAX_SEND_BACKLOG);

        // 跨主机中继配置
        let worker_offset = std::env::var("SFU_WORKER_OFFSET")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        let relay_listen = std::env::var("SFU_RELAY_LISTEN")
            .ok()
            .and_then(|v| v.parse::<SocketAddr>().ok());
        let mut relay_peers: HashMap<u32, SocketAddr> = HashMap::new();
        if let Ok(s) = std::env::var("SFU_RELAY_PEERS") {
            // 格式: "idx=host:port,idx=host:port"
            for part in s.split(',') {
                if let Some((idx, addr)) = part.split_once('=') {
                    if let (Ok(i), Ok(a)) =
                        (idx.trim().parse::<u32>(), addr.trim().parse::<SocketAddr>())
                    {
                        relay_peers.insert(i, a);
                    }
                }
            }
        }

        // 集群控制面 + 全局 worker 分配
        // 纯信令节点 (num_workers=0) 必须显式配置 SFU_TOTAL_WORKERS; 未配置时
        // 至少回退为 1, 避免出现 0 个 worker 的非法拓扑。
        let total_workers = std::env::var("SFU_TOTAL_WORKERS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|v| *v >= num_workers as u32)
            .unwrap_or_else(|| (num_workers as u32).max(1));

        // 房间级 worker 配置 (可选):
        //   SFU_ROOM_WORKERS="roomA=0;roomB=1,2,3;*=0,1,2,3"
        // - 未配置: 每个房间按房间 id 哈希固定到一个 worker (同房间同 worker,
        //   不同房间尽量分散)。
        // - 单机多 worker: 给房间配置多个本地 worker 编号, 房间内参与者轮转分布。
        // - 多主机多 worker: 编号使用整集群全局 worker (0..SFU_TOTAL_WORKERS),
        //   可跨主机; `*` 规则作为未列出房间的兜底。
        let mut room_workers: HashMap<String, Vec<u32>> = HashMap::new();
        if let Ok(s) = std::env::var("SFU_ROOM_WORKERS") {
            for rule in s.split(';') {
                let rule = rule.trim();
                if rule.is_empty() {
                    continue;
                }
                let Some((room, list)) = rule.split_once('=') else {
                    warn!("忽略非法 SFU_ROOM_WORKERS 规则 (缺少 '='): {}", rule);
                    continue;
                };
                let room = room.trim().to_string();
                let workers: Vec<u32> = list
                    .split(',')
                    .filter_map(|w| w.trim().parse::<u32>().ok())
                    .filter(|w| *w < total_workers)
                    .collect();
                if room.is_empty() || workers.is_empty() {
                    warn!("忽略无有效内容的 SFU_ROOM_WORKERS 规则: {}", rule);
                    continue;
                }
                room_workers.insert(room, workers);
            }
        }
        // 可选: 强制所有未配置房间落到同一个 worker (不设则按房间哈希)。
        let default_worker = std::env::var("SFU_DEFAULT_WORKER")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|w| *w < total_workers);

        let control_listen = std::env::var("SFU_CONTROL_LISTEN")
            .ok()
            .and_then(|v| v.parse::<SocketAddr>().ok());
        let mut control_peers: HashMap<u32, SocketAddr> = HashMap::new();
        if let Ok(s) = std::env::var("SFU_CONTROL_PEERS") {
            for part in s.split(',') {
                if let Some((idx, addr)) = part.split_once('=') {
                    if let (Ok(i), Ok(a)) =
                        (idx.trim().parse::<u32>(), addr.trim().parse::<SocketAddr>())
                    {
                        control_peers.insert(i, a);
                    }
                }
            }
        }
        let whip_listen = std::env::var("SFU_WHIP_ADDR")
            .ok()
            .and_then(|v| v.parse::<SocketAddr>().ok());
        let whep_listen = std::env::var("SFU_WHEP_ADDR")
            .ok()
            .and_then(|v| v.parse::<SocketAddr>().ok());
        let signaling = match std::env::var("SFU_SIGNALING") {
            Ok(v) => {
                !(v.is_empty()
                    || v == "0"
                    || v.eq_ignore_ascii_case("off")
                    || v.eq_ignore_ascii_case("false"))
            }
            Err(_) => true,
        };

        let jwt = JwtSettings::from_env();

        Self {
            listen_addr,
            ws_listen_addr,
            quic_listen_addr,
            announce_ip,
            num_workers,
            cpu_base,
            batch_size,
            send_batch_size,
            send_backlog,
            busy_poll_us: 0,
            bwe_initial_bps,
            io_uring: IoUringMode::from_env(),
            sock_buf_bytes,
            worker_offset,
            relay_listen,
            relay_peers,
            total_workers,
            room_workers,
            default_worker,
            control_listen,
            control_peers,
            signaling,
            whip_listen,
            whep_listen,
            jwt,
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
