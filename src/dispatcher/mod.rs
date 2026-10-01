//! 信令分配器 (Signaling Dispatcher) — 多信令机 + 多媒体机的统一入口。
//!
//! ## 为什么要它
//!
//! 本项目的房间成员表 (`SignalingState.rooms` / `senders` / `participant_worker`)
//! 是**进程内内存状态**。当信令部署到多台机器时，如果同一房间的两个客户端连到
//! 不同的信令进程，它们互相看不到对方 (房间内不会广播 `participant_joined`)。
//! 因此必须保证 **同一房间 → 同一信令进程**。这个「房间亲和性」正是本模块
//! 要解决的问题: 客户端先把 `room_id` 交给分配器，拿回该房间应连接的信令地址，
//! 再连过去。
//!
//! 媒体 (worker) 层已经由各信令进程独立按 `room_id` 哈希到全局 worker 编号，
//! 分配器**不参与**媒体分配; 信令进程再通过 gRPC / 本地 channel 把命令下发到
//! 对应 worker (见 `docs/multi-worker-relay.md` §8.2)。
//!
//! ## 分配算法
//!
//! 一致性哈希 (带虚拟节点): 把每台信令机的多个虚拟点放到哈希环上，
//! `room_id` 哈希后沿环顺时针找到第一个**健康**节点。它同时具备:
//! - **确定性**: 同一 `room_id` 永远命中同一节点 (只要集群拓扑不变);
//! - **均衡性**: 不同房间自动分散到各信令机;
//! - **故障转移**: 节点宕机后其房间平滑落到后继节点 (代价是该房间内存态丢失,
//!   客户端重连后会重建)。
//!
//! ## 接口
//!
//! ```text
//!   POST /allocate   {"room_id":"demo","protocol":"ws"}   (也支持 GET ?room_id=)
//!        └─► 200 {"node_id":"cell-a","ws_url":"ws://...","quic_url":"quic://...",
//!                 "whip_url":"http://...","whep_url":"http://..."}
//!   GET  /nodes      列出节点与健康状态
//!   GET  /healthz    分配器自身健康 (含节点数 / 健康节点数)
//! ```
//!
//! 配置:
//! ```bash
//! SFU_DISPATCHER_ADDR=0.0.0.0:5190
//! # id@host:ws:quic:whip:whep[:weight];...  (端口 0 = 该入口禁用)
//! SFU_SIGNALING_NODES="cell-a@10.0.0.1:5174:5173:5180:5181;cell-b@10.0.0.2:5174:5173:5180:5181:2"
//! ```

use crate::signaling::http::{parse_head, query_param, read_request, write_response};
use serde::{Deserialize, Serialize};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

/// 每个权重单位对应的虚拟节点数。越大越均衡, 但哈希环越大。
const VNODES_PER_WEIGHT: u32 = 160;

/// 单台信令机。
#[derive(Debug)]
pub struct SignalingNode {
    pub id: String,
    pub host: String,
    /// WebSocket 信令端口 (0 = 未启用)
    pub ws_port: u16,
    /// QUIC 信令端口 (0 = 未启用; UDP, 不参与 TCP 健康检查)
    pub quic_port: u16,
    /// WHIP 推流接入端口 (0 = 未启用)
    pub whip_port: u16,
    /// WHEP 拉流接入端口 (0 = 未启用)
    pub whep_port: u16,
    /// 权重 (≥1), 用于一致性哈希虚拟节点数量。
    pub weight: u32,
    /// 最近一次健康检查结果。
    pub up: AtomicBool,
}

impl SignalingNode {
    /// 健康检查目标: 优先 WS, 其次 WHIP/WHEP。
    ///
    /// 全部入口都关闭 (纯 QUIC) 时返回 `None` —— 该节点视为始终健康,
    /// 因为 TCP 探针无法探测 QUIC (UDP) 端口。
    fn health_target(&self) -> Option<String> {
        let port = if self.ws_port > 0 {
            self.ws_port
        } else if self.whip_port > 0 {
            self.whip_port
        } else if self.whep_port > 0 {
            self.whep_port
        } else {
            return None;
        };
        Some(format!("{}:{}", host_for_url(&self.host), port))
    }

    fn is_up(&self) -> bool {
        self.up.load(Ordering::Relaxed)
    }

    fn ws_url(&self) -> Option<String> {
        (self.ws_port > 0).then(|| format!("ws://{}:{}", host_for_url(&self.host), self.ws_port))
    }
    fn quic_url(&self) -> Option<String> {
        (self.quic_port > 0)
            .then(|| format!("quic://{}:{}", host_for_url(&self.host), self.quic_port))
    }
    fn whip_url(&self) -> Option<String> {
        (self.whip_port > 0)
            .then(|| format!("http://{}:{}", host_for_url(&self.host), self.whip_port))
    }
    fn whep_url(&self) -> Option<String> {
        (self.whep_port > 0)
            .then(|| format!("http://{}:{}", host_for_url(&self.host), self.whep_port))
    }
}

/// 分配器配置。
#[derive(Debug)]
pub struct DispatcherConfig {
    pub listen: SocketAddr,
    pub nodes: Vec<SignalingNode>,
    pub health_interval: Duration,
    pub health_timeout: Duration,
}

impl DispatcherConfig {
    /// 未设置 `SFU_DISPATCHER_ADDR` 时返回 `None` (本进程不是分配器角色)。
    /// 设置了但配置非法时直接退出 (启动期 fail-fast)。
    pub fn from_env() -> Option<Self> {
        let raw_addr = std::env::var("SFU_DISPATCHER_ADDR").ok()?;
        if raw_addr.is_empty() || raw_addr == "0" || raw_addr.eq_ignore_ascii_case("off") {
            return None;
        }
        let listen: SocketAddr = raw_addr.parse().unwrap_or_else(|e| {
            eprintln!("SFU_DISPATCHER_ADDR 非法 ({}): {}", raw_addr, e);
            std::process::exit(2);
        });

        let raw_nodes = std::env::var("SFU_SIGNALING_NODES").unwrap_or_default();
        let nodes = parse_nodes(&raw_nodes);
        if nodes.is_empty() {
            eprintln!(
                "分配器需要 SFU_SIGNALING_NODES (格式: id@host:ws:quic:whip:whep[:weight];...)，当前为空"
            );
            std::process::exit(2);
        }

        let health_interval = Duration::from_secs(
            env_u64("SFU_DISPATCHER_HEALTH_INTERVAL_SECS")
                .unwrap_or(5)
                .max(1),
        );
        let health_timeout = Duration::from_millis(
            env_u64("SFU_DISPATCHER_HEALTH_TIMEOUT_MS")
                .unwrap_or(1000)
                .max(50),
        );

        Some(Self {
            listen,
            nodes,
            health_interval,
            health_timeout,
        })
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}

/// 解析 `id@host:ws:quic:whip:whep[:weight];...`。
///
/// IPv6 主机用方括号，例如 `cell@[::1]:5174:5173:0:0`。
pub fn parse_nodes(raw: &str) -> Vec<SignalingNode> {
    let mut out = Vec::new();
    for entry in raw.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((id, rest)) = entry.split_once('@') else {
            warn!("忽略非法信令节点 (缺少 '@'): {}", entry);
            continue;
        };
        let id = id.trim();
        if id.is_empty() {
            warn!("忽略非法信令节点 (id 为空): {}", entry);
            continue;
        }
        let fields = split_host_ports(rest);
        if fields.len() < 2 {
            warn!("忽略非法信令节点 (至少需要 host:ws_port): {}", entry);
            continue;
        }
        let port = |i: usize| -> u16 { fields.get(i).and_then(|s| s.parse().ok()).unwrap_or(0) };
        let weight = fields
            .get(5)
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|w| *w > 0)
            .unwrap_or(1);
        let node = SignalingNode {
            id: id.to_string(),
            // 去掉 IPv6 方括号, 统一保存裸地址 (输出时再加回)。
            host: fields[0].trim_matches(|c| c == '[' || c == ']').to_string(),
            ws_port: port(1),
            quic_port: port(2),
            whip_port: port(3),
            whep_port: port(4),
            weight,
            up: AtomicBool::new(false),
        };
        if node.ws_port == 0 && node.whip_port == 0 && node.whep_port == 0 {
            warn!(
                "信令节点 {} 没有可做 TCP 健康检查的端口 (仅 QUIC), 将始终视为健康",
                node.id
            );
        }
        out.push(node);
    }
    out
}

/// 按 `:` 拆分 host 与端口，遇到 `[...]` 形式保留其中的冒号。
fn split_host_ports(s: &str) -> Vec<String> {
    let s = s.trim();
    let mut fields = Vec::new();
    let bytes = s.as_bytes();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut in_brackets = false;
    while i < bytes.len() {
        match bytes[i] {
            b'[' => in_brackets = true,
            b']' => in_brackets = false,
            b':' if !in_brackets => {
                fields.push(s[start..i].to_string());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    fields.push(s[start..].to_string());
    fields
}

/// IPv6 字面量加方括号，普通主机名/IPv4 原样返回。
fn host_for_url(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{}]", host)
    } else {
        host.to_string()
    }
}

/// 一致性哈希环: (哈希值, 节点下标)，按哈希升序。
struct Ring {
    points: Vec<(u64, usize)>,
}

impl Ring {
    fn build(nodes: &[SignalingNode]) -> Self {
        let mut points = Vec::new();
        for (idx, node) in nodes.iter().enumerate() {
            let vnodes = node.weight.max(1) * VNODES_PER_WEIGHT;
            for v in 0..vnodes {
                let key = format!("{}#{}", node.id, v);
                points.push((fnv1a(key.as_bytes()), idx));
            }
        }
        points.sort_unstable_by_key(|p| p.0);
        Self { points }
    }
}

/// FNV-1a 64-bit —— 稳定、无随机种子, 保证跨进程/重启结果一致。
///
/// 末尾再做一次 splitmix64 finalizer: 裸 FNV-1a 对仅尾部不同的短字符串
/// (如 `room-1` / `room-2`) 雪崩性差, 哈希值高度接近, 会让一致性哈希把
/// 大量房间挤到环上的同一段 (全部落到同一节点)。finalizer 充分打散高位。
fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in data {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d049bb133111eb);
    h ^= h >> 31;
    h
}

/// 分配器运行时状态。
pub struct Dispatcher {
    listen: SocketAddr,
    nodes: Vec<SignalingNode>,
    ring: Ring,
    health_interval: Duration,
    health_timeout: Duration,
}

impl Dispatcher {
    pub fn new(cfg: DispatcherConfig) -> Self {
        let ring = Ring::build(&cfg.nodes);
        Self {
            listen: cfg.listen,
            nodes: cfg.nodes,
            ring,
            health_interval: cfg.health_interval,
            health_timeout: cfg.health_timeout,
        }
    }

    /// 为房间挑选一台健康信令机: `room_id` 哈希后沿环找到第一个 up 的节点。
    /// 全部节点都不可用时返回 `None`。
    pub fn allocate<'a>(&'a self, room_id: &str) -> Option<&'a SignalingNode> {
        if self.ring.points.is_empty() {
            return None;
        }
        let h = fnv1a(room_id.as_bytes());
        let n = self.ring.points.len();
        let start = self.ring.points.partition_point(|p| p.0 < h);
        for off in 0..n {
            let idx = (start + off) % n;
            let node = &self.nodes[self.ring.points[idx].1];
            if node.is_up() {
                return Some(node);
            }
        }
        None
    }

    fn healthy_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.is_up()).count()
    }

    async fn health_once(&self) {
        for node in &self.nodes {
            let Some(target) = node.health_target() else {
                // 无 TCP 端口可探测 (纯 QUIC) —— 视为健康。
                node.up.store(true, Ordering::Relaxed);
                continue;
            };
            let ok = matches!(
                tokio::time::timeout(self.health_timeout, TcpStream::connect(&target)).await,
                Ok(Ok(_))
            );
            let prev = node.up.swap(ok, Ordering::Relaxed);
            if prev != ok {
                if ok {
                    info!("信令节点 {} 恢复健康 ({})", node.id, target);
                } else {
                    warn!("信令节点 {} 健康检查失败 ({})", node.id, target);
                }
            }
        }
    }
}

/// 阻塞运行分配器 (创建独立 Tokio runtime)。
pub fn run_blocking(cfg: DispatcherConfig) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("sfu-dispatcher")
        .build()
        .expect("failed to create dispatcher runtime");
    rt.block_on(run(cfg));
}

/// 运行分配器: 先做一次健康检查，再并发跑健康循环 + HTTP 监听。
pub async fn run(cfg: DispatcherConfig) {
    let state = Arc::new(Dispatcher::new(cfg));
    info!(
        "信令分配器监听 {}, 已登记 {} 个信令节点",
        state.listen,
        state.nodes.len()
    );

    // 先探一遍, 避免启动瞬间全部 503。
    state.health_once().await;
    info!(
        "信令节点健康检查完成: {}/{} 可用",
        state.healthy_count(),
        state.nodes.len()
    );

    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(state.health_interval).await;
                state.health_once().await;
            }
        });
    }

    let listener = match TcpListener::bind(state.listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("分配器绑定 {} 失败: {}", state.listen, e);
            return;
        }
    };
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!("分配器 accept 失败: {}", e);
                continue;
            }
        };
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, state).await {
                tracing::debug!("分配器连接结束: {} ({})", peer, e);
            }
        });
    }
}

#[derive(Debug, Deserialize)]
struct AllocateRequest {
    #[serde(default)]
    room_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct AllocateResponse {
    room_id: String,
    node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ws_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    quic_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    whip_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    whep_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct NodeView {
    id: String,
    host: String,
    healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    ws_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    quic_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    whip_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    whep_url: Option<String>,
}

async fn handle(mut stream: TcpStream, state: Arc<Dispatcher>) -> io::Result<()> {
    let Some(req) = read_request(&mut stream).await? else {
        return Ok(());
    };
    let method = req.method.clone();
    let (path, query) = parse_head(&req);

    if method == "OPTIONS" {
        return write_response(&mut stream, "204 No Content", "text/plain", &[], b"").await;
    }

    // 房间 id: 查询串优先, 其次 JSON body。
    let room_from_body = || -> Option<String> {
        serde_json::from_slice::<AllocateRequest>(&req.body)
            .ok()
            .and_then(|r| r.room_id)
    };

    match (method.as_str(), path.as_str()) {
        ("GET", "/healthz") | ("GET", "/health") => {
            let body = serde_json::json!({
                "status": "ok",
                "nodes": state.nodes.len(),
                "healthy": state.healthy_count(),
            });
            write_json(&mut stream, "200 OK", &body).await
        }
        ("GET", "/nodes") => {
            let views: Vec<NodeView> = state
                .nodes
                .iter()
                .map(|n| NodeView {
                    id: n.id.clone(),
                    host: n.host.clone(),
                    healthy: n.is_up(),
                    ws_url: n.ws_url(),
                    quic_url: n.quic_url(),
                    whip_url: n.whip_url(),
                    whep_url: n.whep_url(),
                })
                .collect();
            write_json(&mut stream, "200 OK", &views).await
        }
        ("GET", "/allocate") | ("POST", "/allocate") => {
            let room = query_param(&query, "room_id")
                .or_else(|| query_param(&query, "room"))
                .or_else(room_from_body);
            let Some(room) = room.filter(|r| !r.is_empty()) else {
                return write_response(
                    &mut stream,
                    "400 Bad Request",
                    "application/json",
                    &[],
                    br#"{"error":"missing room_id"}"#,
                )
                .await;
            };
            match state.allocate(&room) {
                Some(node) => {
                    let body = AllocateResponse {
                        room_id: room,
                        node_id: node.id.clone(),
                        ws_url: node.ws_url(),
                        quic_url: node.quic_url(),
                        whip_url: node.whip_url(),
                        whep_url: node.whep_url(),
                    };
                    write_json(&mut stream, "200 OK", &body).await
                }
                None => {
                    write_response(
                        &mut stream,
                        "503 Service Unavailable",
                        "application/json",
                        &[],
                        br#"{"error":"no healthy signaling node"}"#,
                    )
                    .await
                }
            }
        }
        _ => {
            write_response(
                &mut stream,
                "404 Not Found",
                "text/plain",
                &[],
                b"not found",
            )
            .await
        }
    }
}

async fn write_json<T: Serialize>(
    stream: &mut TcpStream,
    status: &str,
    value: &T,
) -> io::Result<()> {
    let body = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    write_response(stream, status, "application/json", &[], &body).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, ws: u16) -> SignalingNode {
        SignalingNode {
            id: id.to_string(),
            host: "10.0.0.1".to_string(),
            ws_port: ws,
            quic_port: 0,
            whip_port: 0,
            whep_port: 0,
            weight: 1,
            up: AtomicBool::new(true),
        }
    }

    #[test]
    fn parse_simple_nodes() {
        let nodes = parse_nodes("cell-a@10.0.0.1:5174:5173:5180:5181; cell-b@10.0.0.2:5174");
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id, "cell-a");
        assert_eq!(nodes[0].host, "10.0.0.1");
        assert_eq!(nodes[0].ws_port, 5174);
        assert_eq!(nodes[0].quic_port, 5173);
        assert_eq!(nodes[0].whip_port, 5180);
        assert_eq!(nodes[0].whep_port, 5181);
        assert_eq!(nodes[0].weight, 1);
        assert_eq!(nodes[1].quic_port, 0);
    }

    #[test]
    fn parse_weight_and_ipv6() {
        let nodes = parse_nodes("a@[::1]:5174:5173:0:0:3");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].host, "::1");
        assert_eq!(nodes[0].weight, 3);
        assert_eq!(nodes[0].health_target().as_deref(), Some("[::1]:5174"));
        assert_eq!(nodes[0].ws_url().as_deref(), Some("ws://[::1]:5174"));
    }

    #[test]
    fn parse_skips_malformed() {
        let nodes = parse_nodes("no-at-sign;b@host-only;c@h:1");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "c");
    }

    #[test]
    fn similar_room_names_spread() {
        // 回归: 裸 FNV-1a 会让 `room-1..32` 这类近似字符串全部落到同一节点。
        let nodes = vec![node("a", 1), node("b", 2)];
        let d = Dispatcher::new(DispatcherConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            nodes,
            health_interval: Duration::from_secs(5),
            health_timeout: Duration::from_millis(100),
        });
        let mut seen = std::collections::HashSet::new();
        for i in 0..32 {
            seen.insert(d.allocate(&format!("room-{}", i)).unwrap().id.clone());
        }
        assert!(seen.len() > 1, "近似房间名也应分散: {:?}", seen);
    }

    #[test]
    fn allocation_is_sticky_and_spread() {
        let nodes = vec![node("a", 1), node("b", 2), node("c", 3), node("d", 4)];
        let d = Dispatcher::new(DispatcherConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            nodes,
            health_interval: Duration::from_secs(5),
            health_timeout: Duration::from_millis(100),
        });
        // 同一房间稳定命中同一节点。
        let first = d.allocate("room-x").unwrap().id.clone();
        for _ in 0..10 {
            assert_eq!(d.allocate("room-x").unwrap().id, first);
        }
        // 大量房间应分散到多个节点。
        let mut seen = std::collections::HashSet::new();
        for i in 0..200 {
            seen.insert(d.allocate(&format!("room-{}", i)).unwrap().id.clone());
        }
        assert!(seen.len() > 1, "房间应分散到多个信令节点: {:?}", seen);
    }

    #[test]
    fn allocation_skips_unhealthy() {
        let nodes = vec![node("a", 1), node("b", 2)];
        // 先把 b 关掉。
        nodes[1].up.store(false, Ordering::Relaxed);
        let d = Dispatcher::new(DispatcherConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            nodes,
            health_interval: Duration::from_secs(5),
            health_timeout: Duration::from_millis(100),
        });
        for i in 0..50 {
            let got = d.allocate(&format!("room-{}", i));
            assert_eq!(got.map(|n| n.id.as_str()), Some("a"));
        }
        // 全部不可用 → None。
        d.nodes[0].up.store(false, Ordering::Relaxed);
        assert!(d.allocate("room-0").is_none());
    }
}
