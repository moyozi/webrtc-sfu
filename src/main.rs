//! 高性能 WebRTC SFU 服务
//!
//! 基于 str0m (Sans I/O WebRTC) + nix recvmmsg 批量收包 + epoll 事件驱动

// 项目包含尚未使用的脚手架代码 (Room, Track, TimerWheel 等)，暂不警告
#![allow(dead_code)]

mod config;
mod core;
mod net;
mod sfu;
mod signaling;

use config::SfuConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, warn, error};

fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    // 解析配置
    let config = SfuConfig::from_args();
    info!(?config, "启动 WebRTC SFU 服务");

    let num_workers = config.num_workers;
    let listen_addr = config.listen_addr;
    let ws_addr = config.ws_listen_addr;

    // 每个 worker 占用一个媒体端口 (base + cpu_id), 防止超出 u16 端口上限。
    let max_workers = (u16::MAX as usize) - (listen_addr.port() as usize) + 1;
    let num_workers = num_workers.min(max_workers);
    if num_workers < config.num_workers {
        warn!(
            "worker 数由 {} 限制为 {} (媒体端口范围不足)",
            config.num_workers, num_workers
        );
    }

    // ── 1. 创建 Worker 线程 ──
    let mut worker_handles = Vec::with_capacity(num_workers);
    let mut worker_cmd_txs = Vec::with_capacity(num_workers);

    for cpu_id in 0..num_workers {
        let (cmd_tx, cmd_rx) = crossbeam_channel::unbounded::<core::WorkerCommand>();
        worker_cmd_txs.push(cmd_tx);

        // 每个 worker 使用独立的媒体端口: base_port + cpu_id。
        //
        // 既然同一个房间的所有参与者都被 select_worker() 固定到同一个 worker,
        // 每个 worker 只需要自己的端口就行。客户端从 SDP 里拿到该 worker 的
        // ICE 候选 (端口即 base_port + cpu_id), 包会直接到达正确的 worker,
        // 不再依赖内核 SO_REUSEPORT 的流哈希 (它无法保证落到持有 Rtc 的 worker)。
        let worker_listen_addr = SocketAddr::new(
            listen_addr.ip(),
            listen_addr.port() + cpu_id as u16,
        );

        let worker_config = core::WorkerConfig {
            cpu_id,
            listen_addr: worker_listen_addr,
            batch_size: config.batch_size,
            send_batch_size: config.send_batch_size,
            busy_poll_us: config.busy_poll_us,
        };

        let handle = std::thread::Builder::new()
            .name(format!("sfu-worker-{}", cpu_id))
            .stack_size(2 * 1024 * 1024) // 2MB 栈
            .spawn(move || {
                core::worker::run_worker(worker_config, cmd_rx);
            })
            .expect("failed to spawn worker thread");

        worker_handles.push(handle);
    }

    info!(
        "已启动 {} 个 worker 线程, 媒体端口范围: {}..{}",
        num_workers,
        listen_addr.port(),
        listen_addr.port() + (num_workers as u16).saturating_sub(1)
    );

    // ── 2. 启动信令服务器 (Tokio Runtime) ──
    let signaling_state = Arc::new(signaling::SignalingState::new(worker_cmd_txs));
    let ws_addr_clone = ws_addr;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .thread_name("sfu-signaling")
        .build()
        .expect("failed to create tokio runtime");

    rt.block_on(async move {
        if let Err(e) = signaling::server::run_server(ws_addr_clone, signaling_state).await {
            error!("信令服务器错误: {}", e);
        }
    });

    // ── 3. 等待 Worker 线程结束 ──
    for handle in worker_handles {
        let _ = handle.join();
    }
}
