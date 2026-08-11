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
use std::sync::Arc;
use tracing::{info, error};

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

    // ── 1. 创建 Worker 线程 ──
    let mut worker_handles = Vec::with_capacity(num_workers);
    let mut worker_cmd_txs = Vec::with_capacity(num_workers);

    for cpu_id in 0..num_workers {
        let (cmd_tx, cmd_rx) = crossbeam_channel::unbounded::<core::WorkerCommand>();
        worker_cmd_txs.push(cmd_tx);

        let worker_config = core::WorkerConfig {
            cpu_id,
            listen_addr,
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

    info!("已启动 {} 个 worker 线程", num_workers);

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
