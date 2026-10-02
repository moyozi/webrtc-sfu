//! 高性能 WebRTC SFU 服务
//!
//! 基于 str0m (Sans I/O WebRTC) + nix recvmmsg 批量收包 + epoll 事件驱动

mod cluster;
mod config;
mod dispatcher;
mod grpc;
mod net;
mod sfu;
mod signaling;
mod worker;

use config::SfuConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info, warn};

fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // 信令分配器角色: 设置 SFU_DISPATCHER_ADDR 时本进程只做信令地址分配,
    // 不启动 worker / 信令服务器。它通常独立部署为一组无状态实例 (前置 VIP/LB)。
    if let Some(dcfg) = dispatcher::DispatcherConfig::from_env() {
        info!(?dcfg, "启动信令分配器");
        dispatcher::run_blocking(dcfg);
        return;
    }

    // 解析配置
    let config = SfuConfig::from_args();
    info!(?config, "启动 WebRTC SFU 服务");

    // JWT 鉴权: 在启动 worker 之前校验配置 (如需要强制鉴权但未配密钥则拒绝启动,
    // 避免启动到一半才失败或裸奔)。worker-only 进程不跑信令, 无需鉴权配置。
    let authenticator = if config.signaling {
        match signaling::auth::Authenticator::from_settings(&config.jwt) {
            Ok(a) => Arc::new(a),
            Err(e) => {
                error!("JWT 鉴权配置错误: {}", e);
                std::process::exit(2);
            }
        }
    } else {
        Arc::new(signaling::auth::Authenticator::disabled())
    };
    if config.signaling {
        if authenticator.is_enabled() {
            info!("JWT 鉴权已启用 (required={})", authenticator.is_required());
        } else {
            warn!("JWT 鉴权未启用 (未配置 SFU_JWT_SECRET / SFU_JWT_PUBLIC_KEY)");
        }
    }

    let num_workers = config.num_workers;
    let listen_addr = config.listen_addr;
    let announce_ip = config.announce_ip;
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

    // 纯信令节点 (SFU_WORKERS=0): 不跑任何 worker, 媒体全部在远端媒体机。
    // 需要保证命令能路由到远端 worker, 否则启动即失败而不是静默丢弃。
    if !config.signaling && num_workers == 0 {
        error!("SFU_SIGNALING=off 且 SFU_WORKERS=0: 本进程既无信令也无 worker, 拒绝启动");
        std::process::exit(2);
    }
    if config.signaling && num_workers == 0 && config.control_peers.is_empty() {
        error!(
            "纯信令节点 (SFU_WORKERS=0) 必须配置 SFU_CONTROL_PEERS 指向远端 worker, \
             否则房间命令无处下发"
        );
        std::process::exit(2);
    }

    // ── 1. 创建 Worker 线程 ──
    //
    // 跨 worker 中继平面: 每个 worker 一对 UnixStream (唤醒管道) +
    // 一条中继 channel + 命令 channel。所有 channel 先建好, 再分别交给各 worker,
    // 使任意 worker 都能向其他 worker 发送中继包/控制命令。
    use std::os::unix::net::UnixStream;

    let mut worker_cmd_txs = Vec::with_capacity(num_workers);
    let mut worker_cmd_rxs = Vec::with_capacity(num_workers);
    let mut relay_txs = Vec::with_capacity(num_workers);
    let mut relay_rxs = Vec::with_capacity(num_workers);
    let mut wake_rxs = Vec::with_capacity(num_workers);
    let mut wake_txs = Vec::with_capacity(num_workers);

    for _ in 0..num_workers {
        // 有界队列: 控制命令容量大 (低频, 不允许丢), 中继媒体容量小 (可丢)。
        // 见 worker::CMD_CHANNEL_CAP / worker::RELAY_CHANNEL_CAP。
        let (ct, cr) = crossbeam_channel::bounded::<worker::WorkerCommand>(worker::CMD_CHANNEL_CAP);
        worker_cmd_txs.push(ct);
        worker_cmd_rxs.push(cr);

        let (rt, rr) = crossbeam_channel::bounded::<worker::RelayedRtp>(worker::RELAY_CHANNEL_CAP);
        relay_txs.push(rt);
        relay_rxs.push(rr);

        let (rx, tx) = UnixStream::pair().expect("UnixStream::pair");
        rx.set_nonblocking(true).expect("nonblocking wake rx");
        tx.set_nonblocking(true).expect("nonblocking wake tx");
        wake_rxs.push(rx);
        wake_txs.push(Arc::new(tx));
    }

    // 跨主机 relay UDP 端点 (可选)。纯信令节点没有本地 worker, 不监听 relay。
    let relay_endpoint = if num_workers == 0 {
        None
    } else {
        match config.relay_listen {
            Some(addr) => match net::relay::RelayEndpoint::bind(addr) {
                Ok(ep) => {
                    info!("跨主机 relay 监听 {}", addr);
                    Some(Arc::new(ep))
                }
                Err(e) => {
                    warn!("绑定 relay 地址 {} 失败, 跨主机中继禁用: {}", addr, e);
                    None
                }
            },
            None => None,
        }
    };

    let relay_hub = Arc::new(worker::RelayHub {
        relay_txs,
        wake_txs,
        cmd_txs: worker_cmd_txs.clone(),
        worker_offset: config.worker_offset,
        local_workers: num_workers as u32,
        peer_addrs: config.relay_peers.clone(),
        endpoint: relay_endpoint.clone(),
        dropped_relay: std::sync::atomic::AtomicU64::new(0),
        dropped_cmd: std::sync::atomic::AtomicU64::new(0),
    });

    // relay 监听线程: 解码跨主机包, 按 target_worker 重新注入本地通道。
    if let Some(ep) = relay_endpoint.clone() {
        let hub = relay_hub.clone();
        std::thread::Builder::new()
            .name("sfu-relay".to_string())
            .spawn(move || {
                let mut buf = [0u8; 4096];
                // 接收端缓存跨主机中继的 PayloadParams (数据包默认不携带)。
                let mut params_cache = net::relay::ParamsCache::default();
                loop {
                    let (n, _src) = match ep.recv_from(&mut buf) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    match net::relay::decode(&buf[..n], &mut params_cache) {
                        Ok(net::relay::Decoded::Rtp(d)) => {
                            if let Some(li) = hub.local_index(d.target_worker) {
                                if let Some(tx) = hub.relay_txs.get(li) {
                                    // 有界队列: 满了丢弃该包 (媒体可丢), 避免阻塞 UDP 接收。
                                    if tx.try_send(d.pkt).is_ok() {
                                        hub.wake(li);
                                    }
                                }
                            }
                        }
                        Ok(net::relay::Decoded::Control(c)) => {
                            worker::dispatch_relay_control(&hub, c);
                        }
                        Err(_) => {}
                    }
                }
            })
            .expect("failed to spawn relay listener");
    }

    let mut worker_handles = Vec::with_capacity(num_workers);
    for cpu_id in 0..num_workers {
        // 每个 worker 使用独立媒体端口: base_port + cpu_id。
        let worker_listen_addr =
            SocketAddr::new(listen_addr.ip(), listen_addr.port() + cpu_id as u16);
        // 对外通告地址: announce_ip + 该 worker 的媒体端口。
        let worker_announce_addr = SocketAddr::new(announce_ip, listen_addr.port() + cpu_id as u16);

        let worker_config = worker::WorkerConfig {
            cpu_id,
            listen_addr: worker_listen_addr,
            announce_addr: worker_announce_addr,
            batch_size: config.batch_size,
            send_batch_size: config.send_batch_size,
            busy_poll_us: config.busy_poll_us,
            bwe_initial_bps: config.bwe_initial_bps,
            sock_buf_bytes: config.sock_buf_bytes,
            self_idx: cpu_id,
            relay: relay_hub.clone(),
            relay_rx: relay_rxs[cpu_id].clone(),
            wake_rx: wake_rxs[cpu_id].try_clone().expect("clone wake rx"),
        };
        let cmd_rx = worker_cmd_rxs[cpu_id].clone();

        let handle = std::thread::Builder::new()
            .name(format!("sfu-worker-{}", cpu_id))
            .stack_size(2 * 1024 * 1024) // 2MB 栈
            .spawn(move || {
                worker::run_worker(worker_config, cmd_rx);
            })
            .expect("failed to spawn worker thread");

        worker_handles.push(handle);
    }

    if num_workers == 0 {
        info!("本进程为纯信令节点 (SFU_WORKERS=0), 不启动任何 worker 线程");
    } else {
        info!(
            "已启动 {} 个 worker 线程, 监听 {} (媒体端口范围 {}..{}), 对外通告 IP {}",
            num_workers,
            listen_addr,
            listen_addr.port(),
            listen_addr.port() + (num_workers as u16).saturating_sub(1),
            announce_ip
        );
    }

    // ── 控制面: 信令 → 本地/远端 worker (远端经 gRPC MediaControl) ──
    let grpc_control = if config.control_peers.is_empty() {
        None
    } else {
        Some(grpc::GrpcControl::new(config.control_peers.clone()))
    };
    let router = Arc::new(cluster::WorkerRouter::new(
        worker_cmd_txs.clone(),
        config.worker_offset,
        num_workers as u32,
        grpc_control,
    ));

    // ── 2. 启动 gRPC / 信令服务 (Tokio Runtime) ──
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .thread_name("sfu-rt")
        .build()
        .expect("failed to create tokio runtime");

    let mut servers: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // 媒体控制 gRPC 服务: 本进程有 worker 且配置了监听地址时提供。
    if !worker_cmd_txs.is_empty() {
        if let Some(addr) = config.control_listen {
            let txs = Arc::new(worker_cmd_txs.clone());
            let offset = config.worker_offset;
            let count = num_workers as u32;
            servers.push(rt.spawn(async move {
                if let Err(e) = grpc::server::serve_media_control(addr, txs, offset, count).await {
                    error!("媒体控制 gRPC 服务错误: {}", e);
                }
            }));
        }
    }

    if !config.signaling {
        info!("本进程为 worker-only (SFU_SIGNALING=off), 不启动信令服务器");
        // 等待 SIGINT/SIGTERM，再优雅停止各 worker。
        rt.block_on(async move {
            if servers.is_empty() {
                wait_for_shutdown().await;
            } else {
                tokio::select! {
                    _ = futures_util::future::join_all(servers) => {
                        error!("媒体控制服务意外结束");
                    }
                    _ = wait_for_shutdown() => info!("收到关闭信号"),
                }
            }
        });
        for tx in &worker_cmd_txs {
            let _ = tx.try_send(worker::WorkerCommand::Shutdown);
        }
        for handle in worker_handles {
            let _ = handle.join();
        }
        return;
    }

    let signaling_state = Arc::new(signaling::SignalingState::new(
        router,
        config.total_workers,
        config.room_workers.clone(),
        config.default_worker,
        authenticator,
    ));

    // QUIC 信令: 生成本地自签名证书 (生产环境应替换为受信证书)
    let quic_tls = match config.quic_listen_addr {
        Some(_) => match signaling::quic::QuicTls::self_signed("localhost") {
            Ok(tls) => {
                info!("QUIC 自签名证书 SHA-256 指纹: {}", tls.fingerprint_sha256());
                Some(tls)
            }
            Err(e) => {
                warn!("生成 QUIC 自签名证书失败, QUIC 信令已禁用: {}", e);
                None
            }
        },
        None => None,
    };

    let ws_state = signaling_state.clone();
    servers.push(rt.spawn(async move {
        if let Err(e) = signaling::server::run_server(ws_addr, ws_state).await {
            error!("WebSocket 信令服务器错误: {}", e);
        }
    }));

    // QUIC 优先作为移动终端的信令通道; WebSocket 作为兼容回退, 两者并存。
    if let (Some(addr), Some(tls)) = (config.quic_listen_addr, quic_tls) {
        let quic_state = signaling_state.clone();
        servers.push(rt.spawn(async move {
            if let Err(e) = signaling::quic::run_server(addr, quic_state, tls).await {
                error!("QUIC 信令服务器错误: {}", e);
            }
        }));
    } else {
        warn!("QUIC 信令未启用, 仅提供 WebSocket 信令");
    }

    // WHIP 推流接入 (FFmpeg/GStreamer/OBS)
    if let Some(addr) = config.whip_listen {
        let state = signaling_state.clone();
        servers.push(rt.spawn(async move {
            if let Err(e) = signaling::whip::run_server(addr, state).await {
                error!("WHIP 信令服务器错误: {}", e);
            }
        }));
    }

    // WHEP 拉流接入 (浏览器/播放器)
    if let Some(addr) = config.whep_listen {
        let state = signaling_state.clone();
        servers.push(rt.spawn(async move {
            if let Err(e) = signaling::whep::run_server(addr, state).await {
                error!("WHEP 信令服务器错误: {}", e);
            }
        }));
    }

    rt.block_on(async move {
        tokio::select! {
            _ = futures_util::future::join_all(servers) => {
                error!("服务任务意外结束");
            }
            _ = wait_for_shutdown() => {
                info!("收到关闭信号, 停止服务");
            }
        }
    });

    // ── 3. 通知 worker 退出并等待线程结束 ──
    for tx in &worker_cmd_txs {
        let _ = tx.try_send(worker::WorkerCommand::Shutdown);
    }
    for handle in worker_handles {
        let _ = handle.join();
    }
}

/// 等待 SIGINT / SIGTERM，任一到达即返回。
///
/// 用于优雅关闭: 停止信令服务器并向各 worker 发送 `WorkerCommand::Shutdown`，
/// 而不是让进程被信号硬杀、直接断开所有会话。
async fn wait_for_shutdown() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    tokio::select! {
        _ = sigterm.recv() => tracing::info!("收到 SIGTERM"),
        _ = sigint.recv() => tracing::info!("收到 SIGINT"),
    }
}
