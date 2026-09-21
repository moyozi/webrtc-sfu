//! 高性能 WebRTC SFU 服务
//!
//! 基于 str0m (Sans I/O WebRTC) + nix recvmmsg 批量收包 + epoll 事件驱动

// 项目包含尚未使用的脚手架代码 (Room, Track, TimerWheel 等)，暂不警告
#![allow(dead_code)]

mod cluster;
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
        let (ct, cr) = crossbeam_channel::unbounded::<core::WorkerCommand>();
        worker_cmd_txs.push(ct);
        worker_cmd_rxs.push(cr);

        let (rt, rr) = crossbeam_channel::unbounded::<core::RelayedRtp>();
        relay_txs.push(rt);
        relay_rxs.push(rr);

        let (rx, tx) = UnixStream::pair().expect("UnixStream::pair");
        rx.set_nonblocking(true).expect("nonblocking wake rx");
        tx.set_nonblocking(true).expect("nonblocking wake tx");
        wake_rxs.push(rx);
        wake_txs.push(Arc::new(tx));
    }

    // 跨主机 relay UDP 端点 (可选)
    let relay_endpoint = match config.relay_listen {
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
    };

    let relay_hub = Arc::new(core::RelayHub {
        relay_txs,
        wake_txs,
        cmd_txs: worker_cmd_txs.clone(),
        worker_offset: config.worker_offset,
        local_workers: num_workers as u32,
        peer_addrs: config.relay_peers.clone(),
        endpoint: relay_endpoint.clone(),
    });

    // relay 监听线程: 解码跨主机包, 按 target_worker 重新注入本地通道。
    if let Some(ep) = relay_endpoint.clone() {
        let hub = relay_hub.clone();
        std::thread::Builder::new()
            .name("sfu-relay".to_string())
            .spawn(move || {
                let mut buf = [0u8; 4096];
                loop {
                    let (n, _src) = match ep.recv_from(&mut buf) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    match net::relay::decode(&buf[..n]) {
                        Ok(net::relay::Decoded::Rtp(d)) => {
                            if let Some(li) = hub.local_index(d.target_worker) {
                                if let Some(tx) = hub.relay_txs.get(li) {
                                    if tx.send(d.pkt).is_ok() {
                                        hub.wake(li);
                                    }
                                }
                            }
                        }
                        Ok(net::relay::Decoded::Control(c)) => {
                            core::dispatch_relay_control(&hub, c);
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
        let worker_listen_addr = SocketAddr::new(
            listen_addr.ip(),
            listen_addr.port() + cpu_id as u16,
        );
        // 对外通告地址: announce_ip + 该 worker 的媒体端口。
        let worker_announce_addr = SocketAddr::new(
            announce_ip,
            listen_addr.port() + cpu_id as u16,
        );

        let worker_config = core::WorkerConfig {
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
                core::worker::run_worker(worker_config, cmd_rx);
            })
            .expect("failed to spawn worker thread");

        worker_handles.push(handle);
    }

    info!(
        "已启动 {} 个 worker 线程, 监听 {} (媒体端口范围 {}..{}), 对外通告 IP {}",
        num_workers,
        listen_addr,
        listen_addr.port(),
        listen_addr.port() + (num_workers as u16).saturating_sub(1),
        announce_ip
    );

    // ── 集群控制面: 信令 → 本地/远端 worker 的统一路由 ──
    let cluster_control = if config.control_peers.is_empty() {
        None
    } else {
        Some(Arc::new(cluster::ClusterControl::new(
            config.control_peers.clone(),
        )))
    };
    let router = Arc::new(cluster::WorkerRouter::new(
        worker_cmd_txs.clone(),
        config.worker_offset,
        num_workers as u32,
        cluster_control,
    ));
    if let Some(addr) = config.control_listen {
        if let Err(e) = cluster::start_listener(
            addr,
            worker_cmd_txs.clone(),
            config.worker_offset,
            num_workers as u32,
        ) {
            warn!("启动集群控制面监听 {} 失败: {}", addr, e);
        }
    }

    // ── 2. 启动信令服务器 (Tokio Runtime) ──
    let signaling_state = Arc::new(signaling::SignalingState::new(
        router,
        config.total_workers,
    ));

    if !config.signaling {
        info!("本进程为 worker-only (SFU_SIGNALING=off), 不启动信令服务器");
        for handle in worker_handles {
            let _ = handle.join();
        }
        return;
    }

    // QUIC 信令: 生成本地自签名证书 (生产环境应替换为受信证书)
    let quic_tls = match config.quic_listen_addr {
        Some(_) => match signaling::quic::QuicTls::self_signed("localhost") {
            Ok(tls) => {
                info!(
                    "QUIC 自签名证书 SHA-256 指纹: {}",
                    tls.fingerprint_sha256()
                );
                Some(tls)
            }
            Err(e) => {
                warn!("生成 QUIC 自签名证书失败, QUIC 信令已禁用: {}", e);
                None
            }
        },
        None => None,
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .thread_name("sfu-signaling")
        .build()
        .expect("failed to create tokio runtime");

    rt.block_on(async move {
        let ws_state = signaling_state.clone();
        let ws_task = tokio::spawn(async move {
            if let Err(e) = signaling::server::run_server(ws_addr, ws_state).await {
                error!("WebSocket 信令服务器错误: {}", e);
            }
        });

        // QUIC 优先作为移动终端的信令通道; WebSocket 作为兼容回退, 两者并存。
        let quic_task = match (config.quic_listen_addr, quic_tls) {
            (Some(addr), Some(tls)) => {
                let quic_state = signaling_state.clone();
                Some(tokio::spawn(async move {
                    if let Err(e) =
                        signaling::quic::run_server(addr, quic_state, tls).await
                    {
                        error!("QUIC 信令服务器错误: {}", e);
                    }
                }))
            }
            _ => {
                warn!("QUIC 信令未启用, 仅提供 WebSocket 信令");
                None
            }
        };

        // WHIP 推流接入 (FFmpeg/GStreamer/OBS)
        let whip_task = config.whip_listen.map(|addr| {
            let state = signaling_state.clone();
            tokio::spawn(async move {
                if let Err(e) = signaling::whip::run_server(addr, state).await {
                    error!("WHIP 信令服务器错误: {}", e);
                }
            })
        });

        let _ = ws_task.await;
        if let Some(t) = quic_task {
            let _ = t.await;
        }
        if let Some(t) = whip_task {
            let _ = t.await;
        }
    });

    // ── 3. 等待 Worker 线程结束 ──
    for handle in worker_handles {
        let _ = handle.join();
    }
}
