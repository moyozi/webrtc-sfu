//! Worker 线程主循环
//!
//! 每个 worker 独占一个 CPU 核心、独立 UDP 端口与 epoll 实例，完全同步运行。
//! 每轮循环依次:
//! 1. 处理跨 worker 中继包 (drain 唤醒管道)
//! 2. `recvmmsg` 批量收包并路由到各自的 str0m 实例
//! 3. 驱动到期定时器
//! 4. 处理信令命令 (crossbeam channel)
//! 5. `sendmmsg` 批量发包
//! 6. `epoll_wait` 睡到最近 deadline

use super::forward::RemotePublisherStats;
use super::forward::{
    forward_relayed_rtp, gc_remote_publisher, request_publisher_keyframe, RelayTxState,
};
use super::output::{drain_and_schedule, drive_due_timers};
use super::packet::{handle_create_participant, process_packet, PendingRtp};
use super::relay::{RelayControl, RelayedRtp};
use super::timer::TimerWheel;
use super::{WorkerCommand, WorkerConfig, WorkerReply};
use crate::net::poll::Poll;
use crate::net::udp::{RecvBatch, SendBatch, UdpSocket, MTU};
use crate::sfu::participant::ParticipantTable;
use crate::sfu::router::Router;
use ahash::AHashMap;
use crossbeam_channel::Receiver;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, Instant};
use str0m::media::{MediaKind, Mid};
use tracing::{debug, error, info, warn};

/// epoll 最长等待时间。
///
/// 定时器已改为 deadline 驱动, 但仍需周期性醒来处理信令命令
/// (命令走 crossbeam channel, 无法唤醒 epoll)。该值即信令命令的最坏延迟。
const MAX_WAIT: Duration = Duration::from_millis(10);

/// Worker 线程主函数
pub fn run_worker(config: WorkerConfig, cmd_rx: Receiver<WorkerCommand>) {
    let cpu_id = config.cpu_id;
    let bwe_initial_bps = config.bwe_initial_bps;
    // 跨 worker 中继: 本 worker 编号、中继总线、接收端与唤醒管道。
    let self_idx = config.self_idx;
    let relay = config.relay.clone();
    // 全局 worker 编号 (跨主机唯一)
    let self_global = relay.worker_offset + self_idx as u32;
    let relay_rx: Receiver<RelayedRtp> = config.relay_rx.clone();
    let wake_rx: UnixStream = config.wake_rx.try_clone().expect("clone wake_rx");
    // 记录远端发布者所在 worker: publisher_id -> worker_idx (仅远端)。
    // 以及每个本地订阅者订阅的远端发布者, 用于离开时注销中继。
    let mut remote_publishers: AHashMap<String, usize> = AHashMap::new();
    // subscriber_id -> [(publisher_id, publisher_mid, publisher_worker)]
    let mut sub_remote: AHashMap<String, Vec<(String, String, usize)>> = AHashMap::new();
    // 远端发布者的层/时间层速率 (从中继包实测)。
    let mut remote_stats: AHashMap<String, RemotePublisherStats> = AHashMap::new();
    info!(cpu_id, self_idx, "Worker 线程启动");

    // ── 1. 设置 CPU 亲和性 ──
    set_cpu_affinity(cpu_id);

    // ── 2. 创建 UDP socket (每个 worker 独立端口, 不再依赖 SO_REUSEPORT) ──
    let socket = match UdpSocket::new(config.listen_addr, false, config.sock_buf_bytes) {
        Ok(s) => s,
        Err(e) => {
            error!(cpu_id, "创建 UDP socket 失败: {}", e);
            return;
        }
    };
    let (rcvbuf, sndbuf) = socket.buffer_sizes();
    info!(
        cpu_id,
        requested = config.sock_buf_bytes,
        rcvbuf,
        sndbuf,
        "UDP socket 缓冲区生效值 (受内核 rmem_max/wmem_max 限制)"
    );

    // 设置 SO_BUSY_POLL (如果配置了)
    if config.busy_poll_us > 0 {
        if let Err(e) = socket.set_busy_poll(config.busy_poll_us) {
            warn!(cpu_id, "设置 SO_BUSY_POLL 失败: {}", e);
        }
    }

    // ── 3. 创建 epoll ──
    let mut poll = match Poll::new() {
        Ok(p) => p,
        Err(e) => {
            error!(cpu_id, "创建 epoll 失败: {}", e);
            return;
        }
    };

    if let Err(e) = poll.add(socket.raw_fd()) {
        error!(cpu_id, "注册 socket 到 epoll 失败: {}", e);
        return;
    }
    // 注册中继唤醒管道读端
    if let Err(e) = poll.add(wake_rx.as_raw_fd()) {
        error!(cpu_id, "注册 wake 管道到 epoll 失败: {}", e);
        return;
    }

    // ── 4. 预分配缓冲区 ──
    let batch_size = config.batch_size;
    let send_batch_size = config.send_batch_size;
    let mut recv_batch = RecvBatch::new(batch_size, MTU);
    let mut send_batch = SendBatch::new(send_batch_size);
    // 媒体暂存缓冲区跨包复用，避免每包新建 Vec
    let mut pending_media: Vec<PendingRtp> = Vec::with_capacity(64);
    // 跨 worker 中继发送侧状态 (目标 worker 去重列表 + 参数补发节流)
    let mut relay_tx = RelayTxState::default();

    // ── 5. 初始化参与者表、路由器与定时器 ──
    let mut table = ParticipantTable::new();
    let mut router = Router::new();
    let mut wheel = TimerWheel::new();

    // 对外通告地址: 绑定在 0.0.0.0, 但 SDP/ICE 候选与 Receive destination
    // 必须使用客户端实际可达的地址 (announce_ip)。
    let announce_addr = config.announce_addr;
    // Receive::new 的 destination 使用实际收到的本地地址。绑定 0.0.0.0 时
    // 通告地址即客户端发往的目标地址, 因此用它而非 0.0.0.0。
    let local_addr = announce_addr;

    info!(
        cpu_id,
        "Worker 就绪, 监听 {} (对外通告 {})", config.listen_addr, announce_addr
    );

    // ── 6. 主事件循环 ──
    loop {
        let now = Instant::now();

        // ═══ Phase 0: 处理跨 worker 中继包 + 清空唤醒管道 ═══
        drain_wake(&wake_rx);
        while let Ok(relayed) = relay_rx.try_recv() {
            forward_relayed_rtp(
                relayed,
                now,
                self_global,
                &mut table,
                &mut router,
                &mut send_batch,
                &mut wheel,
                &relay,
                &mut relay_tx,
                &mut remote_stats,
                &remote_publishers,
            );
        }

        // ═══ Phase 1: 批量收包 (recvmmsg) ═══
        //
        // 使用 recvmmsg 批量接收 UDP 包，单次系统调用最多收 batch_size 个包。
        // 如果收满了一批，继续循环收下一批，直到 EAGAIN。
        // 这是性能的关键路径 — 每次循环零内存分配。

        loop {
            match recv_batch.recv(socket.raw_fd()) {
                Ok(0) => break, // EAGAIN — 没有更多数据
                Ok(n) => {
                    // 处理收到的 n 个包
                    for i in 0..n {
                        let (source, data) = recv_batch.get(i);
                        process_packet(
                            source,
                            data,
                            now,
                            local_addr,
                            self_global,
                            &relay,
                            &mut relay_tx,
                            &mut table,
                            &mut router,
                            &mut send_batch,
                            &mut pending_media,
                            &mut wheel,
                            &remote_publishers,
                        );
                    }
                    // 如果没收满，说明数据已读完
                    if n < batch_size {
                        break;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    warn!(cpu_id, "recvmmsg 错误: {}", e);
                    break;
                }
            }
        }

        // ═══ Phase 2: 驱动到期定时器 ═══
        //
        // 只驱动 deadline 已到的参与者, 不再每 10ms 全量扫描。
        // str0m 是 Sans I/O，需要外部驱动时间前进。

        drive_due_timers(now, &mut table, &mut router, &mut send_batch, &mut wheel);

        // ═══ Phase 3: 处理信令命令 ═══
        //
        // 非阻塞地从 channel 读取信令层发来的命令。
        // 所有命令都在 Worker 线程同步执行，无需锁。

        while let Ok(cmd) = cmd_rx.try_recv() {
            match cmd {
                WorkerCommand::CreateParticipant {
                    room_id,
                    participant_id,
                    sdp_offer,
                    reply_tx,
                } => {
                    handle_create_participant(
                        &mut table,
                        &mut router,
                        &mut send_batch,
                        &mut wheel,
                        participant_id,
                        room_id,
                        sdp_offer,
                        local_addr,
                        bwe_initial_bps,
                        reply_tx,
                    );
                }

                WorkerCommand::AddRemoteCandidate {
                    participant_id,
                    candidate,
                    sdp_mid,
                } => {
                    if let Some(p) = table.get_mut(&participant_id) {
                        p.add_remote_candidate(&candidate, sdp_mid);
                    }
                    // 添加远端候选可能立即触发 ICE 发包, 需及时 drain 并重排定时器
                    drain_and_schedule(
                        &participant_id,
                        &mut table,
                        &mut router,
                        &mut send_batch,
                        &mut wheel,
                    );
                }

                WorkerCommand::RemoveParticipant { participant_id } => {
                    wheel.cancel(&participant_id);
                    if let Some(mut p) = table.remove(&participant_id) {
                        router.remove_participant(&participant_id);
                        p.rtc.disconnect();
                    }
                    // 注销该订阅者在远端发布者 worker 上的中继登记
                    if let Some(subs) = sub_remote.remove(&participant_id) {
                        for (publisher_id, publisher_mid, worker) in subs {
                            // 释放不再被任何本地订阅者引用的远端发布者状态。
                            gc_remote_publisher(
                                &publisher_id,
                                &sub_remote,
                                &mut remote_publishers,
                                &mut remote_stats,
                            );
                            if worker as u32 != self_global {
                                relay.send_control(
                                    worker as u32,
                                    RelayControl::Unregister {
                                        publisher_id,
                                        publisher_mid: Mid::from(publisher_mid.as_str()),
                                        subscriber_id: participant_id.clone(),
                                    },
                                );
                            }
                        }
                    }
                    info!("参与者 {} 已移除", participant_id);
                }

                WorkerCommand::GetPublisherTracks {
                    publisher_id,
                    reply_tx,
                } => {
                    let reply = match table.get_mut(&publisher_id) {
                        Some(p) => {
                            let tracks = p
                                .published_tracks()
                                .into_iter()
                                .map(|(mid, kind)| (mid.to_string(), kind_to_str(kind)))
                                .collect();
                            WorkerReply::PublisherTracks { tracks }
                        }
                        None => WorkerReply::Error {
                            participant_id: publisher_id,
                            error: "publisher 不存在".to_string(),
                        },
                    };
                    let _ = reply_tx.try_send(reply);
                }

                WorkerCommand::Subscribe {
                    room_id: _,
                    subscriber_id,
                    publisher_id,
                    publisher_mid,
                    subscriber_mid,
                    publisher_worker,
                } => {
                    // 本地 Router 登记 (用于本地送达), worker_idx 永远标记为本 worker。
                    router.add_subscription(
                        &subscriber_id,
                        &publisher_id,
                        &publisher_mid,
                        subscriber_mid.as_deref(),
                        self_global as usize,
                    );

                    if publisher_worker as u32 == self_global {
                        // 本地发布者
                        request_publisher_keyframe(&publisher_id, &publisher_mid, None, &mut table);
                        drain_and_schedule(
                            &publisher_id,
                            &mut table,
                            &mut router,
                            &mut send_batch,
                            &mut wheel,
                        );
                    } else {
                        // 远端 (可能跨主机) 发布者: 在发布者 worker 登记中继订阅
                        remote_publishers.insert(publisher_id.clone(), publisher_worker);
                        sub_remote.entry(subscriber_id.clone()).or_default().push((
                            publisher_id.clone(),
                            publisher_mid.clone(),
                            publisher_worker,
                        ));
                        relay.send_control(
                            publisher_worker as u32,
                            RelayControl::Register {
                                publisher_id: publisher_id.clone(),
                                publisher_mid: Mid::from(publisher_mid.as_str()),
                                subscriber_worker: self_global as usize,
                                subscriber_id: subscriber_id.clone(),
                                subscriber_mid: subscriber_mid.as_deref().map(Mid::from),
                            },
                        );
                    }

                    debug!(
                        "订阅: {} -> {}:{} (publisher_worker={})",
                        subscriber_id, publisher_id, publisher_mid, publisher_worker
                    );
                }

                WorkerCommand::Unsubscribe {
                    subscriber_id,
                    publisher_id,
                    publisher_mid,
                    publisher_worker,
                } => {
                    router.remove_subscription(&subscriber_id, &publisher_id, &publisher_mid);
                    if publisher_worker as u32 != self_global {
                        relay.send_control(
                            publisher_worker as u32,
                            RelayControl::Unregister {
                                publisher_id: publisher_id.clone(),
                                publisher_mid: Mid::from(publisher_mid.as_str()),
                                subscriber_id: subscriber_id.clone(),
                            },
                        );
                    }
                    if let Some(v) = sub_remote.get_mut(&subscriber_id) {
                        v.retain(|(p, m, _)| !(p == &publisher_id && m == &publisher_mid));
                    }
                    // 最后一个引用消失时释放远端发布者状态，避免内存泄漏。
                    gc_remote_publisher(
                        &publisher_id,
                        &sub_remote,
                        &mut remote_publishers,
                        &mut remote_stats,
                    );
                }

                WorkerCommand::RegisterRelaySubscriber {
                    publisher_id,
                    publisher_mid,
                    subscriber_worker,
                    subscriber_id,
                    subscriber_mid,
                } => {
                    // 发布者 worker: 登记一个远端订阅者 (worker_idx = 订阅者 worker)。
                    router.add_subscription(
                        &subscriber_id,
                        &publisher_id,
                        &publisher_mid,
                        subscriber_mid.as_deref(),
                        subscriber_worker,
                    );
                    // 新订阅者接入: 让该目标的参数补发节流重置，下一个包立即带参数。
                    relay_tx.params_sent.remove(&(
                        subscriber_worker as u32,
                        Arc::from(publisher_id.as_str()),
                        Mid::from(publisher_mid.as_str()),
                    ));
                    request_publisher_keyframe(&publisher_id, &publisher_mid, None, &mut table);
                    drain_and_schedule(
                        &publisher_id,
                        &mut table,
                        &mut router,
                        &mut send_batch,
                        &mut wheel,
                    );
                }

                WorkerCommand::UnregisterRelaySubscriber {
                    publisher_id,
                    publisher_mid,
                    subscriber_id,
                } => {
                    router.remove_subscription(&subscriber_id, &publisher_id, &publisher_mid);
                }

                WorkerCommand::RequestKeyframe {
                    publisher_id,
                    publisher_mid,
                    rid,
                } => {
                    request_publisher_keyframe(&publisher_id, &publisher_mid, rid, &mut table);
                    drain_and_schedule(
                        &publisher_id,
                        &mut table,
                        &mut router,
                        &mut send_batch,
                        &mut wheel,
                    );
                }

                WorkerCommand::Shutdown => {
                    info!(cpu_id, "Worker 收到 shutdown 信号");
                    return;
                }
            }
        }

        // ═══ Phase 4: 批量发包 (sendmmsg) ═══
        //
        // 将所有收集的 Transmit 包用 sendmmsg 一次发出。

        if !send_batch.is_empty() {
            match send_batch.flush(socket.raw_fd()) {
                Ok(_) => {
                    // 未发出的包已保留在 send_batch 中, 下一轮重试 (不 clear)。
                    // 仅当积压超限时才会有丢弃, 此处给出诊断。
                    let dropped = send_batch.dropped();
                    if dropped > 0 {
                        debug!(cpu_id, "send_batch 积压丢弃累计 {} 包", dropped);
                    }
                }
                Err(e) => {
                    warn!(cpu_id, "sendmmsg 错误: {}", e);
                    // 出错时清空积压, 避免内存无限增长 (连接可能已失效)。
                    send_batch.clear();
                }
            }
        }

        // ═══ Phase 5: epoll 等待 ═══
        //
        // 睡到「最近的定时器 deadline」或 MAX_WAIT 中较早者。
        // 边缘触发模式下，只有新数据到达才会唤醒。

        let wait = wheel
            .next_deadline()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(MAX_WAIT)
            .min(MAX_WAIT);

        match poll.wait(Some(wait)) {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                error!(cpu_id, "epoll_wait 错误: {}", e);
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}
/// `MediaKind` → 协议字符串。
fn kind_to_str(kind: MediaKind) -> String {
    match kind {
        MediaKind::Audio => "audio".to_string(),
        MediaKind::Video => "video".to_string(),
    }
}

/// 清空唤醒管道 (非阻塞读到 EAGAIN)。
fn drain_wake(wake_rx: &UnixStream) {
    use std::io::Read;
    let mut buf = [0u8; 64];
    let mut s = wake_rx;
    loop {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(_) => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        }
    }
}
/// 设置当前线程的 CPU 亲和性，绑定到指定核心
fn set_cpu_affinity(cpu_id: usize) {
    use nix::sched::{sched_setaffinity, CpuSet};
    use nix::unistd::Pid;

    let mut cpuset = CpuSet::new();
    let _ = cpuset.set(cpu_id);

    if let Err(e) = sched_setaffinity(Pid::this(), &cpuset) {
        warn!(cpu_id, "设置 CPU 亲和性失败: {}", e);
    } else {
        debug!(cpu_id, "已绑定到 CPU {}", cpu_id);
    }
}
