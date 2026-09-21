//! Worker 事件循环 — SFU 的核心
//!
//! 每个 Worker 线程运行一个独立的事件循环，包含:
//! 1. recvmmsg 批量收包 → O(1) 路由到 str0m Rtc 实例 → poll_output 收集输出
//! 2. 驱动 Timer (Rtc 超时)
//! 3. 处理信令命令 (通过 channel)
//! 4. sendmmsg 批量发包
//! 5. epoll 等待下一次收包
//!
//! 热路径设计要点:
//! - 收包路由为 O(1) (ParticipantTable 的 ufrag/源地址索引)
//! - 出包直接 move `Transmit.contents`，无 memcpy
//! - RTP mode 包级转发: 不重组帧、不重新分包，`RtpPacket.payload` (`Arc<[u8]>`)
//!   在订阅者间共享，仅引用计数自增
//! - 转发时的订阅者列表以切片借用，无 Vec/ String 分配
//! - 媒体暂存缓冲区跨包复用

use crate::core::timer::TimerWheel;
use crate::core::{RelayHub, RelayedRtp, WorkerCommand, WorkerConfig, WorkerReply};
use crate::net::udp::{UdpSocket, RecvBatch, SendBatch, MTU};
use crate::net::poll::Poll;
use crate::sfu::layer::{is_keyframe, select_layer, vp8_temporal_id, RateWindow};
use crate::sfu::participant::{Participant, ParticipantTable};
use crate::sfu::router::Router;
use ahash::AHashMap;
use arrayvec::ArrayVec;
use crossbeam_channel::{Receiver, Sender};
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, Instant};
use str0m::format::PayloadParams;
use str0m::media::{Mid, Rid};
use str0m::rtp::{ExtensionValues, SeqNo, Ssrc};
use tracing::{info, warn, error, debug, trace};

/// epoll 最长等待时间。
///
/// 定时器已改为 deadline 驱动, 但仍需周期性醒来处理信令命令
/// (命令走 crossbeam channel, 无法唤醒 epoll)。该值即信令命令的最坏延迟。
const MAX_WAIT: Duration = Duration::from_millis(10);

/// 待转发的入站 RTP 包: (所属 mid, RtpPacket)。
///
/// RTP mode 下不再重组完整帧，每个入站 RTP 包独立转发。
/// `RtpPacket.payload` 是 `Arc<[u8]>`，发给多个订阅者时仅增加引用计数，零拷贝。
type PendingRtp = (str0m::media::Mid, str0m::rtp::RtpPacket);

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
        "Worker 就绪, 监听 {} (对外通告 {})",
        config.listen_addr, announce_addr
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
                Ok(0) => break,  // EAGAIN — 没有更多数据
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
                            if worker as u32 != self_global {
                                relay.send_control(
                                    worker as u32,
                                    crate::core::RelayControl::Unregister {
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
                        request_publisher_keyframe(
                            &publisher_id, &publisher_mid, None, &mut table,
                        );
                        drain_and_schedule(
                            &publisher_id, &mut table, &mut router,
                            &mut send_batch, &mut wheel,
                        );
                    } else {
                        // 远端 (可能跨主机) 发布者: 在发布者 worker 登记中继订阅
                        remote_publishers.insert(publisher_id.clone(), publisher_worker);
                        sub_remote
                            .entry(subscriber_id.clone())
                            .or_default()
                            .push((
                                publisher_id.clone(),
                                publisher_mid.clone(),
                                publisher_worker,
                            ));
                        relay.send_control(
                            publisher_worker as u32,
                            crate::core::RelayControl::Register {
                                publisher_id: publisher_id.clone(),
                                publisher_mid: Mid::from(publisher_mid.as_str()),
                                subscriber_worker: self_global as usize,
                                subscriber_id: subscriber_id.clone(),
                                subscriber_mid: subscriber_mid
                                    .as_deref()
                                    .map(Mid::from),
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
                    router.remove_subscription(
                        &subscriber_id,
                        &publisher_id,
                        &publisher_mid,
                    );
                    if publisher_worker as u32 != self_global {
                        relay.send_control(
                            publisher_worker as u32,
                            crate::core::RelayControl::Unregister {
                                publisher_id: publisher_id.clone(),
                                publisher_mid: Mid::from(publisher_mid.as_str()),
                                subscriber_id: subscriber_id.clone(),
                            },
                        );
                    }
                    if let Some(v) = sub_remote.get_mut(&subscriber_id) {
                        v.retain(|(p, m, _)| !(p == &publisher_id && m == &publisher_mid));
                    }
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
                    request_publisher_keyframe(&publisher_id, &publisher_mid, None, &mut table);
                    drain_and_schedule(
                        &publisher_id, &mut table, &mut router,
                        &mut send_batch, &mut wheel,
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
                        &publisher_id, &mut table, &mut router,
                        &mut send_batch, &mut wheel,
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

// ──────────────────────────────────────────────────────────
// 包处理 — 路由 UDP 包到正确的 Rtc 实例
// ──────────────────────────────────────────────────────────

/// 处理单个收到的 UDP 包
///
/// 流程:
/// 1. 尝试从 STUN 包中提取 ICE ufrag (快速路径)
/// 2. 通过 ParticipantTable 的 ufrag/源地址索引 O(1) 定位参与者
/// 3. 喂入 str0m handle_input
/// 4. Drain poll_output → 收集 Transmit 与 RtpPacket 事件
/// 5. 将 RtpPacket 转发给订阅者 (先收集、后转发，规避借用冲突)
#[allow(clippy::too_many_arguments)]
fn process_packet(
    source: SocketAddr,
    data: &[u8],
    now: Instant,
    local_addr: SocketAddr,
    self_global: u32,
    relay: &RelayHub,
    table: &mut ParticipantTable,
    router: &mut Router,
    send_batch: &mut SendBatch,
    pending_media: &mut Vec<PendingRtp>,
    wheel: &mut TimerWheel,
    remote_publishers: &AHashMap<String, usize>,
) {
    if data.is_empty() {
        return;
    }

    // ── 阶段 1: O(1) 查找匹配的参与者 ──
    //
    // STUN Binding Request 的 USERNAME 为 "ufragA:ufragB"。
    // 不同实现里两个 ufrag 的顺序可能不同，因此两段都会尝试匹配。
    // 非 STUN 包 (RTP/RTCP/DTLS) 通过已知源地址匹配。
    let ufrags = extract_ufrags(data);
    let target_id = match table.resolve(ufrags, source) {
        Some(id) => id,
        None => return,
    };

    trace!(
        "rx udp: {} bytes from {}, ufrags={:?}, target={:?}",
        data.len(), source, ufrags, target_id
    );

    // ── 阶段 2: 喂入 str0m 处理并收集输出 ──
    //
    // 由于 Rust 所有权限制，我们不能在遍历一个参与者的同时修改其他参与者。
    // 因此先把本参与者的 Transmit 与 RtpPacket 收集起来，稍后再转发。

    pending_media.clear();

    let mut source_changed = false;
    let mut old_source: Option<SocketAddr> = None;
    let mut reschedule: Option<Instant> = None;

    {
        let participant = match table.get_mut(&target_id) {
            Some(p) => p,
            None => return,
        };

        let receive = match str0m::net::Receive::new(
            str0m::net::Protocol::Udp,
            source,
            local_addr,
            data,
        ) {
            Ok(r) => r,
            Err(_) => return,
        };

        if participant.rtc.handle_input(
            str0m::Input::Receive(now, receive)
        ).is_err() {
            return;
        }

        // 记录/更新远端源地址 (供后续 RTP/RTCP/DTLS 包 O(1) 路由)
        if participant.known_source != Some(source) {
            old_source = participant.known_source;
            participant.known_source = Some(source);
            source_changed = true;
        }

        loop {
            match participant.rtc.poll_output() {
                Ok(str0m::Output::Transmit(transmit)) => {
                    // 直接 move str0m 生成的 Vec<u8>，避免逐包 memcpy。
                    send_batch.push(transmit.destination, transmit.contents.into());
                }

                Ok(str0m::Output::Event(event)) => {
                    match event {
                        str0m::Event::Connected => {
                            info!("参与者 {} 已连接", target_id);
                        }
                        str0m::Event::IceConnectionStateChange(state) => {
                            debug!("参与者 {} ICE: {:?}", target_id, state);
                        }
                        str0m::Event::MediaAdded(media) => {
                            info!(
                                "参与者 {} 发布媒体 mid={:?} kind={:?}",
                                target_id, media.mid, media.kind
                            );
                            router.add_publisher(
                                target_id.to_string(),
                                media.mid,
                                media.kind,
                            );
                        }
                        str0m::Event::RtpPacket(pkt) => {
                            // RTP mode: 直接收集 RTP 包。优先用 RTP header 扩展里的
                            // mid 定位所属媒体; 缺失时回退用 SSRC 反查接收流。
                            let mid = pkt.header.ext_vals.mid.or_else(|| {
                                participant
                                    .rtc
                                    .direct_api()
                                    .stream_rx(&pkt.header.ssrc)
                                    .map(|s| s.mid())
                            });
                            if let Some(mid) = mid {
                                pending_media.push((mid, pkt));
                            }
                        }
                        _ => {
                            debug!("参与者 {} 事件: {:?}", target_id, event);
                        }
                    }
                }

                Ok(str0m::Output::Timeout(t)) => {
                    // 记录下次唤醒时刻, 只在变化时更新定时器表 (避免每包一次哈希写入)
                    if participant.next_timeout != Some(t) {
                        participant.next_timeout = Some(t);
                        reschedule = Some(t);
                    }
                    break;
                }
                Err(e) => {
                    warn!("poll_output 错误: {:?}", e);
                    break;
                }
            }
        }
    }

    // 源地址变化时同步 O(1) 路由索引 (仅连接建立/ICE 迁移时发生)
    if source_changed {
        table.update_source_index(&target_id, old_source, source);
    }

    // 重排本参与者的定时器 (deadline 未变时 schedule 内部直接返回)
    if let Some(t) = reschedule {
        wheel.schedule(&target_id, t);
    }

    // ── 阶段 3: 转发媒体数据给订阅者 ──
    //
    // 现在我们已经释放了对当前参与者的借用，
    // 可以安全地访问所有参与者进行转发。
    for (mid, pkt) in pending_media.drain(..) {
        forward_rtp_to_subscribers(
            &target_id,
            mid,
            pkt,
            now,
            self_global,
            table,
            router,
            send_batch,
            wheel,
            relay,
            remote_publishers,
        );
    }
}

/// 从 STUN 包中提取 ICE ufrag 的两个片段 (零分配，借用入参)
///
/// STUN Binding Request 的 USERNAME 属性为 `<ufragA>:<ufragB>`，
/// 不同实现中两段的顺序可能不同，因此返回两段供调用方匹配。
fn extract_ufrags(data: &[u8]) -> Option<(&str, &str)> {
    // STUN 包最少 20 字节头部
    if data.len() < 20 {
        return None;
    }

    // 检查 STUN magic cookie: 0x2112A442 (字节 4-7)
    if data[4..8] != [0x21, 0x12, 0xA4, 0x42] {
        return None;
    }

    // 遍历 STUN 属性查找 USERNAME (type 0x0006)
    let mut offset = 20; // STUN 头部 20 字节
    while offset + 4 <= data.len() {
        let attr_type = u16::from_be_bytes([data[offset], data[offset + 1]]);
        let attr_len = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;
        offset += 4;

        if offset + attr_len > data.len() {
            break;
        }

        if attr_type == 0x0006 {
            // USERNAME 属性
            let username = &data[offset..offset + attr_len];
            let username_str = std::str::from_utf8(username).ok()?;
            let mut parts = username_str.split(':');
            if let (Some(a), Some(b)) = (parts.next(), parts.next()) {
                return Some((a, b));
            }
        }

        // 对齐到 4 字节边界
        offset += (attr_len + 3) & !3;
    }

    None
}

// ──────────────────────────────────────────────────────────
// 媒体转发 — SFU 核心操作
// ──────────────────────────────────────────────────────────

/// 解析 BWE 事件中的带宽估计 (bit/s)。
fn bwe_kind_bps(kind: &str0m::bwe::BweKind) -> u64 {
    match kind {
        str0m::bwe::BweKind::Twcc(b) => b.as_u64(),
        str0m::bwe::BweKind::Remb(_, b) => b.as_u64(),
        _ => 0,
    }
}

/// 转发源上下文: 统一「本地入站包」与「跨 worker 中继包」。
struct SourceCtx<'a> {
    publisher_id: &'a str,
    mid: Mid,
    params: PayloadParams,
    layers: &'a [(Rid, u64)],
    temporal: &'a [(u8, u64)],
    incoming_rid: Option<Rid>,
    incoming_tid: Option<u8>,
    packet_keyframe: bool,
    seq_no: SeqNo,
    timestamp: u32,
    marker: bool,
    ext_vals: &'a ExtensionValues,
    ssrc: Ssrc,
    arrival: Instant,
    payload: &'a Arc<[u8]>,
}

/// 远端发布者的层/时间层速率统计 (从中继包实测)。
#[derive(Default)]
struct RemotePublisherStats {
    layers: AHashMap<Ssrc, RemoteLayer>,
}

struct RemoteLayer {
    mid: Mid,
    rid: Option<Rid>,
    rate: RateWindow,
    tid_rates: [RateWindow; 4],
}

impl RemotePublisherStats {
    fn observe(
        &mut self,
        ssrc: Ssrc,
        mid: Mid,
        rid: Option<Rid>,
        tid: Option<u8>,
        bytes: usize,
        now: Instant,
    ) {
        let e = self.layers.entry(ssrc).or_insert_with(|| RemoteLayer {
            mid,
            rid,
            rate: RateWindow::new(now),
            tid_rates: std::array::from_fn(|_| RateWindow::new(now)),
        });
        if e.rid.is_none() {
            e.rid = rid;
        }
        e.rate.add(bytes, now);
        if let Some(t) = tid {
            e.tid_rates[(t & 3) as usize].add(bytes, now);
        }
    }

    fn layer_rates(&self, mid: Mid) -> ArrayVec<(Rid, u64), 5> {
        let mut v: ArrayVec<(Rid, u64), 5> = ArrayVec::new();
        for l in self.layers.values() {
            if l.mid == mid {
                if let Some(rid) = l.rid {
                    let _ = v.try_push((rid, l.rate.bps()));
                }
            }
        }
        v.sort_by_key(|(_, bps)| *bps);
        v
    }

    fn temporal_rates(&self, mid: Mid) -> ArrayVec<(u8, u64), 4> {
        let mut own = [0u64; 4];
        for l in self.layers.values().filter(|l| l.mid == mid) {
            for (t, o) in own.iter_mut().enumerate() {
                *o += l.tid_rates[t].bps();
            }
        }
        if own[1..].iter().all(|&b| b == 0) {
            return ArrayVec::new();
        }
        let mut cum = [0u64; 4];
        let mut acc = 0u64;
        for t in 0..4usize {
            acc += own[t];
            cum[t] = acc;
        }
        let mut v: ArrayVec<(u8, u64), 4> = ArrayVec::new();
        for t in 0..4usize {
            if t == 0 || own[t] > 0 {
                let _ = v.try_push((t as u8, cum[t]));
            }
        }
        v
    }
}

/// 直接向本地发布者的接收流请求关键帧。
fn request_publisher_keyframe(
    publisher_id: &str,
    publisher_mid: &str,
    rid: Option<Rid>,
    table: &mut ParticipantTable,
) {
    if let Some(p) = table.get_mut(publisher_id) {
        let mid = Mid::from(publisher_mid);
        if let Some(stream) = p.rtc.direct_api().stream_rx_by_mid(mid, rid) {
            stream.request_keyframe(str0m::media::KeyframeRequestKind::Pli);
        }
    }
}

/// 请求关键帧: 本地直接请求, 远端发送跨 worker 命令。返回是否为本地发布者。
fn request_keyframe(
    publisher_id: &str,
    mid: Mid,
    rid: Option<Rid>,
    table: &mut ParticipantTable,
    remote_publishers: &AHashMap<String, usize>,
    relay: &RelayHub,
) -> bool {
    if let Some(p) = table.get_mut(publisher_id) {
        if let Some(stream) = p.rtc.direct_api().stream_rx_by_mid(mid, rid) {
            stream.request_keyframe(str0m::media::KeyframeRequestKind::Pli);
        }
        true
    } else if let Some(&w) = remote_publishers.get(publisher_id) {
        relay.send_control(
            w as u32,
            crate::core::RelayControl::RequestKeyframe {
                publisher_id: publisher_id.to_string(),
                publisher_mid: mid,
                rid,
            },
        );
        false
    } else {
        false
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

/// 把一个个入站包送达所有订阅者: 本地写 `write_rtp`, 远端经 `RelayHub` 中继。
///
/// 同时完成: simulcast 选层 + 关键帧门控切换、SVC 时间层降级、逐订阅者序号重写、
/// BWE 更新、PLI 回传。
#[allow(clippy::too_many_arguments)]
fn deliver_to_subscribers(
    self_global: u32,
    src: &SourceCtx<'_>,
    now: Instant,
    table: &mut ParticipantTable,
    router: &mut Router,
    send_batch: &mut SendBatch,
    wheel: &mut TimerWheel,
    relay: &RelayHub,
    remote_publishers: &AHashMap<String, usize>,
) {
    let subscribers = router.subscribers(src.publisher_id, src.mid);
    if subscribers.is_empty() {
        return;
    }

    let simulcast = src.incoming_rid.is_some();
    let svc = !simulcast && src.temporal.len() > 1;
    let total_layer_bps: u64 = src.layers.iter().map(|(_, b)| *b).sum();

    let mut keyframe_requests: Vec<(String, str0m::media::KeyframeRequest)> = Vec::new();
    let mut switch_requests: Vec<Option<Rid>> = Vec::new();
    // 需要中继的远端 worker (去重)
    let mut remote_workers: ArrayVec<usize, 16> = ArrayVec::new();

    for sub in subscribers {
        // 远端订阅者 (跨线程或跨主机): 只记录目标 worker, 实际由中继通道送达。
        if sub.worker_idx as u32 != self_global {
            if !remote_workers.contains(&sub.worker_idx) {
                let _ = remote_workers.try_push(sub.worker_idx);
            }
            continue;
        }

        let subscriber = match table.get_mut(&sub.subscriber_id) {
            Some(p) => p,
            None => continue,
        };
        let subscriber_mid = sub.subscriber_mid;

        // 入站 PT → 订阅者本地 PT。
        let local_pt = match subscriber.rtc.codec_config().match_params(src.params) {
            Some(local) => local.pt(),
            None => {
                warn!(
                    "订阅者 {} 未协商到与发布者 {} 匹配的编码 (mid={:?}, pt={:?})",
                    sub.subscriber_id, src.publisher_id, subscriber_mid, src.params.pt()
                );
                continue;
            }
        };

        subscriber.ensure_tx_stream(subscriber_mid);

        let desired = if total_layer_bps > 0 {
            total_layer_bps
        } else {
            src.temporal.last().map(|(_, b)| *b).unwrap_or(0)
        };
        subscriber.maybe_set_desired_bitrate(desired, now);

        // ── 层选择 / 过滤 ──
        let mut forward = true;
        {
            let budget = subscriber.egress_bitrate().unwrap_or(0);
            let state = subscriber.layer_state_mut(subscriber_mid, now);

            if simulcast {
                if Participant::should_eval_layer(state, now) {
                    let target = if budget == 0 {
                        src.layers.last().map(|(r, _)| *r)
                    } else {
                        select_layer(src.layers, budget, 0.9)
                            .or_else(|| src.layers.first().map(|(r, _)| *r))
                    };
                    if target != state.current && target != state.pending {
                        state.pending = target;
                        if target.is_some() {
                            switch_requests.push(target);
                        }
                    }
                }
                match state.current {
                    Some(cur) if src.incoming_rid == Some(cur) => {}
                    _ => {
                        if state.pending.is_some()
                            && state.pending == src.incoming_rid
                            && src.packet_keyframe
                        {
                            state.current = state.pending.take();
                        } else if state.current.is_none()
                            && state.pending.is_none()
                            && src.packet_keyframe
                        {
                            state.current = src.incoming_rid;
                        } else {
                            forward = false;
                        }
                    }
                }
            } else if svc {
                if Participant::should_eval_layer(state, now) {
                    let effective = (budget as f64 * 0.95) as u64;
                    let target = if budget == 0 {
                        src.temporal.last().map(|(t, _)| *t)
                    } else {
                        src.temporal
                            .iter()
                            .rev()
                            .find(|(_, bps)| *bps <= effective)
                            .map(|(t, _)| *t)
                            .or_else(|| src.temporal.first().map(|(t, _)| *t))
                    };
                    if target != state.max_tid {
                        state.max_tid = target;
                    }
                }
                if let (Some(tid), Some(max)) = (src.incoming_tid, state.max_tid) {
                    if tid > max {
                        forward = false;
                    }
                }
            }
        }
        if !forward {
            continue;
        }

        // 逐订阅者重写序号。
        let seq = subscriber.next_tx_seq(subscriber_mid);
        {
            let mut api = subscriber.rtc.direct_api();
            let Some(stream) = api.stream_tx_by_mid(subscriber_mid, None) else {
                continue;
            };
            let write = str0m::rtp::RtpWrite::new(
                local_pt,
                seq,
                src.timestamp,
                src.arrival,
                src.payload.clone(),
            )
            .marker(src.marker)
            .ext_vals(src.ext_vals.clone())
            .nackable(true);
            stream.write_rtp(write);
        }

        loop {
            match subscriber.rtc.poll_output() {
                Ok(str0m::Output::Transmit(transmit)) => {
                    send_batch.push(transmit.destination, transmit.contents.into());
                }
                Ok(str0m::Output::Event(str0m::Event::KeyframeRequest(req))) => {
                    keyframe_requests.push((sub.subscriber_id.clone(), req));
                }
                Ok(str0m::Output::Event(str0m::Event::EgressBitrateEstimate(est))) => {
                    subscriber.set_egress_bitrate(bwe_kind_bps(&est));
                }
                Ok(str0m::Output::Timeout(t)) => {
                    if subscriber.next_timeout != Some(t) {
                        subscriber.next_timeout = Some(t);
                        wheel.schedule(&sub.subscriber_id, t);
                    }
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    }

    // ── 中继给远端订阅者所在 worker (每目标 worker 一份, payload Arc 共享) ──
    if !remote_workers.is_empty() {
        let relayed = RelayedRtp {
            publisher_id: Arc::from(src.publisher_id),
            publisher_mid: src.mid,
            params: src.params,
            seq_no: src.seq_no,
            timestamp: src.timestamp,
            marker: src.marker,
            ext_vals: src.ext_vals.clone(),
            rid: src.incoming_rid,
            ssrc: src.ssrc,
            arrival: src.arrival,
            payload: src.payload.clone(),
        };
        for w in remote_workers {
            relay.relay(w as u32, relayed.clone());
        }
    }

    // ── PLI 回传 (层切换 + 订阅者主动) ──
    let mut local_drain: Vec<String> = Vec::new();
    for rid in switch_requests {
        if request_keyframe(src.publisher_id, src.mid, rid, table, remote_publishers, relay) {
            local_drain.push(src.publisher_id.to_string());
        }
    }
    for (subscriber_id, req) in keyframe_requests {
        let targets = router.publisher_for_subscriber_mid(&subscriber_id, req.mid);
        for (pub_id, pub_mid) in targets {
            if request_keyframe(&pub_id, pub_mid, req.rid, table, remote_publishers, relay) {
                local_drain.push(pub_id);
            }
        }
    }
    for pub_id in local_drain {
        drain_and_schedule(&pub_id, table, router, send_batch, wheel);
    }
}

/// 本地入站 RTP 包 → 观测层速率 → 送达。
#[allow(clippy::too_many_arguments)]
fn forward_rtp_to_subscribers(
    publisher_id: &str,
    publisher_mid: Mid,
    pkt: str0m::rtp::RtpPacket,
    now: Instant,
    self_global: u32,
    table: &mut ParticipantTable,
    router: &mut Router,
    send_batch: &mut SendBatch,
    wheel: &mut TimerWheel,
    relay: &RelayHub,
    remote_publishers: &AHashMap<String, usize>,
) {
    let (params, layers, temporal) = {
        let publisher = match table.get_mut(publisher_id) {
            Some(p) => p,
            None => return,
        };
        let params = match publisher.payload_params_for(pkt.header.payload_type) {
            Some(p) => p,
            None => return,
        };
        let tid = if params.spec().codec == str0m::format::Codec::Vp8 {
            vp8_temporal_id(&pkt.payload)
        } else {
            None
        };
        publisher.observe_rx_packet(
            pkt.header.ssrc,
            publisher_mid,
            pkt.header.ext_vals.rid,
            tid,
            pkt.payload.len() + pkt.header.header_len,
            now,
        );
        let layers = publisher.layer_rates(publisher_mid);
        let temporal = publisher.temporal_rates(publisher_mid);
        (params, layers, temporal)
    };

    let codec = params.spec().codec;
    let incoming_rid = pkt.header.ext_vals.rid;
    let svc = incoming_rid.is_none() && temporal.len() > 1;
    let packet_keyframe = codec.is_video() && is_keyframe(codec, &pkt.payload);
    let incoming_tid = if svc {
        vp8_temporal_id(&pkt.payload)
    } else {
        None
    };

    let src = SourceCtx {
        publisher_id,
        mid: publisher_mid,
        params,
        layers: &layers,
        temporal: &temporal,
        incoming_rid,
        incoming_tid,
        packet_keyframe,
        seq_no: pkt.seq_no,
        timestamp: pkt.header.timestamp,
        marker: pkt.header.marker,
        ext_vals: &pkt.header.ext_vals,
        ssrc: pkt.header.ssrc,
        arrival: pkt.timestamp,
        payload: &pkt.payload,
    };

    deliver_to_subscribers(
        self_global, &src, now, table, router, send_batch, wheel, relay, remote_publishers,
    );
}

/// 跨 worker 中继包 → 实测远端层速率 → 送达本地订阅者。
#[allow(clippy::too_many_arguments)]
fn forward_relayed_rtp(
    relayed: RelayedRtp,
    now: Instant,
    self_global: u32,
    table: &mut ParticipantTable,
    router: &mut Router,
    send_batch: &mut SendBatch,
    wheel: &mut TimerWheel,
    relay: &RelayHub,
    remote_stats: &mut AHashMap<String, RemotePublisherStats>,
    remote_publishers: &AHashMap<String, usize>,
) {
    {
        if !remote_stats.contains_key(&*relayed.publisher_id) {
            remote_stats.insert(
                relayed.publisher_id.to_string(),
                RemotePublisherStats::default(),
            );
        }
        let stats = remote_stats.get_mut(&*relayed.publisher_id).unwrap();
        let tid = if relayed.params.spec().codec == str0m::format::Codec::Vp8 {
            vp8_temporal_id(&relayed.payload)
        } else {
            None
        };
        stats.observe(
            relayed.ssrc,
            relayed.publisher_mid,
            relayed.rid,
            tid,
            relayed.payload.len(),
            now,
        );
    }

    let stats = remote_stats.get(&*relayed.publisher_id).unwrap();
    let layers = stats.layer_rates(relayed.publisher_mid);
    let temporal = stats.temporal_rates(relayed.publisher_mid);

    let codec = relayed.params.spec().codec;
    let incoming_rid = relayed.rid;
    let svc = incoming_rid.is_none() && temporal.len() > 1;
    let incoming_tid = if svc {
        vp8_temporal_id(&relayed.payload)
    } else {
        None
    };
    let packet_keyframe = codec.is_video() && is_keyframe(codec, &relayed.payload);

    let src = SourceCtx {
        publisher_id: &relayed.publisher_id,
        mid: relayed.publisher_mid,
        params: relayed.params,
        layers: &layers,
        temporal: &temporal,
        incoming_rid,
        incoming_tid,
        packet_keyframe,
        seq_no: relayed.seq_no,
        timestamp: relayed.timestamp,
        marker: relayed.marker,
        ext_vals: &relayed.ext_vals,
        ssrc: relayed.ssrc,
        arrival: relayed.arrival,
        payload: &relayed.payload,
    };

    deliver_to_subscribers(
        self_global, &src, now, table, router, send_batch, wheel, relay, remote_publishers,
    );
}

// ──────────────────────────────────────────────────────────
// Timer 驱动 — 批量驱动所有 Rtc 实例的定时器
// ──────────────────────────────────────────────────────────

/// 驱动所有已到期的 Rtc 定时器并收集输出
fn drive_due_timers(
    now: Instant,
    table: &mut ParticipantTable,
    router: &mut Router,
    send_batch: &mut SendBatch,
    wheel: &mut TimerWheel,
) {
    for id in wheel.pop_due(now) {
        // 驱动时间前进。
        // 成功驱动后把 `next_timeout` 清空: 该 id 已从定时器堆中弹出,
        // 必须强制重新登记下次 deadline, 即使值恰好未变。
        let alive = match table.get_mut(&id) {
            Some(p) => {
                let ok = p.rtc.handle_input(str0m::Input::Timeout(now)).is_ok();
                if ok {
                    p.next_timeout = None;
                }
                ok
            }
            None => false,
        };
        if !alive {
            wheel.cancel(&id);
            continue;
        }
        drain_and_schedule(&id, table, router, send_batch, wheel);
    }
}

/// 把某个参与者的输出 drain 到 `Output::Timeout`，并把下次 deadline 排入 TimerWheel。
///
/// 这是 str0m「送出/收到一个输入后必须 drain 到 Timeout」约定在本 SFU 中的落地点:
///   - `Transmit`  → 收集到 send_batch
///   - `MediaAdded` → 注册发布轨道到 Router
///   - `Timeout(t)` → 记录并重排定时器, 然后结束
///
/// `next_timeout` 只在变化时才更新 TimerWheel, 避免热路径反复写定时器表。
fn drain_and_schedule(
    id: &str,
    table: &mut ParticipantTable,
    router: &mut Router,
    send_batch: &mut SendBatch,
    wheel: &mut TimerWheel,
) {
    let mut keyframe_requests: Vec<str0m::media::KeyframeRequest> = Vec::new();

    {
        let participant = match table.get_mut(id) {
            Some(p) => p,
            None => return,
        };

        // Drain 输出 (Transmit + 事件)。
        // 必须处理 MediaAdded 事件, 否则发布者轨道不会注册到 Router,
        // 后续 subscribe 会找不到发布轨道而失败。
        loop {
            match participant.rtc.poll_output() {
                Ok(str0m::Output::Transmit(transmit)) => {
                    send_batch.push(transmit.destination, transmit.contents.into());
                }
                Ok(str0m::Output::Event(event)) => match event {
                    str0m::Event::Connected => {
                        info!("参与者 {} 已连接", id);
                    }
                    str0m::Event::IceConnectionStateChange(state) => {
                        debug!("参与者 {} ICE: {:?}", id, state);
                    }
                    str0m::Event::MediaAdded(media) => {
                        info!(
                            "参与者 {} 发布媒体 mid={:?} kind={:?}",
                            id, media.mid, media.kind
                        );
                        router.add_publisher(id.to_string(), media.mid, media.kind);
                    }
                    str0m::Event::KeyframeRequest(req) => {
                        // 订阅者的 PLI/FIR: 收集后回传给对应发布者。
                        keyframe_requests.push(req);
                    }
                    str0m::Event::EgressBitrateEstimate(est) => {
                        // 订阅者链路的带宽估计, 用于层选择。
                        participant.set_egress_bitrate(bwe_kind_bps(&est));
                    }
                    other => {
                        debug!("参与者 {} 事件: {:?}", id, other);
                    }
                },
                Ok(str0m::Output::Timeout(t)) => {
                    if participant.next_timeout != Some(t) {
                        participant.next_timeout = Some(t);
                        wheel.schedule(id, t);
                    }
                    break;
                }
                Err(e) => {
                    warn!("参与者 {} poll_output 错误: {:?}", id, e);
                    break;
                }
            }
        }
    }

    // participant 的可变借用已结束，可以安全地访问其他参与者。
    //
    // 注意: 这里只处理**本地**发布者 (远端发布者的 PLI 在中继热路径
    // `deliver_to_subscribers` 中通过 RequestKeyframe 命令回传)。
    for req in keyframe_requests {
        let targets = router.publisher_for_subscriber_mid(id, req.mid);
        for (pub_id, pub_mid) in targets {
            if let Some(p) = table.get_mut(&pub_id) {
                if let Some(stream) = p.rtc.direct_api().stream_rx_by_mid(pub_mid, req.rid) {
                    stream.request_keyframe(str0m::media::KeyframeRequestKind::Pli);
                }
                drain_and_schedule(&pub_id, table, router, send_batch, wheel);
            }
        }
    }
}

// ──────────────────────────────────────────────────────────
// 信令命令处理
// ──────────────────────────────────────────────────────────

/// 处理创建参与者命令
#[allow(clippy::too_many_arguments)]
fn handle_create_participant(
    table: &mut ParticipantTable,
    router: &mut Router,
    send_batch: &mut SendBatch,
    wheel: &mut TimerWheel,
    participant_id: String,
    room_id: String,
    sdp_offer: String,
    local_addr: SocketAddr,
    bwe_initial_bps: u64,
    reply_tx: Sender<WorkerReply>,
) {
    // 创建新的 Rtc 实例 (local_addr 为对外通告地址, 决定 ICE host 候选)
    let mut participant = match crate::sfu::participant::Participant::new(
        participant_id.clone(),
        room_id.clone(),
        local_addr,
        bwe_initial_bps,
    ) {
        Ok(p) => p,
        Err(e) => {
            let _ = reply_tx.send(WorkerReply::Error {
                participant_id,
                error: format!("创建 Rtc 失败: {}", e),
            });
            return;
        }
    };

    // 处理 SDP offer
    match participant.accept_sdp_offer(&sdp_offer) {
        Ok(sdp_answer) => {
            // 收集本地 ICE 候选
            let candidates = participant.local_candidates();

            let _ = reply_tx.send(WorkerReply::SdpAnswer {
                participant_id: participant_id.clone(),
                sdp_answer,
                local_candidates: candidates,
            });

            // 添加到参与者表 (同时建立 ufrag 索引)
            table.insert(participant);

            // 立即 drain 初始输出 (STUN Binding Request / DTLS 握手) 并排入定时器。
            // 否则首次唤醒要等下一个定时器, ICE 建立会被推迟。
            drain_and_schedule(&participant_id, table, router, send_batch, wheel);
        }
        Err(e) => {
            let _ = reply_tx.send(WorkerReply::Error {
                participant_id,
                error: format!("SDP 协商失败: {}", e),
            });
        }
    }
}

// ──────────────────────────────────────────────────────────
// CPU 亲和性
// ──────────────────────────────────────────────────────────

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

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;

    /// RemotePublisherStats 应能按 RID 选出层、按 TID 计算累计时间层。
    #[test]
    fn remote_stats_layers_and_temporal() {
        let now = Instant::now();
        let mid = Mid::from("1");
        let mut s = RemotePublisherStats::default();

        // 跨过 500ms 窗口 (首包 +100ms, 到 +600ms 触发)
        for i in 1..=6u64 {
            let t = now + Duration::from_millis(100 * i);
            s.observe(Ssrc::from(10), mid, Some(Rid::from("l")), None, 1000, t);
            s.observe(Ssrc::from(20), mid, Some(Rid::from("h")), None, 2000, t);
            // SVC 时间层 (单 SSRC)
            s.observe(Ssrc::from(30), mid, None, Some(0), 1000, t);
            s.observe(Ssrc::from(30), mid, None, Some(1), 1000, t);
        }

        let layers = s.layer_rates(mid);
        assert_eq!(layers.len(), 2);
        assert_eq!(layers[0].0, Rid::from("l"));
        assert_eq!(layers[1].0, Rid::from("h"));
        assert!(layers[0].1 <= layers[1].1);

        let tr = s.temporal_rates(mid);
        assert_eq!(tr.len(), 2);
        assert_eq!(tr[0].0, 0);
        assert_eq!(tr[1].0, 1);
        assert!(tr[1].1 > tr[0].1);

        // 其它 mid 无数据
        assert!(s.layer_rates(Mid::from("9")).is_empty());
        assert!(s.temporal_rates(Mid::from("9")).is_empty());
    }

    /// RelayHub: 向目标 worker 投递中继包并唤醒。
    #[test]
    fn relay_hub_delivers_and_wakes() {
        let (rt, rr) = unbounded::<RelayedRtp>();
        let (wrx, wtx) = UnixStream::pair().unwrap();
        wrx.set_nonblocking(true).unwrap();
        wtx.set_nonblocking(true).unwrap();

        let hub = RelayHub {
            relay_txs: vec![rt],
            wake_txs: vec![Arc::new(wtx)],
            cmd_txs: vec![],
            worker_offset: 0,
            local_workers: 1,
            peer_addrs: Default::default(),
            endpoint: None,
        };

        let pkt = RelayedRtp {
            publisher_id: Arc::from("pub"),
            publisher_mid: Mid::from("0"),
            params: str0m::format::CodecConfig::new_with_defaults().params()[0],
            seq_no: SeqNo::default(),
            timestamp: 0,
            marker: false,
            ext_vals: ExtensionValues::default(),
            rid: None,
            ssrc: Ssrc::from(1),
            arrival: Instant::now(),
            payload: Arc::from(&[1u8, 2, 3][..]),
        };
        hub.relay(0, pkt);

        assert!(rr.try_recv().is_ok(), "中继包应送达目标 worker");
        // 唤醒管道应可读到字节
        use std::io::Read;
        let mut buf = [0u8; 8];
        let mut r = &wrx;
        assert!(r.read(&mut buf).unwrap_or(0) > 0, "应写入唤醒字节");
    }

    fn sample_pkt() -> RelayedRtp {
        RelayedRtp {
            publisher_id: Arc::from("pub"),
            publisher_mid: Mid::from("0"),
            params: str0m::format::CodecConfig::new_with_defaults().params()[0],
            seq_no: SeqNo::from(1),
            timestamp: 0,
            marker: false,
            ext_vals: ExtensionValues::default(),
            rid: None,
            ssrc: Ssrc::from(1),
            arrival: Instant::now(),
            payload: Arc::from(&[1u8, 2, 3][..]),
        }
    }

    /// 目标 worker 在远端进程时, `RelayHub::relay` 应经 UDP 发送, 对端可解码。
    #[test]
    fn relay_routes_remote_over_udp() {
        use crate::net::relay::{decode, Decoded, RelayEndpoint};

        let sender = Arc::new(
            RelayEndpoint::bind("127.0.0.1:0".parse().unwrap()).unwrap(),
        );
        let receiver = RelayEndpoint::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let receiver_addr = receiver.local_addr().unwrap();

        let mut peers = std::collections::HashMap::new();
        peers.insert(1u32, receiver_addr);
        let hub = RelayHub {
            relay_txs: vec![],
            wake_txs: vec![],
            cmd_txs: vec![],
            worker_offset: 0,
            local_workers: 1,
            peer_addrs: peers,
            endpoint: Some(sender),
        };

        // 目标全局编号 1 属于另一台主机
        hub.relay(1, sample_pkt());

        let mut buf = [0u8; 2048];
        let (n, _src) = receiver.recv_from(&mut buf).unwrap();
        match decode(&buf[..n]).unwrap() {
            Decoded::Rtp(d) => {
                assert_eq!(d.target_worker, 1);
                assert_eq!(&*d.pkt.payload, &[1u8, 2, 3]);
            }
            _ => panic!("expected RTP"),
        }
    }

    /// 解码后的跨主机控制消息应注入目标本地 worker 的命令通道。
    #[test]
    fn dispatch_control_injects_command() {
        use crate::net::relay::DecodedControl;
        let (ctx, crx) = unbounded::<WorkerCommand>();
        let hub = RelayHub {
            relay_txs: vec![],
            wake_txs: vec![],
            cmd_txs: vec![ctx],
            worker_offset: 0,
            local_workers: 1,
            peer_addrs: Default::default(),
            endpoint: None,
        };
        crate::core::dispatch_relay_control(
            &hub,
            DecodedControl::RequestKeyframe {
                target_worker: 0,
                publisher_id: "p".into(),
                publisher_mid: Mid::from("0"),
                rid: None,
            },
        );
        match crx.try_recv().unwrap() {
            WorkerCommand::RequestKeyframe { publisher_id, .. } => {
                assert_eq!(publisher_id, "p")
            }
            _ => panic!("expected RequestKeyframe"),
        }
    }
}
