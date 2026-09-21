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
//! - 媒体帧以 `Arc<[u8]>` 在订阅者间共享，无拷贝
//! - 转发时的订阅者列表以切片借用，无 Vec/ String 分配
//! - 媒体暂存缓冲区跨包复用

use crate::core::timer::TimerWheel;
use crate::core::{WorkerConfig, WorkerCommand, WorkerReply};
use crate::net::udp::{UdpSocket, RecvBatch, SendBatch, MTU};
use crate::net::poll::Poll;
use crate::sfu::participant::ParticipantTable;
use crate::sfu::router::Router;
use crossbeam_channel::{Receiver, Sender};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn, error, debug, trace};

/// epoll 最长等待时间。
///
/// 定时器已改为 deadline 驱动, 但仍需周期性醒来处理信令命令
/// (命令走 crossbeam channel, 无法唤醒 epoll)。该值即信令命令的最坏延迟。
const MAX_WAIT: Duration = Duration::from_millis(10);

/// 待转发的媒体帧: (mid, 媒体时间, 编码参数, 到达时间, 帧数据)
type PendingMedia = (
    str0m::media::Mid,
    str0m::media::MediaTime,
    str0m::format::PayloadParams,
    Instant,
    Arc<[u8]>,
);

/// Worker 线程主函数
pub fn run_worker(config: WorkerConfig, cmd_rx: Receiver<WorkerCommand>) {
    let cpu_id = config.cpu_id;
    info!(cpu_id, "Worker 线程启动");

    // ── 1. 设置 CPU 亲和性 ──
    set_cpu_affinity(cpu_id);

    // ── 2. 创建 UDP socket (每个 worker 独立端口, 不再依赖 SO_REUSEPORT) ──
    let socket = match UdpSocket::new(config.listen_addr, false) {
        Ok(s) => s,
        Err(e) => {
            error!(cpu_id, "创建 UDP socket 失败: {}", e);
            return;
        }
    };

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

    // ── 4. 预分配缓冲区 ──
    let batch_size = config.batch_size;
    let send_batch_size = config.send_batch_size;
    let mut recv_batch = RecvBatch::new(batch_size, MTU);
    let mut send_batch = SendBatch::new(send_batch_size);
    // 媒体暂存缓冲区跨包复用，避免每包新建 Vec
    let mut pending_media: Vec<PendingMedia> = Vec::with_capacity(64);

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
                            &mut table,
                            &mut router,
                            &mut send_batch,
                            &mut pending_media,
                            &mut wheel,
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
                        info!("参与者 {} 已移除", participant_id);
                    }
                }

                WorkerCommand::Subscribe {
                    room_id: _,
                    subscriber_id,
                    publisher_id,
                    publisher_mid,
                    subscriber_mid,
                } => {
                    router.add_subscription(
                        &subscriber_id,
                        &publisher_id,
                        &publisher_mid,
                        subscriber_mid.as_deref(),
                    );

                    // 向发布者请求关键帧, 让新订阅者尽快拿到可解码的帧 (尤其是视频)。
                    if let Some(publisher) = table.get_mut(&publisher_id) {
                        let mid = str0m::media::Mid::from(publisher_mid.as_str());
                        if let Some(mut writer) = publisher.rtc.writer(mid) {
                            let kind = str0m::media::KeyframeRequestKind::Pli;
                            if writer.is_request_keyframe_possible(kind) {
                                let _ = writer.request_keyframe(None, kind);
                            }
                        }
                    }

                    // 关键帧请求会产生 RTCP 输出, 立即 drain 并重排定时器
                    drain_and_schedule(
                        &publisher_id,
                        &mut table,
                        &mut router,
                        &mut send_batch,
                        &mut wheel,
                    );

                    debug!("订阅: {} -> {}:{}", subscriber_id, publisher_id, publisher_mid);
                }

                WorkerCommand::Unsubscribe {
                    subscriber_id,
                    publisher_id,
                    publisher_mid,
                } => {
                    router.remove_subscription(
                        &subscriber_id,
                        &publisher_id,
                        &publisher_mid,
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
            if let Err(e) = send_batch.flush(socket.raw_fd()) {
                warn!(cpu_id, "sendmmsg 错误: {}", e);
            }
            send_batch.clear();
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
/// 4. Drain poll_output → 收集 Transmit 与 MediaData 事件
/// 5. 将 MediaData 转发给订阅者 (先收集、后转发，规避借用冲突)
#[allow(clippy::too_many_arguments)]
fn process_packet(
    source: SocketAddr,
    data: &[u8],
    now: Instant,
    local_addr: SocketAddr,
    table: &mut ParticipantTable,
    router: &mut Router,
    send_batch: &mut SendBatch,
    pending_media: &mut Vec<PendingMedia>,
    wheel: &mut TimerWheel,
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
    // 因此先把本参与者的 Transmit 与 MediaData 收集起来，稍后再转发。

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
                        str0m::Event::MediaData(md) => {
                            // 帧数据是 Arc<[u8]>，直接 move，供订阅者共享，零拷贝。
                            pending_media.push((
                                md.mid,
                                md.time,
                                md.params,
                                md.network_time,
                                md.data,
                            ));
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
    for (mid, time, params, network_time, media_data) in pending_media.drain(..) {
        forward_media_to_subscribers(
            &target_id,
            mid,
            time,
            params,
            network_time,
            media_data,
            table,
            router,
            send_batch,
            wheel,
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

/// 将媒体数据转发给所有订阅者
///
/// 这是 SFU 的核心操作。流程:
/// 1. 通过 Router 取订阅者切片 (零分配)
/// 2. 对每个订阅者: 用 Rtc.writer(mid) 写入媒体 → drain poll_output 收集 Transmit
///
/// `data` 为 `Arc<[u8]>`，多个订阅者共享同一底层帧，仅做引用计数自增，
/// 完全不拷贝帧内容。
#[allow(clippy::too_many_arguments)]
fn forward_media_to_subscribers(
    publisher_id: &str,
    publisher_mid: str0m::media::Mid,
    time: str0m::media::MediaTime,
    params: str0m::format::PayloadParams,
    network_time: Instant,
    data: Arc<[u8]>,
    table: &mut ParticipantTable,
    router: &Router,
    send_batch: &mut SendBatch,
    wheel: &mut TimerWheel,
) {
    // 直接借用路由表内的订阅者切片 (无 Vec 分配、无 ID 克隆)
    let subscribers = router.subscribers(publisher_id, publisher_mid);
    if subscribers.is_empty() {
        return;
    }

    debug!(
        "转发: {} mid={:?} -> {} 订阅者",
        publisher_id, publisher_mid, subscribers.len()
    );

    for sub in subscribers {
        let subscriber = match table.get_mut(&sub.subscriber_id) {
            Some(p) => p,
            None => continue,
        };
        let subscriber_mid = sub.subscriber_mid;

        // 使用 str0m Writer 转发媒体。
        //
        // 关键: 发布者与订阅者各自协商的 PT 可能不同, 不能直接把发布者的
        // pt 写进去 (Writer::write 会返回 RtcError::UnknownPt)。必须用
        // Writer::match_params 把入站 PayloadParams 映射为订阅者本地的 PT。
        if let Some(writer) = subscriber.rtc.writer(subscriber_mid) {
            match writer.match_params(params) {
                Some(local_pt) => {
                    // wallclock 使用入站首个包的到达时间 (str0m 推荐做法)。
                    // `data.clone()` 只增加 Arc 引用计数，无数据拷贝。
                    if writer.write(local_pt, network_time, time, data.clone()).is_ok() {
                        // Drain 订阅者的输出，收集 Transmit 包
                        loop {
                            match subscriber.rtc.poll_output() {
                                Ok(str0m::Output::Transmit(transmit)) => {
                                    send_batch.push(
                                        transmit.destination,
                                        transmit.contents.into(),
                                    );
                                }
                                Ok(str0m::Output::Timeout(t)) => {
                                    // 重排该订阅者的定时器 (仅 deadline 变化时写表)
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
                    } else {
                        warn!(
                            "订阅者 {} 写入媒体失败 (mid={:?}, pt={:?})",
                            sub.subscriber_id, subscriber_mid, local_pt
                        );
                    }
                }
                None => {
                    warn!(
                        "订阅者 {} 未协商到与发布者 {} 匹配的编码 (mid={:?}, pt={:?})",
                        sub.subscriber_id, publisher_id, subscriber_mid, params.pt()
                    );
                }
            }
        }
    }
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
    reply_tx: Sender<WorkerReply>,
) {
    // 创建新的 Rtc 实例 (local_addr 为对外通告地址, 决定 ICE host 候选)
    let mut participant = match crate::sfu::participant::Participant::new(
        participant_id.clone(),
        room_id.clone(),
        local_addr,
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
