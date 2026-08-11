//! Worker 事件循环 — SFU 的核心
//!
//! 每个 Worker 线程运行一个独立的事件循环，包含:
//! 1. recvmmsg 批量收包 → 路由到 str0m Rtc 实例 → poll_output 收集输出
//! 2. 驱动 Timer (Rtc 超时)
//! 3. 处理信令命令 (通过 channel)
//! 4. sendmmsg 批量发包
//! 5. epoll 等待下一次收包

use crate::core::{WorkerConfig, WorkerCommand, WorkerReply};
use crate::net::udp::{UdpSocket, RecvBatch, SendBatch, MTU};
use crate::net::poll::Poll;
use crate::sfu::participant::Participant;
use crate::sfu::router::Router;
use ahash::AHashMap;
use crossbeam_channel::{Receiver, Sender};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tracing::{info, warn, error, debug};

/// Timer 驱动间隔 — 每 10ms 驱动一次所有 Rtc 实例的定时器
const TIMER_INTERVAL: Duration = Duration::from_millis(10);

/// Worker 线程主函数
pub fn run_worker(config: WorkerConfig, cmd_rx: Receiver<WorkerCommand>) {
    let cpu_id = config.cpu_id;
    info!(cpu_id, "Worker 线程启动");

    // ── 1. 设置 CPU 亲和性 ──
    set_cpu_affinity(cpu_id);

    // ── 2. 创建 UDP socket (SO_REUSEPORT) ──
    let socket = match UdpSocket::new(config.listen_addr, true) {
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

    // ── 5. 初始化参与者表和路由器 ──
    let mut participants: AHashMap<String, Participant> = AHashMap::new();
    let mut router = Router::new();
    let mut next_timer = Instant::now() + TIMER_INTERVAL;

    // 本地地址 (用于 Receive::new 的 destination)
    let local_addr = config.listen_addr;

    info!(cpu_id, "Worker 就绪, 监听 {}", config.listen_addr);

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
                            &mut participants,
                            &mut router,
                            &mut send_batch,
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

        // ═══ Phase 2: 驱动 Timer ═══
        //
        // 每 TIMER_INTERVAL 驱动一次所有 Rtc 实例的定时器。
        // str0m 是 Sans I/O，需要外部驱动时间前进。

        if now >= next_timer {
            drive_timers(&mut participants, &mut send_batch, now);
            next_timer = now + TIMER_INTERVAL;
        }

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
                        &mut participants,
                        &mut router,
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
                    if let Some(p) = participants.get_mut(&participant_id) {
                        p.add_remote_candidate(&candidate, sdp_mid);
                    }
                }

                WorkerCommand::RemoveParticipant { participant_id } => {
                    if let Some(mut p) = participants.remove(&participant_id) {
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
                } => {
                    router.add_subscription(
                        &subscriber_id,
                        &publisher_id,
                        &publisher_mid,
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
        // 等待下一次有数据可读，或超时后继续循环驱动 Timer。
        // 边缘触发模式下，只有新数据到达才会唤醒。

        let timeout = next_timer.checked_duration_since(now);
        match poll.wait(timeout) {
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
/// 2. 通过 ufrag 查表找到对应的 Rtc 实例
/// 3. 如果不是 STUN 包，用源地址匹配
/// 4. 喂入 str0m handle_input
/// 5. Drain poll_output → 收集 Transmit 和 MediaData 事件
/// 6. 将 MediaData 转发给订阅者 (需要访问其他参与者，分两阶段处理)
fn process_packet(
    source: SocketAddr,
    data: &[u8],
    now: Instant,
    local_addr: SocketAddr,
    participants: &mut AHashMap<String, Participant>,
    router: &mut Router,
    send_batch: &mut SendBatch,
) {
    if data.is_empty() {
        return;
    }

    // ── 阶段 1: 查找匹配的参与者 ──
    //
    // 尝试通过 ICE ufrag 快速查找 (STUN 包)
    // STUN Binding Request 的 USERNAME 属性格式: "remote_ufrag:local_ufrag"
    let target_id = extract_ufrag(data)
        .and_then(|ufrag| {
            participants.iter()
                .find_map(|(id, p)| {
                    if p.matches_ufrag(&ufrag) {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
        })
        .or_else(|| {
            // 非 STUN 包 (RTP/RTCP/DTLS)，通过已知源地址匹配
            participants.iter()
                .find_map(|(id, p)| {
                    if p.matches_source(source) {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
        });

    let participant_id = match target_id {
        Some(id) => id,
        None => return,
    };

    // ── 阶段 2: 喂入 str0m 处理 ──
    let participant = match participants.get_mut(&participant_id) {
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

    // 记录已知源地址
    participant.known_source = Some(source);

    // ── 阶段 3: Drain 输出，收集 Transmit 和 MediaData 事件 ──
    //
    // 由于 Rust 所有权限制，我们不能在遍历一个参与者的同时修改其他参与者。
    // 因此分两步:
    // 1. 先收集当前参与者的所有输出 (Transmit + MediaData 事件)
    // 2. 再处理 MediaData 事件 (转发给其他订阅者)

    let mut pending_media: Vec<(str0m::media::Mid, str0m::media::Pt, str0m::media::MediaTime, bytes::Bytes)> = Vec::new();

    loop {
        match participant.rtc.poll_output() {
            Ok(str0m::Output::Transmit(transmit)) => {
                send_batch.push(
                    transmit.destination,
                    bytes::Bytes::copy_from_slice(&transmit.contents),
                );
            }

            Ok(str0m::Output::Event(event)) => {
                match event {
                    str0m::Event::Connected => {
                        info!("参与者 {} 已连接", participant_id);
                    }
                    str0m::Event::IceConnectionStateChange(state) => {
                        debug!("参与者 {} ICE: {:?}", participant_id, state);
                    }
                    str0m::Event::MediaAdded(media) => {
                        info!(
                            "参与者 {} 发布媒体 mid={:?} kind={:?}",
                            participant_id, media.mid, media.kind
                        );
                        router.add_publisher(
                            participant_id.clone(),
                            media.mid,
                            media.kind,
                        );
                    }
                    str0m::Event::MediaData(md) => {
                        // 收集媒体数据，稍后转发
                        pending_media.push((
                            md.mid,
                            md.pt,
                            md.time,
                            bytes::Bytes::copy_from_slice(&md.data),
                        ));
                    }
                    _ => {
                        debug!("参与者 {} 事件: {:?}", participant_id, event);
                    }
                }
            }

            Ok(str0m::Output::Timeout(_)) => break,
            Err(e) => {
                warn!("poll_output 错误: {:?}", e);
                break;
            }
        }
    }

    // ── 阶段 4: 转发媒体数据给订阅者 ──
    //
    // 现在我们已经释放了对当前参与者的借用，
    // 可以安全地访问所有参与者进行转发。
    for (mid, pt, time, media_data) in pending_media {
        forward_media_to_subscribers(
            &participant_id,
            mid,
            pt,
            time,
            &media_data,
            participants,
            router,
            send_batch,
        );
    }
}

/// 从 STUN 包中提取 ICE ufrag
///
/// STUN Binding Request 的 USERNAME 属性格式: "remote_ufrag:local_ufrag"
/// 我们提取 local_ufrag 部分 (SFU 的 ufrag) 来查找对应的 Rtc 实例。
fn extract_ufrag(data: &[u8]) -> Option<String> {
    // STUN 包最少 20 字节头部
    if data.len() < 20 {
        return None;
    }

    // 检查 STUN magic cookie: 0x2112A442 (字节 4-7)
    if &data[4..8] != &[0x21, 0x12, 0xA4, 0x42] {
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
            // 格式: "remote_ufrag:local_ufrag"
            let username_str = std::str::from_utf8(username).ok()?;
            let parts: Vec<&str> = username_str.split(':').collect();
            if parts.len() == 2 {
                return Some(parts[1].to_string()); // local_ufrag
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
/// 1. 通过 Router 查找订阅者列表
/// 2. 对每个订阅者:
///    a. 从 participants 中获取订阅者的 Participant
///    b. 使用 Rtc.writer(mid) 写入媒体数据
///    c. Drain 订阅者的 poll_output，收集 Transmit 包
///
/// 注意: 媒体数据使用 Bytes (引用计数)，多个订阅者共享同一底层数据，零拷贝。
fn forward_media_to_subscribers(
    publisher_id: &str,
    publisher_mid: str0m::media::Mid,
    pt: str0m::media::Pt,
    time: str0m::media::MediaTime,
    data: &bytes::Bytes,
    participants: &mut AHashMap<String, Participant>,
    router: &Router,
    send_batch: &mut SendBatch,
) {
    // 查找订阅者列表
    let subscribers: Vec<(String, str0m::media::Mid)> = router
        .get_subscribers(publisher_id, publisher_mid)
        .iter()
        .map(|sub| (sub.subscriber_id.clone(), sub.subscriber_mid))
        .collect();

    if subscribers.is_empty() {
        return;
    }

    debug!(
        "转发: {} mid={:?} -> {} 订阅者",
        publisher_id, publisher_mid, subscribers.len()
    );

    // 遍历订阅者，逐个转发
    for (subscriber_id, subscriber_mid) in subscribers {
        let subscriber = match participants.get_mut(&subscriber_id) {
            Some(p) => p,
            None => continue,
        };

        // 使用 str0m Writer 写入媒体数据
        // str0m 内部处理: SRTP 加密 + RTP 打包 + 时间戳调整
        // Writer::write 参数顺序: (pt, wallclock, rtp_time, data)
        if let Some(writer) = subscriber.rtc.writer(subscriber_mid) {
            if writer.write(pt, Instant::now(), time, data.as_ref()).is_ok() {
                // Drain 订阅者的输出，收集 Transmit 包
                loop {
                    match subscriber.rtc.poll_output() {
                        Ok(str0m::Output::Transmit(transmit)) => {
                            send_batch.push(
                                transmit.destination,
                                bytes::Bytes::copy_from_slice(&transmit.contents),
                            );
                        }
                        Ok(str0m::Output::Timeout(_)) => break,
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
            }
        }
    }
}

// ──────────────────────────────────────────────────────────
// Timer 驱动 — 批量驱动所有 Rtc 实例的定时器
// ──────────────────────────────────────────────────────────

/// 驱动所有参与者的 Rtc 定时器并收集输出
fn drive_timers(
    participants: &mut AHashMap<String, Participant>,
    send_batch: &mut SendBatch,
    now: Instant,
) {
    for participant in participants.values_mut() {
        // 驱动时间前进
        if participant.rtc.handle_input(
            str0m::Input::Timeout(now)
        ).is_err() {
            continue;
        }

        // Drain 输出 (只处理 Transmit，不处理 MediaData)
        loop {
            match participant.rtc.poll_output() {
                Ok(str0m::Output::Transmit(transmit)) => {
                    send_batch.push(
                        transmit.destination,
                        bytes::Bytes::copy_from_slice(&transmit.contents),
                    );
                }
                Ok(str0m::Output::Timeout(_)) => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }
    }
}

// ──────────────────────────────────────────────────────────
// 信令命令处理
// ──────────────────────────────────────────────────────────

/// 处理创建参与者命令
fn handle_create_participant(
    participants: &mut AHashMap<String, Participant>,
    _router: &mut Router,
    participant_id: String,
    room_id: String,
    sdp_offer: String,
    local_addr: SocketAddr,
    reply_tx: Sender<WorkerReply>,
) {
    // 创建新的 Rtc 实例
    let mut participant = match Participant::new(
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

            // 添加到参与者表
            participants.insert(participant_id, participant);
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
