//! UDP 包 → str0m 实例的 O(1) 路由
//!
//! 每个入站 UDP 包通过 ICE ufrag (STUN) 或已知源地址定位到参与者，喂入
//! 对应的 `str0m::Rtc`，并把产生的事件/媒体交给转发层处理。同时处理
//! 信令命令中「创建参与者」的 SDP 协商。

use super::forward::{forward_rtp_to_subscribers, RelayTxState};
use super::output::drain_and_schedule;
use super::relay::RelayHub;
use super::timer::TimerWheel;
use super::WorkerReply;
use crate::net::udp::SendBatch;
use crate::sfu::participant::ParticipantTable;
use crate::sfu::router::Router;
use ahash::AHashMap;
use crossbeam_channel::Sender;
use std::net::SocketAddr;
use std::time::Instant;
use tracing::{debug, info, trace, warn};

/// 待转发的入站 RTP 包: (所属 mid, RtpPacket)。
///
/// RTP mode 下不再重组完整帧，每个入站 RTP 包独立转发。
pub(super) type PendingRtp = (str0m::media::Mid, str0m::rtp::RtpPacket);

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
pub(super) fn process_packet(
    source: SocketAddr,
    data: &[u8],
    now: Instant,
    local_addr: SocketAddr,
    self_global: u32,
    relay: &RelayHub,
    relay_tx: &mut RelayTxState,
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
        data.len(),
        source,
        ufrags,
        target_id
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

        let receive =
            match str0m::net::Receive::new(str0m::net::Protocol::Udp, source, local_addr, data) {
                Ok(r) => r,
                Err(_) => return,
            };

        if participant
            .rtc
            .handle_input(str0m::Input::Receive(now, receive))
            .is_err()
        {
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
                            participant.record_media_added(
                                media.mid,
                                media.kind,
                                media.direction,
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
            relay_tx,
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
// 信令命令处理
// ──────────────────────────────────────────────────────────

/// 处理创建参与者命令
#[allow(clippy::too_many_arguments)]
pub(super) fn handle_create_participant(
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
