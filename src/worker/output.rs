//! str0m 输出的 drain 与定时器调度
//!
//! str0m 是 Sans I/O: 每次 `handle_input` 后必须把 `poll_output` drain 到
//! `Output::Timeout`，并把该 deadline 登记进 [`TimerWheel`]。本模块集中
//! 处理「收集 Transmit / 事件 → 重排定时器」这段逻辑。

use super::forward::bwe_kind_bps;
use super::timer::TimerWheel;
use crate::net::udp::SendBatch;
use crate::sfu::participant::ParticipantTable;
use crate::sfu::router::Router;
use std::time::Instant;
use tracing::{debug, info, warn};

// ──────────────────────────────────────────────────────────
// Timer 驱动 — 批量驱动所有 Rtc 实例的定时器
// ──────────────────────────────────────────────────────────

/// 驱动所有已到期的 Rtc 定时器并收集输出
pub(super) fn drive_due_timers(
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
pub(super) fn drain_and_schedule(
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
