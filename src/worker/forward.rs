//! RTP 包级转发 — SFU 核心
//!
//! 入站 RTP 包 (本地解密或跨 worker 中继) 在这里完成:
//! simulcast 选层 + 关键帧门控切换、SVC 时间层降级、逐订阅者序号重写、
//! PT 映射缓存、BWE/PLI 处理，以及向远端 worker 的中继投递。
//!
//! 转发热路径要求零拷贝: `RtpPacket.payload` 是 `Arc<[u8]>`，发给多个
//! 订阅者仅增加引用计数；订阅者列表以切片借用，不分配 `Vec`。

use super::output::drain_and_schedule;
use super::relay::{RelayControl, RelayHub, RelayedRtp};
use super::timer::TimerWheel;
use crate::net::udp::SendBatch;
use crate::sfu::layer::{is_keyframe, select_layer, vp8_temporal_id, RateWindow};
use crate::sfu::participant::{Participant, ParticipantTable, TxTrack};
use crate::sfu::router::Router;
use ahash::AHashMap;
use arrayvec::ArrayVec;
use std::sync::Arc;
use std::time::{Duration, Instant};
use str0m::format::PayloadParams;
use str0m::media::{Mid, Rid};
use str0m::rtp::{ExtensionValues, SeqNo, Ssrc};
use tracing::warn;

/// 跨主机中继重新携带 `PayloadParams` 的最小间隔。
///
/// 数据包默认不携带参数 (接收端缓存)，每隔该间隔补发一次以容忍中继 UDP 丢包。
const RELAY_PARAMS_REFRESH: Duration = Duration::from_millis(500);

/// `params_sent` 超过该数量时清理长时间无流量的条目 (防止长期运行内存增长)。
const RELAY_PARAMS_PRUNE_AT: usize = 4_096;

/// 跨 worker 中继发送侧的每线程状态。
///
/// `workers` 在每次投递中复用 (避免每包分配); `params_sent` 记录每个
/// (目标 worker, 发布者, mid) 上次携带参数的时刻。
#[derive(Default)]
pub(super) struct RelayTxState {
    // (fields pub(super) 以便 event_loop 处理注册命令时重置参数补发状态)
    pub(super) workers: Vec<usize>,
    pub(super) params_sent: AHashMap<(u32, Arc<str>, Mid), ParamsSendState>,
}

pub(super) struct ParamsSendState {
    last: Instant,
    count: u8,
}

// ──────────────────────────────────────────────────────────
// 媒体转发 — SFU 核心操作
// ──────────────────────────────────────────────────────────

/// 解析 BWE 事件中的带宽估计 (bit/s)。
pub(super) fn bwe_kind_bps(kind: &str0m::bwe::BweKind) -> u64 {
    match kind {
        str0m::bwe::BweKind::Twcc(b) => b.as_u64(),
        str0m::bwe::BweKind::Remb(_, b) => b.as_u64(),
        _ => 0,
    }
}

/// 转发源上下文: 统一「本地入站包」与「跨 worker 中继包」。
pub(super) struct SourceCtx<'a> {
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
pub(super) struct RemotePublisherStats {
    layers: AHashMap<Ssrc, RemoteLayer>,
}

pub(super) struct RemoteLayer {
    mid: Mid,
    rid: Option<Rid>,
    rate: RateWindow,
    tid_rates: [RateWindow; 4],
}

impl RemotePublisherStats {
    pub(super) fn observe(
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

    pub(super) fn layer_rates(&self, mid: Mid) -> ArrayVec<(Rid, u64), 5> {
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

    pub(super) fn temporal_rates(&self, mid: Mid) -> ArrayVec<(u8, u64), 4> {
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

/// 若没有本地订阅者仍引用该远端发布者, 清理其登记与速率统计。
///
/// 远端发布者条目在 `Subscribe` 时建立, 必须在最后一个订阅关系消失时移除,
/// 否则长期运行会持续泄漏内存，且 `request_keyframe` 会一直向已离开的发布者发命令。
pub(super) fn gc_remote_publisher(
    publisher_id: &str,
    sub_remote: &AHashMap<String, Vec<(String, String, usize)>>,
    remote_publishers: &mut AHashMap<String, usize>,
    remote_stats: &mut AHashMap<String, RemotePublisherStats>,
) {
    let referenced = sub_remote
        .values()
        .any(|v| v.iter().any(|(p, _, _)| p == publisher_id));
    if !referenced {
        remote_publishers.remove(publisher_id);
        remote_stats.remove(publisher_id);
    }
}

/// 直接向本地发布者的接收流请求关键帧。
pub(super) fn request_publisher_keyframe(
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
            RelayControl::RequestKeyframe {
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
    relay_tx: &mut RelayTxState,
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
    // 需要中继的远端 worker (去重)。复用每线程缓冲，避免每包分配，也不再有 16 个上限。
    relay_tx.workers.clear();

    for sub in subscribers {
        // 远端订阅者 (跨线程或跨主机): 只记录目标 worker, 实际由中继通道送达。
        if sub.worker_idx as u32 != self_global {
            if !relay_tx.workers.contains(&sub.worker_idx) {
                relay_tx.workers.push(sub.worker_idx);
            }
            continue;
        }

        let subscriber = match table.get_mut(&sub.subscriber_id) {
            Some(p) => p,
            None => continue,
        };
        let subscriber_mid = sub.subscriber_mid;

        let desired = if total_layer_bps > 0 {
            total_layer_bps
        } else {
            src.temporal.last().map(|(_, b)| *b).unwrap_or(0)
        };
        subscriber.maybe_set_desired_bitrate(desired, now);
        let budget = subscriber.egress_bitrate().unwrap_or(0);

        // 一次查找取回该订阅者转发轨道的全部状态 (序号/层选择/流声明/PT 缓存)。
        let track = subscriber
            .tx_tracks
            .entry(subscriber_mid)
            .or_insert_with(|| TxTrack::new(now));

        // 入站 PT → 订阅者本地 PT (按 PayloadParams 缓存，避免每包 match_params)。
        let local_pt = if track.cached_params == Some(src.params) {
            track.cached_pt
        } else {
            match subscriber.rtc.codec_config().match_params(src.params) {
                Some(local) => {
                    track.cached_params = Some(src.params);
                    track.cached_pt = local.pt();
                    local.pt()
                }
                None => {
                    warn!(
                        "订阅者 {} 未协商到与发布者 {} 匹配的编码 (mid={:?}, pt={:?})",
                        sub.subscriber_id,
                        src.publisher_id,
                        subscriber_mid,
                        src.params.pt()
                    );
                    continue;
                }
            }
        };

        // 发送流只声明一次 (mid 尚未协商时保持未声明, 下一包重试)。
        if !track.declared && subscriber.rtc.media(subscriber_mid).is_some() {
            if subscriber
                .rtc
                .direct_api()
                .stream_tx_by_mid(subscriber_mid, None)
                .is_none()
            {
                let main = Ssrc::from(subscriber.next_ssrc);
                subscriber.next_ssrc += 1;
                let rtx = Ssrc::from(subscriber.next_ssrc);
                subscriber.next_ssrc += 1;
                subscriber.rtc.direct_api().declare_stream_tx(
                    main,
                    Some(rtx),
                    subscriber_mid,
                    None,
                );
            }
            track.declared = true;
        }

        // ── 层选择 / 过滤 ──
        let mut forward = true;
        {
            let state = &mut track.layer;

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
        let seq = track.seq.inc();
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
    if !relay_tx.workers.is_empty() {
        // 无流量 (已无远端订阅) 的条目定期清理，避免长期运行内存增长。
        if relay_tx.params_sent.len() > RELAY_PARAMS_PRUNE_AT {
            relay_tx
                .params_sent
                .retain(|_, st| now.saturating_duration_since(st.last) < Duration::from_secs(5));
        }
        let publisher_id: Arc<str> = Arc::from(src.publisher_id);
        let relayed = RelayedRtp {
            publisher_id: publisher_id.clone(),
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
        for i in 0..relay_tx.workers.len() {
            let w = relay_tx.workers[i];
            // 参数按 (目标 worker, 发布者, mid) 节流: 新订阅或超过刷新间隔时携带。
            let include = {
                let st = relay_tx
                    .params_sent
                    .entry((w as u32, publisher_id.clone(), src.mid))
                    .or_insert(ParamsSendState {
                        last: now,
                        count: 0,
                    });
                let include =
                    st.count < 2 || now.saturating_duration_since(st.last) >= RELAY_PARAMS_REFRESH;
                if include {
                    st.last = now;
                    st.count = st.count.saturating_add(1);
                }
                include
            };
            relay.relay(w as u32, relayed.clone(), include);
        }
    }

    // ── PLI 回传 (层切换 + 订阅者主动) ──
    let mut local_drain: Vec<String> = Vec::new();
    for rid in switch_requests {
        if request_keyframe(
            src.publisher_id,
            src.mid,
            rid,
            table,
            remote_publishers,
            relay,
        ) {
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
pub(super) fn forward_rtp_to_subscribers(
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
    relay_tx: &mut RelayTxState,
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
        self_global,
        &src,
        now,
        table,
        router,
        send_batch,
        wheel,
        relay,
        relay_tx,
        remote_publishers,
    );
}

/// 跨 worker 中继包 → 实测远端层速率 → 送达本地订阅者。
#[allow(clippy::too_many_arguments)]
pub(super) fn forward_relayed_rtp(
    relayed: RelayedRtp,
    now: Instant,
    self_global: u32,
    table: &mut ParticipantTable,
    router: &mut Router,
    send_batch: &mut SendBatch,
    wheel: &mut TimerWheel,
    relay: &RelayHub,
    relay_tx: &mut RelayTxState,
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
        self_global,
        &src,
        now,
        table,
        router,
        send_batch,
        wheel,
        relay,
        relay_tx,
        remote_publishers,
    );
}
