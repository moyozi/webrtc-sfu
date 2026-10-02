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
use crate::sfu::layer::{
    is_keyframe, select_layer, select_svc, vp8_temporal_id, vp9_sid_tid, RateWindow, SvcTarget,
};
use crate::sfu::participant::{
    Participant, ParticipantTable, TxTrack, LAYER_EVAL_INTERVAL, SWITCH_KEYFRAME_TIMEOUT,
};
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

/// 从编码 + payload 解析 SVC 层索引 `(sid, tid)`。
/// 无法解析时返回 `(None, None)`，调用方据此按非 SVC 处理 (不过滤)。
fn parse_layer_ids(codec: str0m::format::Codec, payload: &[u8]) -> (Option<u8>, Option<u8>) {
    match codec {
        str0m::format::Codec::Vp8 => (Some(0), vp8_temporal_id(payload)),
        str0m::format::Codec::Vp9 => match vp9_sid_tid(payload) {
            Some((s, t)) => (Some(s), Some(t)),
            None => (None, None),
        },
        _ => (None, None),
    }
}

/// 转发源上下文: 统一「本地入站包」与「跨 worker 中继包」。
pub(super) struct SourceCtx<'a> {
    publisher_id: &'a str,
    mid: Mid,
    params: PayloadParams,
    layers: &'a [(Rid, u64)],
    svc: &'a [SvcTarget],
    incoming_rid: Option<Rid>,
    incoming_sid: Option<u8>,
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
    /// SVC: [spatial_id][temporal_id] → 码率
    svc: Box<[[RateWindow; 4]; 8]>,
}

impl RemotePublisherStats {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn observe(
        &mut self,
        ssrc: Ssrc,
        mid: Mid,
        rid: Option<Rid>,
        sid: Option<u8>,
        tid: Option<u8>,
        bytes: usize,
        now: Instant,
    ) {
        let e = self.layers.entry(ssrc).or_insert_with(|| RemoteLayer {
            mid,
            rid,
            rate: RateWindow::new(now),
            svc: Box::new(std::array::from_fn(|_| {
                std::array::from_fn(|_| RateWindow::new(now))
            })),
        });
        if e.rid.is_none() {
            e.rid = rid;
        }
        e.rate.add(bytes, now);
        if let (Some(sid), Some(tid)) = (sid, tid) {
            e.svc[(sid & 0x07) as usize][(tid & 0x03) as usize].add(bytes, now);
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

    /// 该 mid 的 SVC 候选目标层 `(sid, tid) → 累计 bps`，按质量升序。
    /// 与 `Participant::svc_candidates` 语义一致 (跨 worker 中继包共用逻辑)。
    pub(super) fn svc_candidates(&self, mid: Mid) -> ArrayVec<SvcTarget, 32> {
        let mut own = [[0u64; 4]; 8];
        let mut max_sid = 0u8;
        let mut max_tid = 0u8;
        let mut layered = false;
        for l in self.layers.values().filter(|l| l.mid == mid) {
            for (s, row) in l.svc.iter().enumerate() {
                for (t, w) in row.iter().enumerate() {
                    let b = w.bps();
                    if b == 0 {
                        continue;
                    }
                    own[s][t] += b;
                    max_sid = max_sid.max(s as u8);
                    max_tid = max_tid.max(t as u8);
                    if s > 0 || t > 0 {
                        layered = true;
                    }
                }
            }
        }
        let mut v: ArrayVec<SvcTarget, 32> = ArrayVec::new();
        if !layered {
            return v;
        }
        for s in 0..=max_sid as usize {
            for t in 0..=max_tid as usize {
                let cum: u64 = own[..=s]
                    .iter()
                    .flat_map(|row| row[..=t].iter())
                    .sum();
                if cum > 0 {
                    let _ = v.try_push(((s as u8, t as u8), cum));
                }
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
        p.request_keyframe_at(Mid::from(publisher_mid), rid, Instant::now());
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
    now: Instant,
) -> bool {
    if let Some(p) = table.get_mut(publisher_id) {
        p.request_keyframe_at(mid, rid, now);
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
    let svc = !simulcast && !src.svc.is_empty();

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

        // 目标码率 = 最高可选层的实际码率 (同一时刻只会发一层, 不能用各层之和,
        // 否则 BWE 会过度探测)。SVC 取最高候选的累计码率。
        let desired = if let Some((_, b)) = src.layers.last() {
            *b
        } else {
            src.svc.last().map(|(_, b)| *b).unwrap_or(0)
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
                // 切换超时: 目标层迟迟没有关键帧时回退, 避免永久黑屏。
                if let Some(ps) = state.pending_since {
                    if now.saturating_duration_since(ps) >= SWITCH_KEYFRAME_TIMEOUT {
                        if state.current.is_none() {
                            // 从未有过画面: 强制接受目标层, 先出画再说。
                            if let Some(t) = state.pending {
                                state.current = Some(t);
                                switch_requests.push(Some(t));
                            }
                        }
                        state.pending = None;
                        state.pending_since = None;
                        // 立即重新评估, 重试关键帧请求。
                        state.last_eval = now.checked_sub(LAYER_EVAL_INTERVAL).unwrap_or(now);
                    }
                }
                if Participant::should_eval_layer(state, now) {
                    let target = if budget == 0 {
                        src.layers.last().map(|(r, _)| *r)
                    } else {
                        select_layer(src.layers, budget, 0.9)
                            .or_else(|| src.layers.first().map(|(r, _)| *r))
                    };
                    if target != state.current && target != state.pending {
                        state.pending = target;
                        state.pending_since = target.map(|_| now);
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
                            state.pending_since = None;
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
                    let target = if budget == 0 {
                        src.svc.last().map(|(st, _)| *st)
                    } else {
                        select_svc(src.svc, budget, 0.95)
                            .or_else(|| src.svc.first().map(|(st, _)| *st))
                    };
                    if let Some((s, t)) = target {
                        state.max_sid = Some(s);
                        state.max_tid = Some(t);
                    }
                }
                // 丢弃高于目标 (sid, tid) 的包 (SVC 分层解码所需子集)。
                if let (Some(sid), Some(tid), Some(max_sid), Some(max_tid)) = (
                    src.incoming_sid,
                    src.incoming_tid,
                    state.max_sid,
                    state.max_tid,
                ) {
                    if sid > max_sid || tid > max_tid {
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
            now,
        ) {
            local_drain.push(src.publisher_id.to_string());
        }
    }
    for (subscriber_id, req) in keyframe_requests {
        let targets = router.publisher_for_subscriber_mid(&subscriber_id, req.mid);
        for (pub_id, pub_mid) in targets {
            if request_keyframe(&pub_id, pub_mid, req.rid, table, remote_publishers, relay, now) {
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
    let (params, layers, svc_layers, incoming_sid, incoming_tid) = {
        let publisher = match table.get_mut(publisher_id) {
            Some(p) => p,
            None => return,
        };
        let params = match publisher.payload_params_for(pkt.header.payload_type) {
            Some(p) => p,
            None => return,
        };
        let (sid, tid) = parse_layer_ids(params.spec().codec, &pkt.payload);
        publisher.observe_rx_packet(
            pkt.header.ssrc,
            publisher_mid,
            pkt.header.ext_vals.rid,
            sid,
            tid,
            pkt.payload.len() + pkt.header.header_len,
            now,
        );
        let layers = publisher.layer_rates(publisher_mid);
        // 仅无 RID 的 VP8/VP9 流才可能是 SVC; 其余跳过候选计算 (热路径)。
        let svc_layers = if pkt.header.ext_vals.rid.is_none() && sid.is_some() && tid.is_some() {
            publisher.svc_candidates(publisher_mid)
        } else {
            ArrayVec::new()
        };
        (params, layers, svc_layers, sid, tid)
    };

    let codec = params.spec().codec;
    let incoming_rid = pkt.header.ext_vals.rid;
    let packet_keyframe = codec.is_video() && is_keyframe(codec, &pkt.payload);

    let src = SourceCtx {
        publisher_id,
        mid: publisher_mid,
        params,
        layers: &layers,
        svc: &svc_layers,
        incoming_rid,
        incoming_sid,
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
    let (sid, tid) = parse_layer_ids(relayed.params.spec().codec, &relayed.payload);
    {
        if !remote_stats.contains_key(&*relayed.publisher_id) {
            remote_stats.insert(
                relayed.publisher_id.to_string(),
                RemotePublisherStats::default(),
            );
        }
        let stats = remote_stats.get_mut(&*relayed.publisher_id).unwrap();
        stats.observe(
            relayed.ssrc,
            relayed.publisher_mid,
            relayed.rid,
            sid,
            tid,
            relayed.payload.len(),
            now,
        );
    }

    let stats = remote_stats.get(&*relayed.publisher_id).unwrap();
    let layers = stats.layer_rates(relayed.publisher_mid);
    let svc_layers = if relayed.rid.is_none() && sid.is_some() && tid.is_some() {
        stats.svc_candidates(relayed.publisher_mid)
    } else {
        ArrayVec::new()
    };

    let codec = relayed.params.spec().codec;
    let incoming_rid = relayed.rid;
    let packet_keyframe = codec.is_video() && is_keyframe(codec, &relayed.payload);

    let src = SourceCtx {
        publisher_id: &relayed.publisher_id,
        mid: relayed.publisher_mid,
        params: relayed.params,
        layers: &layers,
        svc: &svc_layers,
        incoming_rid,
        incoming_sid: sid,
        incoming_tid: tid,
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
