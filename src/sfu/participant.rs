//! 参与者 — 封装 str0m Rtc 实例
//!
//! 每个参与者拥有一个独立的 str0m::Rtc 实例，处理:
//! - ICE 连接建立
//! - DTLS 握手
//! - SRTP 加解密
//! - SDP 协商
//! - 媒体收发

use crate::sfu::layer::RateWindow;
use ahash::AHashMap;
use arrayvec::ArrayVec;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use str0m::bwe::Bitrate;
use str0m::format::PayloadParams;
use str0m::media::{Direction, MediaKind, Mid, Pt, Rid};
use str0m::rtp::{SeqNo, Ssrc};
use tracing::{debug, info, warn};

/// 发布者的一个入站 RTP 层 (simulcast RID 或 SVC 中的一条流)。
pub struct LayerRx {
    /// 所属媒体 mid
    pub mid: Mid,
    /// simulcast RID (非 simulcast 为 None)
    pub rid: Option<Rid>,
    /// 该层的码率估计
    pub rate: RateWindow,
    /// SVC: 按时间层 (TID 0..3) 分别估计的码率
    pub tid_rates: [RateWindow; 4],
}

/// 订阅者对某条发布轨道的层选择状态。
pub struct LayerState {
    /// 当前正在转发的 simulcast 层 (None = 尚未确定)
    pub current: Option<Rid>,
    /// 正在等待关键帧的目标 simulcast 层
    pub pending: Option<Rid>,
    /// SVC: 当前允许的最大时间层 (None = 不过滤)
    pub max_tid: Option<u8>,
    /// 上次重新评估目标的时刻
    pub last_eval: Instant,
}

/// 重新评估层选择的最小间隔
const LAYER_EVAL_INTERVAL: Duration = Duration::from_millis(500);

/// 订阅者向某个 mid 转发时需要的全部可变状态。
///
/// 这是把原先分散在 `tx_seq` / `layer_states` / `ensure_tx_stream` 里的状态
/// 合并后的单一结构，热路径取一次 `tx_tracks` 条目即可读写全部字段。
pub struct TxTrack {
    /// 下一个发送序号 (层切换/丢层时由 SFU 重新编号，保证订阅者看到的连续)。
    pub seq: SeqNo,
    /// 层选择状态 (simulcast / SVC 时间层)。
    pub layer: LayerState,
    /// 是否已向 str0m 声明发送流 (main/rtx SSRC)。
    pub declared: bool,
    /// 缓存「发布者 PayloadParams」，避免每包重新匹配编解码。
    pub cached_params: Option<PayloadParams>,
    /// 与 `cached_params` 对应的订阅者本地 PT。
    pub cached_pt: Pt,
}

impl TxTrack {
    pub fn new(now: Instant) -> Self {
        Self {
            seq: SeqNo::default(),
            layer: LayerState {
                current: None,
                pending: None,
                max_tid: None,
                last_eval: now - LAYER_EVAL_INTERVAL,
            },
            declared: false,
            cached_params: None,
            cached_pt: Pt::from(0u8),
        }
    }
}

/// 更新 BWE desired bitrate 的最小间隔
const DESIRED_BITRATE_INTERVAL: Duration = Duration::from_secs(2);

/// 参与者 — 封装 str0m Rtc 实例和相关状态
pub struct Participant {
    /// 参与者唯一 ID
    pub id: String,

    /// str0m Rtc 实例 — 处理所有 WebRTC 协议
    pub rtc: str0m::Rtc,

    /// 已知的远端源地址 (ICE 连接建立后记录，用于后续 RTP/RTCP 快速路由)
    pub known_source: Option<SocketAddr>,

    /// 本地 ICE ufrag (用于快速包路由)
    pub local_ufrag: String,

    /// 上次 `poll_output()` 返回的 `Output::Timeout` 时刻。
    /// 用于避免收包热路径反复对定时器表做哈希查找: 只有该值变化时才更新 TimerWheel。
    pub next_timeout: Option<Instant>,

    /// 下一个可分配的 SSRC (RTP mode 下每个转发轨道需要一对 main/rtx SSRC)。
    /// 只在单个 Rtc 内需要唯一，因此从固定基址递增即可。
    pub next_ssrc: u32,

    /// 作为发布者: 入站层速率 (SSRC → 层信息)。
    rx_layers: AHashMap<Ssrc, LayerRx>,

    /// 作为订阅者: 每条转发轨道的发送状态 (subscriber_mid → state)。
    ///
    /// 合并了序号计数、层选择、发送流声明与 PT 映射缓存，使媒体转发热路径
    /// 对每个订阅者每包只做一次 HashMap 查找。
    pub tx_tracks: AHashMap<Mid, TxTrack>,

    /// 作为发布者: 已协商的入站媒体 (mid → kind)。仅记录 SFU 接收方向。
    /// 供 WHEP 等“按发布者订阅”查询发布轨道。
    published: AHashMap<Mid, MediaKind>,

    /// 带宽估计 (bit/s)。订阅者侧来自 `Event::EgressBitrateEstimate`。
    egress_bitrate: Option<u64>,

    /// 上次更新 BWE desired bitrate 的时刻 (节流)。
    last_desired_update: Instant,

    /// 本地绑定地址
    local_addr: SocketAddr,
}

impl Participant {
    /// 创建新参与者
    ///
    /// 创建 str0m Rtc 实例，配置 ICE Lite 模式 (SFU 不需要完整的 ICE 实现)
    pub fn new(
        id: String,
        room_id: String,
        local_addr: SocketAddr,
        bwe_initial_bps: u64,
    ) -> Result<Self, String> {
        // 创建 Rtc 实例
        // SFU 使用 ICE Lite 模式 — 只接受 host 类型的候选，不做 server reflexive

        // 预先生成 ICE 凭证 (str0m 的 local_ice_credentials 在 RtcConfig 上，不在 Rtc 上)
        let ice_creds = str0m::IceCreds::new();
        let local_ufrag = ice_creds.ufrag.clone();

        // RTP mode: 绕过 str0m 内部的 depacketize/packetize, 收发改用 RtpPacket /
        // StreamTx::write_rtp。SFU 只做包级转发, 不重组完整帧。
        //
        // BWE: 订阅者侧启用带宽估计 (TWCC/REMB), 用于 Simulcast/SVC 层选择。
        let now = Instant::now();
        let rtc = str0m::Rtc::builder()
            .set_ice_lite(true)
            .set_rtp_mode(true)
            .enable_bwe(Some(Bitrate::bps(bwe_initial_bps.max(50_000))))
            .set_local_ice_credentials(ice_creds)
            .build(now);

        info!(room = %room_id, "参与者 {} 创建, ufrag={}", id, local_ufrag);

        Ok(Self {
            id,
            rtc,
            known_source: None,
            local_ufrag,
            next_timeout: None,
            next_ssrc: 1000,
            rx_layers: AHashMap::new(),
            tx_tracks: AHashMap::new(),
            published: AHashMap::new(),
            egress_bitrate: None,
            // 置为「久远以前」，使首个包即可应用 desired bitrate。
            last_desired_update: now - DESIRED_BITRATE_INTERVAL,
            local_addr,
        })
    }

    /// 记录一个入站 RTP 包的层速率。
    pub fn observe_rx_packet(
        &mut self,
        ssrc: Ssrc,
        mid: Mid,
        rid: Option<Rid>,
        tid: Option<u8>,
        bytes: usize,
        now: Instant,
    ) {
        let entry = self.rx_layers.entry(ssrc).or_insert_with(|| LayerRx {
            mid,
            rid,
            rate: RateWindow::new(now),
            tid_rates: std::array::from_fn(|_| RateWindow::new(now)),
        });
        // rid 可能首包缺失 (无 RID 扩展) 后续才出现，补齐。
        if entry.rid.is_none() {
            entry.rid = rid;
        }
        entry.rate.add(bytes, now);
        if let Some(tid) = tid {
            entry.tid_rates[(tid & 0x03) as usize].add(bytes, now);
        }
    }

    /// 该 mid 的 SVC 时间层累计码率 (tid, 累计 bps)，按 tid 升序。
    ///
    /// `cum[t]` = 所有 `tid <= t` 的时间层码率之和，即「目标时间层为 t 时」
    /// 的发送码率。若只有一个时间层 (tid 0) 则返回空。
    /// 使用栈上 `ArrayVec`，热路径零堆分配。
    pub fn temporal_rates(&self, mid: Mid) -> ArrayVec<(u8, u64), 4> {
        // 先统计每个时间层自身的码率，再求前缀和得到累计码率。
        let mut own = [0u64; 4];
        for l in self.rx_layers.values().filter(|l| l.mid == mid) {
            for (t, o) in own.iter_mut().enumerate() {
                *o += l.tid_rates[t].bps();
            }
        }
        // 只有存在 >0 的高时间层才认为是 SVC 时间分层。
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

    /// 该 mid 上所有 simulcast 层及其码率，按码率升序。
    ///
    /// 使用栈上 `ArrayVec` (最大 5 层)，热路径零堆分配。
    pub fn layer_rates(&self, mid: Mid) -> ArrayVec<(Rid, u64), 5> {
        let mut v: ArrayVec<(Rid, u64), 5> = ArrayVec::new();
        for l in self.rx_layers.values() {
            if l.mid != mid {
                continue;
            }
            if let Some(rid) = l.rid {
                if v.try_push((rid, l.rate.bps())).is_err() {
                    break;
                }
            }
        }
        v.sort_by_key(|(_, bps)| *bps);
        v
    }

    /// 订阅者链路的带宽估计 (bit/s)。
    pub fn egress_bitrate(&self) -> Option<u64> {
        self.egress_bitrate
    }

    /// 更新带宽估计 (来自 `Event::EgressBitrateEstimate`)。
    pub fn set_egress_bitrate(&mut self, bps: u64) {
        self.egress_bitrate = Some(bps);
    }

    /// 设置 BWE 的 desired bitrate (节流)。
    ///
    /// 告诉 BWE 系统「希望能发送到多少」，它会通过探测 (padding) 逼近该值。
    /// 传入所有可能层码率之和，使高带宽订阅者能升到最高层。
    pub fn maybe_set_desired_bitrate(&mut self, bps: u64, now: Instant) {
        if bps == 0 {
            return;
        }
        if now.saturating_duration_since(self.last_desired_update) < DESIRED_BITRATE_INTERVAL {
            return;
        }
        self.last_desired_update = now;
        self.rtc
            .bwe()
            .set_desired_bitrate(Bitrate::bps(bps.max(300_000)));
    }

    /// 是否到达重新评估层选择的时刻 (同时刷新 `last_eval`)。
    pub fn should_eval_layer(state: &mut LayerState, now: Instant) -> bool {
        if now.saturating_duration_since(state.last_eval) >= LAYER_EVAL_INTERVAL {
            state.last_eval = now;
            true
        } else {
            false
        }
    }

    /// 记录 `Event::MediaAdded`: 仅保留 SFU 接收方向 (发布者轨道)。
    ///
    /// 发布者 (sendonly) 在 SFU 侧为 RecvOnly; 订阅者 (recvonly) 为 SendOnly。
    /// 重协商移除/改变方向时同步删除。
    pub fn record_media_added(&mut self, mid: Mid, kind: MediaKind, direction: Direction) {
        if matches!(direction, Direction::RecvOnly | Direction::SendRecv) {
            self.published.insert(mid, kind);
        } else {
            self.published.remove(&mid);
        }
    }

    /// 已发布的 (mid, kind) 列表 (仅发布者轨道)。
    pub fn published_tracks(&self) -> Vec<(Mid, MediaKind)> {
        self.published.iter().map(|(m, k)| (*m, *k)).collect()
    }

    /// 查找入站 PT 对应的 PayloadParams (含 RTX)。
    ///
    /// RTP mode 下 `Event::RtpPacket` 不携带 params，需要按 PT 回查本 Rtc
    /// 已协商的 codec 配置。
    pub fn payload_params_for(&self, pt: Pt) -> Option<PayloadParams> {
        self.rtc
            .codec_config()
            .find(|p| p.pt() == pt || p.resend() == Some(pt))
            .copied()
    }

    /// 处理远端 SDP offer，返回 SDP answer
    ///
    /// 这是 WebRTC 连接建立的核心步骤:
    /// 1. 解析 SDP offer
    /// 2. 配置媒体方向 (recvonly for publisher, sendonly for subscriber)
    /// 3. 生成 SDP answer
    /// 4. 添加本地 ICE 候选
    pub fn accept_sdp_offer(&mut self, sdp_offer: &str) -> Result<String, String> {
        // 解析 SDP offer (str0m 的 SdpOffer 在 change 模块，使用 from_sdp_string 解析原始 SDP 文本)
        let offer = str0m::change::SdpOffer::from_sdp_string(sdp_offer)
            .map_err(|e| format!("解析 SDP offer 失败: {}", e))?;

        // 使用 str0m SDP API 接受 offer
        // accept_offer 内部会处理 ICE 凭证匹配、编解码协商等，无需手动提取远端 ufrag
        let sdp_api = self.rtc.sdp_api();
        let answer = sdp_api
            .accept_offer(offer)
            .map_err(|e| format!("接受 SDP offer 失败: {}", e))?;

        // 添加本地 ICE 候选 (host 类型)
        // Candidate::host 参数顺序: (addr, proto)
        let candidate = str0m::Candidate::host(
            local_addr_to_candidate_addr(self.local_addr()),
            str0m::net::Protocol::Udp,
        )
        .map_err(|e| format!("创建 ICE 候选失败: {}", e))?;

        self.rtc.add_local_candidate(candidate);

        // 转换为字符串
        let answer_str = answer.to_string();

        info!(
            "参与者 {} SDP 协商完成, answer 长度={}",
            self.id,
            answer_str.len()
        );

        Ok(answer_str)
    }

    /// 添加远端 ICE 候选 (Trickle ICE)
    pub fn add_remote_candidate(&mut self, candidate_str: &str, sdp_mid: Option<String>) {
        // sdp_mid 目前未使用 — str0m 的 add_remote_candidate 内部通过 SDP 协商确定 mid
        let _ = sdp_mid;
        // 解析候选字符串
        // 格式: candidate:<foundation> <component> <protocol> <priority> <addr> <port> typ <type> [raddr <raddr> rport <rport>]
        match str0m::Candidate::from_sdp_string(candidate_str) {
            Ok(candidate) => {
                self.rtc.add_remote_candidate(candidate);
                debug!("参与者 {} 添加远端候选: {}", self.id, candidate_str);
            }
            Err(e) => {
                warn!(
                    "参与者 {} 解析 ICE 候选失败: {} - {}",
                    self.id, candidate_str, e
                );
            }
        }
    }

    /// 获取本地 ICE 候选列表 (用于发送给客户端)
    pub fn local_candidates(&self) -> Vec<String> {
        // 注意: ICE candidate 的 connection-address 与 port 必须用空格分隔的
        // 两个字段 (如 `... 192.168.31.112 5175 typ host`),
        // 不能用 SocketAddr 的 `ip:port` 形式, 否则浏览器 addIceCandidate
        // 会报 "Error processing ICE candidate" 而拒绝, 导致 ICE 无法建立。
        let addr = self.local_addr();
        vec![format!(
            "candidate:1 1 udp 2122252543 {} {} typ host",
            addr.ip(),
            addr.port()
        )]
    }

    /// 本地地址
    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// 将 SocketAddr 转换为 Candidate 地址格式
fn local_addr_to_candidate_addr(addr: SocketAddr) -> SocketAddr {
    addr
}

// ──────────────────────────────────────────────────────────
// ParticipantTable — O(1) 包路由索引
// ──────────────────────────────────────────────────────────

/// 参与者表 + 包路由索引
///
/// 热路径 (`process_packet`) 对每个 UDP 包都需要定位参与者:
///   - STUN 包: 用 ICE ufrag 匹配
///   - RTP/RTCP/DTLS 包: 用远端源地址匹配
///
/// 早期实现对参与者做 O(N) 线性扫描，每包一次，参与者增多时是灾难性的。
/// 这里用两张索引把路由降为 O(1):
///   - `ufrag_index`: local_ufrag → participant_id
///   - `source_index`: 远端源地址 → participant_id
///
/// 索引值为 `Arc<str>`: 查找命中后克隆一次 Arc 只是原子自增，不产生堆分配，
/// 同时避免持有对索引的借用而无法再可变借用参与者表。
pub struct ParticipantTable {
    participants: AHashMap<String, Participant>,
    /// local ICE ufrag → participant_id
    ufrag_index: AHashMap<String, Arc<str>>,
    /// 远端源地址 → participant_id
    source_index: AHashMap<SocketAddr, Arc<str>>,
}

impl ParticipantTable {
    pub fn new() -> Self {
        Self {
            participants: AHashMap::new(),
            ufrag_index: AHashMap::new(),
            source_index: AHashMap::new(),
        }
    }

    /// 插入参与者并建立 ufrag 索引
    pub fn insert(&mut self, p: Participant) {
        let id: Arc<str> = Arc::from(p.id.as_str());
        self.ufrag_index.insert(p.local_ufrag.clone(), id.clone());
        if let Some(source) = p.known_source {
            self.source_index.insert(source, id.clone());
        }
        self.participants.insert(p.id.clone(), p);
    }

    /// 移除参与者并清理索引
    pub fn remove(&mut self, id: &str) -> Option<Participant> {
        let p = self.participants.remove(id)?;
        self.ufrag_index.remove(&p.local_ufrag);
        // 源地址索引可能因 ICE 迁移存在多条历史映射，统一清理，
        // 保证任何指向已移除参与者的包都会被丢弃而不是路由到错误的 Rtc。
        // 断连不是热路径，O(N) 可接受。
        self.source_index.retain(|_, v| &**v != id);
        Some(p)
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut Participant> {
        self.participants.get_mut(id)
    }

    /// O(1) 解析包所属参与者
    ///
    /// 先尝试 STUN ufrag (两段顺序都试)，再退回源地址匹配。
    /// STUN USERNAME 为 `ufragA:ufragB`，两段顺序因实现而异，
    /// 只要任一段等于本地 ufrag 即可命中。
    pub fn resolve(&self, ufrags: Option<(&str, &str)>, source: SocketAddr) -> Option<Arc<str>> {
        if let Some((a, b)) = ufrags {
            if let Some(id) = self.ufrag_index.get(a) {
                return Some(id.clone());
            }
            if let Some(id) = self.ufrag_index.get(b) {
                return Some(id.clone());
            }
        }
        self.source_index.get(&source).cloned()
    }

    /// 参与者远端源地址变化时同步索引。
    ///
    /// 调用方在持有参与者可变借用时已更新 `known_source` 字段，这里只负责
    /// 同步索引，避免热路径为了更新索引再做一次参与者查找。
    pub fn update_source_index(&mut self, id: &str, old: Option<SocketAddr>, new: SocketAddr) {
        if let Some(old) = old {
            if self.source_index.get(&old).map(|v| &**v) == Some(id) {
                self.source_index.remove(&old);
            }
        }
        self.source_index.insert(new, Arc::from(id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_by_ufrag_and_source() {
        let mut table = ParticipantTable::new();
        let p = Participant::new(
            "p1".into(),
            "room".into(),
            "127.0.0.1:5000".parse().unwrap(),
            1_000_000,
        )
        .unwrap();
        let ufrag = p.local_ufrag.clone();
        table.insert(p);

        // 两段 ufrag 任意一段匹配
        assert_eq!(
            table
                .resolve(Some((&ufrag, "remote")), "1.2.3.4:9".parse().unwrap())
                .as_deref(),
            Some("p1")
        );
        assert_eq!(
            table
                .resolve(Some(("remote", &ufrag)), "1.2.3.4:9".parse().unwrap())
                .as_deref(),
            Some("p1")
        );

        // 源地址尚未记录时无法命中
        let src: SocketAddr = "1.2.3.4:9".parse().unwrap();
        assert!(table.resolve(None, src).is_none());

        // 记录后可通过源地址命中，移除后索引清理
        table.update_source_index("p1", None, src);
        assert_eq!(table.resolve(None, src).as_deref(), Some("p1"));

        assert!(table.remove("p1").is_some());
        assert!(table.resolve(Some((&ufrag, "remote")), src).is_none());
        assert!(table.resolve(None, src).is_none());
    }

    /// 入站 simulcast 层速率按 RID 统计并按码率升序返回。
    #[test]
    fn layer_rates_sorted_ascending() {
        let now = Instant::now();
        let mut p = Participant::new(
            "p1".into(),
            "room".into(),
            "127.0.0.1:5000".parse().unwrap(),
            1_000_000,
        )
        .unwrap();
        let mid = Mid::from("1");

        // 跨越 500ms 统计窗口 (首包在 +100ms, 到 +600ms 触发)。
        let low: Ssrc = 10.into();
        let high: Ssrc = 20.into();
        for i in 1..=6u64 {
            let t = now + Duration::from_millis(100 * i);
            p.observe_rx_packet(low, mid, Some(Rid::from("l")), None, 1000, t);
            p.observe_rx_packet(high, mid, Some(Rid::from("h")), None, 2000, t);
        }

        let rates = p.layer_rates(mid);
        assert_eq!(rates.len(), 2);
        assert_eq!(rates[0].0, Rid::from("l"));
        assert_eq!(rates[1].0, Rid::from("h"));
        assert!(rates[0].1 <= rates[1].1);

        // 非 simulcast mid 无层信息。
        assert!(p.layer_rates(Mid::from("0")).is_empty());
    }

    /// SVC 时间层累计码率随 tid 递增。
    #[test]
    fn temporal_rates_are_cumulative() {
        let now = Instant::now();
        let mut p = Participant::new(
            "p1".into(),
            "room".into(),
            "127.0.0.1:5000".parse().unwrap(),
            1_000_000,
        )
        .unwrap();
        let mid = Mid::from("1");
        let ssrc: Ssrc = 7.into();

        for i in 1..=6u64 {
            let t = now + Duration::from_millis(100 * i);
            // tid0 每个时间点 2 个包, tid1 1 个包。
            p.observe_rx_packet(ssrc, mid, None, Some(0), 1000, t);
            p.observe_rx_packet(ssrc, mid, None, Some(0), 1000, t);
            p.observe_rx_packet(ssrc, mid, None, Some(1), 1000, t);
        }

        let tr = p.temporal_rates(mid);
        assert_eq!(tr.len(), 2);
        assert_eq!(tr[0].0, 0);
        assert_eq!(tr[1].0, 1);
        // 累计: tid1 包含 tid0。
        assert!(tr[1].1 > tr[0].1);
    }
}
