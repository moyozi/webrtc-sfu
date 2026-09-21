//! 参与者 — 封装 str0m Rtc 实例
//!
//! 每个参与者拥有一个独立的 str0m::Rtc 实例，处理:
//! - ICE 连接建立
//! - DTLS 握手
//! - SRTP 加解密
//! - SDP 协商
//! - 媒体收发

use ahash::AHashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn, debug};

/// 参与者 — 封装 str0m Rtc 实例和相关状态
pub struct Participant {
    /// 参与者唯一 ID
    pub id: String,

    /// 所在房间 ID
    pub room_id: String,

    /// str0m Rtc 实例 — 处理所有 WebRTC 协议
    pub rtc: str0m::Rtc,

    /// 已知的远端源地址 (ICE 连接建立后记录，用于后续 RTP/RTCP 快速路由)
    pub known_source: Option<SocketAddr>,

    /// 本地 ICE ufrag (用于快速包路由)
    pub local_ufrag: String,

    /// 远端 ICE ufrag
    pub remote_ufrag: Option<String>,

    /// 是否已连接 (ICE + DTLS 完成)
    pub connected: bool,

    /// 上次 `poll_output()` 返回的 `Output::Timeout` 时刻。
    /// 用于避免收包热路径反复对定时器表做哈希查找: 只有该值变化时才更新 TimerWheel。
    pub next_timeout: Option<Instant>,

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
    ) -> Result<Self, String> {
        // 创建 Rtc 实例
        // SFU 使用 ICE Lite 模式 — 只接受 host 类型的候选，不做 server reflexive

        // 预先生成 ICE 凭证 (str0m 的 local_ice_credentials 在 RtcConfig 上，不在 Rtc 上)
        let ice_creds = str0m::IceCreds::new();
        let local_ufrag = ice_creds.ufrag.clone();

        let rtc = str0m::Rtc::builder()
            .set_ice_lite(true)
            .set_local_ice_credentials(ice_creds)
            .build(Instant::now());

        info!("参与者 {} 创建, ufrag={}", id, local_ufrag);

        Ok(Self {
            id,
            room_id,
            rtc,
            known_source: None,
            local_ufrag,
            remote_ufrag: None,
            connected: false,
            next_timeout: None,
            local_addr,
        })
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
        ).map_err(|e| format!("创建 ICE 候选失败: {}", e))?;

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
    pub fn add_remote_candidate(
        &mut self,
        candidate_str: &str,
        sdp_mid: Option<String>,
    ) {
        // sdp_mid 目前未使用 — str0m 的 add_remote_candidate 内部通过 SDP 协商确定 mid
        let _ = sdp_mid;
        // 解析候选字符串
        // 格式: candidate:<foundation> <component> <protocol> <priority> <addr> <port> typ <type> [raddr <raddr> rport <rport>]
        match str0m::Candidate::from_sdp_string(candidate_str) {
            Ok(candidate) => {
                self.rtc.add_remote_candidate(candidate);
                debug!(
                    "参与者 {} 添加远端候选: {}",
                    self.id, candidate_str
                );
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

    /// 检查此参与者是否匹配给定的 ICE ufrag
    pub fn matches_ufrag(&self, ufrag: &str) -> bool {
        self.local_ufrag == ufrag
    }

    /// 检查此参与者的已知源地址是否匹配
    pub fn matches_source(&self, source: SocketAddr) -> bool {
        self.known_source == Some(source)
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

    /// 遍历所有参与者 (供 Timer 驱动使用)
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut Participant)> {
        self.participants.iter_mut()
    }

    /// O(1) 解析包所属参与者
    ///
    /// 先尝试 STUN ufrag (两段顺序都试)，再退回源地址匹配。
    /// STUN USERNAME 为 `ufragA:ufragB`，两段顺序因实现而异，
    /// 只要任一段等于本地 ufrag 即可命中。
    pub fn resolve(
        &self,
        ufrags: Option<(&str, &str)>,
        source: SocketAddr,
    ) -> Option<Arc<str>> {
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
    pub fn update_source_index(
        &mut self,
        id: &str,
        old: Option<SocketAddr>,
        new: SocketAddr,
    ) {
        if let Some(old) = old {
            if self.source_index.get(&old).map(|v| &**v) == Some(id) {
                self.source_index.remove(&old);
            }
        }
        self.source_index.insert(new, Arc::from(id));
    }

    pub fn len(&self) -> usize {
        self.participants.len()
    }

    pub fn is_empty(&self) -> bool {
        self.participants.is_empty()
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
}
