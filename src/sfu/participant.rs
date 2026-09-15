//! 参与者 — 封装 str0m Rtc 实例
//!
//! 每个参与者拥有一个独立的 str0m::Rtc 实例，处理:
//! - ICE 连接建立
//! - DTLS 握手
//! - SRTP 加解密
//! - SDP 协商
//! - 媒体收发

use std::net::SocketAddr;
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
