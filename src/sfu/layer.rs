//! 转发层选择 — BWE 驱动的 **Simulcast (RID)** 与 **SVC (时间层)** 选择
//!
//! SFU 需要为每个订阅者挑选合适的视频层，使其码率不超过该订阅者链路的
//! 可用带宽估计 (BWE)。本模块只包含与 str0m 无关的纯逻辑，便于单测:
//!
//! - `RateWindow`：基于 500ms 滑动窗口 + EWMA 的码率估计，用于测量每个
//!   入站层 (simulcast RID / SVC 流) 的实际码率；
//! - `select_layer`：在预算内选择码率不超限的最高层；
//! - `is_keyframe`：复用 str0m 的编解码器关键帧探测，用于层切换时确认
//!   目标层已有关键帧；
//! - `vp8_temporal_id`：解析 VP8 RTP payload descriptor 的时间层索引；
//! - `vp9_sid_tid`：解析 VP9 RTP payload descriptor 的 (空间层, 时间层)；
//! - `select_svc`：在预算内选择质量最高的 SVC 目标层 (sid, tid)。
//!
//! > 说明: str0m 的公开 API 不暴露 VP9/AV1 的层索引，这里自行解析 payload
//! > descriptor。目前已支持 **VP8 时间层** 与 **VP9 空间层 + 时间层**
//! > (non-flexible 模式)；AV1 与 VP9 flexible 模式暂不解析, 遇到时按非 SVC
//! > 处理 (不过滤, 全转发), 保证兼容性。

use std::time::{Duration, Instant};

use str0m::format::Codec;
use str0m::media::Rid;

/// 码率估计窗口长度
const RATE_WINDOW: Duration = Duration::from_millis(500);

/// EWMA 平滑系数 (新样本权重 1/4)
const EWMA_SHIFT: u32 = 2;

/// 滑动窗口 + EWMA 的码率估计 (bit/s)
pub struct RateWindow {
    /// 当前窗口内累计字节数
    bytes: u64,
    /// 当前窗口起始时刻
    window_start: Instant,
    /// 平滑后的码率 (bit/s)
    bps: u64,
}

impl RateWindow {
    pub fn new(now: Instant) -> Self {
        Self {
            bytes: 0,
            window_start: now,
            bps: 0,
        }
    }

    /// 记录一个到达包 (payload 字节数)。
    pub fn add(&mut self, bytes: usize, now: Instant) {
        self.bytes += bytes as u64;

        let elapsed = now.saturating_duration_since(self.window_start);
        if elapsed >= RATE_WINDOW {
            let secs = elapsed.as_secs_f64().max(1e-6);
            let instant_bps = (self.bytes as f64 * 8.0 / secs) as u64;

            // EWMA: 首个样本直接采用，之后按 1/4 权重平滑。
            self.bps = if self.bps == 0 {
                instant_bps
            } else {
                let blended =
                    self.bps as i64 + ((instant_bps as i64 - self.bps as i64) >> EWMA_SHIFT);
                blended.max(0) as u64
            };

            self.bytes = 0;
            self.window_start = now;
        }
    }

    /// 当前平滑码率 (bit/s)。窗口未结束时返回上次估算值。
    pub fn bps(&self) -> u64 {
        self.bps
    }
}

/// 在预算内选择码率不超限的最高层。
///
/// * `layers` 需按码率升序排列；
/// * `budget_bps` 为可用带宽估计；
/// * `headroom` 为冗余系数 (例如 0.9 表示最多用 90% 带宽)。
///
/// 返回 `None` 表示连最低层都超出预算 (调用方可回退到最低层)。
pub fn select_layer(layers: &[(Rid, u64)], budget_bps: u64, headroom: f64) -> Option<Rid> {
    let effective = (budget_bps as f64 * headroom.max(0.0)) as u64;
    layers
        .iter()
        .rev()
        .find(|(_, bps)| *bps <= effective)
        .map(|(rid, _)| *rid)
}

/// SVC 候选目标层: `(spatial_id, temporal_id)` → 解码该目标所需的**累计**码率。
///
/// 累计 = 所有 `sid <= S` 且 `tid <= T` 的层码率之和。
pub type SvcTarget = ((u8, u8), u64);

/// 在预算内选择质量最高的 SVC 目标层。
///
/// * `candidates` 需按**质量升序**排列 (先 `sid` 后 `tid`)；
/// * `budget_bps` 为可用带宽估计；`headroom` 为冗余系数 (如 0.95)。
///
/// 返回 `None` 表示连最低候选都超出预算 (调用方可回退到最低层)。
/// 策略为**空间优先**: 在预算内优先升空间层, 再升时间层。
pub fn select_svc(
    candidates: &[SvcTarget],
    budget_bps: u64,
    headroom: f64,
) -> Option<(u8, u8)> {
    let effective = (budget_bps as f64 * headroom.max(0.0)) as u64;
    candidates
        .iter()
        .rev()
        .find(|(_, bps)| *bps <= effective)
        .map(|(st, _)| *st)
}

/// 判断一个 RTP payload 是否以关键帧开始 (用于层切换确认)。
pub fn is_keyframe(codec: Codec, payload: &[u8]) -> bool {
    use str0m::format::{
        detect_av1_keyframe, detect_h264_keyframe, detect_h265_keyframe, detect_h266_keyframe,
        detect_vp8_keyframe, detect_vp9_keyframe,
    };

    match codec {
        Codec::Vp8 => detect_vp8_keyframe(payload),
        Codec::Vp9 => detect_vp9_keyframe(payload),
        Codec::Av1 => detect_av1_keyframe(payload),
        Codec::H264 => detect_h264_keyframe(payload),
        Codec::H265 => detect_h265_keyframe(payload),
        Codec::H266 => detect_h266_keyframe(payload),
        _ => false,
    }
}

/// 解析 VP8 RTP payload descriptor，返回时间层索引 (TID)。
///
/// RFC 7741: 若未携带 TID 字段则视为基础层 (0)。解析失败返回 `None`。
/// 用于 SVC 时间层降级 (丢弃 TID 高于目标值的包)。
pub fn vp8_temporal_id(payload: &[u8]) -> Option<u8> {
    let b0 = *payload.first()?;

    // X 位未设置 → 无扩展，基础时间层。
    if b0 & 0x80 == 0 {
        return Some(0);
    }

    let ext = *payload.get(1)?;
    let has_picture_id = ext & 0x80 != 0;
    let has_tl0_pic_idx = ext & 0x40 != 0;
    let has_tid = ext & 0x20 != 0;
    let has_key_idx = ext & 0x10 != 0;

    let mut off = 2usize;

    if has_picture_id {
        let p0 = *payload.get(off)?;
        // 15-bit PictureID 占 2 字节，7-bit 占 1 字节。
        off += if p0 & 0x80 != 0 { 2 } else { 1 };
    }
    if has_tl0_pic_idx {
        off += 1;
    }

    // TID 与 KEYIDX 共用同一字节 (任一存在即存在该字节)。
    if has_tid || has_key_idx {
        let b = *payload.get(off)?;
        if has_tid {
            return Some((b >> 6) & 0x03);
        }
    }

    Some(0)
}

/// 解析 VP9 RTP payload descriptor，返回 `(spatial_id, temporal_id)`。
///
/// 依据 RFC 9628。仅解析 **non-flexible 模式** (F=0，每个包承载单个空间层):
/// 该模式覆盖浏览器 K-SVC 的常见用法。遇到 flexible 模式 (F=1)、缺少层信息
/// 或解析失败时返回 `None`，调用方据此视为非 SVC、不做层过滤。
pub fn vp9_sid_tid(payload: &[u8]) -> Option<(u8, u8)> {
    let b0 = *payload.first()?;
    let has_picture_id = b0 & 0x80 != 0; // I
    let has_layer_indices = b0 & 0x20 != 0; // L
    let flexible = b0 & 0x10 != 0; // F
    if flexible {
        return None;
    }

    let mut off = 1usize;
    if has_picture_id {
        let p = *payload.get(off)?;
        // 15-bit PictureID 占 2 字节，7-bit 占 1 字节。
        off += if p & 0x80 != 0 { 2 } else { 1 };
    }
    if !has_layer_indices {
        // 无层信息 → 单层。
        return Some((0, 0));
    }
    // TL0PICIDX (存在但 SFU 不需要)
    let _ = *payload.get(off)?;
    off += 1;
    // TID(3) | U(1) | SID(3) | D(1)
    let b = *payload.get(off)?;
    let tid = (b >> 5) & 0x07;
    let sid = (b >> 1) & 0x07;
    Some((sid, tid))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rid(s: &str) -> Rid {
        Rid::from(s)
    }

    #[test]
    fn select_highest_within_budget() {
        let layers = vec![
            (rid("l"), 150_000),
            (rid("m"), 500_000),
            (rid("h"), 1_500_000),
        ];
        // 预算充足 → 最高层
        assert_eq!(select_layer(&layers, 2_000_000, 0.9), Some(rid("h")));
        // 预算 600k → 中层 (h 超限)
        assert_eq!(select_layer(&layers, 600_000, 0.9), Some(rid("m")));
        // 预算 200k → 低层
        assert_eq!(select_layer(&layers, 200_000, 0.9), Some(rid("l")));
        // 预算过低 → None
        assert_eq!(select_layer(&layers, 50_000, 0.9), None);
        // 空列表
        assert_eq!(select_layer(&[], 1_000_000, 0.9), None);
    }

    #[test]
    fn rate_window_ewma_and_window() {
        let t0 = Instant::now();
        let mut w = RateWindow::new(t0);
        // 第一个窗口: 100kB / 0.5s = 1.6 Mbit/s
        w.add(100_000, t0 + Duration::from_millis(500));
        assert_eq!(w.bps(), 1_600_000);

        // 第二个窗口: 50kB / 0.5s = 0.8 Mbit/s
        // EWMA: 1.6M + (0.8M - 1.6M)/4 = 1.4M
        w.add(50_000, t0 + Duration::from_millis(1000));
        assert_eq!(w.bps(), 1_400_000);

        // 窗口未结束 → 保持不变
        w.add(10_000, t0 + Duration::from_millis(1100));
        assert_eq!(w.bps(), 1_400_000);
    }

    #[test]
    fn vp8_temporal_id_parsing() {
        // 无扩展 → 0
        assert_eq!(vp8_temporal_id(&[0x00, 0x01]), Some(0));

        // X=1, T=1, L=0 (ext = 0x20)，TID 字节 = 0b10_000000 → tid=2
        let p = [0x80, 0x20, 0b10_000000, 0x00];
        assert_eq!(vp8_temporal_id(&p), Some(2));

        // X=1, L=1, T=1 (ext=0x60)：TL0PICIDX 1 字节后是 TID 字节
        let p = [0x80, 0x60, 0xAA, 0b01_000000, 0x00];
        assert_eq!(vp8_temporal_id(&p), Some(1));

        // 7-bit PictureID + TID: [0x80, 0xA0, pid, tid, ...]
        let p = [0x80, 0xA0, 0x12, 0b11_000000, 0x00];
        assert_eq!(vp8_temporal_id(&p), Some(3));

        // 无 TID 字段 → 0
        let p = [0x80, 0x00, 0x00];
        assert_eq!(vp8_temporal_id(&p), Some(0));

        // 截断 → None
        assert_eq!(vp8_temporal_id(&[]), None);
        assert_eq!(vp8_temporal_id(&[0x80]), None);
    }

    #[test]
    fn vp9_sid_tid_parsing() {
        // 无扩展 (L=0) → 单层 (0,0)
        assert_eq!(vp9_sid_tid(&[0x00]), Some((0, 0)));

        // L=1: TL0PICIDX + TID/SID 字节。0x42 = TID=2, SID=1
        assert_eq!(vp9_sid_tid(&[0x20, 0xAA, 0x42]), Some((1, 2)));

        // I=1 (7-bit PictureID) + L=1: [flags, pid, tl0, tid/sid]
        // 0xA0 = I|L; pid=0x12 (7-bit); tl0=0xAA; byte=0b00_111_0_1_0 → tid=0, sid=3
        assert_eq!(vp9_sid_tid(&[0xA0, 0x12, 0xAA, 0x06]), Some((3, 0)));

        // I=1 (15-bit PictureID, pid 首字节最高位=1) + L=1
        assert_eq!(vp9_sid_tid(&[0xA0, 0x80, 0x01, 0xAA, 0x63]), Some((1, 3)));

        // flexible 模式 (F=1) → None
        assert_eq!(vp9_sid_tid(&[0x10, 0x00]), None);

        // 截断 → None
        assert_eq!(vp9_sid_tid(&[]), None);
        assert_eq!(vp9_sid_tid(&[0x20]), None);
        assert_eq!(vp9_sid_tid(&[0x20, 0xAA]), None);
    }

    #[test]
    fn select_svc_spatial_first() {
        // 候选按 (sid,tid) 升序, 码率为累计值
        let c: Vec<SvcTarget> = vec![
            ((0, 0), 100_000),
            ((0, 1), 400_000),
            ((1, 0), 300_000),
            ((1, 1), 600_000),
        ];
        // 充足 → 最高 (1,1)
        assert_eq!(select_svc(&c, 2_000_000, 0.95), Some((1, 1)));
        // 350k: (1,1)600 超, (1,0)300 满足 → 空间优先选 (1,0)
        assert_eq!(select_svc(&c, 350_000, 0.95), Some((1, 0)));
        // 200k: (1,0)300 超, (0,1)400 超, (0,0)100 满足
        assert_eq!(select_svc(&c, 200_000, 0.95), Some((0, 0)));
        // 过低 → None
        assert_eq!(select_svc(&c, 50_000, 0.95), None);
    }
}
