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
//! - `vp8_temporal_id`：解析 VP8 RTP payload descriptor 的时间层索引，
//!   用于 SVC 时间层降级。
//!
//! > 说明: str0m 的公开 API 不暴露 VP9/AV1 的空间层索引 (仅在 frame-level
//! > 的 `MediaData.codec_extra` 中)，因此本 SFU 目前只能对 **VP8 时间层**
//! > 做 SVC 降级；VP9/AV1 空间层选择需要自行解析 payload descriptor。

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
}
