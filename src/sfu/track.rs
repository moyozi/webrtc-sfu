//! 轨道表示 — 发布/订阅的媒体轨道
//!
//! 每个轨道代表一路媒体流 (音频或视频)。
//! Simulcast 场景下，一个视频轨道可能包含多个层 (RID)。

use str0m::media::{Mid, MediaKind};

/// 轨道方向
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// 发布 (sendonly)
    Publish,
    /// 订阅 (recvonly)
    Subscribe,
}

/// 媒体轨道
#[derive(Debug, Clone)]
pub struct Track {
    /// 轨道 Mid (str0m 中的 media ID)
    pub mid: Mid,
    /// 媒体类型
    pub kind: MediaKind,
    /// 方向
    pub direction: Direction,
    /// Simulcast 层 (RID 列表)
    /// 对于非 simulcast 轨道，此列表为空
    pub rids: Vec<String>,
}

impl Track {
    /// 创建音频轨道
    pub fn audio(mid: Mid, direction: Direction) -> Self {
        Self {
            mid,
            kind: MediaKind::Audio,
            direction,
            rids: Vec::new(),
        }
    }

    /// 创建视频轨道
    pub fn video(mid: Mid, direction: Direction) -> Self {
        Self {
            mid,
            kind: MediaKind::Video,
            direction,
            rids: Vec::new(),
        }
    }

    /// 是否为 simulcast
    pub fn is_simulcast(&self) -> bool {
        !self.rids.is_empty()
    }
}

/// Simulcast 层选择策略
#[derive(Debug, Clone)]
pub enum LayerSelection {
    /// 自动选择 (基于带宽估计)
    Auto,
    /// 指定层
    Fixed(String),
    /// 只选择最低层
    Lowest,
    /// 只选择最高层
    Highest,
}
