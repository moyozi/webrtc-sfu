//! UI ↔ WebRTC worker 之间的线程安全事件。
//!
//! worker 线程 (reactor-webrtc + 信令) 通过 crossbeam channel 把事件发给
//! Slint 主线程, 主线程用 `slint::Timer` 排空并更新模型, 从而避免在
//! 非 UI 线程触碰 Slint 对象。

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Success,
    Warn,
    Error,
}

impl LogLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            LogLevel::Info => "info",
            LogLevel::Success => "success",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubStatus {
    Idle,
    Connecting,
    Subscribed,
    Error,
}

impl SubStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            SubStatus::Idle => "idle",
            SubStatus::Connecting => "connecting",
            SubStatus::Subscribed => "subscribed",
            SubStatus::Error => "error",
        }
    }
}

#[derive(Debug)]
pub enum UiEvent {
    /// 追加一条日志
    Log { level: LogLevel, msg: String },

    /// 发布连接建立, 进入通话页
    Joined { room_id: String },

    /// 已离开通话
    Left,

    /// 新的远端参与者出现 (自动发起订阅)
    ParticipantJoined { id: String },

    /// 远端参与者离开
    ParticipantLeft { id: String },

    /// 订阅状态变化
    SubscriptionUpdate {
        id: String,
        status: SubStatus,
        has_video: bool,
    },

    /// 本地合成视频帧 (RGBA)
    LocalVideo {
        rgba: Vec<u8>,
        width: u32,
        height: u32,
    },

    /// 远端视频帧 (RGBA)
    RemoteVideo {
        id: String,
        rgba: Vec<u8>,
        width: u32,
        height: u32,
    },

    /// PC 连接状态变化
    ConnectionState { pc_id: String, state: String },

    /// 致命错误 (加入失败 / 信令断开等)
    Error { msg: String },
}
