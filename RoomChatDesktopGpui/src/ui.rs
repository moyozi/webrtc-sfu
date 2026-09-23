//! UI ↔ WebRTC worker 之间的线程安全事件。
//!
//! worker 线程 (reactor-webrtc + 信令) 通过 [`UiSender`] 把事件发给 GPUI 主线程;
//! 主线程在一个异步任务里 `recv().await` 排空事件并更新视图状态, 因此 worker
//! 绝不会直接触碰任何 GPUI 对象 (`App`/`Entity` 都不是 `Send`)。
//!
//! 选用 `async-channel`: 它的 `Sender` 可 clone、可从同步线程 `send_blocking`,
//! 而 `Receiver` 本身实现了 `Future`, 可以直接在 GPUI 的执行器里 await。

use async_channel::{Receiver, SendError, Sender};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Success,
    Warn,
    Error,
}

impl LogLevel {
    #[allow(dead_code)] // 保留与信令/日志文本的映射, 便于调试与协议对齐
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
    #[allow(dead_code)] // 保留与字符串状态的映射, 与 Slint 版保持一致
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

    /// 本地视频帧 (BGRA, 已按 GPUI `RenderImage` 期望的通道顺序)
    LocalVideo {
        bgra: Vec<u8>,
        width: u32,
        height: u32,
    },

    /// 远端视频帧 (BGRA)
    RemoteVideo {
        id: String,
        bgra: Vec<u8>,
        width: u32,
        height: u32,
    },

    /// PC 连接状态变化
    ConnectionState { pc_id: String, state: String },

    /// 致命错误 (加入失败 / 信令断开等)
    Error { msg: String },
}

/// 可 clone 的 UI 事件发送端。
///
/// 包一层 `async_channel::Sender`, 暴露与 crossbeam 相同的 `send` 方法, 这样
/// worker 侧 (sfu/media) 的既有 `let _ = ui_tx.send(event);` 调用无需改动。
#[derive(Clone)]
pub struct UiSender(Sender<UiEvent>);

impl UiSender {
    /// 创建事件通道, 分别给 worker (发送端) 与主线程 (接收端)。
    pub fn channel() -> (Self, Receiver<UiEvent>) {
        let (tx, rx) = async_channel::unbounded();
        (Self(tx), rx)
    }

    /// 从同步线程发送事件 (无界通道, 不会阻塞)。
    pub fn send(&self, ev: UiEvent) -> Result<(), SendError<UiEvent>> {
        self.0.send_blocking(ev)
    }
}
