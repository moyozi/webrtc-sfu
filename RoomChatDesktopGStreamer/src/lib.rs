//! RoomChat 桌面客户端 (GStreamer / webrtcbin 版) 的可复用库。
//!
//! UI 无关的核心模块放在这里, 供 `roomchat-desktop-gstreamer` 主程序以及
//! `headless` 冒烟测试程序共用。

pub mod media;
pub mod protocol;
pub mod sfu;
pub mod signaling;
pub mod ui;
