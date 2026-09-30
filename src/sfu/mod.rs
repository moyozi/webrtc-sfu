//! SFU 核心逻辑 — 参与者管理与 RTP 路由
//!
//! - [`participant`] — 封装 str0m `Rtc` 实例与包路由索引 (`ParticipantTable`)
//! - [`router`] — 发布者→订阅者路由表
//! - [`layer`] — simulcast/SVC 选层与码率估计

pub mod layer;
pub mod participant;
pub mod router;
