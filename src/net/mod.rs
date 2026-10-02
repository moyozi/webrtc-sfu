//! 网络层 — UDP socket + I/O 事件驱动
//!
//! 支持两种收包/等待后端, 启动时探测选择:
//! - **io_uring** ([`uring`]): 常驻 `RECVMSG` + `io_uring_enter` 计时等待 (内核 5.11+);
//! - **epoll + recvmmsg** ([`poll`] + [`udp`]): 回退路径。
//!
//! 发送统一走 [`udp::SendBatch`] 的 `sendmmsg`。

pub mod poll;
pub mod relay;
pub mod udp;
pub mod uring;
