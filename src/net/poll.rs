//! Epoll 封装 — 基于 nix 的边缘触发 epoll
//!
//! 使用 Edge-Triggered 模式，配合 recvmmsg 循环读到 EAGAIN，
//! 避免水平触发的重复唤醒。

use nix::sys::epoll::{
    Epoll, EpollEvent, EpollFlags, EpollCreateFlags,
};
use std::os::fd::{BorrowedFd, RawFd};
use std::time::Duration;

/// epoll 事件封装
pub struct Poll {
    epoll: Epoll,
    /// 事件缓冲区 (预分配)
    events: Vec<EpollEvent>,
}

impl Poll {
    /// 创建 epoll 实例
    pub fn new() -> std::io::Result<Self> {
        let epoll = Epoll::new(EpollCreateFlags::EPOLL_CLOEXEC)
            .map_err(nix_to_io)?;

        Ok(Self {
            epoll,
            events: vec![EpollEvent::empty(); 16],
        })
    }

    /// 注册文件描述符到 epoll
    ///
    /// 使用 EPOLLIN (可读) + EPOLLET (边缘触发)
    pub fn add(&self, fd: RawFd) -> std::io::Result<()> {
        let event = EpollEvent::new(
            EpollFlags::EPOLLIN | EpollFlags::EPOLLET,
            fd as u64,
        );

        // nix 0.29: Epoll::add 接受 impl AsFd，需要 BorrowedFd
        // SAFETY: fd 在此调用期间有效
        let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
        self.epoll.add(borrowed, event).map_err(nix_to_io)?;

        Ok(())
    }

    /// 等待事件
    ///
    /// `timeout`: 最大等待时间。0 = 立即返回，None = 无限等待
    ///
    /// 返回就绪的事件数量。事件缓冲区在 `Poll::new` 时预分配，
    /// 本方法不再分配任何内存 (热路径每轮事件循环都会调用)。
    ///
    /// 注意: `timeout` 超过 65535ms 会被截断 (epoll 超时单位限制)。
    pub fn wait(&mut self, timeout: Option<Duration>) -> std::io::Result<usize> {
        // nix 0.29: Epoll::wait 泛型超时接受 Option<u16> (None = 无限等待)
        let timeout_ms: Option<u16> =
            timeout.map(|d| d.as_millis().min(u16::MAX as u128) as u16);

        let n = self.epoll.wait(
            &mut self.events,
            timeout_ms,
        ).map_err(nix_to_io)?;

        Ok(n)
    }
}

fn nix_to_io(e: nix::Error) -> std::io::Error {
    match e {
        nix::Error::EAGAIN => std::io::Error::from(std::io::ErrorKind::WouldBlock),
        _ => std::io::Error::from_raw_os_error(e as i32),
    }
}
