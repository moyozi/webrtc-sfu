//! io_uring I/O 后端 — 批量收包 + 统一等待
//!
//! 与 `poll.rs`(epoll) + `udp.rs`(recvmmsg/sendmmsg) 相对: 本模块用 io_uring
//! 常驻挂载 `IORING_OP_RECVMSG`, 数据到达即产生完成事件, 省去每轮
//! `recvmmsg` 系统调用。等待用 `io_uring_enter` 的 `EXT_ARG` 超时
//! (`SubmitArgs::timespec`), 无需单独的 timeout SQE, 也无需 epoll。
//!
//! 采用「请求常驻」模式: 初始挂载 `capacity` 个 recv + 1 个 wake 读;
//! 每处理一个完成事件立即重新挂载同一缓冲区, 因此不会出现提交后又取消的风暴。
//!
//! 发送仍走 `udp.rs` 的 `sendmmsg`(已批量, 且发送方向没有等待语义)。
//!
//! 仅在内核支持时启用 (见 [`UringDriver::new`]); 否则调用方回退 epoll。
//! 需要内核 5.11+ (EXT_ARG 计时等待); 本环境为 7.x。

use io_uring::{opcode, types, IoUring};
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::fd::RawFd;
use std::time::Duration;

/// 完成事件批量上限 (一次 reap 最多处理这么多)。
const MAX_COMPLETIONS: usize = 512;

/// 唤醒管道读请求的 `user_data` 标记 (recv 槽位用 0..capacity)。
const WAKE_TAG: u64 = u64::MAX;

/// io_uring 使用策略 (来自 `SFU_IO_URING`)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoUringMode {
    /// 支持则用, 否则回退 (默认)
    Auto,
    /// 强制使用; 不可用则启动失败
    On,
    /// 禁用, 始终用 epoll
    Off,
}

impl IoUringMode {
    pub fn from_env() -> Self {
        match std::env::var("SFU_IO_URING") {
            Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
                "" | "auto" => Self::Auto,
                "on" | "1" | "true" | "yes" => Self::On,
                "off" | "0" | "false" | "no" => Self::Off,
                _ => Self::Auto,
            },
            Err(_) => Self::Auto,
        }
    }
}

/// 一个常驻 recv 槽位: 数据缓冲区 + 内核 `msghdr`。
///
/// 自引用结构: `msg.msg_iov` / `msg.msg_name` 指向本结构内的 `iov` / `name`。
/// 因此必须在 [`RecvSlot::bind`] 之后保持地址稳定 —— 槽位存放在
/// `Vec<RecvSlot>` 中, 一次性构造完成后不再增删, 地址即稳定。
struct RecvSlot {
    buf: Vec<u8>,
    name: libc::sockaddr_storage,
    iov: libc::iovec,
    msg: libc::msghdr,
}

impl RecvSlot {
    fn new(mtu: usize) -> Self {
        RecvSlot {
            buf: vec![0u8; mtu],
            name: unsafe { std::mem::zeroed() },
            iov: libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 0,
            },
            msg: unsafe { std::mem::zeroed() },
        }
    }

    /// 绑定自引用指针 (iov/msg 指向本结构内的字段)。
    /// 必须在槽位就位于最终地址后调用, 且之后不得再移动。
    fn bind(&mut self) {
        self.iov = libc::iovec {
            iov_base: self.buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: self.buf.len(),
        };
        self.msg = unsafe { std::mem::zeroed() };
        self.msg.msg_name = &mut self.name as *mut _ as *mut libc::c_void;
        self.msg.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        self.msg.msg_iov = &mut self.iov as *mut _;
        self.msg.msg_iovlen = 1;
    }

    /// 重新挂载前复位长度 (内核完成后会把它改为实际地址长度)。
    fn reset_name_len(&mut self) {
        self.msg.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    }
}

/// io_uring 驱动: 常驻 recv + wake 读, 统一等待。
pub struct UringDriver {
    ring: IoUring,
    fd: RawFd,
    wake_fd: RawFd,
    slots: Vec<RecvSlot>,
    wake_buf: Box<[u8; 64]>,
    completions: arrayvec::ArrayVec<(u64, i32), MAX_COMPLETIONS>,
}

impl UringDriver {
    /// 探测并创建 io_uring 驱动; 不支持时返回 `None` (调用方回退 epoll)。
    ///
    /// 探测点:
    ///
    /// - `IoUring::builder().build()` 失败 (旧内核 / seccomp 禁用 io_uring);
    /// - `submit_with_args` 不支持 `EXT_ARG` (需要内核 5.11+)。
    pub fn new(fd: RawFd, wake_fd: RawFd, capacity: usize) -> Option<Self> {
        let entries = (((capacity + 8).next_power_of_two()) * 2).max(64) as u32;
        let ring = IoUring::builder().build(entries).ok()?;

        // 探测 EXT_ARG 计时等待 (submit_with_args)。空环等待 1ms 应返回 ETIME。
        {
            let ts = types::Timespec::new().sec(0).nsec(1_000_000);
            let args = types::SubmitArgs::new().timespec(&ts);
            match ring.submitter().submit_with_args(1, &args) {
                Ok(_) => {}
                Err(e) if e.raw_os_error() == Some(libc::ETIME) => {}
                Err(e) if e.raw_os_error() == Some(libc::EINTR) => {}
                Err(_) => return None,
            }
        }

        let mut slots: Vec<RecvSlot> =
            (0..capacity).map(|_| RecvSlot::new(crate::net::udp::MTU)).collect();
        // 构造完成后地址不再变化, 在此绑定自引用指针。
        for slot in slots.iter_mut() {
            slot.bind();
        }
        let mut d = Self {
            ring,
            fd,
            wake_fd,
            slots,
            wake_buf: Box::new([0u8; 64]),
            completions: arrayvec::ArrayVec::new(),
        };
        for i in 0..d.slots.len() {
            d.arm_recv(i);
        }
        d.arm_wake();
        Some(d)
    }

    /// 处理后端名 (日志用)。
    pub fn name(&self) -> &'static str {
        "io_uring"
    }

    /// 处理就绪的完成事件: 对每个收到的数据包调用 `on_packet` (随后立即重新挂载)。
    ///
    /// 非阻塞; 在调用前应已通过 [`UringDriver::wait`] 等待过完成事件。
    pub fn process_ready<F: FnMut(SocketAddr, &[u8])>(&mut self, mut on_packet: F) {
        // 先把就绪的 CQE 收集到本地, 避免在持有 completion 借用时再操作 ring。
        self.completions.clear();
        {
            let mut cq = self.ring.completion();
            cq.sync();
            for cqe in cq {
                if self.completions.is_full() {
                    break;
                }
                self.completions.push((cqe.user_data(), cqe.result()));
            }
        }

        // 不能直接 for-in 遍历 `self.completions` (循环内需要 `&mut self` 重新挂载)。
        #[allow(clippy::needless_range_loop)]
        for idx in 0..self.completions.len() {
            let (tag, res) = self.completions[idx];
            if tag == WAKE_TAG {
                // 唤醒管道有数据: 读干净并重新挂载读请求。
                drain_fd(self.wake_fd);
                self.arm_wake();
                continue;
            }
            let i = tag as usize;
            if i >= self.slots.len() {
                continue;
            }
            if res > 0 {
                let len = res as usize;
                let addr = parse_sockaddr(&self.slots[i].name, self.slots[i].msg.msg_namelen)
                    .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0));
                on_packet(addr, &self.slots[i].buf[..len]);
            }
            // 无论成功/出错都重新挂载, 保持槽位常驻。
            self.arm_recv(i);
        }
    }

    /// 等待至少一个完成事件, 最多 `timeout` (超时返回 `Ok`)。
    pub fn wait(&mut self, timeout: Duration) -> io::Result<()> {
        let ts = types::Timespec::from(timeout);
        let args = types::SubmitArgs::new().timespec(&ts);
        match self.ring.submitter().submit_with_args(1, &args) {
            Ok(_) => Ok(()),
            Err(e) if e.raw_os_error() == Some(libc::ETIME) => Ok(()),
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn arm_recv(&mut self, i: usize) {
        self.slots[i].reset_name_len();
        let msg_ptr: *mut libc::msghdr = &mut self.slots[i].msg;
        let op = opcode::RecvMsg::new(types::Fd(self.fd), msg_ptr)
            .build()
            .user_data(i as u64);
        self.push(&op);
    }

    fn arm_wake(&mut self) {
        let op = opcode::Read::new(
            types::Fd(self.wake_fd),
            self.wake_buf.as_mut_ptr(),
            self.wake_buf.len() as u32,
        )
        .build()
        .user_data(WAKE_TAG);
        self.push(&op);
    }

    /// 提交一个 SQE; 队列满时先 submit 腾出空间再重试。
    fn push(&mut self, op: &io_uring::squeue::Entry) {
        let mut sq = self.ring.submission();
        if unsafe { sq.push(op) }.is_err() {
            drop(sq);
            let _ = self.ring.submitter().submit();
            let mut sq = self.ring.submission();
            // 仍失败说明队列异常; 丢弃该请求 (下次数据到来会补上)。
            let _ = unsafe { sq.push(op) };
        }
    }
}

/// 解析 `sockaddr_storage` 为 `SocketAddr` (仅 IPv4/IPv6)。
fn parse_sockaddr(sa: &libc::sockaddr_storage, len: libc::socklen_t) -> Option<SocketAddr> {
    match sa.ss_family as libc::c_int {
        libc::AF_INET if (len as usize) >= std::mem::size_of::<libc::sockaddr_in>() => {
            let sin = unsafe { &*(sa as *const _ as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let port = u16::from_be(sin.sin_port);
            Some(SocketAddr::new(IpAddr::V4(ip), port))
        }
        libc::AF_INET6 if (len as usize) >= std::mem::size_of::<libc::sockaddr_in6>() => {
            let sin6 = unsafe { &*(sa as *const _ as *const libc::sockaddr_in6) };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            Some(SocketAddr::new(IpAddr::V6(ip), port))
        }
        _ => None,
    }
}

/// 非阻塞读干净一个 fd (唤醒管道)。
fn drain_fd(fd: RawFd) {
    let mut buf = [0u8; 64];
    loop {
        let n = unsafe {
            libc::read(
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len() as libc::size_t,
            )
        };
        if n <= 0 {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ipv4_sockaddr() {
        let mut sa: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        unsafe {
            let sin = &mut *(&mut sa as *mut _ as *mut libc::sockaddr_in);
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = 5175u16.to_be();
            sin.sin_addr.s_addr = u32::from(Ipv4Addr::new(192, 168, 1, 10)).to_be();
        }
        let addr = parse_sockaddr(&sa, std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t);
        assert_eq!(addr, Some("192.168.1.10:5175".parse().unwrap()));
    }

    #[test]
    fn parse_bad_family_returns_none() {
        let sa: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        assert_eq!(parse_sockaddr(&sa, 0), None);
    }
}
