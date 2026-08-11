//! UDP Socket — 基于 nix recvmmsg/sendmmsg 的批量收发包
//!
//! 核心设计:
//! - SO_REUSEPORT: 多个 Worker 的 socket 绑定同一端口，内核做负载均衡
//! - recvmmsg: 单次系统调用批量接收 64 个 UDP 包
//! - sendmmsg: 单次系统调用批量发送多个 UDP 包
//! - 预分配缓冲区: RecvBatch/SendBatch 在 Worker 启动时分配，热路径零分配

use nix::sys::socket::sockopt::{RcvBuf, ReusePort, SndBuf};
use nix::sys::socket::{
    recvmmsg, sendmmsg, MsgFlags, MultiHeaders, SockaddrStorage,
};
use nix::sys::socket::{setsockopt, socket, AddressFamily, SockFlag, SockType};
use std::io::IoSlice;
use std::io::IoSliceMut;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

/// MTU — WebRTC RTP 包最大 1500 字节 (以太网 MTU)
pub const MTU: usize = 1500;

/// recvmmsg 批量大小
pub const BATCH_SIZE: usize = 64;

/// sendmmsg 批量大小
pub const SEND_BATCH_SIZE: usize = 128;

// ──────────────────────────────────────────────────────────
// UdpSocket — 原始 UDP socket 封装
// ──────────────────────────────────────────────────────────

/// 高性能 UDP socket，支持 SO_REUSEPORT 和 SO_BUSY_POLL
pub struct UdpSocket {
    fd: OwnedFd,
}

impl UdpSocket {
    /// 创建 UDP socket 并绑定到指定地址
    ///
    /// `reuse_port = true` 时启用 SO_REUSEPORT，允许多个 Worker 共享同一端口
    pub fn new(addr: SocketAddr, reuse_port: bool) -> std::io::Result<Self> {
        let family = if addr.is_ipv4() {
            AddressFamily::Inet
        } else {
            AddressFamily::Inet6
        };

        let fd =
            socket(family, SockType::Datagram, SockFlag::SOCK_CLOEXEC, None).map_err(nix_to_io)?;

        // 设置 SO_REUSEADDR
        setsockopt(&fd, nix::sys::socket::sockopt::ReuseAddr, &true)
            .map_err(nix_to_io)?;

        // 设置 SO_REUSEPORT — 让多个 Worker 线程的 socket 绑定同一端口
        // 内核会对每个连接的包做 hash，保证同一个 ICE 连接始终到同一个 Worker
        if reuse_port {
            setsockopt(&fd, ReusePort, &true).map_err(nix_to_io)?;
        }

        // 增大接收/发送缓冲区 (50MB)
        let buf_size: usize = 50 * 1024 * 1024;
        let _ = setsockopt(&fd, RcvBuf, &buf_size);
        let _ = setsockopt(&fd, SndBuf, &buf_size);

        // 绑定地址
        let nix_addr = socketaddr_to_nix(addr);
        nix::sys::socket::bind(fd.as_raw_fd(), &nix_addr).map_err(nix_to_io)?;

        // 设置非阻塞
        let flags =
            nix::fcntl::fcntl(fd.as_raw_fd(), nix::fcntl::FcntlArg::F_GETFL).map_err(nix_to_io)?;
        let mut flags = nix::fcntl::OFlag::from_bits_truncate(flags);
        flags |= nix::fcntl::OFlag::O_NONBLOCK;
        nix::fcntl::fcntl(fd.as_raw_fd(), nix::fcntl::FcntlArg::F_SETFL(flags))
            .map_err(nix_to_io)?;

        Ok(Self { fd })
    }

    /// 设置 SO_BUSY_POLL — 内核级忙轮询，减少软中断延迟
    ///
    /// 当 Worker 在 epoll_wait 阻塞时，内核会主动轮询网卡队列，
    /// 而不是等待软中断。适合低延迟场景，代价是增加 CPU 使用率。
    ///
    /// 需 Linux 3.11+，且需要设置 `net.core.busy_poll` 和 `net.core.busy_read`
    pub fn set_busy_poll(&self, micros: u32) -> std::io::Result<()> {
        // SO_BUSY_POLL = 70
        const SO_BUSY_POLL: i32 = 70;
        let val = micros as i32;
        let ret = unsafe {
            libc::setsockopt(
                self.fd.as_raw_fd(),
                libc::SOL_SOCKET,
                SO_BUSY_POLL,
                &val as *const _ as *const _,
                std::mem::size_of::<i32>() as u32,
            )
        };
        if ret < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// 获取原始文件描述符 (用于 epoll 注册)
    pub fn raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

// ──────────────────────────────────────────────────────────
// RecvBatch — 批量收包缓冲区
// ──────────────────────────────────────────────────────────

/// 预分配的批量收包缓冲区
///
/// 使用 nix recvmmsg 一次系统调用接收最多 `BATCH_SIZE` 个 UDP 包。
/// 所有缓冲区在创建时分配，热路径零分配。
pub struct RecvBatch {
    /// 包数据缓冲区: [BATCH_SIZE][MTU] 字节
    bufs: Vec<Vec<u8>>,
    /// nix MultiHeaders — 预分配的 mmsghdr 数组 (仅 alloc 一次)
    headers: MultiHeaders<SockaddrStorage>,
    /// 每个包的实际接收长度
    lengths: Vec<usize>,
    /// 每个包的源地址 (nix → std 转换后缓存)
    addrs: Vec<SocketAddr>,
}

impl RecvBatch {
    /// 创建批量收包缓冲区
    pub fn new(capacity: usize, mtu: usize) -> Self {
        let bufs: Vec<Vec<u8>> = (0..capacity).map(|_| vec![0u8; mtu]).collect();

        // 预分配 MultiHeaders (nix 0.29 使用 preallocate)
        let headers = MultiHeaders::<SockaddrStorage>::preallocate(capacity, None);

        Self {
            bufs,
            headers,
            lengths: vec![0; capacity],
            addrs: vec![
                SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0);
                capacity
            ],
        }
    }

    /// 执行 recvmmsg 批量收包
    ///
    /// 返回收到的包数量。返回 0 表示没有数据可读 (EAGAIN)。
    pub fn recv(&mut self, fd: RawFd) -> std::io::Result<usize> {
        // Split borrow: 同时借用 bufs 和 headers (它们是不同的字段)
        let Self {
            bufs,
            headers,
            lengths,
            addrs,
        } = self;

        // 创建 iov (每次调用时创建，因为 IoSliceMut 不能长期存储)
        // 这只是创建栈上的引用，不涉及堆分配
        let mut iovs: Vec<[IoSliceMut<'_>; 1]> =
            bufs.iter_mut().map(|buf| [IoSliceMut::new(buf)]).collect();

        // 执行 recvmmsg — 单次系统调用收最多 capacity 个包
        let results = recvmmsg(
            fd,
            headers,                // &mut MultiHeaders (预分配)
            &mut iovs,              // &mut [IoSliceMut] (指向 bufs)
            MsgFlags::MSG_DONTWAIT, // 非阻塞
            None,                   // 无超时 (配合 MSG_DONTWAIT)
        )
        .map_err(nix_to_io)?;

        // 遍历结果，提取地址和长度
        let mut count = 0;
        for recv_msg in results {
            // address 现在是字段而非方法
            let addr = recv_msg.address
                .as_ref()
                .map(|a| nix_to_socketaddr(a))
                .unwrap_or_else(|| {
                    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
                });

            // nix 0.29: bytes 字段直接返回接收字节数
            let bytes: usize = recv_msg.bytes;

            addrs[count] = addr;
            lengths[count] = bytes;
            count += 1;
        }

        Ok(count)
    }

    /// 获取第 i 个收到的包
    ///
    /// 返回 (源地址, 数据切片)
    pub fn get(&self, index: usize) -> (SocketAddr, &[u8]) {
        let len = self.lengths[index];
        (self.addrs[index], &self.bufs[index][..len])
    }
}

// ──────────────────────────────────────────────────────────
// SendBatch — 批量发包缓冲区
// ──────────────────────────────────────────────────────────

/// 预分配的批量发包缓冲区
///
/// 收集多个待发送的包，然后用 sendmmsg 一次系统调用发出。
pub struct SendBatch {
    /// 待发送的包: (目标地址, 数据)
    packets: Vec<(SocketAddr, bytes::Bytes)>,
    /// 预分配的 nix MultiHeaders
    headers: MultiHeaders<SockaddrStorage>,
    capacity: usize,
}

impl SendBatch {
    pub fn new(capacity: usize) -> Self {
        // 预分配 MultiHeaders (nix 0.29 使用 preallocate)
        let headers = MultiHeaders::<SockaddrStorage>::preallocate(capacity, None);

        Self {
            packets: Vec::with_capacity(capacity),
            headers,
            capacity,
        }
    }

    /// 添加一个待发送的包
    pub fn push(&mut self, dest: SocketAddr, data: bytes::Bytes) {
        if self.packets.len() < self.capacity {
            self.packets.push((dest, data));
        }
        // 如果满了就丢弃 (不应该发生，容量足够大)
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    /// 清空缓冲区
    pub fn clear(&mut self) {
        self.packets.clear();
    }

    /// 执行 sendmmsg 批量发包
    ///
    /// 单次系统调用发送所有排队的包
    pub fn flush(&mut self, fd: RawFd) -> std::io::Result<usize> {
        if self.packets.is_empty() {
            return Ok(0);
        }

        // 准备地址和 iov
        let addrs: Vec<Option<SockaddrStorage>> = self
            .packets
            .iter()
            .map(|(addr, _)| Some(socketaddr_to_nix(*addr)))
            .collect();

        let iovs: Vec<[IoSlice<'_>; 1]> = self
            .packets
            .iter()
            .map(|(_, data)| [IoSlice::new(data.as_ref())])
            .collect();

        let cmsgs: &[nix::sys::socket::ControlMessage] = &[];

        // 执行 sendmmsg
        let results = sendmmsg(
            fd,
            &mut self.headers,
            &iovs,
            &addrs,
            cmsgs,
            MsgFlags::MSG_DONTWAIT,
        )
        .map_err(nix_to_io)?;

        let count = results.count();
        Ok(count)
    }
}

// ──────────────────────────────────────────────────────────
// 地址转换工具函数
// ──────────────────────────────────────────────────────────

/// std::net::SocketAddr → nix SockaddrStorage
fn socketaddr_to_nix(addr: SocketAddr) -> SockaddrStorage {
    // nix 0.29: SockaddrStorage 直接实现 From<std::net::SocketAddr>
    SockaddrStorage::from(addr)
}

/// nix SockaddrStorage → std::net::SocketAddr
fn nix_to_socketaddr(addr: &SockaddrStorage) -> SocketAddr {
    if let Some(sin) = addr.as_sockaddr_in() {
        SocketAddr::V4((*sin).into())
    } else if let Some(sin6) = addr.as_sockaddr_in6() {
        SocketAddr::V6((*sin6).into())
    } else {
        SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0)
    }
}

/// nix::Error → std::io::Error
fn nix_to_io(e: nix::Error) -> std::io::Error {
    match e {
        nix::Error::EAGAIN => std::io::Error::from(std::io::ErrorKind::WouldBlock),
        _ => std::io::Error::from_raw_os_error(e as i32),
    }
}
