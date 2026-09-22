//! UDP Socket — 基于 nix recvmmsg/sendmmsg 的批量收发包
//!
//! 核心设计:
//! - SO_REUSEPORT: 多个 Worker 的 socket 绑定同一端口，内核做负载均衡
//! - recvmmsg: 单次系统调用批量接收 64 个 UDP 包
//! - sendmmsg: 单次系统调用批量发送多个 UDP 包
//! - 预分配缓冲区: RecvBatch/SendBatch 在 Worker 启动时分配，热路径零分配
//! - Transmit 的 Vec<u8> 直接 move 进发送队列，避免逐包 memcpy

use nix::sys::socket::sockopt::{RcvBuf, ReusePort, SndBuf};
use nix::sys::socket::getsockopt;
use nix::sys::socket::{
    recvmmsg, sendmmsg, MsgFlags, MultiHeaders, SockaddrStorage,
};
use nix::sys::socket::{setsockopt, socket, AddressFamily, SockFlag, SockType};
use arrayvec::ArrayVec;
use std::io::IoSlice;
use std::io::IoSliceMut;
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

/// MTU — WebRTC RTP 包最大 1500 字节 (以太网 MTU)
pub const MTU: usize = 1500;

/// recvmmsg 批量大小
pub const BATCH_SIZE: usize = 64;

/// sendmmsg 批量大小
pub const SEND_BATCH_SIZE: usize = 128;

/// SendBatch 最大积压包数 (约 8192 × 1.5KB ≈ 12MB)。
/// 超过时丢弃最旧的包，避免发送缓冲长时间满时内存无限增长。
pub const MAX_SEND_BACKLOG: usize = 8192;

/// recv/send iov 栈上数组的最大容量。
///
/// 批量收发需要 `IoSlice`/`IoSliceMut` 切片，而它们借用 `bufs`/`packets`，
/// 无法作为结构体字段 (自引用)。这里用固定容量的 `ArrayVec` 在栈上构造，
/// 避免每次系统调用都堆分配一个 `Vec`。
pub const MAX_BATCH: usize = 256;

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
    pub fn new(addr: SocketAddr, reuse_port: bool, buf_bytes: usize) -> std::io::Result<Self> {
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

        // 增大接收/发送缓冲区。实际生效值受内核 `net.core.rmem_max`/
        // `wmem_max` 限制 (被截断)，这里回读并记录实际值，避免“以为设了很大”。
        let buf_size = buf_bytes.max(212_992);
        let _ = setsockopt(&fd, RcvBuf, &buf_size);
        let _ = setsockopt(&fd, SndBuf, &buf_size);
        let actual_rcv = getsockopt(&fd, RcvBuf).unwrap_or(0);
        let actual_snd = getsockopt(&fd, SndBuf).unwrap_or(0);
        tracing::debug!(
            requested = buf_size,
            rcvbuf = actual_rcv,
            sndbuf = actual_snd,
            "UDP socket 缓冲区已设置 (实际值受内核 rmem_max/wmem_max 限制)"
        );

        // 绑定地址
        let nix_addr = socketaddr_to_nix(addr);
        nix::sys::socket::bind(fd.as_raw_fd(), &nix_addr).map_err(nix_to_io)?;

        // 设置非阻塞
        // nix 0.30+ 的 fcntl 接收 AsFd，不再接受裸 fd。
        let flags =
            nix::fcntl::fcntl(&fd, nix::fcntl::FcntlArg::F_GETFL).map_err(nix_to_io)?;
        let mut flags = nix::fcntl::OFlag::from_bits_truncate(flags);
        flags |= nix::fcntl::OFlag::O_NONBLOCK;
        nix::fcntl::fcntl(&fd, nix::fcntl::FcntlArg::F_SETFL(flags)).map_err(nix_to_io)?;

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

    /// 回读当前 socket 实际生效的 (rcvbuf, sndbuf) 字节数
    pub fn buffer_sizes(&self) -> (usize, usize) {
        let rcv = getsockopt(&self.fd, RcvBuf).unwrap_or(0);
        let snd = getsockopt(&self.fd, SndBuf).unwrap_or(0);
        (rcv, snd)
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
        assert!(
            capacity <= MAX_BATCH,
            "recv capacity {} 超过 MAX_BATCH={}",
            capacity,
            MAX_BATCH
        );
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

        // 固定容量栈上数组构造 iov，避免每次 recvmmsg 堆分配 Vec。
        //
        // 用 `MaybeUninit` 而不是 `ArrayVec`: `ArrayVec` 实现了 `Drop`，而 nix 的
        // 返回迭代器把 iov 的借用生命周期与 headers 统一为整个函数，会触发
        // drop-check 报错。`MaybeUninit` 数组不实现 `Drop`，不受影响。
        let mut storage: [MaybeUninit<[IoSliceMut<'_>; 1]>; MAX_BATCH] =
            [const { MaybeUninit::uninit() }; MAX_BATCH];
        let n_bufs = bufs.len();
        for (slot, buf) in storage.iter_mut().zip(bufs.iter_mut()) {
            slot.write([IoSliceMut::new(buf)]);
        }

        // SAFETY: 上面的循环已初始化前 n_bufs 个元素。
        let iovs: &mut [[IoSliceMut<'_>; 1]] = unsafe {
            std::slice::from_raw_parts_mut(
                storage.as_mut_ptr().cast::<[IoSliceMut<'_>; 1]>(),
                n_bufs,
            )
        };

        // 执行 recvmmsg — 单次系统调用收最多 capacity 个包
        let results = recvmmsg(
            fd,
            headers,                // &mut MultiHeaders (预分配)
            iovs.iter_mut(),        // 栈上 [IoSliceMut; 1] 数组
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
                .map(nix_to_socketaddr)
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
    /// 待发送的包: (目标地址, 数据)。数据为 str0m Transmit 移出的 Vec，
    /// 避免 `copy_from_slice` 造成的逐包堆分配与内存拷贝。
    packets: Vec<(SocketAddr, Vec<u8>)>,
    /// 预分配的 nix MultiHeaders
    headers: MultiHeaders<SockaddrStorage>,
    capacity: usize,
    /// 因积压超限而丢弃的包数 (诊断用)
    dropped: u64,
}

impl SendBatch {
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity <= MAX_BATCH,
            "send capacity {} 超过 MAX_BATCH={}",
            capacity,
            MAX_BATCH
        );
        // 预分配 MultiHeaders (nix 0.29 使用 preallocate)
        let headers = MultiHeaders::<SockaddrStorage>::preallocate(capacity, None);

        Self {
            packets: Vec::with_capacity(capacity),
            headers,
            capacity,
            dropped: 0,
        }
    }

    /// 添加一个待发送的包 (接管 data 所有权，零拷贝)
    ///
    /// 不再因超出 `capacity` 而丢弃: `flush` 会按 `capacity` 分块多次
    /// sendmmsg 发出。大关键帧 x 多订阅者时不会丢包。
    pub fn push(&mut self, dest: SocketAddr, data: Vec<u8>) {
        // 积压超限: 丢弃最旧的包，保留新鲜数据。
        if self.packets.len() >= MAX_SEND_BACKLOG {
            let overflow = self.packets.len() - MAX_SEND_BACKLOG + 1;
            self.packets.drain(0..overflow);
            self.dropped += overflow as u64;
        }
        self.packets.push((dest, data));
    }

    /// 累计因积压超限丢弃的包数
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    /// 清空缓冲区
    pub fn clear(&mut self) {
        self.packets.clear();
    }

    /// 执行 sendmmsg 批量发包。
    ///
    /// 尽可能发送队列中的包 (每 `capacity` 个一次 sendmmsg)。
    /// 若内核发送缓冲已满 (EAGAIN)，`sendmmsg` 只发出前缀，**未发出的部分
    /// 保留在队列中**，由下一轮事件循环重试，而不是静默丢弃。
    /// 返回本次实际发出的包数。
    pub fn flush(&mut self, fd: RawFd) -> std::io::Result<usize> {
        if self.packets.is_empty() {
            return Ok(0);
        }

        let chunk_size = self.capacity.max(1);
        let cmsgs: &[nix::sys::socket::ControlMessage] = &[];
        let mut total = 0usize;
        let mut done = 0usize; // 已成功发出的前缀长度

        while done < self.packets.len() {
            let end = (done + chunk_size).min(self.packets.len());
            let chunk = &self.packets[done..end];

            // addrs 不借用 packets，用 ArrayVec (栈上) 即可。
            let addrs: ArrayVec<Option<SockaddrStorage>, MAX_BATCH> = chunk
                .iter()
                .map(|(addr, _)| Some(socketaddr_to_nix(*addr)))
                .collect();

            // iov 借用 packets，同 recv: 用无 Drop 的 MaybeUninit 栈数组。
            let mut storage: [MaybeUninit<[IoSlice<'_>; 1]>; MAX_BATCH] =
                [const { MaybeUninit::uninit() }; MAX_BATCH];
            for (slot, (_, data)) in storage.iter_mut().zip(chunk.iter()) {
                slot.write([IoSlice::new(data.as_slice())]);
            }

            // SAFETY: 上面的循环已初始化前 chunk.len() 个元素。
            let iovs: &[[IoSlice<'_>; 1]] = unsafe {
                std::slice::from_raw_parts(
                    storage.as_ptr().cast::<[IoSlice<'_>; 1]>(),
                    chunk.len(),
                )
            };

            let results = sendmmsg(
                fd,
                &mut self.headers,
                iovs.iter(),
                addrs.as_slice(),
                cmsgs,
                MsgFlags::MSG_DONTWAIT,
            )
            .map_err(nix_to_io)?;

            let sent = results.count();
            total += sent;
            done += sent;

            if sent < chunk.len() {
                // 发送缓冲已满: 保留未发出的尾部，下一轮重试。
                break;
            }
        }

        // 移除已成功发出的前缀，保留未发出的包。
        if done > 0 {
            self.packets.drain(0..done);
        }

        Ok(total)
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
