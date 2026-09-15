# 高性能 WebRTC SFU 服务设计文档

> 基于 **str0m** (Sans I/O WebRTC) + **nix recvmmsg** 批量收包 + **epoll** 事件驱动的 SFU 架构

---

## 1. 架构总览

```
                        ┌─────────────────────────────────────────────────┐
                        │              Signaling Layer (Tokio)             │
                        │   WebSocket Server · SDP 交换 · Room 管理        │
                        └────────┬──────────────────────────┬─────────────┘
                                 │ join/leave               │ SDP answer
                                 ▼                          ▼
                    ┌────────────────────────────────────────────────┐
                    │          Cross-Thread Command Channel            │
                    │   (crossbeam SPSC, 无锁, 每个 worker 一个)       │
                    └────┬───────────┬───────────┬───────────┬────────┘
                         │           │           │           └────────┐
                         ▼           ▼           ▼                    ▼
  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    ┌──────────┐
  │ Worker 0 │  │ Worker 1 │  │ Worker 2 │  │ Worker 3 │    │ Worker N │
  │ CPU 0    │  │ CPU 1    │  │ CPU 2    │  │ CPU 3    │    │ CPU N    │
  │ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │    │ ┌──────┐ │
  │ │ UDP  │ │  │ │ UDP  │ │  │ │ UDP  │ │  │ │ UDP  │ │    │ │ UDP  │ │
  │ │Socket│ │  │ │Socket│ │  │ │Socket│ │  │ │Socket│ │    │ │Socket│ │
  │ │REUSE│  │  │ │REUSE│  │  │ │REUSE│  │  │ │REUSE│  │    │ │REUSE│  │
  │ │PORT  │ │  │ │PORT  │ │  │ │PORT  │ │  │ │PORT  │ │    │ │PORT  │ │
  │ └──┬───┘ │  │ └──┬───┘ │  │ └──┬───┘ │  │ └──┬───┘ │    │ └──┬───┘ │
  │    │     │  │    │     │  │    │     │  │    │     │    │    │     │
  │  recvmmsg  │  │  recvmmsg  │  │  recvmmsg  │  │  recvmmsg  │  │  recvmmsg
  │  (batch×64)│  │  (batch×64)│  │  (batch×64)│  │  (batch×64)│  │  (batch×64)
  │    │     │  │    │     │  │    │     │  │    │     │    │    │     │
  │  ┌──▼───┐ │  │ ┌──▼───┐ │  │ ┌──▼───┐ │  │ ┌──▼───┐ │    │ ┌──▼───┐ │
  │  │Rtc×M │ │  │ │Rtc×M │ │  │ │Rtc×M │ │  │ │Rtc×M │ │    │ │Rtc×M │ │
  │  │str0m │ │  │ │str0m │ │  │ │str0m │ │  │ │str0m │ │    │ │str0m │ │
  │  └──┬───┘ │  │ └──┬───┘ │  │ └──┬───┘ │  │ └──┬───┘ │    │ └──┬───┘ │
  │  ┌──▼───┐ │  │ ┌──▼───┐ │  │ ┌──▼───┐ │  │ ┌──▼───┐ │    │ ┌──▼───┐ │
  │  │Router│ │  │ │Router│ │  │ │Router│ │  │ │Router│ │    │ │Router│ │
  │  └──┬───┘ │  │ └──┬───┘ │  │ └──┬───┘ │  │ └──┬───┘ │    │ └──┬───┘ │
  │  sendmmsg │  │  sendmmsg │  │  sendmmsg │  │  sendmmsg │    │  sendmmsg
  └──────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
         ↑                                                       ↑
         └──────────── 房间分片: 每个房间固定到一个 Worker ──────────┘
                      每个 Worker 使用独立媒体端口 (base+i)
```

> **为什么不用 SO_REUSEPORT 共享同一端口？**
> 内核的 SO_REUSEPORT 按四元组哈希选择 socket，**无法保证**包落到持有该
> participant 的 worker，于是包会被其它 worker 丢弃。因此改为「房间级分片」：
> 信令层用 `select_worker(room_id)` 把同一房间的所有参与者（发布者 + 订阅连接）
> 固定到同一个 worker；每个 worker 监听自己的媒体端口 `base_port + cpu_id`，
> 并在 ICE 候选里公布该端口。这样客户端直接连到正确的 worker，且同一房间的
> 发布/订阅转发始终在 worker 内部完成，无需跨 worker 通信。
> 代价：需要放行一个媒体端口段 (端口数 = worker 数)。

### 核心设计原则

| 原则 | 实现方式 |
|------|----------|
| **Share-Nothing** | 每个 Worker 线程独占一个 CPU 核心，拥有独立的 UDP socket (独立端口)、Rtc 实例、路由表，无共享可变状态；房间级分片保证同房间不跨 Worker |
| **Batch I/O** | 使用 `recvmmsg` 批量收包 (64 packets/syscall)、`sendmmsg` 批量发包，减少 80%+ 系统调用 |
| **Sans I/O 驱动** | str0m 不做任何网络 I/O，由 Worker 事件循环主动喂数据和驱动时间 |
| **零拷贝路由** | RTP 包在参与者间转发使用 `Bytes` 引用计数，避免内存拷贝 |
| **CPU 亲和性** | 每个 Worker 绑定到固定 CPU 核心，减少 cache miss 和上下文切换 |

---

## 2. 线程模型

### 2.1 Thread-per-Core (Share-Nothing)

```
┌──────────────────────────────────────────────────────────┐
│                    Main Thread                           │
│  · 解析配置                                              │
│  · 创建 N 个 Worker 线程 (N = CPU 核心数)                │
│  · 启动 Tokio Runtime 跑信令服务器                       │
│  · 等待 shutdown 信号                                    │
└──────────────────────────────────────────────────────────┘
          │
          ├──► Worker Thread 0  (CPU affinity: core 0)
          │    · 独立 UDP socket (独立媒体端口: base_port + core_id)
          │    · 独立 epoll 实例
          │    · 独立 Rtc 实例池
          │    · 独立路由表
          │    · 无锁运行
          │
          ├──► Worker Thread 1  (CPU affinity: core 1)
          │    · ... (同上)
          │
          └──► Worker Thread N  (CPU affinity: core N)
               · ... (同上)

┌──────────────────────────────────────────────────────────┐
│              Tokio Runtime (信令线程池)                   │
│  · WebSocket 连接管理                                    │
│  · SDP offer/answer 交换                                 │
│  · Room 创建/销毁                                        │
│  · 通过 crossbeam channel 向 Worker 发送命令              │
│  · 从 Worker 接收 SDP answer 回传给客户端                │
└──────────────────────────────────────────────────────────┘
```

### 2.2 跨线程通信

信令层 (async) 与媒体层 (sync) 之间通过 **无锁 SPSC channel** 通信：

```rust
// 信令 → Worker (命令)
enum WorkerCommand {
    CreateParticipant { room_id, participant_id, sdp_offer, reply_tx },
    AddRemoteCandidate { participant_id, candidate },
    RemoveParticipant { participant_id },
    Subscribe { subscriber_id, publisher_id, mid },
    Unsubscribe { subscriber_id, publisher_id, mid },
}

// Worker → 信令 (回复)
enum WorkerReply {
    SdpAnswer { participant_id, sdp_answer },
    ParticipantConnected { participant_id },
    ParticipantDisconnected { participant_id },
    Error { participant_id, error },
}
```

---

## 3. I/O 模型 — recvmmsg + epoll

### 3.1 批量收包流程

```
                    epoll_wait(fd, timeout)
                           │
                    ┌──────▼──────┐
                    │  data ready? │
                    └──┬─────┬────┘
                       │     │ no
                       │     └──► 驱动 Timer + 发送超时包
              yes     │
                       ▼
              ┌────────────────┐
              │  recvmmsg(fd,  │  ← 单次系统调用收 64 个包
              │  batch=64,     │
              │  MSG_DONTWAIT) │
              └───────┬────────┘
                      │
              ┌───────▼────────┐
              │  遍历 64 个包   │
              │  对每个包:      │
              │  1. 解析 ICE    │
              │     ufrag       │
              │  2. 查表找到    │
              │     Rtc 实例    │
              │  3. handle_input│
              │  4. poll_output │
              │     → Transmit  │
              │     → Event     │
              └───────┬────────┘
                      │
              ┌───────▼────────┐
              │  收集所有        │
              │  Transmit 包    │
              │  到 SendBatch   │
              └───────┬────────┘
                      │
              ┌───────▼────────┐
              │  sendmmsg(fd,  │  ← 单次系统调用发 N 个包
              │  send_batch)   │
              └────────────────┘
                      │
              ┌───────▼────────┐
              │  循环 recvmmsg  │  ← 如果收满 64 个包，
              │  直到 EAGAIN    │    说明还有更多数据
              └────────────────┘
```

### 3.2 关键优化

| 优化点 | 说明 |
|--------|------|
| **房间级分片** | `select_worker(room_id)` 将同一房间固定到同一 Worker；每个 Worker 监听独立媒体端口 `base_port + cpu_id`，ICE 候选公布该端口，避免内核 SO_REUSEPORT 把包分到错误 Worker |
| **SO_BUSY_POLL** | 内核级忙轮询，减少软中断延迟 (需 Linux 3.11+, 适合低延迟场景) |
| **Edge-Triggered epoll** | 配合 recvmmsg 循环读到 EAGAIN，避免 Level-Triggered 的重复唤醒 |
| **预分配缓冲区** | RecvBatch/SendBatch 在 Worker 启动时分配，热路径零分配 |
| **MSG_DONTWAIT** | recvmmsg 非阻塞模式，避免在没有数据时阻塞 |

### 3.3 recvmmsg vs recvfrom 性能对比

```
                    传统 recvfrom              recvmmsg (batch=64)
                    ──────────────             ───────────────────
  收 64 个包:        64 次 syscall             1 次 syscall
  上下文切换:        64 次                     1 次
  用户→内核拷贝:     64 次                     1 次 (批量)
  典型延迟:          ~50μs (64×0.8μs)          ~2μs
```

---

## 4. str0m 集成设计

### 4.1 Sans I/O 驱动模式

str0m 是 **Sans I/O** 实现 — `Rtc` 实例本身不做任何网络 I/O，也没有内部线程。
所有操作通过 API 调用同步完成：

```rust
// str0m 核心 API (v0.21.0)
pub enum Input<'a> {
    Timeout(Instant),                    // 驱动时间前进
    Receive(Instant, Receive<'a>),       // 喂入网络数据包
}

pub enum Output {
    Timeout(Instant),                    // 下一次需要 Timeout 的时间
    Transmit(Transmit),                  // 需要通过 UDP 发出的包
    Event(Event),                        // 事件 (媒体到达、连接状态等)
}

// 核心循环不变量: 每次 handle_input 后必须完整 drain poll_output
// 直到返回 Output::Timeout，才能进行下一次 handle_input
```

### 4.2 每参与者一个 Rtc 实例

```
  ┌─────────────────────────────────────────────────┐
  │                Worker Thread                     │
  │                                                  │
  │  ┌─────────────┐  ┌─────────────┐  ┌──────────┐│
  │  │ Participant │  │ Participant │  │Participant││
  │  │   Alice     │  │    Bob      │  │  Carol   ││
  │  │ ┌─────────┐ │  │ ┌─────────┐ │  │┌─────────┐││
  │  │ │  Rtc    │ │  │ │  Rtc    │ │  ││  Rtc    │││
  │  │ │ (str0m) │ │  │ │ (str0m) │ │  ││ (str0m) │││
  │  │ └─────────┘ │  │ └─────────┘ │  │└─────────┘││
  │  │  publishes:  │  │  publishes:  │  │ publishes:│
  │  │  - video     │  │  - audio     │  │ - video  ││
  │  │  - audio     │  │              │  │          ││
  │  │  subscribes: │  │  subscribes: │  │subscribes:│
  │  │  - Bob.audio │  │  - Alice.vid │  │ -Alice.vid││
  │  │  - Carol.vid │  │  - Alice.aud │  │ -Bob.aud ││
  │  └─────────────┘  └─────────────┘  └──────────┘│
  │                                                  │
  │  ┌─────────────────────────────────────────────┐│
  │  │              Router (路由表)                  ││
  │  │  publisher_track → [subscriber_writer, ...]  ││
  │  └─────────────────────────────────────────────┘│
  └─────────────────────────────────────────────────┘
```

### 4.3 包路由流程

```rust
// 1. recvmmsg 收到包
let (source, data) = recv_batch.get(i);

// 2. 查找匹配的 Rtc 实例 (通过 ICE ufrag 或 accepts())
let participant = find_participant(source, data);

// 3. 喂入 str0m 处理 (解密 SRTP、处理 STUN/DTLS)
let receive = Receive::new(Protocol::Udp, source, local_addr, data)?;
participant.rtc.handle_input(Input::Receive(now, receive))?;

// 4. Drain outputs
while let Ok(output) = participant.rtc.poll_output() {
    match output {
        Output::Transmit(t) => {
            // 需要发送回客户端的包 (STUN/DTLS/SRTP)
            send_batch.push(t.destination, &t.contents);
        }
        Output::Event(Event::MediaData { mid, pt, time, data, .. }) => {
            // 5. 收到媒体数据，通过 Router 转发给订阅者
            router.forward(participant.id, mid, pt, time, &data, &mut send_batch);
        }
        Output::Event(_) => { /* 处理其他事件 */ }
        Output::Timeout(_) => break,
    }
}

// 6. Router.forward 内部:
//    对每个订阅者, 用其 Rtc.writer(mid) 写入媒体数据
//    然后 drain 订阅者的 poll_output → 收集 Transmit → send_batch
```

---

## 5. SFU 路由引擎

### 5.1 路由表结构

```rust
struct Router {
    /// 发布者 → 发布的轨道
    /// key: (participant_id, mid)
    /// value: 轨道元信息 (类型、simulcast layers 等)
    publishers: HashMap<(ParticipantId, Mid), PublishedTrack>,

    /// 订阅关系: 发布轨道 → 订阅者列表
    /// 每个订阅者包含: 订阅者 ID + 用于写入的 Writer
    subscriptions: HashMap<(ParticipantId, Mid), Vec<Subscription>>,

    /// 反向索引: 订阅者 → 订阅了哪些轨道
    subscriber_tracks: HashMap<ParticipantId, Vec<(ParticipantId, Mid)>>,
}

struct PublishedTrack {
    publisher_id: ParticipantId,
    mid: Mid,
    kind: MediaKind,         // Audio / Video
    simulcast_layers: Vec<Rid>,  // 空表示非 simulcast
    current_layer: Option<Rid>,  // 当前活跃层
}

struct Subscription {
    subscriber_id: ParticipantId,
    mid: Mid,                    // 订阅者在自己的 Rtc 中的 mid
    selected_layer: Option<Rid>, // 选择的 simulcast 层
}
```

### 5.2 Simulcast 层选择

```
  Publisher (Alice) 发送 3 层视频:
  
  Layer "low"   (180p,  150kbps)  ──────┐
  Layer "mid"   (360p,  500kbps)  ──┐   │
  Layer "high"  (720p, 1500kbps) ─┐ │   │
                                 │ │   │
                          ┌──────▼─▼───▼──────┐
                          │     SFU Router     │
                          └──┬─────┬──────┬───┘
                             │     │      │
                ┌────────────▼┐ ┌──▼────┐ ┌▼────────────┐
                │ Subscriber  │ │ Sub B │ │  Subscriber │
                │     A       │ │       │ │     C       │
                │ 带宽: 2Mbps │ │1Mbps  │ │  500kbps    │
                │ 选择: high  │ │ mid   │ │  low        │
                └─────────────┘ └───────┘ └─────────────┘
```

### 5.3 RTP 转发优化

```rust
impl Router {
    fn forward(
        &mut self,
        publisher_id: ParticipantId,
        mid: Mid,
        pt: u8,
        time: MediaTime,
        data: &[u8],
        send_batch: &mut SendBatch,
    ) {
        // 1. 查找订阅者
        let subs = match self.subscriptions.get(&(publisher_id, mid)) {
            Some(s) => s,
            None => return,
        };

        // 2. 对每个订阅者转发
        for sub in subs {
            let participant = match self.participants.get_mut(&sub.subscriber_id) {
                Some(p) => p,
                None => continue,
            };

            // 3. 使用 str0m Writer 写入媒体数据
            //    str0m 内部处理 SRTP 加密、RTP 打包
            if let Some(writer) = participant.rtc.writer(sub.mid) {
                if writer.write(now, time, pt, data).is_ok() {
                    // 4. Drain 输出，收集 Transmit
                    while let Ok(output) = participant.rtc.poll_output() {
                        match output {
                            Output::Transmit(t) => {
                                send_batch.push(t.destination, &t.contents);
                            }
                            Output::Timeout(_) => break,
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}
```

---

## 6. 内存管理

### 6.1 缓冲区池化

```rust
/// 预分配的收包缓冲区 (Worker 启动时分配，热路径零分配)
struct RecvBatch {
    /// 64 × 1500 字节的包缓冲区
    bufs: Vec<Vec<u8>>,
    /// nix MultiHeaders (预分配 mmsghdr 数组)
    headers: MultiHeaders<SockaddrStorage>,
}

/// 预分配的发包缓冲区
struct SendBatch {
    /// 待发送的包列表 (目标地址 + 数据)
    packets: Vec<(SocketAddr, Bytes)>,
    headers: MultiHeaders<SockaddrStorage>,
}
```

### 6.2 零拷贝策略

| 数据路径 | 策略 |
|----------|------|
| 收包 → str0m | `Receive::new()` 接收 `&[u8]` 引用，零拷贝 |
| str0m → Router | `Event::MediaData.data` 为 `Vec<u8>`，转换为 `Bytes` |
| Router → 订阅者 str0m | `Bytes` 引用计数克隆，多订阅者共享同一底层数据 |
| str0m → sendmmsg | `Transmit.contents` 为 `Bytes`，直接传给 sendmmsg |

---

## 7. Worker 事件循环 (核心)

```rust
fn run_worker(config: WorkerConfig, cmd_rx: Receiver<WorkerCommand>) {
    // 1. 初始化
    // 每个 worker 使用独立媒体端口 (base_port + cpu_id)
    let socket = UdpSocket::new(config.listen_addr, false);
    let poll = Epoll::new();
    poll.add(socket.fd(), EPOLLIN | EPOLLET);

    let mut recv_batch = RecvBatch::new(BATCH_SIZE, MTU);
    let mut send_batch = SendBatch::new(SEND_BATCH_SIZE);
    let mut participants: HashMap<ParticipantId, Participant> = HashMap::new();
    let mut router = Router::new();
    let mut next_timeout = Instant::now() + Duration::from_millis(100);

    // 2. 设置 CPU 亲和性
    set_cpu_affinity(config.cpu_id);

    // 3. 主循环
    loop {
        let now = Instant::now();

        // ── Phase 1: 批量收包 ──
        loop {
            match socket.recvmmsg(&mut recv_batch) {
                Ok(0) => break,  // EAGAIN, 没有更多数据
                Ok(n) => {
                    for i in 0..n {
                        let (source, data) = recv_batch.get(i);
                        process_packet(source, data, now, &mut participants,
                                       &mut router, &mut send_batch);
                    }
                    if n < BATCH_SIZE { break; }  // 没收满，说明数据已读完
                }
                Err(_) => break,
            }
        }

        // ── Phase 2: 驱动 Timer ──
        if now >= next_timeout {
            for participant in participants.values_mut() {
                participant.rtc.handle_input(Input::Timeout(now)).ok();
                drain_output(participant, &mut router, &mut send_batch);
            }
            next_timeout = now + Duration::from_millis(10);
        }

        // ── Phase 3: 处理信令命令 ──
        while let Ok(cmd) = cmd_rx.try_recv() {
            handle_command(cmd, &mut participants, &mut router);
        }

        // ── Phase 4: 批量发包 ──
        if !send_batch.is_empty() {
            socket.sendmmsg(&mut send_batch).ok();
            send_batch.clear();
        }

        // ── Phase 5: epoll 等待 ──
        let timeout = next_timeout.saturating_duration_since(now);
        poll.wait(timeout);  // 阻塞直到有数据或超时
    }
}
```

---

## 8. 信令服务器

### 8.1 WebSocket 协议

一条 WebSocket 连接可同时承载多个 RTCPeerConnection，用 `pc_id` 区分：

- `pc_id = "pub"`：发布连接（sendonly），加入房间
- `pc_id = "sub-<publisherId>"`：订阅连接（recvonly），不加入房间

同一个用户无论订阅多少路远端，都只维持 **一条** WebSocket；服务端为每个 `pc_id`
创建一个独立的 str0m `Rtc` 实例，但共用同一条信令通道。如发布者媒体变更，
订阅端仍复用同一连接。

```json
// 客户端 → 服务器: SDP Offer (发布连接)
{
  "type": "join",
  "pc_id": "pub",
  "room_id": "room-123",
  "participant_id": "alice-uuid",
  "sdp": "v=0\r\no=- 123456 2 IN IP4 127.0.0.1\r\n...",
  "role": "publisher"
}

// 客户端 → 服务器: SDP Offer (订阅连接, 复用同一条 WebSocket)
{
  "type": "join",
  "pc_id": "sub-bob-uuid",
  "room_id": "room-123",
  "participant_id": "alice-uuid-recv-bob-uuid",
  "sdp": "v=0\r\no=- 123456 2 IN IP4 127.0.0.1\r\n...",
  "role": "subscriber"
}
// role: "publisher" (默认, 发布并加入房间) 或 "subscriber" (recvonly 订阅连接)
// subscriber 连接不会加入房间, 也不会触发 participant_joined 广播

// 服务器 → 客户端: SDP Answer (带 pc_id, 客户端据此路由到对应 PC)
{
  "type": "sdp_answer",
  "pc_id": "sub-bob-uuid",
  "sdp": "v=0\r\no=- 789012 2 IN IP4 127.0.0.1\r\n...",
  "ice_candidates": ["candidate:1 1 udp 2122252543 1.2.3.4 5175 typ host"]
}

// 客户端 → 服务器: Trickle ICE Candidate
{
  "type": "ice_candidate",
  "pc_id": "sub-bob-uuid",
  "candidate": "candidate:842163049 1 udp 1677729535 ...",
  "sdp_mid": "0",
  "sdp_mline_index": 0
}

// 客户端 → 服务器: 订阅轨道
{
  "type": "subscribe",
  "pc_id": "sub-bob-uuid",
  "publisher_id": "bob-uuid",
  "track_mid": "1"
}

// 客户端 → 服务器: 关闭某个 PC 会话
{ "type": "leave", "pc_id": "sub-bob-uuid" }
```

### 8.2 信令流程

```
  Browser                  Signaling Server              Worker Thread
     │                            │                            │
     │── WebSocket Connect ──────►│                            │
     │                            │                            │
     │── join (SDP Offer) ───────►│                            │
     │                            │── WorkerCommand::Create ──►│
     │                            │   {sdp_offer, reply_tx}    │
     │                            │                            │
     │                            │                     ┌──────┤
     │                            │                     │ Rtc::new()
     │                            │                     │ sdp_api().accept_offer()
     │                            │                     │ add_local_candidate()
     │                            │                     │ drain poll_output()
     │                            │                     └──────┤
     │                            │◄── SdpAnswer ──────────────│
     │◄── sdp_answer ─────────────│                            │
     │                            │                            │
     │── ice_candidate ─────────►│── AddRemoteCandidate ─────►│
     │                            │                            │
     │   (ICE connectivity check happens via UDP media path)   │
     │                            │                            │
     │◄── participant_connected ──│◄── Connected event ────────│
     │                            │                            │
     │── subscribe ──────────────►│── Subscribe command ──────►│
     │                            │                            │
     │   (RTP media flows directly via UDP, not through signaling) │
```

---

## 9. 性能指标预期

| 指标 | 预期值 | 说明 |
|------|--------|------|
| 单 Worker 并发连接 | 500-1000 | 取决于 simulcast 层数和带宽 |
| 包转发延迟 (P99) | < 1ms | recvmmsg + 零拷贝路由 |
| 吞吐量 (包/秒) | 100K+ | 64 包/批 × 2000 批/秒 |
| CPU 利用率 | > 95% | 忙轮询模式下接近满载 |
| 内存/连接 | ~2MB | str0m Rtc 实例 + 缓冲区 |

---

## 10. 项目结构

```
webrtc-sfu/
├── Cargo.toml
├── README.md                    ← 本文件
└── src/
    ├── main.rs                  ← 入口: 启动 workers + signaling
    ├── config.rs                ← 配置定义
    ├── net/
    │   ├── mod.rs
    │   ├── udp.rs               ← recvmmsg/sendmmsg UDP socket (nix)
    │   └── poll.rs              ← epoll 封装 (nix)
    ├── core/
    │   ├── mod.rs
    │   ├── worker.rs            ← Worker 事件循环 (核心)
    │   └── timer.rs             ← 定时器轮
    ├── sfu/
    │   ├── mod.rs
    │   ├── room.rs              ← Room 管理
    │   ├── participant.rs       ← Participant + str0m Rtc 封装
    │   ├── router.rs            ← RTP 路由引擎
    │   └── track.rs             ← 轨道表示
    └── signaling/
        ├── mod.rs
        └── server.rs            ← WebSocket 信令服务器
```

---

## 11. 构建 & 运行

```bash
# 需要 Linux (recvmmsg/epoll/SO_REUSEPORT 是 Linux 特有 API)
# Rust 1.75+

# 构建 (release 模式, LTO + 单 codegen-unit 优化)
cargo build --release

# 运行
RUST_LOG=info ./target/release/sfu-server --port 5004 --ws-port 8080 --workers 4

# 环境变量调优
SO_BUSY_POLL=50          # 内核忙轮询微秒数 (0=禁用)
BATCH_SIZE=64            # recvmmsg 批量大小
```

---

## 12. 关键 API 对照

### str0m v0.21.0

```rust
// 创建 Rtc 实例
let rtc = Rtc::builder()
    .set_ice_lite(true)         // SFU 使用 ICE Lite
    .enable_bwe()                // 启用带宽估计
    .build(Instant::now());

// SDP 协商
let mut sdp = rtc.sdp_api();
let answer = sdp.accept_offer(&sdp_offer)?;

// 添加 ICE 候选
rtc.add_local_candidate(Candidate::host(addr, Protocol::Udp));

// 输入网络包
let receive = Receive::new(Protocol::Udp, source, local_addr, &buf)?;
rtc.handle_input(Input::Receive(now, receive))?;

// 驱动时间
rtc.handle_input(Input::Timeout(now))?;

// 获取输出
match rtc.poll_output()? {
    Output::Transmit(t) => { /* 发送 t.contents → t.destination */ }
    Output::Event(e) => { /* 处理事件 */ }
    Output::Timeout(t) => { /* 记录下次超时时间 */ }
}

// 检查是否接受输入 (用于多路复用)
if rtc.accepts(&input) { /* 这个包属于这个 Rtc */ }

// 发送媒体 (用于 SFU 转发)
let writer = rtc.writer(mid);
writer.write(now, time, pt, &data)?;
```

### nix v0.31 recvmmsg

```rust
use nix::sys::socket::{recvmmsg, MultiHeaders, MsgFlags, SockaddrStorage};

// 预分配 headers (仅 alloc 一次)
let mut headers = MultiHeaders::<SockaddrStorage>::alloc(&iovs);

// 每次收包: 创建 iovs → recvmmsg
let mut iovs: Vec<[IoSliceMut<'_>; 1]> = bufs.iter_mut()
    .map(|b| [IoSliceMut::new(b)]).collect();
let results = recvmmsg(fd, &mut headers, &mut iovs, MsgFlags::empty(), None)?;

for recv_msg in results {
    let addr = recv_msg.address();      // 源地址
    let bytes = recv_msg.iovs();        // 收到的数据
}
```

---

## 13. 扩展方向

- **io_uring**: 用 `io_uring` 替代 `epoll + recvmmsg`，进一步减少系统调用 (Linux 5.1+)
- **Kernel BPF**: 用 eBPF 做更精细的包路由和负载均衡
- **QUIC 传输**: 支持基于 QUIC 的 WebRTC (WebTransport)
- **分布式 SFU**: 跨机器 Room 联邦，支持更大规模会议
- **GPU 加速**: 视频转码/合流 (SFU + MCU 混合模式)
