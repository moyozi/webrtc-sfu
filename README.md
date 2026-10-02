# 高性能 WebRTC SFU 服务设计文档

> 基于 **str0m** (Sans I/O WebRTC) + **nix recvmmsg** 批量收包 + **epoll** 事件驱动的 SFU 架构

---

> 📖 **部署与各功能使用请看 [`docs/manual.md`](docs/manual.md)**（安装、配置、浏览器、
> FFmpeg/GStreamer/OBS 推流、单机/多 worker/跨主机集群、压测、排障）。
> **完整部署手册（单机单进程 / 多信令机+多媒体节点，逐变量说明）见
> [`docs/deployment.md`](docs/deployment.md)。**
> 跨线程/跨主机中继设计见 [`docs/multi-worker-relay.md`](docs/multi-worker-relay.md)。
> 信令多机部署（客户端按 `room_id` 分配信令机）见 [`docs/signaling-dispatcher.md`](docs/signaling-dispatcher.md)。
> **单核 fan-out 容量实测与 I/O 后端对比见 [`docs/capacity.md`](docs/capacity.md)。**

---

## 1. 架构总览

```
                        ┌─────────────────────────────────────────────────┐
                        │              Signaling Layer (Tokio)             │
                        │  QUIC (优先) + WebSocket · SDP 交换 · Room 管理  │
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
> participant 的 worker，于是包会被其它 worker 丢弃。因此每个 worker 监听
> 自己的媒体端口 `base_port + cpu_id`，并在 ICE 候选里公布该端口；信令层通过
> `assign_worker` 按房间分配 worker。**默认按房间 id 哈希把每个房间固定到一个
> worker**（同房间同 worker，不同房间自动分散）；单房间要跨 worker（单机多
> worker / 多主机多 worker）需要显式配置 `SFU_ROOM_WORKERS`（见下）。

### 单房间多 Worker（跨线程订阅）

> **默认不启用**：同一房间的参与者默认全部落在哈希得到的那一个 worker 上。
> 若要让某个房间跨 worker，需要显式配置该房间，例如
> `SFU_ROOM_WORKERS="load=0,1,2,3"`（单机多 worker）或
> `SFU_ROOM_WORKERS="load=0,2,4,6"`（多主机多 worker，编号为整集群全局 worker）。

配置后，同一房间可跨多个 worker：订阅者与发布者若落在不同 worker，媒体经
**进程内中继平面**转发 —— 发布者 worker 解密出明文 RTP，通过 `RelayHub` 把
`Arc<[u8]>` 载荷经 channel 送给订阅者所在 worker，后者用自己的 `Rtc`
完成 PT 映射 + 逐订阅者 SRTP。详见 [docs/multi-worker-relay.md](docs/multi-worker-relay.md)。

```
Publisher(w0) --解密--> forward --local--> w0 本地订阅者
                             \--RelayHub--> w1/w2 订阅者(write_rtp+SRTP)
```

`num_workers=1` 时无中继，行为与单 worker 完全一致。跨主机中继在设计中，
复用同一送达路径，仅把 `RelayHub` 换成 UDP 中继（见设计文档 §8）。

### 核心设计原则

| 原则 | 实现方式 |
|------|----------|
| **Share-Nothing** | 每个 Worker 线程独占一个 CPU 核心，拥有独立的 UDP socket (独立端口)、Rtc 实例、路由表；显式配置 `SFU_ROOM_WORKERS` 后同一房间可跨 Worker（经 `RelayHub` 中继），单房间不再受单核上限约束 |
| **Batch I/O** | 使用 `recvmmsg` 批量收包 (64 packets/syscall)、`sendmmsg` 批量发包，减少 80%+ 系统调用 |
| **Sans I/O 驱动** | str0m 不做任何网络 I/O，由 Worker 事件循环主动喂数据和驱动时间 |
| **零拷贝路由** | RTP mode 包级转发: `RtpPacket.payload` (`Arc<[u8]>`) 引用计数共享，不重组帧/不重新分包 |
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
│  · WebSocket / QUIC 连接管理                             │
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

## 3. I/O 模型 — io_uring / recvmmsg + epoll

收包与等待后端在启动时探测选择 (见 `src/net/uring.rs`):

- **io_uring** (内核 5.11+, 默认 `SFU_IO_URING=auto`): 常驻 `IORING_OP_RECVMSG`
  (每个 worker 挂载 `batch_size` 个), 数据到达即产生完成事件; 等待用
  `io_uring_enter` 的 `EXT_ARG` 计时超时, 省去每轮 `recvmmsg` 系统调用。
- **epoll + recvmmsg**: io_uring 不可用 (旧内核 / seccomp 禁用) 或
  `SFU_IO_URING=off` 时回退。

发送方向统一走 `sendmmsg` (`src/net/udp.rs`)。

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
| **跨线程中继** | 发布者 worker 解密 RTP 后经 `RelayHub` (channel + 唤醒管道) 把明文载荷中继给订阅者 worker，后者完成 PT 映射与 SRTP；单房间可跨多核 |
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
        Output::Event(Event::RtpPacket(pkt)) => {
            // 5. RTP mode: 收到单个 RTP 包 (不重组帧), 通过 Router 转发给订阅者
            router.forward(participant.id, mid, pkt, &mut send_batch);
        }
        Output::Event(_) => { /* 处理其他事件 */ }
        Output::Timeout(_) => break,
    }
}

// 6. Router.forward 内部:
//    对每个订阅者, 用其 DirectApi::stream_tx(mid).write_rtp(pkt) 写入 RTP 包
//    (仅改 PT/SSRC, 不重新分包), 然后 drain 订阅者的 poll_output → send_batch
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

### 5.3 RTP 包级转发 (RTP mode)

本项目所有 `Rtc` 都以 `set_rtp_mode(true)` 构建: str0m **不再做 depacketize /
packetize**, `Event::MediaData`（完整帧）被 `Event::RtpPacket`（单个 RTP 包）取代。
因此转发是**包级**的: 收到一个 RTP 包 → 对每个订阅者只做 PT 映射 + `write_rtp`
重新加密，不重组帧、不重新分包。

```rust
impl Router {
    fn forward(
        &mut self,
        publisher_id: ParticipantId,
        mid: Mid,
        pkt: RtpPacket,
        send_batch: &mut SendBatch,
    ) {
        // 1. 查找订阅者
        let subs = match self.subscriptions.get(&(publisher_id, mid)) {
            Some(s) => s,
            None => return,
        };

        // 2. 入站 PT → 发布者编码参数 (RtpPacket 不携带 params, 需回查)
        let params = publisher.payload_params_for(pkt.header.payload_type)?;

        // 3. 对每个订阅者转发
        for sub in subs {
            let participant = match self.participants.get_mut(&sub.subscriber_id) {
                Some(p) => p,
                None => continue,
            };

            // 4. 映射为订阅者本地 PT, 并确保发送流已声明 (main + rtx SSRC)
            let local_pt = participant.rtc.codec_config().match_params(params)?.pt();
            participant.ensure_tx_stream(sub.mid);

            // 5. 包级透传: payload 为 Arc<[u8]> 共享, 仅改 PT/SSRC;
            //    SRTP 加密由 str0m 在 poll_output 时完成
            let mut api = participant.rtc.direct_api();
            if let Some(stream) = api.stream_tx_by_mid(sub.mid, None) {
                stream.write_rtp(
                    RtpWrite::new(local_pt, pkt.seq_no, pkt.header.timestamp,
                                 pkt.timestamp, pkt.payload.clone())
                        .marker(pkt.header.marker)
                        .ext_vals(pkt.header.ext_vals.clone())
                        .nackable(true),
                );
            }

            // 6. Drain 输出，收集 Transmit (含 NACK 重传)
            while let Ok(output) = participant.rtc.poll_output() {
                match output {
                    Output::Transmit(t) => send_batch.push(t.destination, t.contents.into()),
                    Output::Timeout(_) => break,
                    _ => {}
                }
            }
        }
    }
}
```

### 5.4 带宽估计与分层自适应 (BWE / Simulcast / SVC)

每个 `Rtc` 都以 `enable_bwe` 构建。订阅者连接会收到 TWCC/REMB 反馈，
str0m 产生 `Event::EgressBitrateEstimate`（下行可用带宽），SFU 据此为每个
订阅者选层。

**Simulcast (RID)**

1. 发布者每条 simulcast 流带独立 SSRC + RID 扩展；
2. SFU 用 500ms 滑动窗口 + EWMA 测量每层实际码率（`RateWindow`）；
3. 每 500ms 按订阅者 BWE（留 10% 余量）选出码率不超限的**最高层**；
4. 切层时向发布者该 RID 发 PLI，**等到目标层关键帧到达**才真正切换，
   避免花屏；若目标层 1s 内始终无关键帧则回退/重试，避免永久黑屏；
5. 逐订阅者**重写 RTP 序号**，使切层/丢层后订阅者看到的序号始终连续
   （也是 NACK 重传正确性的前提）。

**SVC (空间层 + 时间层)**

对单条 SVC 流（无 RID），自行解析 payload descriptor 取层索引：

| 编码 | 解析 | 支持层 |
|---|---|---|
| VP8 | `vp8_temporal_id` | 时间层 TID |
| VP9 | `vp9_sid_tid` (RFC 9628, non-flexible) | 空间层 SID + 时间层 TID |
| AV1 / VP9 flexible | — | 暂不解析, 视为非 SVC 全转发 |

SFU 按 `[sid][tid]` 分别测量码率，再按「解码目标层所需的全部数据」计算累计
码率，在 BWE 预算内选**质量最高的候选**（策略为空间优先，再升时间层），
并丢弃高于目标 `(sid, tid)` 的包；同样依赖逐订阅者序号重写。

> `svc_candidates` 同时用于本地发布者与跨主机中继发布者，两条路径选层逻辑一致。

**通用**

- 订阅者目标码率 (`desired_bitrate`) 取**最高可选层的实际码率**（而非各层之和），
  避免 BWE 过度探测；
- 关键帧请求 (PLI) 带 500ms 节流，抑制多订阅者同时触发导致的 PLI 风暴。

```
  Publisher (3-layer simulcast)
  ┌─ rid=h 1.5M ─┐
  ├─ rid=m 500k ─┤
  └─ rid=l 150k ─┘
        │
        ▼   ┌──────────────────────────────────────┐
   ─────►   │ Worker: 按每订阅者 BWE 选 RID + 重写序号  │
            └───────┬───────────────┬───────────────┘
                    │               │
              Sub A BWE 2M     Sub B BWE 300k
              → rid=h          → rid=l
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
| str0m → Router | RTP mode 下 `Event::RtpPacket.payload` 为 `Arc<[u8]>`，多订阅者共享 |
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

信令服务器同时提供两种传输，承载**完全相同**的 JSON 协议：

| 传输 | 默认端口 | 适用场景 | 网络切换 |
|------|----------|----------|----------|
| **QUIC** (quinn) | `5173/udp` | 移动端 / 原生客户端（优先） | 连接迁移，IP 变化不断连 |
| **WebSocket** | `5174/tcp` | 浏览器 / 兼容回退 | TCP 断开需重连 |

二者共用 `signaling/session.rs` 中的同一套会话状态机，因此房间、订阅、
Worker 交互行为完全一致，不会出现 QUIC 与 WebSocket 语义分叉。

```
        ┌────────────┐   ws://host:5174   ┌─────────────────┐
        │  浏览器     │ ─────────────────► │ server.rs (WS)  │ ─┐
        └────────────┘                    └─────────────────┘  │
                                                               ├─► session.rs ─► Worker
        ┌────────────┐  quic://host:5173  ┌─────────────────┐  │   (protocol.rs)
        │ 移动端 App  │ ─────────────────► │  quic.rs (quinn)│ ─┘
        └────────────┘                    └─────────────────┘
```

### 8.1 消息协议（传输无关）

一条信令连接可同时承载多个 RTCPeerConnection，用 `pc_id` 区分：

- `pc_id = "pub"`：发布连接（sendonly），加入房间
- `pc_id = "sub-<publisherId>"`：订阅连接（recvonly），不加入房间

同一个用户无论订阅多少路远端，都只维持 **一条** WebSocket；服务端为每个 `pc_id`
创建一个独立的 str0m `Rtc` 实例，但共用同一条信令通道。如发布者媒体变更，
订阅端仍复用同一连接。

**客户端 → 服务器**

| type | 字段 | 说明 |
|------|------|------|
| `hello` | `protocol_version?`, `client?` | 可选握手，协商版本 → `welcome` |
| `ping` | `t?` | 应用层心跳 → `pong`（带 `t`） |
| `join` | `pc_id`, `room_id`, `participant_id`, `sdp`, `role?`, `token?` | 创建发布/订阅 PC |
| `renegotiate` | `pc_id`, `sdp`, `token?` | 对已存在 PC 重新协商 (ICE restart) → `sdp_answer` |
| `ice_candidate` | `pc_id`, `candidate`, `sdp_mid?` | Trickle ICE |
| `subscribe` | `pc_id`, `publisher_id`, `track_mid`, `subscriber_mid` | 订阅一条轨道（必须给出订阅者自身 mid） |
| `unsubscribe` | `pc_id`, `publisher_id`, `track_mid` | 取消订阅 |
| `update_media` | `pc_id`, `audio_enabled?`, `video_enabled?` | 静音/关摄像头，广播给房间 |
| `get_room_state` | `room_id?` | 请求成员快照（重连/刷新） |
| `leave` | `pc_id` | 关闭 PC 会话 → `left` |

**服务器 → 客户端**

| type | 说明 |
|------|------|
| `welcome` | 协议版本与服务端能力集 (`features`) |
| `pong` | 心跳回复 |
| `sdp_answer` | SDP answer + 内联 ICE 候选 |
| `joined` | `join` 成功确认，携带成员快照 `participants`（每项含 `tracks: [{mid,kind}]` 轨道列表） |
| `left` | `leave` 成功确认 |
| `participant_joined` | 成员加入广播，含其 `tracks` |
| `participant_left` | 成员离开广播 |
| `participant_tracks` | 某成员发布的轨道列表变化（如开始/停止屏幕共享），订阅者据此调整订阅 |
| `participant_updated` | 成员媒体状态变化（静音/关摄像头） |
| `subscribed` / `unsubscribed` | 订阅/取消订阅确认（含 `track_mid`） |
| `room_state` | 成员快照（响应 `get_room_state`） |
| `error` | `code` + `error`：`bad_request` / `unauthorized` / `already_joined` / `unknown_session` / `worker_error` / `worker_unavailable` / `unsupported_protocol` / `internal` |

```json
// 客户端 → 服务器: 握手 (可选) / 心跳
{ "type": "hello", "protocol_version": 1, "client": "roomchat-web" }
{ "type": "ping", "t": 1735689600000 }

// 服务器 → 客户端: 握手回复 / 心跳回复
{ "type": "welcome", "protocol_version": 1, "server": "webrtc-sfu/0.1.0",
  "features": ["heartbeat", "join_ack", "room_state", "media_state"] }
{ "type": "pong", "t": 1735689600000 }

// 客户端 → 服务器: SDP Offer (发布连接)
{
  "type": "join",
  "pc_id": "pub",
  "room_id": "room-123",
  "participant_id": "alice-uuid",
  "sdp": "v=0\r\no=- 123456 2 IN IP4 127.0.0.1\r\n...",
  "role": "publisher",
  "token": "<JWT>"
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
// token: 用户鉴权 JWT; WebSocket/WHIP 也可在握手头/查询参数中携带 (见 8.4)

// 服务器 → 客户端: SDP Answer (带 pc_id, 客户端据此路由到对应 PC)
{
  "type": "sdp_answer",
  "pc_id": "sub-bob-uuid",
  "sdp": "v=0\r\no=- 789012 2 IN IP4 127.0.0.1\r\n...",
  "ice_candidates": ["candidate:1 1 udp 2122252543 1.2.3.4 5175 typ host"]
}

// 服务器 → 客户端: join 确认 + 房间成员快照 (不含自己)
{
  "type": "joined",
  "pc_id": "pub",
  "participant_id": "alice-uuid",
  "room_id": "room-123",
  "role": "publisher",
  "participants": [
    { "participant_id": "bob-uuid", "audio_enabled": true, "video_enabled": false }
  ]
}

// 客户端 → 服务器: Trickle ICE Candidate
{
  "type": "ice_candidate",
  "pc_id": "sub-bob-uuid",
  "candidate": "candidate:842163049 1 udp 1677729535 ...",
  "sdp_mid": "0"
}

// 客户端 → 服务器: 订阅轨道 / 取消订阅
{
  "type": "subscribe",
  "pc_id": "sub-bob-uuid",
  "publisher_id": "bob-uuid",
  "track_mid": "1"
}
{ "type": "unsubscribe", "pc_id": "sub-bob-uuid", "publisher_id": "bob-uuid", "track_mid": "1" }

// 服务器 → 客户端: 订阅确认
{ "type": "subscribed", "pc_id": "sub-bob-uuid", "publisher_id": "bob-uuid", "track_mid": "1" }

// 客户端 → 服务器: 静音/关摄像头 → 广播给房间内其他人
{ "type": "update_media", "pc_id": "pub", "audio_enabled": false }
{ "type": "participant_updated", "participant_id": "alice-uuid", "audio_enabled": false, "video_enabled": true }

// 客户端 → 服务器: 请求房间成员快照
{ "type": "get_room_state", "room_id": "room-123" }
{ "type": "room_state", "room_id": "room-123",
  "participants": [{ "participant_id": "alice-uuid", "audio_enabled": false, "video_enabled": true }] }

// 服务器 → 客户端: 错误 (带错误码)
{ "type": "error", "pc_id": "pub", "code": "unknown_session", "error": "未知 pc_id, 请先 join" }

// 客户端 → 服务器: 关闭某个 PC 会话 / 服务器确认
{ "type": "leave", "pc_id": "sub-bob-uuid" }
{ "type": "left", "pc_id": "sub-bob-uuid" }
```

### 8.2 QUIC 传输与连接迁移

WebSocket 基于 TCP，四元组 (src ip:port, dst ip:port) 标识连接。移动终端从
WiFi 切到蜂窝时本机 IP 改变，TCP 连接必然断开，需要重新握手、重新 join、
重新协商 SDP。

QUIC 连接由 **Connection ID** 标识，与 IP/端口解耦。切换网络后客户端把底层
UDP socket 重新绑定到新接口，已有连接通过一次路径验证
（`PATH_CHALLENGE` / `PATH_RESPONSE`）迁移到新路径即可继续使用，**信令会话、
房间成员关系与 SDP 协商结果全部保留**。服务端无需任何特殊处理（quinn 自动
按 Connection ID 路由），只需避免把业务状态绑定到 `remote_address()`。

**帧格式**（单条双向流上承载多条 JSON 消息，长度前缀分帧）：

```
┌──────────────┬───────────────────────┐
│ u32 BE len   │ JSON 负载 (len 字节)   │
└──────────────┴───────────────────────┘
```

**TLS 证书**：服务端启动时用 `rcgen` 生成自签名证书并打印 SHA-256 指纹：

```
INFO QUIC 自签名证书 SHA-256 指纹: 3595d0c1...ce95
INFO QUIC 信令服务器启动: quic://192.168.31.112:5173
```

客户端可固定校验该指纹（`examples/quic_signaling_client.rs` 已实现）。生产环境
应换用受信证书；浏览器接入 QUIC 需经 **WebTransport**（HTTP/3），当前 RoomChatWeb
页面仍走 WebSocket。

**参考客户端**（含 `/migrate` 迁移演示）：

```bash
cargo run --example quic_signaling_client -- 192.168.31.112:5173 <指纹hex>
# 交互: 输入 JSON 发送; /migrate 192.168.31.112 重绑 socket 触发迁移
```

### 8.3 信令流程

```
  Browser                  Signaling Server              Worker Thread
     │                            │                            │
     │── QUIC / WebSocket Connect ─►│                            │
     │── hello / ping ───────────►│                            │
     │◄── welcome / pong ─────────│                            │
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
     │◄── joined (成员快照) ──────│                            │
     │◄── participant_joined ✱ ───│ (既有成员; 与快照去重)       │
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

### 8.4 JWT 用户鉴权

信令服务支持可选的 JWT 鉴权。未配置密钥时功能完全关闭 (兼容既有 Demo)；
一旦配置 `SFU_JWT_SECRET` (HMAC) 或 `SFU_JWT_PUBLIC_KEY` (RS/ES/EdDSA)，
默认强制所有 `join` 携带有效 token。

| 环境变量 | 说明 |
|----------|------|
| `SFU_JWT_SECRET` | HMAC 对称密钥 (HS256/384/512) |
| `SFU_JWT_PUBLIC_KEY` / `SFU_JWT_PUBLIC_KEY_FILE` | 非对称公钥 PEM (RS/ES/EdDSA) |
| `SFU_JWT_ALGORITHM` | 可选，缺省按密钥类型推断 (`HS256` / `RS256`) |
| `SFU_JWT_ISSUER` / `SFU_JWT_AUDIENCE` | 可选，校验 `iss` / `aud` |
| `SFU_JWT_REQUIRED` | 配置密钥后默认 `on`；设 `off` 则“有 token 就校验”而不是强制 |

**token 传递方式**：

- **WebSocket**: 握手请求头 `Authorization: Bearer <jwt>`，或 `ws://host:5174/?token=<jwt>`；
- **QUIC**: 无 HTTP 头，放在 `join` 消息的 `token` 字段；
- **WHIP**: `Authorization: Bearer <jwt>` 头，或 `POST /whip?room=x&token=<jwt>`。

**claims 约束**（防止冒充）：

```json
{ "sub": "alice", "exp": 1735689600, "room": "room-1", "role": "publisher" }
```

- `sub`（必填）：用户唯一标识。发布者 `participant_id` 必须等于 `sub`；
  订阅者必须为 `<sub>-recv-<publisherId>`。
- `exp`（必填）：过期时间（Unix 秒）。
- `room` / `role`（可选）：限定可加入的房间 / 角色（`role=subscriber` 只能订阅，
  `publisher` 或省略则既可发布也可订阅）。

启动时会打印 `JWT 鉴权已启用 (required=true)`；强制模式下校验失败返回 401
(WebSocket 在握手段拒绝，WHIP 在 HTTP 响应中返回 401)。密钥不会写入日志。

本地联调可生成测试 token：

```bash
SFU_JWT_SECRET=dev-secret ./scripts/gen-jwt.sh alice room-1 publisher 3600
# 输出 eyJhbGciOi... 即为 JWT
```

### 8.5 信令 → 媒体：控制面 gRPC

信令进程与 worker 进程可分离（worker-only 进程设 `SFU_SIGNALING=off`）。
信令对**远端 worker** 的命令通道由早期的自研 TCP 控制面改为 **gRPC**
（`proto/sfu.proto` 的 `MediaControl`），媒体面仍走 UDP。

```
  signaling ──gRPC MediaControl(CreateParticipant/SendCommand)──► 远端 worker 进程
  本地 worker: 进程内 channel          跨主机中继 RTP: UDP (net::relay, 点对点)
```

| 环境变量 | 说明 |
|----------|------|
| `SFU_CONTROL_LISTEN` | 本进程 `MediaControl` gRPC 监听（worker 进程） |
| `SFU_CONTROL_PEERS` | `全局worker=host:port,...`，远端 worker → 其 gRPC 地址 |
| `SFU_SIGNALING` | `on`（默认）/ `off`（worker-only） |
| `SFU_WORKERS` | `0` = **纯信令节点**（不跑 worker，媒体全在远端） |

**纯信令节点**（`SFU_WORKERS=0` + `SFU_SIGNALING=on`）只维护房间状态并跑
WS/QUIC/WHIP/WHEP，不启动任何 worker、不绑定媒体端口；房间命令经 gRPC 下发到
远端媒体机。必须同时配置 `SFU_TOTAL_WORKERS` 与 `SFU_CONTROL_PEERS`，否则启动即
报错退出。示例见 `deploy/signaling-only.env`。

`CreateParticipant` 为同步 RPC（返回 SDP answer），其余命令 fire-and-forget
（`SendCommand`）。跨主机媒体中继 RTP 仍按 `SFU_RELAY_PEERS` 点对点 UDP 直连。

> 构建需安装 `protoc`（编译 `proto/sfu.proto`）。

### 8.6 信令多机部署：分配器

信令也能多机部署。由于房间成员表是信令进程的**内存状态**，同一 `room_id` 的
连接必须落到同一台信令机；为此提供**信令分配器**：客户端先向它请求信令地址，
再连接。它与 SFU 是同一个二进制，设 `SFU_DISPATCHER_ADDR` 即只跑分配器：

```bash
SFU_DISPATCHER_ADDR=0.0.0.0:5190 \
SFU_SIGNALING_NODES="cell-a@10.0.0.1:5174:5173:5180:5181;cell-b@10.0.0.2:5174:5173:5180:5181:2" \
./target/release/sfu-server
```

```
POST /allocate {"room_id":"demo"}
  → {"node_id":"cell-a","ws_url":"ws://10.0.0.1:5174","quic_url":"quic://...", ...}
```

按带虚拟节点的一致性哈希把房间稳定映射到健康信令机，节点宕机自动顺延。
分配器也可用 `SFU_DISPATCHER_COHOST=on` **与信令/媒体服务同进程启动**
（后台线程），适合单机一体化或信令机自带分配器。
详见 [`docs/signaling-dispatcher.md`](docs/signaling-dispatcher.md)。

> **完整的两种部署形态（单机单进程 / 多信令机 + 多媒体节点）与所有环境变量的
> 逐条说明见 [`docs/deployment.md`](docs/deployment.md)。**

---

## 9. 性能指标预期

**实测单核 fan-out 容量**（1 路 1.5 Mbps@30fps 视频 + Opus 64 kbps 音频；
订阅端多进程并行；完整环境、方法与数据见 [`docs/capacity.md`](docs/capacity.md)）：

| 订阅内容 | 单核满质量路数 | 此时 Worker CPU | 每订阅者 |
|---|---:|---:|---|
| 音 + 视频 | ≈ 200 | ≈ 79% | ~1.56 Mbps，~200 pps |
| 只拉视频 | ≈ 250 | ≈ 75% | ~1.5 Mbps，~150 pps |
| 只拉音频 | ≈ 500 | ≈ 70% | 64 kbps，50 pps |

- 瓶颈是**每订阅者的每包转发量**（SRTP 加密 + `write_rtp`），单核约 **2.5–4 万次转发/秒**。
- **io_uring 与 epoll 在满质量区间 CPU 基本一致**；过载时 io_uring 投递高约 28–40%。
- **单房间跨 2 worker**：≈ 300–320 路（约 1.3–1.5× 单核，但总 CPU 多 ~60%；
  同进程跨 worker 不编码，开销来自目标 worker 重跑每包处理，详见 `docs/capacity.md` §5）。
- 更高并发优先靠多 Worker **按房间分散**（`SFU_WORKERS`）；只有单房间确实要更大容量，
  才用 `SFU_ROOM_WORKERS` 跨 worker。

**设计目标**（非实测）：

| 指标 | 预期值 | 说明 |
|------|--------|------|
| 包转发延迟 (P99) | < 1ms | recvmmsg / io_uring + 零拷贝路由 |
| 内存/连接 | ~2MB | str0m Rtc 实例 + 缓冲区 |

---

## 10. 项目结构

```
webrtc-sfu/
├── Cargo.toml
├── build.rs                   ← 编译 gRPC proto (tonic-build)
├── proto/sfu.proto            ← 信令 → 媒体 gRPC 控制面定义 (MediaControl)
├── README.md                    ← 本文件
└── src/
    ├── main.rs                  ← 入口: 启动 workers + signaling + gRPC 控制面
    ├── config.rs                ← 配置定义 (含 gRPC 控制地址)
    ├── dispatcher/              ← 信令分配器 (多信令机时按 room_id 分配信令地址)
    │   └── mod.rs               ← 一致性哈希 + 健康检查 + /allocate HTTP
    ├── grpc/                    ← 信令 → 媒体 gRPC 控制面
    │   ├── mod.rs               ← proto 类型与模块接线
    │   ├── convert.rs           ← WorkerCommand ↔ gRPC Command 转换
    │   ├── client.rs            ← GrpcControl 同步门面
    │   └── server.rs            ← MediaControlSvc
    ├── net/                     ← 网络原语
    │   ├── mod.rs
    │   ├── udp.rs               ← recvmmsg/sendmmsg UDP socket (nix)
    │   ├── poll.rs              ← epoll 封装 (nix)
    │   └── relay.rs             ← 跨主机中继的紧凑二进制编解码 + UDP 端点
    ├── worker/                  ← per-core 事件循环与跨 worker 中继
    │   ├── mod.rs               ← WorkerConfig/Command/Reply、模块接线
    │   ├── event_loop.rs        ← Worker 线程主循环 (核心)
    │   ├── packet.rs            ← UDP 包 → str0m 实例路由 + 创建参与者
    │   ├── forward.rs           ← RTP 包级转发 / simulcast·SVC 选层 / 中继投递
    │   ├── output.rs            ← str0m 输出 drain 与定时器调度
    │   ├── relay.rs             ← 跨 worker 中继总线 (RelayHub)
    │   └── timer.rs             ← deadline 驱动的最小堆定时器
    ├── sfu/                     ← SFU 核心数据结构
    │   ├── mod.rs
    │   ├── participant.rs       ← Participant + str0m Rtc 封装 + 路由索引
    │   ├── router.rs            ← 发布者→订阅者路由表
    │   └── layer.rs             ← simulcast/SVC 选层与码率估计
    ├── signaling/               ← 信令层 (WebSocket / QUIC / WHIP)
    │   ├── mod.rs               ← SignalingState (房间表 / sender 表 / worker 选择)
    │   ├── auth.rs              ← JWT 用户鉴权
    │   ├── protocol.rs          ← 信令 JSON 协议 (与传输无关)
    │   ├── session.rs           ← 传输无关会话状态机 (WS/QUIC 共用)
    │   ├── server.rs            ← WebSocket 传输层
    │   ├── quic.rs              ← QUIC 传输层 (quinn) + 自签名证书
    │   ├── http.rs             ← WHIP/WHEP 共用 HTTP 原语 + SDP 工具
    │   ├── whip.rs             ← WHIP 推流接入 (HTTP ingest)
    │   └── whep.rs             ← WHEP 拉流接入 (HTTP egress)
    └── cluster/                 ← 控制面路由
        └── mod.rs               ← WorkerRouter: 本地 channel / 远端 gRPC
```

---

## 11. 构建 & 运行

```bash
# 需要 Linux (recvmmsg/epoll/SO_REUSEPORT 是 Linux 特有 API)
# Rust 1.75+
# 构建依赖: protoc (编译 proto/sfu.proto 生成 gRPC 代码)
#   apt install protobuf-compiler   /   dnf install protobuf-compiler

# 构建 (release 模式, LTO + 单 codegen-unit 优化)
cargo build --release

# 运行
# 监听地址一律绑定 0.0.0.0, 对外可达地址由 SFU_ANNOUNCE_IP 单独指定
SFU_ANNOUNCE_IP=203.0.113.10 RUST_LOG=info ./target/release/sfu-server
# 默认端口: QUIC 信令 udp/5173, WebSocket 信令 tcp/5174, 媒体 udp/5175..

# 常用环境变量
SFU_ANNOUNCE_IP=203.0.113.10  # 对外通告 IP — 写入 SDP/ICE host 候选 (必须为客户端可达地址)
SFU_WORKERS=4                 # 本进程 worker 线程数 (媒体端口数); 0 = 纯信令节点 (不跑 worker)
SFU_CPU_BASE=0                # worker i pin 到 CPU (cpu_base+i); 同机多进程时错开核心
SFU_DEFAULT_WORKER=0          # 可选: 强制未配置房间全落该 worker; 不设则按房间哈希
SFU_ROOM_WORKERS="load=0;roomA=1,2;*=0,1,2,3"
                              # 房间 -> 全局 worker 列表; 缺省按房间哈希固定到单个 worker。
                              # 单机多 worker / 多主机多 worker 均在此显式配置
                              # (编号为整集群全局 worker; 跨 worker 订阅经 RelayHub 中继)
SFU_WS_ADDR=0.0.0.0:5174      # WebSocket 信令监听地址 (默认 0.0.0.0:5174)
SFU_QUIC_ADDR=0.0.0.0:5173    # QUIC 信令监听地址; 设为 0/off/空 则禁用 QUIC
SFU_MEDIA_ADDR=0.0.0.0:5175   # 媒体 UDP 绑定地址 (默认 0.0.0.0:5175)
SFU_BWE_INITIAL_BPS=1000000   # BWE 初始带宽估计 (bit/s, 默认 1 Mbps)
SFU_SOCK_BUF_MB=16            # UDP socket 收发缓冲区目标 (MB); 实际值受内核 rmem_max/wmem_max 限制
SFU_IO_URING=auto             # 收包/等待后端: auto(支持则用 io_uring)/on(强制)/off(epoll)
SFU_BATCH_SIZE=64             # recvmmsg / io_uring 批量收包大小 (≤256)
SFU_SEND_BATCH=128            # 每次 sendmmsg 的最大包数 (≤256)
SFU_SEND_BACKLOG=8192         # 发送队列积压上限 (超限丢最旧包); 高 fan-out 可调大

# JWT 用户鉴权 (可选; 未配置任何密钥则关闭)
SFU_JWT_SECRET=change-me      # HMAC 对称密钥 (HS256/384/512)
# SFU_JWT_PUBLIC_KEY_FILE=/etc/sfu/jwt-pub.pem  # 或非对称公钥 (RS/ES/EdDSA)
# SFU_JWT_ALGORITHM=RS256     # 可选, 缺省按密钥类型推断
# SFU_JWT_ISSUER=https://idp  # 可选, 校验 iss
# SFU_JWT_AUDIENCE=webrtc-sfu # 可选, 校验 aud
# SFU_JWT_REQUIRED=on         # 配置密钥后默认 on; off=有 token 才校验

# 信令 / worker 进程分离 + 跨主机 (控制面 gRPC, 媒体面 UDP relay)
SFU_SIGNALING=on              # worker-only 进程设 off
SFU_TOTAL_WORKERS=4           # 整集群 worker 总数 (全局编号 0..total)
SFU_WORKER_OFFSET=0           # 本进程 worker 全局编号起始
SFU_CONTROL_LISTEN=0.0.0.0:5400                    # worker 进程: MediaControl gRPC 监听
SFU_CONTROL_PEERS=2=10.0.0.2:5400,3=10.0.0.2:5400  # 信令进程: 远端 worker -> 其 gRPC 地址
SFU_RELAY_LISTEN=0.0.0.0:5390                      # 本进程 relay UDP 监听 (启用跨主机中继)
SFU_RELAY_PEERS=2=10.0.0.2:5390,3=10.0.0.2:5390    # 远端 worker -> 对端 relay UDP 地址

# 信令分配器 (客户端按 room_id 向它请求信令地址 -> 房间亲和)
SFU_DISPATCHER_ADDR=0.0.0.0:5190
SFU_DISPATCHER_COHOST=on      # on = 嵌入式(与信令/媒体同进程); 不设 = 独占进程
SFU_SIGNALING_NODES="self@203.0.113.10:5174:5173:5180:5181"  # id@host:ws:quic:whip:whep[:weight]

# WHIP/WHEP 接入
SFU_WHIP_ADDR=0.0.0.0:5180    # WHIP HTTP 监听; 不设则不启用
SFU_WHEP_ADDR=0.0.0.0:5181    # WHEP HTTP 监听; 不设则不启用
```

> `busy_poll_us`(0) 目前是**编译期常量**，没有对应环境变量。

> **系统 socket 缓冲区**：内核默认 `rmem_max/wmem_max` 仅 4MB，会把 socket 缓冲
> 截断到 ~8MB，高 fanout 时造成丢包。建议部署前调大（见 `scripts/sysctl-sfu.sh`）：
> ```bash
> sudo sysctl -w net.core.rmem_max=67108864 net.core.wmem_max=67108864
> sudo sysctl -w net.core.rmem_default=4194304 net.core.wmem_default=4194304
> sudo sysctl -w net.core.netdev_max_backlog=250000
> ```
> 启动日志会打印每个 worker 实际生效的 `rcvbuf/sndbuf`，便于确认。

> **监听地址 vs 对外通告地址**：服务始终绑定 `0.0.0.0`（由防火墙控制暴露面），
> 而 ICE host 候选中的地址取自 `SFU_ANNOUNCE_IP`。若未显式配置，启动时会自动
> 探测本机出口 IP（探测失败则回退 `127.0.0.1` 并告警）。多 worker 时每个 worker
> 的通告端口 = 媒体端口 = `SFU_MEDIA_ADDR` 端口 + worker 序号。

---

## 11.1 WHIP 推流接入 (FFmpeg / GStreamer / OBS)

启用 `SFU_WHIP_ADDR=0.0.0.0:5180` 后，任意 WHIP 客户端可直接以 WebRTC 推流，
SFU 把它当作普通发布者（房间内 WebSocket 订阅者照常 `subscribe`）。

**FFmpeg**（8.x 自带 `-f whip`，会自行完成 ICE/DTLS/SRTP）:
```bash
ffmpeg -re -f lavfi -i testsrc2=size=1280x720:rate=30 -f lavfi -i sine=frequency=440 \
  -c:v libopenh264 -b:v 2000k -g 60 -pix_fmt yuv420p \
  -c:a libopus -b:a 96k -ar 48000 -ac 2 \
  -f whip "http://SFU_HOST:5180/whip?room=demo"
```
> FFmpeg 的 whip muxer **视频仅支持 H.264**（`libx264`/`libopenh264`/`h264_nvenc`…），
> 音频支持 Opus。H.264 + Opus 正好是 WebRTC 可直通的编码，无需转码。

**GStreamer**:
```bash
# 推荐 gst-plugins-rs 的 whipsink (webrtc 插件)
gst-launch-1.0 videotestsrc is-live=true ! video/x-raw,width=1280,height=720,framerate=30/1 \
  ! x264enc tune=zerolatency ! rtph264pay config-interval=1 pt=96 \
  ! whipsink whip-endpoint="http://SFU_HOST:5180/whip?room=demo"
# 或用 webrtcbin 自建 WHIP 客户端；本机的 avmux_whip 因无 location 属性不可用
```

**OBS**: 添加「WHIP」输出，服务器填 `http://SFU_HOST:5180/whip?room=demo`。

**接口**（RFC 9725 子集）:
| 方法 | 路径 | 作用 |
|------|------|------|
| `OPTIONS` | `/whip` | CORS / 能力探测 |
| `POST` | `/whip?room=<房间>` | 提交 SDP offer，返回 `201` + SDP answer（含 SFU ICE 候选），响应头 `Location` / `X-Participant-Id` |
| `PATCH` | `/whip/<id>` | trickle ICE (`application/trickle-ice-sdpfrag`) |
| `DELETE` | `/whip/<id>` | 停止推流 |

## 11.2 WHEP 拉流接入 (浏览器 / 播放器)

WHEP 是 WHIP 的对称能力（e**g**ress）：启用 `SFU_WHEP_ADDR=0.0.0.0:5181`
后，任意 WHEP 客户端可拉取某发布者的音视频。服务端把 WHEP 客户端当作普通
**订阅者**：按客户端 offer 的 m-section 与发布者实际轨道按 kind 匹配后自动
`subscribe`（无需客户端知道发布者的 mid）。

```bash
# 拉取 room=demo 中发布者 alice 的音视频
curl -X POST -H 'Content-Type: application/sdp' \
  --data-binary @offer.sdp \
  "http://SFU_HOST:5181/whep?room=demo&publisher=alice"
# 本仓库附带简易客户端 (str0m):
#   cargo run --example whep_client -- --url http://SFU_HOST:5181 --room demo --publisher alice
```

**接口**（与 WHIP 对称）:
| 方法 | 路径 | 作用 |
|------|------|------|
| `OPTIONS` | `/whep` | CORS / 能力探测 |
| `POST` | `/whep?room=<房间>&publisher=<发布者>` | 提交 SDP offer（recvonly），返回 `201` + SDP answer，响应头 `Location` / `X-Participant-Id`；未知发布者返回 `404` |
| `PATCH` | `/whep/<id>` | trickle ICE |
| `DELETE` | `/whep/<id>` | 停止拉流 |

> 启用 JWT 鉴权时，WHEP 同样需要 `Authorization: Bearer` 或 `?token=`。

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

// 发送媒体 (SFU 转发, RTP mode)
let mut api = rtc.direct_api();
let stream = api.stream_tx_by_mid(mid, None)?;
stream.write_rtp(
    str0m::rtp::RtpWrite::new(pt, seq_no, time, wallclock, payload)
        .marker(marker)
        .nackable(true),
);
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
- **WebTransport**: 在现有 quinn 端点上叠加 HTTP/3，让浏览器也能直接使用 QUIC 信令
- **分布式 SFU**: 跨机器 Room 联邦，支持更大规模会议
- **GPU 加速**: 视频转码/合流 (SFU + MCU 混合模式)
