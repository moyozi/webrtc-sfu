# 单房间多 Worker / 跨线程 & 跨主机订阅

> **实现状态**
> - 第一阶段（跨线程订阅）: ✅ 已实现 (`RelayHub` + `deliver_to_subscribers`，
>   分别在 `src/worker/relay.rs` 与 `src/worker/forward.rs`)，
>   已用 `SFU_WORKERS=4 SFU_ROOM_WORKERS="load=0,1,2,3" + 压测工具` 验证：
>   分布到 4 个 worker 的订阅者均能满速收流。
> - 第二阶段（跨主机媒体中继）: ✅ 已实现并单测：
>   - `src/net/relay.rs`: 中继 RTP/控制消息的紧凑二进制编解码 + UDP 端点；
>   - `RelayHub` 支持**全局 worker 编号**：本进程范围内走进程内 channel，
>     范围外经 UDP 发往对端进程的 relay 端口；
>   - 每进程一个 relay 监听线程，解码后按 `target_worker` 重新注入本地中继/命令通道，
>     **完全复用** `forward_relayed_rtp` 与命令处理；
>   - 已单测: RTP/控制回环、UDP 真实收发、跨主机路由、控制注入。
> - 剩余（集群控制面）: ⏳ 见 §8 末尾 —— 需要把**信令对 worker 的命令**
>   (`CreateParticipant` 等) 也跨主机路由，并在一个房间跨主机时分配全局 worker。

## 1. 现状与问题

- `SignalingState` 默认按房间 id 哈希把**同一房间的所有参与者**固定到同一个
  worker（同房间同 worker，不同房间自动分散）；每个 worker 是单线程事件循环，
  独占一张 `ParticipantTable` + `Router`。要让房间跨 worker 需在
  `SFU_ROOM_WORKERS` 中显式配置。
- 因此**单房间容量 = 单核容量**（压测约 150–200 路 @1.5Mbps）。
- 跨 worker 无法转发媒体，因为订阅者的 SRTP/RTP 状态在各自 worker 的
  `Rtc` 里，而发布者的 `Rtc` 在另一个 worker。

## 2. 目标

1. **单房间多 worker（跨线程订阅）**：同一房间的参与者可分布到多个 worker，
   订阅关系跨 worker 建立与转发。
2. **跨主机订阅**：在 1 的基础上，把 worker 间中继从「进程内 channel」替换为
   「跨主机 UDP 中继」，支持多机横向扩展。

## 3. 核心设计：发布者 worker 作为源，中继明文 RTP

不改动 WebRTC/SRTP 语义，只增加一个**进程内中继平面**：

```
Publisher(MID A, worker 0)                    Subscriber(MID B, worker 1)
        │                                              ▲
        │ 入站 SRTP → 解密 → RtpPacket                   │ write_rtp(B) → SRTP
        ▼                                              │
  forward_to_subscribers ──local──► worker0 本地订阅者   │
        │                                              │
        └──remote──► RelayHub[1] (channel + wake fd) ───┘
                         RelayedRtp{params,seq,ts,marker,ext,rid,payload}
```

关键点：
- 中继的是 **已解密的 RTP 载荷 + 元数据**（`Arc<[u8]>`，进程内零拷贝）。
- 每个 worker 仍用自己的 `Rtc` 完成 **PT 映射 + 逐订阅者 SRTP 加密 + 序号重写**，
  所以订阅者的加密/拥塞/BWE 行为与本地订阅完全一致。
- 订阅关系**同时**登记在两处：
  - 订阅者 worker 的 `Router`：`(publisher_id, mid) -> [本地订阅者]`（用于送达）；
  - 发布者 worker 的 `Router`：`(publisher_id, mid) -> [远端订阅者]`（用于中继）。
  用 `Subscription.worker_idx` 区分。

## 4. 控制流

```
Subscriber join → assign_worker(room)   // 默认同房间同 worker; 配置后房间跨 worker
Subscribe(publisher_id, publisher_mid, publisher_worker)
        │
        ├─ publisher_worker == self ─► 本地 add_subscription + PLI
        └─ 否则:
             self.Router.add_subscription(self_idx)                 // 本地送达用
             cmd_txs[publisher_worker].send(RegisterRelaySubscriber) // 远端中继用
```

`RegisterRelaySubscriber` 在发布者 worker 上：
```
Router.add_subscription(subscriber_id, mid, subscriber_mid, worker_idx=subscriber_worker)
```
发布者 worker 的 `forward` 遍历订阅者时，`worker_idx != self` 的收集为「中继目标」，
每个目标 worker 只发一份 `RelayedRtp`。

## 5. 层选择（Simulcast/SVC）

订阅者 worker 从**中继包**中自行测量各层速率（`rid`/`tid` 随包携带），
再结合本地订阅者的 BWE 选层 —— 与本地发布者路径共用同一套逻辑。
因此**发布者 worker 中继所有层**，不做 per-subscriber 过滤（进程内几乎零成本）。
跨主机时可按需增加 BWE 回传以在中继端裁剪（见 §8）。

## 6. 唤醒

worker 事件循环目前只 epoll 一个 UDP fd。中继到达必须立即唤醒：
- 每 worker 一个 `UnixStream::pair()`；read 端注册进 epoll，write 端 `Arc` 共享；
- 发布者 worker 推送 `RelayedRtp` 后向目标 worker 的 write 端写 1 字节；
- 目标 worker epoll 醒来后 drain read 端 + `relay_rx.try_recv()` 处理。

## 7. 清理

- `RemoveParticipant(sub_id)`：订阅者 worker 依据 `sub_publishers[sub_id]`
  向各发布者 worker 发 `UnregisterRelaySubscriber`，并从本地 `Router` 移除。
- 发布者离开：发布者 worker 移除其轨道；远端订阅者 worker 收不到中继包自然停发，
  并可通过 `RemoveParticipant` 广播后的 unsubscribe 流程清理。

## 8. 跨主机（第二阶段）

把「进程内 `RelayHub`」抽象为 `RelayTransport`：

```rust
trait RelayTransport: Send + Sync {
    fn send(&self, target: WorkerId, pkt: RelayedRtp);
    fn wake(&self, target: WorkerId);
}
```

- 进程内实现：channel + UnixStream（本方案的第一阶段）。
- 跨主机实现：每个 worker 额外监听一个 **relay UDP 端口**；
  `RelayedRtp` 按自定义包头序列化（publisher_id/mid/params/rid/seq/ts/marker/ext +
  payload）发送到目标主机对应 worker 的 relay 地址。
  目标端 relay socket 在 epoll 中，收到后 decode 再走同一 `deliver_to_subscribers`。
- 需要一份**集群拓扑**（worker → (host, relay_addr)），由配置下发/服务发现提供。

### 8.1 已实现（媒体中继传输）

- `RelayHub` 使用**全局 worker 编号**：`[worker_offset, worker_offset+local_workers)`
  为本地，其余为远端。
- `RelayHub::relay(target_global, pkt)` / `send_control(target_global, ctrl)`：
  本地 → 进程内 channel + `UnixStream` 唤醒；远端 → `net::relay` 编码 + UDP。
- 每进程一个 `sfu-relay` 监听线程：`recv_from` → `decode` → 按 `target_worker`
  重新注入对应本地 worker 的 `relay_txs`/`cmd_txs` 并唤醒。
- 配置：
  ```bash
  SFU_WORKER_OFFSET=0            # 本进程 worker 全局编号起始
  SFU_RELAY_LISTEN=0.0.0.0:5390  # 本进程 relay UDP 端口
  SFU_RELAY_PEERS=1=10.0.0.2:5390,2=10.0.0.2:5390   # 远端全局 worker -> relay 地址
  ```

### 8.2 已实现：集群控制面（信令 → 远端 worker）

采用「**集中信令 + worker-only 主机**」部署模型：一个进程跑信令，其余进程只跑
worker（`SFU_SIGNALING=off`）。信令通过 `src/cluster` 把命令路由到任意全局 worker：

- **全局 worker 分配**：`assign_worker` **默认按房间 id 哈希固定到一个全局
  worker**（可用 `SFU_DEFAULT_WORKER` 强制全落同一 worker）；只有
  `SFU_ROOM_WORKERS` 显式配置的房间才会跨 worker —— 单个 worker 钉定, 多个
  worker 则在房间内轮转（全局编号, 可跨主机）。
- **远端 worker 命令通道**：`cluster::ClusterControl` + TCP 控制监听；`WorkerRouter`
  对本地走 channel、对远端走 TCP。`CreateParticipant` 用 `req_id` 请求/响应关联
  （SDP answer 经同一连接回传），其余命令 fire-and-forget。
- **媒体面**: 跨主机 RTP/控制经 UDP relay（§8.1）。
- **配置**:
  ```bash
  SFU_TOTAL_WORKERS=4
  SFU_WORKER_OFFSET=2             # 进程 B 的全局起始
  SFU_SIGNALING=off               # 进程 B 只跑 worker
  SFU_CONTROL_LISTEN=0.0.0.0:5401
  SFU_CONTROL_PEERS=0=10.0.0.1:5400,1=10.0.0.1:5400
  # 信令进程(A) 上配置房间跨主机:
  # SFU_ROOM_WORKERS="load=0,1,2,3"
  ```
- **验证**: 同机两进程（模拟两主机，信令+worker0,1 / worker-only+worker2,3），
  16 订阅者满速 17.3 Mbps；B 进程经控制面创建 16 个参与者并跨主机收流。

### 8.3 剩余优化

1. **房间目录**：若放弃集中信令、各主机独立信令，需要跨主机同步房间成员。
2. **中继裁剪**：当前发布者中继**所有 simulcast 层**，跨主机浪费带宽；
   可把订阅者 BWE 经控制面回传，在中继端按订阅者裁剪层。

## 9. 兼容性

`num_workers == 1` 且未配置 relay 时：所有参与者 local，`publisher_worker == self`，
不产生任何中继，行为与现状完全一致（零回归）。
