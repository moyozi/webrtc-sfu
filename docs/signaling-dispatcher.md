# 信令分配器 (Signaling Dispatcher)

> 适用场景：**信令多机部署 + 媒体多机部署**。客户端不再硬编码某一台信令机，
> 而是先向分配器询问「我该连哪台信令机」，拿回地址后再建立 WebSocket / QUIC / WHIP / WHEP。

---

## 1. 为什么需要它

本项目的房间成员表是**信令进程内的内存状态**：

- `SignalingState.rooms` —— 房间成员列表；
- `SignalingState.senders` —— 每个参与者的推送通道 (WebSocket / QUIC)；
- `SignalingState.participant_worker` —— 参与者 → 全局 worker 的映射。

当信令只跑一个进程时这些状态天然一致。但信令一旦多机部署，**同一房间的两个人
若连到不同的信令机，彼此不可见**：房间广播 `participant_joined` / `participant_left`
只在本进程内发生，订阅关系也无从建立。

因此必须保证一条不变式：

> **同一个 `room_id` 的所有连接，必须落到同一台信令进程。**

分配器的唯一职责就是把 `room_id` 稳定地映射到一台**健康**的信令机，并把这个
地址告诉客户端。**媒体 (worker) 的分配不归它管** —— 每台信令机都用同一套
`assign_worker(room_id)` 规则把房间哈希到全局 worker，再由信令机通过本地
channel / gRPC 把命令下发到对应 worker（详见
[`multi-worker-relay.md`](multi-worker-relay.md) §8.2）。

---

## 2. 架构

```text
                        客户端 (浏览器 / App / FFmpeg / OBS)
                                      │
                      ① POST /allocate {"room_id":"demo"}
                                      ▼
                    ┌──────────────────────────────────────┐
                    │  信令分配器 sfu-dispatcher (无状态)    │
                    │  · 一致性哈希 room_id → 信令机         │
                    │  · 周期性 TCP 健康检查                 │
                    │  · 返回 ws/quic/whip/whep 地址         │
                    └──────────────────────────────────────┘
                                      │ ② 返回信令地址
                                      ▼
              ┌───────────────────────┴───────────────────────┐
              ▼                                                ▼
   ┌───────────────────────┐                       ┌───────────────────────┐
   │ 信令机 A  (SFU_SIGNALING=on) │                  │ 信令机 B  (SFU_SIGNALING=on) │
   │  WS 5174 / QUIC 5173  │                       │  WS 5174 / QUIC 5173  │
   └──────────┬────────────┘                       └──────────┬────────────┘
              │ gRPC MediaControl / 本地 channel                 │
     ┌────────┴────────┐                             ┌─────────┴────────┐
     ▼                 ▼                             ▼                  ▼
  worker 0          worker 1                      worker 2           worker 3
  (媒体机 1)        (媒体机 1)                    (媒体机 2)         (媒体机 2)
     └──── 跨主机媒体中继 UDP 5390 (SFU_RELAY_PEERS) ────┘
```

- **分配器**：无状态、可水平部署多个，前面挂 VIP / LB；只要配置相同，
  对同一 `room_id` 的分配结果一致。
- **信令机**：跑 WebSocket / QUIC / WHIP / WHEP，持有房间内存态。
- **媒体机**：只跑 worker，媒体面 RTP 点对点 UDP 直连。

---

## 3. 分配算法：带虚拟节点的一致性哈希

1. 每台信令机按 `weight` 生成 `weight × 160` 个虚拟点，散列到 u64 哈希环上；
   `id` 参与哈希，保证跨进程/重启结果一致。
2. `room_id` 哈希后沿环**顺时针**找到第一个**健康**节点。
3. 该节点即为此房间的信令机；同房间后续请求必然命中同一节点。

由此得到三个性质：

| 性质 | 说明 |
|---|---|
| 确定性 | 拓扑不变时，同一 `room_id` 永远同一台信令机；无需共享状态 |
| 均衡性 | 不同房间自动分散；`weight` 可让高配机器多分流量 |
| 故障转移 | 节点宕机后其房间顺延到环上的后继节点；节点恢复后房间自动迁回 |

> ⚠️ 哈希函数末尾额外做了一次 **splitmix64 finalizer**。裸 FNV-1a 对
> `room-1` / `room-2` 这类仅尾部不同的字符串雪崩性差，哈希值高度接近，
> 会让大量房间挤到环上同一段（全部落到同一节点）。finalizer 充分打散高位。

### 故障转移的真实语义

信令机的房间状态在**内存**里。节点宕机时：

- 分配器会把它的房间改派到后继节点；
- 但这些房间的成员表/推送通道随原进程一起丢失；
- 客户端需要**重连并重新 join**，房间状态由重连的客户端重建。

因此分配器解决的是「地址分配 + 房间亲和」，**不是**共享房间目录。如果要求
节点宕机后房间成员零丢失，需要额外引入共享目录（Redis 等）或让客户端全量
重连重建（见 §7）。

---

## 4. 启动与配置

分配器与 SFU 服务是**同一个二进制** `sfu-server`，用环境变量切换角色：
只要设置了 `SFU_DISPATCHER_ADDR`，进程就**只运行分配器**，不再启动 worker /
信令服务器。

```bash
SFU_DISPATCHER_ADDR=0.0.0.0:5190 \
SFU_SIGNALING_NODES="cell-a@10.0.0.1:5174:5173:5180:5181;cell-b@10.0.0.2:5174:5173:5180:5181:2" \
./target/release/sfu-server
```

| 变量 | 默认 | 说明 |
|---|---|---|
| `SFU_DISPATCHER_ADDR` | 未设 | 分配器 HTTP 监听地址；设 `0`/`off`/空 视为不启用 |
| `SFU_SIGNALING_NODES` | 无 (必填) | 信令机清单，见下 |
| `SFU_DISPATCHER_HEALTH_INTERVAL_SECS` | `5` | 健康检查周期 (秒) |
| `SFU_DISPATCHER_HEALTH_TIMEOUT_MS` | `1000` | 单次 TCP 连接探测超时 (毫秒) |

### 节点清单格式

```text
id@host:ws:quic:whip:whep[:weight];id2@...
```

- `id`：节点唯一标识，参与哈希，请勿随意变更；
- `host`：IPv4 / 主机名 / IPv6 字面量（IPv6 用方括号，如 `[::1]`）；
- 端口依次为 `ws:quic:whip:whep`，`0` 表示该入口禁用；
- `weight` 可选，默认 `1`，越大分到的房间越多；至少需要 `host` 和 `ws` 两个字段。

例：

```bash
# 两台机器，各入口全开，第二台权重 2
SFU_SIGNALING_NODES="cell-a@10.0.0.1:5174:5173:5180:5181;cell-b@10.0.0.2:5174:5173:5180:5181:2"
# 只用 WS + QUIC
SFU_SIGNALING_NODES="a@10.0.0.1:5174:5173:0:0"
```

### 健康检查

默认对每个节点的 **WS 端口**做 TCP connect（WS 未开时依次回退到 WHIP / WHEP 端口）。
设计取舍：

- **TCP connect 探活**：轻量、零侵入，能发现进程挂掉 / 端口不可达；
- 只开 QUIC（UDP）而没有 TCP 入口的节点无法用 TCP 探测，会被视为**始终健康**，
  此时请自行保证其存活（或为它开放一个 WS 入口）。启动日志会给出告警。

---

## 5. HTTP 接口

所有响应带 CORS 头，浏览器可直接调用。

### `POST /allocate`（或 `GET /allocate?room_id=...`）

请求：

```json
{ "room_id": "demo" }
```

响应 `200`：

```json
{
  "room_id": "demo",
  "node_id": "cell-a",
  "ws_url":   "ws://10.0.0.1:5174",
  "quic_url": "quic://10.0.0.1:5173",
  "whip_url": "http://10.0.0.1:5180",
  "whep_url": "http://10.0.0.1:5181"
}
```

未启用 / 无健康的端口对应的字段会被省略。缺少 `room_id` 返回 `400`；
所有节点都不健康返回 `503`。

### `GET /nodes`

列出节点及健康状态（运维 / 排障）：

```json
[
  { "id": "cell-a", "host": "10.0.0.1", "healthy": true,
    "ws_url": "ws://10.0.0.1:5174", "quic_url": "quic://10.0.0.1:5173" }
]
```

### `GET /healthz`

分配器自身健康状态：

```json
{ "status": "ok", "nodes": 2, "healthy": 2 }
```

---

## 6. 客户端集成

客户端拿到 `ws_url` / `quic_url` 后再连，加入房间时带上同一个 `room_id` 即可。

```ts
// 1) 向分配器要地址
const alloc = await fetch("https://dispatcher.example.com/allocate", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ room_id: roomId }),
}).then((r) => r.json());

// 2) 连 WS，后续协议完全不变
const ws = new WebSocket(alloc.ws_url);
ws.onopen = () => ws.send(JSON.stringify({
  type: "join", pc_id: "pub", room_id: roomId, participant_id: "alice", sdp: offer,
}));
```

WHIP/WHEP 客户端：

```bash
# 先查 WHEP 地址，再 POST 到 /whep?room=...
WHEP=$(curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"room_id":"demo"}' https://dispatcher.example.com/allocate | jq -r .whep_url)
curl -X POST -H 'Content-Type: application/sdp' --data-binary @offer.sdp "$WHEP/whep?room=demo"
```

要点：

- **同一个房间必须用同一个 `room_id`**（大小写、空格敏感），否则会被分到不同信令机；
- 断线重连时**重新调用 `/allocate`**，拿到当前健康节点再重连；若原节点已宕机，
  会拿到后继节点并重新 join；
- 分配器失败时可回退到直连某台信令机（配置里保留一台兜底地址）。

---

## 7. 部署拓扑建议

### 7.1 信令机与媒体机解耦（推荐起点）

- 信令机：N 台，均设 `SFU_SIGNALING=on`，配置相同的 `SFU_TOTAL_WORKERS` /
  `SFU_ROOM_WORKERS` / `SFU_CONTROL_PEERS`；
- 媒体机：M 台，设 `SFU_SIGNALING=off`、各自的 `SFU_WORKER_OFFSET` /
  `SFU_CONTROL_LISTEN`、`SFU_RELAY_LISTEN` / `SFU_RELAY_PEERS`；
- 任意信令机都要能通过 gRPC 抵达任意 worker（`SFU_CONTROL_PEERS` 填全量远端 worker），
  这样房间哈希到的 worker 与信令机所在机器无关。
- 分配器：1~2 台无状态实例，前置 VIP/LB。

`deploy/` 下提供了 `cluster-a.env` / `cluster-b.env` 的单信令机 + 双媒体机样例，
扩展到多信令机时：把 `SFU_SIGNALING=on` 的配置复制到多台信令机，给分配器填全部
信令机地址即可。

### 7.2 Cell 模式（信令与 worker 同机，减少跨机 gRPC）

每台机器跑「1 信令进程 + 若干 worker」，分配器直接把房间分到**持有其 worker
的那台机器**的信令进程。要点是让分配器的房间哈希规则与信令的
`assign_worker(room_id)` 规则一致（保证房间 → worker → 机器 → 信令机一致），
从而信令命令全部走本地 channel，跨机只保留媒体中继。此模式需要把 worker 拓扑
纳入分配器配置，属于后续增强（见 §8）。

---

## 8. 已知限制与后续演进

1. **房间状态不共享**：节点宕机导致房间内存态丢失，需客户端重连重建。
   彻底方案是引入跨信令机的房间目录（Redis / 一致性 KV）或 gossip；
   在此之前，分配器的一致性哈希已能保证稳态下的房间亲和。
2. **恢复期的房间分裂**：节点恢复后，分配器会把新请求迁回该节点，而老连接仍在
   后继节点上。可加「恢复后冷却期（drain）」或让客户端在收到分配结果变化时重连。
3. **分配器自身高可用**：多实例 + VIP/LB；实例无状态，健康状态由各自探测得出。
   也可在分配器前再用 DNS/网关做一层，分配器只作为「选机服务」。
4. **Cell 模式**：如 §7.2，让分配器感知 worker 拓扑以减少跨机控制面流量。
5. **更丰富的调度策略**：当前只做房间亲和 + 一致性哈希；如需按连接数 / CPU /
   地域 / 延迟调度，可在分配器读取各信令机的上报指标后改用加权最少连接等策略，
   但**必须保持房间亲和**（同一房间不能被拆到两台机器）。
