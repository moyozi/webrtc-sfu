# 部署手册（单机单进程 / 多信令机 + 多媒体节点）

> 本文覆盖两种典型部署形态的**完整配置**，并逐条解释**所有环境变量**。
> 相关设计细节另见：
> - [`manual.md`](manual.md) — 使用手册与配置速查
> - [`multi-worker-relay.md`](multi-worker-relay.md) — 多 worker / 跨主机中继
> - [`signaling-dispatcher.md`](signaling-dispatcher.md) — 信令分配器设计

---

## 0. 一个二进制，四种角色

整个服务只有一个可执行文件 `sfu-server`。它靠环境变量决定自己扮演哪种角色，
**不需要编译多个版本、也不需要不同的启动参数**。

| 角色 | 开启方式 | 做什么 | 不做什么 |
|---|---|---|---|
| **一体化单进程** | 默认（什么都不设） | 信令 + N 个 worker，全在一台机器一个进程 | — |
| **纯信令节点** | `SFU_WORKERS=0` + `SFU_SIGNALING=on` | 维护房间状态、跑 WS/QUIC/WHIP/WHEP | 不跑 worker、不绑定媒体端口 |
| **纯媒体节点**（worker-only） | `SFU_SIGNALING=off` | 跑 worker、媒体转发、gRPC 控制面、跨主机中继 | 不跑信令、不维护房间状态 |
| **信令分配器（独占）** | `SFU_DISPATCHER_ADDR=...` | 把 `room_id` 稳定映射到一台健康信令机 | 不跑 worker、不跑信令 |
| **信令分配器（嵌入）** | 上者 + `SFU_DISPATCHER_COHOST=on` | 后台线程跑分配器，**主进程照常**跑信令/媒体 | — |

> 分配器既可以独立部署，也可以**与信令/媒体服务同进程启动**（`SFU_DISPATCHER_COHOST=on`），
> 适合单机一体化或「信令机自带分配器」拓扑。

角色判定在 `main.rs` 里的顺序是：

1. **先看 `SFU_DISPATCHER_ADDR`**：
   - 默认（未设 `SFU_DISPATCHER_COHOST`）→ 本进程**只当分配器**，其它
     `SFU_*` 一律忽略（只有 `RUST_LOG` 生效）；
   - 设了 `SFU_DISPATCHER_COHOST=on` → 后台线程启动分配器，**继续**按 2 启动
     信令/媒体服务。
2. 否则读取其余配置，再看 `SFU_SIGNALING` 与 `SFU_WORKERS`：
   - `SFU_SIGNALING=off` → 纯媒体节点（`SFU_WORKERS` 必须 ≥ 1）；
   - `SFU_SIGNALING=on` 且 `SFU_WORKERS=0` → 纯信令节点；
   - `SFU_SIGNALING=on` 且 `SFU_WORKERS≥1` → 一体化（信令 + 本地 worker）。

### 两个「面」要分清楚

- **控制面**：信令 → worker 的指令（创建参与者、订阅、离开…）。
  同进程走内存 channel；跨进程/跨主机走 **gRPC**（`MediaControl`）。
- **媒体面**：真正的 RTP/RTCP 音视频包。
  同进程走零拷贝 channel；跨主机走 **点对点 UDP relay**（`net::relay`）。

理解这两条线路，就能理解后面所有配置项：`SFU_CONTROL_*` 管控制面，
`SFU_RELAY_*` 管媒体面。

### 全局 worker 编号（贯穿全文的核心概念）

整个集群里的每个 worker 线程都有一个**全局唯一编号**，范围 `0 .. SFU_TOTAL_WORKERS`。
每台机器上的 worker 只占用其中一段：

```
本机 worker 全局编号 = SFU_WORKER_OFFSET .. SFU_WORKER_OFFSET + SFU_WORKERS
```

- 信令通过「房间 → 全局编号」的规则选中一个 worker，再把命令发过去；
- 选中本地 worker 就走内存 channel，选中远端 worker 就走 gRPC；
- 媒体跨主机时，relay 用全局编号找到对端机器的 UDP 地址。

> **最容易配错的地方**就是全局编号：`SFU_TOTAL_WORKERS` 和每台机器的
> `SFU_WORKER_OFFSET` / `SFU_WORKERS` 在**所有节点上必须自洽**。

---

## 1. 端口与协议总览

| 端口（默认） | 协议 | 用途 | 由谁监听 | 对应变量 |
|---|---|---|---|---|
| 5173 | UDP | QUIC 信令（移动端优先） | 信令节点 | `SFU_QUIC_ADDR` |
| 5174 | TCP | WebSocket 信令 | 信令节点 | `SFU_WS_ADDR` |
| 5175, 5176, … | UDP | 媒体（worker 0,1,… 各占一个） | 媒体节点 | `SFU_MEDIA_ADDR` |
| 5180 | TCP | WHIP 推流接入（可选） | 信令节点 | `SFU_WHIP_ADDR` |
| 5181 | TCP | WHEP 拉流接入（可选） | 信令节点 | `SFU_WHEP_ADDR` |
| 5400 + | TCP | gRPC 媒体控制面（信令 → 远端 worker） | 媒体节点 | `SFU_CONTROL_LISTEN` |
| 5390 + | UDP | 跨主机媒体中继 | 媒体节点 | `SFU_RELAY_LISTEN` |
| 5190 | TCP | 分配器 HTTP 接口 | 分配器 | `SFU_DISPATCHER_ADDR` |

> 端口只是默认值，全部可改。媒体端口是**一段连续端口**
> `base .. base + SFU_WORKERS - 1`，防火墙要整段放行。

---

## 2. 单机单进程部署

### 2.1 适用场景

- 开发自测、小规模生产、单房间/少量并发。
- 一台机器一个 `sfu-server`，同时承担信令与媒体，**没有任何跨进程通信**，
  延迟最低、配置最简单。

此时的进程内数据流：

```
客户端 ──WS/QUIC──► 信令（房间状态）
                        │ 本地 channel（进程内，零拷贝）
                        ▼
                 worker 0..N ──UDP──► 客户端媒体
```

### 2.2 最小启动命令

```bash
export SFU_ANNOUNCE_IP=192.168.1.10   # 必填：客户端可达的本机 IP
export SFU_WORKERS=4                   # 用 4 个 worker（占 4 个媒体端口）
./target/release/sfu-server
```

启动后监听：

- `udp/5173` QUIC 信令
- `tcp/5174` WebSocket 信令
- `udp/5175..5178` 媒体（4 个 worker）

`SFU_ANNOUNCE_IP` 会写进 SDP 的 ICE host 候选；服务实际绑定 `0.0.0.0`，
但告诉客户端「请连这个 IP」，所以必须是**客户端真实可达**的地址
（内网 IP、公网 IP 或 NAT 映射后的地址）。

### 2.3 完整配置示例（带注释）

`/etc/sfu/sfu.env`（systemd `EnvironmentFile`）：

```bash
# ── 必填 ───────────────────────────────────────────
# 对外通告 IP：写入 ICE host 候选，必须客户端可达。
SFU_ANNOUNCE_IP=192.168.1.10

# worker 线程数 = 媒体端口数，建议 ≤ 物理核数。
SFU_WORKERS=4

# ── 监听地址（默认 0.0.0.0，一般不用改）─────────────
SFU_WS_ADDR=0.0.0.0:5174
SFU_MEDIA_ADDR=0.0.0.0:5175      # worker i 用 5175+i
SFU_QUIC_ADDR=0.0.0.0:5173       # 设 0/off/空 可禁用 QUIC
# SFU_WHIP_ADDR=0.0.0.0:5180     # 需要推流接入才启用
# SFU_WHEP_ADDR=0.0.0.0:5181     # 需要拉流接入才启用

# ── 房间与 worker 分配（可选）──────────────────────
# 默认：每个房间按 room_id 哈希固定到一个 worker（同房间同 worker，
#       不同房间自动分散）。需要单房间跨 worker 才配下面这行。
# SFU_ROOM_WORKERS="roomA=0,1;roomB=2;*=0"
# 强制未配置房间全落某个 worker：
# SFU_DEFAULT_WORKER=0

# ── 调优（可选）────────────────────────────────────
SFU_SOCK_BUF_MB=16              # socket 收发缓冲目标（受内核上限截断）
SFU_BWE_INITIAL_BPS=1000000     # 订阅者初始带宽估计（bit/s，最小 50000）

# ── JWT 鉴权（可选；不配密钥则关闭）────────────────
# SFU_JWT_SECRET=<强随机密钥>
# SFU_JWT_PUBLIC_KEY_FILE=/etc/sfu/jwt-pub.pem
# SFU_JWT_ALGORITHM=RS256
# SFU_JWT_ISSUER=https://idp.example.com
# SFU_JWT_AUDIENCE=webrtc-sfu
# SFU_JWT_REQUIRED=on

RUST_LOG=info
```

### 2.4 systemd 单元

`deploy/sfu-server.service` 已提供模板：

```ini
[Unit]
Description=webrtc-sfu server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/sfu/sfu.env
ExecStart=/opt/sfu/sfu-server
WorkingDirectory=/opt/sfu
Restart=on-failure
RestartSec=2
LimitNOFILE=524288          # 高并发连接需要较大 fd 上限
# Nice=-5                   # 可选：调度优先级

[Install]
WantedBy=multi-user.target
```

启停：

```bash
systemctl daemon-reload
systemctl enable --now sfu-server
journalctl -u sfu-server -f
```

### 2.5 防火墙与内核参数

```bash
# 单机单进程需要放行的端口
firewall-cmd --add-port=5173/udp --permanent     # QUIC
firewall-cmd --add-port=5174/tcp --permanent     # WebSocket
firewall-cmd --add-port=5175-5178/udp --permanent # 媒体（worker 段）
firewall-cmd --reload
```

内核网络参数（收发缓冲、backlog 等）可用仓库脚本一次性调好，需 root：

```bash
sudo ./scripts/sysctl-sfu.sh
```

### 2.6 客户端如何连接

客户端**只需要信令地址**，媒体地址由服务端在 SDP 里自动给出：

| 客户端 | 连接地址 |
|---|---|
| 浏览器 / App（WebSocket） | `ws://192.168.1.10:5174` |
| 移动端（QUIC，连接迁移） | `quic://192.168.1.10:5173` |
| FFmpeg / OBS 推流（WHIP） | `http://192.168.1.10:5180/whip?room=...` |
| 播放器拉流（WHEP） | `http://192.168.1.10:5181/whep?room=demo&publisher=alice` |

### 2.7 快速验证

```bash
# 用仓库自带的合成发布器压 2 路，验证媒体路径
cargo run --release --example load_publisher -- \
    --url ws://127.0.0.1:5174 --room demo --count 2 --duration 10
```

看到 `已连接` 与收发统计即正常。更多验证命令见 [`manual.md`](manual.md)。

### 2.8 单机 + 内嵌分配器（推荐：客户端始终先请求分配器）

即使只部署一台机器，也推荐把**分配器与 SFU 同进程启动**，这样客户端可以
**统一先用分配器拿地址**，无需为单机/多机写两套逻辑：

```bash
SFU_ANNOUNCE_IP=192.168.1.10
SFU_WORKERS=4

# 内嵌分配器（与上面的信令 + worker 同进程）
SFU_DISPATCHER_ADDR=0.0.0.0:5190
SFU_DISPATCHER_COHOST=on
# 信令机清单：这里只有本机一个节点
SFU_SIGNALING_NODES="self@192.168.1.10:5174:5173:5180:5181"

RUST_LOG=info
```

启动后同一个进程监听 5190（分配器）、5174（WS）、5173（QUIC）、5175+（媒体）。
客户端流程：

```text
POST http://192.168.1.10:5190/allocate  {"room_id":"demo"}
  → {"node_id":"self","ws_url":"ws://192.168.1.10:5174"}
  → 连接 ws_url 并发起 join
```

Web 客户端（`RoomChatWeb`）已内置这套逻辑：在「信令分配器地址」填
`http://192.168.1.10:5190` 即可；它也兼容直接填 `ws://192.168.1.10:5174`
（直连模式，跳过分配器）。

**测试期关闭 JWT 鉴权**：只要**不配置任何密钥**（`SFU_JWT_SECRET` /
`SFU_JWT_PUBLIC_KEY`），鉴权即整体关闭，客户端无需携带 token。

> 若已配置密钥、只想放宽要求，可设 `SFU_JWT_REQUIRED=off`（有 token 才校验，
> 无 token 也放行）。

---

## 3. 多信令机 + 多媒体节点部署

### 3.1 为什么信令和媒体要拆开

- **媒体节点**吃 CPU/带宽，希望跑满物理核、绑定大页/多队列，最好**无状态**、
  可随时替换；
- **信令节点**持有**房间内存态**（成员表、推送通道），需要保证**同一房间的
  连接落到同一台信令机**；
- 两者伸缩策略不同：媒体按并发量扩，信令按连接数扩。

拆开后的数据流：

```
                     客户端（浏览器 / App / FFmpeg / OBS）
                                  │
                 ① POST /allocate {"room_id":"demo"}
                                  ▼
                  ┌──────────────────────────────┐
                  │ 信令分配器 dispatcher（无状态） │  ← 一致哈希 room→信令机
                  │ 返回 ws/quic/whip/whep 地址    │     周期 TCP 健康检查
                  └──────────────────────────────┘
                                  │ ② 返回信令地址
                                  ▼
        ┌───────────────┬───────────────┬───────────────┐
        ▼               ▼               ▼
   信令机 A         信令机 B         信令机 C    （SFU_WORKERS=0，纯信令）
   房间内存态       房间内存态       房间内存态
        │ gRPC          │ gRPC          │ gRPC      （控制面）
        └───────┬───────┴───────┬───────┘
                ▼               ▼
         媒体机 1          媒体机 2          （SFU_SIGNALING=off，只有 worker）
         worker 0,1        worker 2,3
                └─── UDP relay（媒体面，点对点）───┘
```

### 3.2 三条必须遵守的不变式

只要记住这三条，多机部署就不会错：

1. **同一房间 → 同一信令机**。
   房间成员表在信令进程内存里，同一房间连到不同信令机就互相看不见。
   靠**分配器**的一致性哈希保证。
2. **同一房间 → 同一 worker**。
   信令的 `assign_worker(room_id)` 把房间哈希到一个全局 worker；
   所有信令机都用**相同**的 `SFU_TOTAL_WORKERS` / `SFU_ROOM_WORKERS` /
   `SFU_DEFAULT_WORKER`，结果才一致。
3. **任意信令机都要能触达任意 worker**。
   通过 `SFU_CONTROL_PEERS`（远端 worker 的 gRPC 地址）补齐；本地 worker 自动
   走进程内 channel。

### 3.3 规划示例：2 信令机 + 2 媒体机 + 4 worker

先画一张全局编号表，后面每一处配置都对照它：

| 机器 | 角色 | `SFU_WORKERS` | `SFU_WORKER_OFFSET` | 全局编号 |
|---|---|---|---|---|
| sig-a `10.0.0.1` | 纯信令 | 0 | （不用） | — |
| sig-b `10.0.0.2` | 纯信令 | 0 | （不用） | — |
| med-1 `10.0.0.11` | 媒体 | 2 | 0 | 0, 1 |
| med-2 `10.0.0.12` | 媒体 | 2 | 2 | 2, 3 |

- `SFU_TOTAL_WORKERS=4`（所有节点一致）
- 控制面地址：med-1 `10.0.0.11:5400`、med-2 `10.0.0.12:5400`
- 中继地址：med-1 `10.0.0.11:5390`、med-2 `10.0.0.12:5390`
- 信令地址：sig-a `10.0.0.1:5174/5173`、sig-b `10.0.0.2:5174/5173`

### 3.4 媒体节点配置（med-1 / med-2）

媒体节点**只跑 worker**，`SFU_SIGNALING=off`。

`med-1`（全局编号 0,1）：

```bash
SFU_SIGNALING=off
SFU_WORKERS=2
SFU_WORKER_OFFSET=0
SFU_TOTAL_WORKERS=4

# 本机媒体：worker 0 占 udp/5175，worker 1 占 udp/5176
SFU_MEDIA_ADDR=0.0.0.0:5175
SFU_ANNOUNCE_IP=10.0.0.11

# 控制面：接收来自信令机的 gRPC 命令
SFU_CONTROL_LISTEN=0.0.0.0:5400

# 媒体中继：本机监听 5390；远端 worker 2,3 在 med-2 的 5390
SFU_RELAY_LISTEN=0.0.0.0:5390
SFU_RELAY_PEERS=2=10.0.0.12:5390,3=10.0.0.12:5390

SFU_SOCK_BUF_MB=16
RUST_LOG=info
```

`med-2`（全局编号 2,3）：

```bash
SFU_SIGNALING=off
SFU_WORKERS=2
SFU_WORKER_OFFSET=2
SFU_TOTAL_WORKERS=4

SFU_MEDIA_ADDR=0.0.0.0:5175       # worker 2→5175, worker 3→5176
SFU_ANNOUNCE_IP=10.0.0.12

SFU_CONTROL_LISTEN=0.0.0.0:5400

SFU_RELAY_LISTEN=0.0.0.0:5390
SFU_RELAY_PEERS=0=10.0.0.11:5390,1=10.0.0.11:5390

SFU_SOCK_BUF_MB=16
RUST_LOG=info
```

> 说明：
> - 媒体节点**不需要** `SFU_JWT_*`（不跑信令，不做鉴权）。
> - 媒体节点**不需要** `SFU_WS_ADDR` / `SFU_QUIC_ADDR` / `SFU_WHIP_ADDR`，
>   它们不会监听这些端口。
> - `SFU_CONTROL_LISTEN` 只有本进程有 worker 时才会真正监听。
> - 如果某房间要跨 med-1/med-2，所有相关媒体机的 `SFU_RELAY_PEERS` 都要互相填全。

### 3.5 信令节点配置（sig-a / sig-b）

纯信令节点：`SFU_WORKERS=0` + `SFU_SIGNALING=on`。它没有本地 worker，
所以**必须**用 `SFU_CONTROL_PEERS` 把全部 4 个 worker 指出来。

`sig-a` / `sig-b`（两者配置**完全一致**，除了 `SFU_ANNOUNCE_IP` 不需要）：

```bash
SFU_SIGNALING=on
SFU_WORKERS=0                 # 纯信令：不跑 worker、不绑媒体端口
SFU_TOTAL_WORKERS=4

# 信令入口
SFU_WS_ADDR=0.0.0.0:5174
SFU_QUIC_ADDR=0.0.0.0:5173
# SFU_WHIP_ADDR=0.0.0.0:5180
# SFU_WHEP_ADDR=0.0.0.0:5181

# 控制面：所有远端 worker 的 gRPC 地址（全局编号 -> 地址）
SFU_CONTROL_PEERS=0=10.0.0.11:5400,1=10.0.0.11:5400,2=10.0.0.12:5400,3=10.0.0.12:5400

# 房间分片：必须与媒体机一致（见 3.6）
# SFU_ROOM_WORKERS="load=0,1,2,3"

# JWT 鉴权（只在信令节点配）
# SFU_JWT_SECRET=<强随机密钥>
# SFU_JWT_REQUIRED=on

RUST_LOG=info
```

**关键点**：`SFU_TOTAL_WORKERS`、`SFU_ROOM_WORKERS`、`SFU_DEFAULT_WORKER`
必须在**所有信令节点上完全相同**，否则同一房间可能被分到不同 worker。

> 启动校验：纯信令节点若忘记配 `SFU_CONTROL_PEERS`，进程会直接报错退出
> （`exit 2`），而不是静默丢弃命令。

### 3.6 房间与 worker 的映射（可选但重要）

默认规则（不配任何变量时）：**每个房间按 `room_id` 哈希固定到一个全局 worker**。
多数场景这就够了——同房间的人都在同一个 worker 上，同机转发、零中继。

需要「一个房间跨多台机器」（例如超大房间横向扩展）时才配：

```bash
# load 房间的参与者轮转到全局 worker 0~3（跨 med-1 / med-2）
# 其余房间按 room_id 哈希固定到单个 worker
SFU_ROOM_WORKERS=load=0,1,2,3
```

格式说明（`;` 分隔多条规则）：

| 写法 | 含义 |
|---|---|
| `roomA=3` | roomA 钉定到全局 worker 3 |
| `roomB=0,1,2,3` | roomB 内参与者轮转分布到 0~3（单房间跨 worker） |
| `*=0,1,2,3` | 未列出的房间兜底规则 |
| 不写 | 未列出的房间按 `room_id` 哈希固定到单个 worker |

跨 worker 就会跨主机，媒体需要经 **UDP relay** 中继（见 3.4 的 `SFU_RELAY_PEERS`）。
注意当前实现会把发布者的**所有 simulcast 层**都中继给订阅端主机，跨主机带宽偏大。

### 3.7 分配器配置（dispatcher）

分配器无状态、可水平部署多个，前面挂 VIP/LB。它和 SFU 是同一个二进制：

```bash
SFU_DISPATCHER_ADDR=0.0.0.0:5190
SFU_SIGNALING_NODES="sig-a@10.0.0.1:5174:5173:0:0;sig-b@10.0.0.2:5174:5173:0:0"
SFU_DISPATCHER_HEALTH_INTERVAL_SECS=5
SFU_DISPATCHER_HEALTH_TIMEOUT_MS=1000
# 也可省去独立实例, 直接嵌到某台信令机上 (见 3.7.1)
# SFU_DISPATCHER_COHOST=on
RUST_LOG=info
```

#### 3.7.1 嵌入模式（与信令/媒体同进程）

不想单独维护分配器实例时，可以把它嵌到任意一个已部署的 `sfu-server` 进程里：

```bash
# 在已有的信令机 / 一体化节点上追加:
SFU_DISPATCHER_ADDR=0.0.0.0:5190
SFU_DISPATCHER_COHOST=on
SFU_SIGNALING_NODES="sig-a@10.0.0.1:5174:5173:0:0;sig-b@10.0.0.2:5174:5173:0:0"
```

- 分配器在其独立线程 + 独立 Tokio runtime 上运行，不干扰媒体路径；
- 同一进程内的信令节点也会被登记进 `SFU_SIGNALING_NODES` 并参与健康检查；
- 分配器启动时会做一轮短周期补探（每 500ms，最多 20 次），避免刚起来的
  本地信令节点在首个探测窗口内被误判为不健康。

> 嵌入模式下，主机宕机会同时失去分配器与它托管的信令节点。对可用性要求高的
> 生产环境，仍建议分配器独立多实例 + VIP/LB。

`SFU_SIGNALING_NODES` 格式：

```
id@host:ws:quic:whip:whep[:weight];id2@...
```

| 字段 | 说明 |
|---|---|
| `id` | 节点唯一标识，**参与哈希，请勿随意变更** |
| `host` | IPv4 / 主机名 / IPv6 字面量（IPv6 用方括号 `[::1]`） |
| `ws:quic:whip:whep` | 四个端口，`0` 表示该入口禁用 |
| `weight` | 可选，默认 1；越大分配到的房间越多 |

健康检查对 `ws`（没有则 `whip`/`whep`）做 TCP connect；纯 QUIC（UDP）节点无法
探测，会被视为**始终健康**，启动日志会告警。

接口：

| 方法 | 路径 | 作用 |
|---|---|---|
| POST / GET | `/allocate` | body `{"room_id":"demo"}` 或 `?room_id=demo`，返回该房间应连的信令地址 |
| GET | `/nodes` | 列出节点及健康状态 |
| GET | `/healthz` | 分配器自身健康（节点数 / 健康数） |

`/allocate` 响应示例：

```json
{
  "room_id": "demo",
  "node_id": "sig-a",
  "ws_url":   "ws://10.0.0.1:5174",
  "quic_url": "quic://10.0.0.1:5173"
}
```

未启用/无健康端口的字段会被省略；缺少 `room_id` 返回 400；全不健康返回 503。

### 3.8 客户端集成

客户端启动时先向分配器要地址，再连信令，后续协议完全不变：

```ts
const alloc = await fetch("https://dispatcher.example.com/allocate", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ room_id: roomId }),
}).then((r) => r.json());

const ws = new WebSocket(alloc.ws_url);
ws.onopen = () => ws.send(JSON.stringify({
  type: "join", room_id: roomId, participant_id: "alice", sdp: offer,
}));
```

- 同一房间必须用同一个 `room_id`（大小写、空格敏感）；
- 断线重连时**重新调用 `/allocate`**，拿到当前健康节点再重连；
- 分配器不可用时，可回退直连某台信令机（客户端保留兜底地址）。

WHIP/WHEP 客户端同理：先拿 `whip_url` / `whep_url`，再 POST SDP。

### 3.9 防火墙矩阵

| 源 | 目的 | 端口/协议 | 用途 |
|---|---|---|---|
| 客户端 | 分配器 | 5190/tcp | 地址分配 |
| 客户端 | 信令机 | 5174/tcp、5173/udp | 信令 |
| 客户端 | 信令机 | 5180/5181/tcp | WHIP/WHEP（可选） |
| 客户端 | 媒体机 | 5175..5175+N-1/udp | 媒体 |
| 信令机 | 媒体机 | 5400/tcp | gRPC 控制面 |
| 媒体机 | 媒体机 | 5390/udp | 跨主机媒体中继 |

同一网段可用安全组/iptables 收窄，只放信令机 → 媒体机的 5400 即可。

### 3.10 扩容与故障语义

- **加媒体机**：分配新的全局编号段（改 `SFU_WORKER_OFFSET`），更新
  `SFU_TOTAL_WORKERS` 和所有信令机的 `SFU_CONTROL_PEERS`，更新所有媒体机的
  `SFU_RELAY_PEERS`。房间哈希会因 `TOTAL_WORKERS` 变化而重排，已有房间建议
  在低峰期操作。
- **加信令机**：新节点配置与现有信令机完全一致，把它加进分配器的
  `SFU_SIGNALING_NODES` 即可，分配器会自动分流。
- **信令机宕机**：分配器把它的房间顺延到环上的后继信令机。但房间成员表在内存里，
  **会丢失**，客户端需重连并重新 join 重建。
- **媒体机宕机**：落在它上面的 worker 不可用，相关参与者需重新分配
  （当前需客户端重连重建；后续可加重调度）。

> 分配器解决的是「地址分配 + 房间亲和」，**不是**共享房间目录。若要求信令机
> 宕机后房间成员零丢失，需要额外引入 Redis 等共享目录（当前未实现）。

### 3.11 上线前 checklist

- [ ] 所有节点的 `SFU_TOTAL_WORKERS` 相同，且 ≥ 所有 `OFFSET+WORKERS` 的最大值
- [ ] 每台媒体机的全局编号段互不重叠
- [ ] 每台信令机的 `SFU_CONTROL_PEERS` 覆盖**所有**远端 worker
- [ ] 每台媒体机的 `SFU_RELAY_PEERS` 覆盖所有**其它**媒体机的 worker
      （仅当房间会跨机时需要）
- [ ] 所有信令机的 `SFU_ROOM_WORKERS` / `SFU_DEFAULT_WORKER` 完全一致
- [ ] `SFU_ANNOUNCE_IP` 在每台**媒体机**上都是客户端可达的本机 IP
- [ ] 防火墙按 3.9 放行
- [ ] 分配器的 `SFU_SIGNALING_NODES` 列出了全部信令机
- [ ] 用 `GET /nodes` 确认所有信令机 health 为 true

---

## 4. 环境变量完整参考

以下按用途分组。「生效角色」中的简称：**一体化** = 信令+worker 同进程；
**信令** = 纯信令节点或一体化；**媒体** = 纯媒体节点或一体化；
**分配器** = `SFU_DISPATCHER_ADDR` 被设置时。

### 4.1 角色开关

| 变量 | 默认 | 取值 | 说明 | 生效角色 |
|---|---|---|---|---|
| `SFU_DISPATCHER_ADDR` | 未设 | 地址 / `0` / `off` / 空 | 设置后本进程当分配器；默认**独占**，设 `SFU_DISPATCHER_COHOST=on` 则与其他服务同进程 | 分配器 |
| `SFU_DISPATCHER_COHOST` | `off` | `on`/`off`（也认 `0`/`false`/`no`/空为关） | 分配器**嵌入**模式: 后台线程跑分配器，主进程继续启动信令/媒体 | 分配器 |
| `SFU_SIGNALING` | `on` | `on`/`off`（也认 `0`/`false`/空为关闭） | 是否跑信令服务器；`off` = worker-only | 全部 |
| `SFU_WORKERS` | `1` | `0` ~ 约 `65535 - 媒体端口` | worker 线程数=媒体端口数；**`0` = 纯信令节点** | 全部 |

### 4.2 信令入口（信令节点）

| 变量 | 默认 | 说明 |
|---|---|---|
| `SFU_WS_ADDR` | `0.0.0.0:5174` | WebSocket 信令绑定地址 |
| `SFU_QUIC_ADDR` | `SFU_WS_ADDR 的 IP:5173` | QUIC 信令绑定地址；设 `0`/`off`/空 禁用 |
| `SFU_WHIP_ADDR` | 未设 | WHIP 推流 HTTP 监听；不设则不启用 |
| `SFU_WHEP_ADDR` | 未设 | WHEP 拉流 HTTP 监听；不设则不启用 |

> QUIC 默认由 `SFU_WS_ADDR` 的 IP 推导而来。若 `SFU_WS_ADDR=0.0.0.0:5174`，
> QUIC 默认就是 `0.0.0.0:5173`。

### 4.3 媒体与 worker（媒体节点 / 一体化）

| 变量 | 默认 | 说明 |
|---|---|---|
| `SFU_MEDIA_ADDR` | `0.0.0.0:5175` | 媒体 UDP 绑定基地址；**worker i 用 `端口+i`** |
| `SFU_ANNOUNCE_IP` | 自动探测 | **对外通告 IP**，写入 ICE host 候选。生产务必显式设置；纯信令节点可忽略 |
| `SFU_SOCK_BUF_MB` | `16` | socket 收发缓冲区目标（MB），受内核 `rmem_max`/`wmem_max` 截断 |
| `SFU_IO_URING` | `auto` | 收包/等待 I/O 后端：`auto`(支持则用 io_uring, 否则回退 epoll) / `on`(强制, 不可用则拒绝启动) / `off`(始终 epoll+recvmmsg) |
| `SFU_BWE_INITIAL_BPS` | `1000000` | 订阅者侧 BWE 初始带宽（bit/s），最小 50000 |
| `SFU_WORKER_OFFSET` | `0` | 本进程 worker 的**全局编号起始** |
| `SFU_TOTAL_WORKERS` | `max(SFU_WORKERS,1)` | **整集群** worker 总数，全局编号范围 `0..total` |

> 下列参数目前是**编译期常量**，暂无环境变量：`batch_size=64`、
> `send_batch_size=128`、`busy_poll_us=0`。

### 4.4 集群拓扑与房间分配（信令节点；媒体节点也应保持一致）

| 变量 | 默认 | 说明 |
|---|---|---|
| `SFU_DEFAULT_WORKER` | 未设 | 强制**未配置房间**全落该全局 worker；不设则按 `room_id` 哈希 |
| `SFU_ROOM_WORKERS` | 未设 | 房间级分片：`room=w1,w2;room2=w3;*=...`。列表长度 1 = 钉定；>1 = 房间内轮转 |

- 编号一律是**全局编号**（`0..SFU_TOTAL_WORKERS`），可跨主机；
- 未匹配任何规则且无 `*` 兜底的房间，按 `room_id` 哈希固定到单个 worker；
- 所有信令节点上的这两项必须一致。

### 4.5 控制面 gRPC（信令 → 远端 worker）

| 变量 | 默认 | 说明 | 生效角色 |
|---|---|---|---|
| `SFU_CONTROL_LISTEN` | 未设 | 本进程 `MediaControl` gRPC 监听地址；**有本地 worker 才会监听** | 媒体 |
| `SFU_CONTROL_PEERS` | 未设 | `全局worker=host:port,...`，远端 worker → 其 gRPC 地址 | 信令 |

例：

```bash
# 媒体机
SFU_CONTROL_LISTEN=0.0.0.0:5400
# 信令机
SFU_CONTROL_PEERS=0=10.0.0.11:5400,1=10.0.0.11:5400,2=10.0.0.12:5400,3=10.0.0.12:5400
```

### 4.6 跨主机媒体中继（UDP，媒体节点）

| 变量 | 默认 | 说明 |
|---|---|---|
| `SFU_RELAY_LISTEN` | 未设 | 本机收发跨主机中继 RTP 的 UDP 端点；不设则不做跨主机中继 |
| `SFU_RELAY_PEERS` | 未设 | `全局worker=host:port,...`，远端 worker → 对端中继 UDP 地址 |

只有「房间会跨机」时才需要配置。同机 worker 之间走进程内 channel，不占 relay。

### 4.7 分配器

| 变量 | 默认 | 说明 |
|---|---|---|
| `SFU_SIGNALING_NODES` | 未设（**必填**） | `id@host:ws:quic:whip:whep[:weight];...` |
| `SFU_DISPATCHER_HEALTH_INTERVAL_SECS` | `5`（最小 1） | 健康检查周期（秒） |
| `SFU_DISPATCHER_HEALTH_TIMEOUT_MS` | `1000`（最小 50） | 单次 TCP 探测超时（毫秒） |

### 4.8 JWT 用户鉴权（仅信令节点）

不配置任何密钥时整体关闭。配置任一密钥后**默认强制**所有 join 携带有效 token。

| 变量 | 默认 | 说明 |
|---|---|---|
| `SFU_JWT_SECRET` | 未设 | HMAC 对称密钥（HS256/384/512） |
| `SFU_JWT_PUBLIC_KEY` | 未设 | 非对称公钥 PEM 内容（RS/ES/EdDSA） |
| `SFU_JWT_PUBLIC_KEY_FILE` | 未设 | 非对称公钥 PEM 文件路径 |
| `SFU_JWT_ALGORITHM` | 按密钥推断 | 如 `HS256` / `RS256` |
| `SFU_JWT_ISSUER` | 未设 | 校验 `iss` |
| `SFU_JWT_AUDIENCE` | 未设 | 校验 `aud` |
| `SFU_JWT_REQUIRED` | 配置密钥后 `on` | `off`/`0`/`false`/`no` = 有 token 才校验，无 token 放行 |

`SFU_JWT_PUBLIC_KEY` 与 `SFU_JWT_PUBLIC_KEY_FILE` 二选一；与 `SFU_JWT_SECRET` 也二选一。

### 4.9 日志

| 变量 | 默认 | 说明 |
|---|---|---|
| `RUST_LOG` | `info` | 日志过滤，如 `info`、`debug`、`sfu_server::worker::event_loop=debug` |

---

## 5. 常见问题 / 排障

| 现象 | 原因与排查 |
|---|---|
| 纯信令节点启动即退出，日志 `必须配置 SFU_CONTROL_PEERS` | `SFU_WORKERS=0` 时没有远端地址，所有命令无处下发。补 `SFU_CONTROL_PEERS` |
| `SFU_SIGNALING=off` + `SFU_WORKERS=0` 启动即退出 | 该进程既无信令又无 worker，无意义。二者至少开一个 |
| 客户端能 join 但收不到别人的流 | 房间被分到了不同 worker/信令机。检查所有信令机 `SFU_TOTAL_WORKERS` / `SFU_ROOM_WORKERS` 是否一致；是否用了分配器保证房间亲和 |
| 跨主机房间没有媒体 | 未配 `SFU_RELAY_LISTEN`/`SFU_RELAY_PEERS`，或房间没有用 `SFU_ROOM_WORKERS` 显式跨机 |
| 信令机连不上远端 worker | `SFU_CONTROL_PEERS` 地址/端口错，或防火墙未放行 5400/tcp |
| ICE 候选里是 `127.0.0.1` | `SFU_ANNOUNCE_IP` 未设且自动探测失败；**媒体机**必须显式设置为客户端可达 IP |
| 媒体端口不够 | worker 端口 = `SFU_MEDIA_ADDR 端口 + i`，注意 u16 上限；调小 `SFU_WORKERS` 或换基端口 |
| 启动无关的 `SFU_ANNOUNCE_IP` 告警 | 纯信令节点不会产生候选，已静默；媒体节点仍需设置 |
| QUIC 连不上但 WS 正常 | `SFU_QUIC_ADDR` 被设成 `0`/`off`，或 UDP 5173 未放行 |
| 分配器返回 503 | 所有信令机健康检查失败：`GET /nodes` 查看；确认 WS（或 WHIP/WHEP）端口可达 |
| 分配器把房间派到不同机器 | `room_id` 大小写/空格不一致；或节点清单变了导致哈希环变化 |
