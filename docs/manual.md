# webrtc-sfu 部署与使用手册

> 适用版本：当前仓库 `main`（str0m 0.21 / RTP mode / 单房间多 worker / 跨主机中继 / WHIP 推流接入）

---

## 目录

1. [系统概述](#1-系统概述)
2. [环境要求与端口清单](#2-环境要求与端口清单)
3. [编译](#3-编译)
4. [系统调优（部署前必做）](#4-系统调优部署前必做)
5. [配置项完整参考](#5-配置项完整参考)
6. [快速启动（单机）](#6-快速启动单机)
7. [多 worker 横向扩展](#7-多-worker-横向扩展)
8. [跨主机集群部署](#8-跨主机集群部署)
9. [浏览器使用（RoomChatWeb）](#9-浏览器使用roomchatweb)
10. [WHIP 推流接入（FFmpeg / GStreamer / OBS）](#10-whip-推流接入ffmpeg--gstreamer--obs)
11. [信令协议（JSON）](#11-信令协议json)
12. [媒体功能说明](#12-媒体功能说明)
13. [压测工具](#13-压测工具)
14. [运维与排障](#14-运维与排障)
15. [安全注意事项](#15-安全注意事项)

---

## 1. 系统概述

```
                   ┌─────────────────────────────────────────────────────┐
   浏览器 / App ──ws/quic──► │ 信令层 (WS 5174 / QUIC 5173 / WHIP 5180)              │
                   │   assign_worker() → 按房间哈希分配全局 worker                │
                   └───────────────┬─────────────────────────────────────┘
                                   │ WorkerCommand (本地 channel / 跨主机 TCP 5400)
        ┌──────────────────────────┼──────────────────────────┐
        ▼                          ▼                          ▼
  ┌───────────┐             ┌───────────┐              ┌───────────┐
  │ worker 0  │             │ worker 1  │   ...        │ worker N  │
  │ UDP 5175  │             │ UDP 5176  │              │ UDP 5175+N│
  │ Rtp/ICE/  │             │           │              │           │
  │ DTLS/SRTP │◄── RelayHub (进程内) / UDP 5390 (跨主机) ─►│           │
  └───────────┘             └───────────┘              └───────────┘
```

- **信令层**：WebSocket / QUIC 承载同一套 JSON 协议；WHIP 走 HTTP。
- **Worker**：每 worker 一个线程 + 独立 UDP 媒体端口，独占 CPU 核（Share-Nothing），
  内部跑 str0m（RTP mode）。默认同房间同 worker；配置 `SFU_ROOM_WORKERS` 后
  同一房间可跨 worker/主机。
- **RelayHub**：跨 worker 媒体中继。进程内走 channel，跨主机走 UDP。
- **ClusterControl**：跨主机时，信令通过 TCP 把命令路由到远端 worker。

---

## 2. 环境要求与端口清单

### 2.1 环境

| 项 | 要求 |
|---|---|
| 操作系统 | **Linux**（依赖 `recvmmsg` / `sendmmsg` / `epoll` / `sched_setaffinity`） |
| Rust | 1.75+（edition 2021） |
| 内存 | 单 worker 约 200MB 起；大规模按订阅数增长 |
| CPU | 每 worker 独占一核（建议 `SFU_WORKERS ≤ 物理核数`） |

### 2.2 端口清单

| 用途 | 协议 | 默认 | 是否必开 | 说明 |
|---|---|---|---|---|
| WebSocket 信令 | TCP | 5174 | 是 | `SFU_WS_ADDR` |
| QUIC 信令 | UDP | 5173 | 否 | `SFU_QUIC_ADDR`；移动端优先 |
| 媒体（RTP/RTCP/STUN/DTLS） | UDP | 5175 … 5175+N-1 | 是 | `SFU_MEDIA_ADDR`，N=`SFU_WORKERS` |
| WHIP 推流接入 | TCP | 5180 | 按需 | `SFU_WHIP_ADDR` |
| 跨主机中继 | UDP | 5390 | 跨主机时 | `SFU_RELAY_LISTEN` |
| 跨主机控制 | TCP | 5400 | 跨主机时 | `SFU_CONTROL_LISTEN` |

> 媒体端口按 worker 递增：worker i = `SFU_MEDIA_ADDR` 端口 + i。
> 防火墙需放行 **[base, base+N-1] 整段 UDP**。

---

## 3. 编译

```bash
git clone <repo> && cd webrtc-sfu
cargo build --release            # 产出 target/release/sfu-server
cargo build --release --examples # 压测/示例工具（可选）
```

产物：
| 文件 | 用途 |
|---|---|
| `target/release/sfu-server` | 服务端 |
| `target/release/examples/load_publisher` | 压测发布器 |
| `target/release/examples/load_subscriber` | 压测订阅器 |
| `target/release/examples/quic_signaling_client` | QUIC 信令调试客户端 |

---

## 4. 系统调优（部署前必做）

### 4.1 内核网络参数

仓库自带脚本 `scripts/sysctl-sfu.sh`：

```bash
sudo bash scripts/sysctl-sfu.sh
```

核心是把 UDP socket 缓冲区上限从默认 4MB 调大（默认会被内核截断，高 fanout 时丢包）：

```bash
sudo sysctl -w net.core.rmem_max=67108864
sudo sysctl -w net.core.wmem_max=67108864
sudo sysctl -w net.core.rmem_default=4194304
sudo sysctl -w net.core.wmem_default=4194304
sudo sysctl -w net.core.netdev_max_backlog=250000
sudo sysctl -w net.ipv4.udp_mem="1556480 2075648 3112960"
```

启动日志会打印每个 worker **实际生效**的 `rcvbuf/sndbuf`，据此确认：

```
UDP socket 缓冲区生效值 ... requested=16777216 rcvbuf=33554432 sndbuf=33554432
```

### 4.2 文件描述符

每个参与者占用 1 个客户端 UDP fd + 信令连接；大规模压测/接入前：

```bash
ulimit -n 524288
```

### 4.3 防火墙示例（firewalld）

```bash
sudo firewall-cmd --permanent --add-port=5174/tcp     # WS
sudo firewall-cmd --permanent --add-port=5173/udp     # QUIC
sudo firewall-cmd --permanent --add-port=5175-5178/udp # 媒体段 (N=4)
sudo firewall-cmd --permanent --add-port=5180/tcp     # WHIP
sudo firewall-cmd --reload
```

---

## 5. 配置项完整参考

所有配置均为**环境变量**（无配置文件）。

| 变量 | 默认 | 说明 |
|---|---|---|
| `SFU_ANNOUNCE_IP` | 自动探测 | **对外通告 IP**，写入 SDP/ICE host 候选。生产必须显式设置（客户端可达地址）。 |
| `SFU_WORKERS` | `1` | worker 线程数（= 媒体端口数）。 |
| `SFU_DEFAULT_WORKER` | 未设 | 可选覆盖：强制未配置房间全落该 worker（默认按房间 id 哈希固定到单个 worker）。 |
| `SFU_ROOM_WORKERS` | 未设 | 房间级分片：`room=w1,w2;*=...`，配置多个 worker 后房间才跨 worker（单机多 worker / 多主机多 worker 都需配置）。 |
| `SFU_WS_ADDR` | `0.0.0.0:5174` | WebSocket 信令绑定地址。 |
| `SFU_MEDIA_ADDR` | `0.0.0.0:5175` | 媒体 UDP 绑定基地址。 |
| `SFU_QUIC_ADDR` | `0.0.0.0:5173` | QUIC 信令地址；设 `0`/`off`/空 则禁用。 |
| `SFU_BWE_INITIAL_BPS` | `1000000` | 订阅者侧 BWE 初始带宽（bit/s）。 |
| `SFU_SOCK_BUF_MB` | `16` | 媒体 socket 收/发缓冲区目标（MB），受内核上限截断。 |
| `SFU_WHIP_ADDR` | 未设 | WHIP 推流 HTTP 监听；不设则不启用。 |
| `SFU_SIGNALING` | `on` | 本进程是否跑信令；worker-only 进程设 `off`。 |
| `SFU_TOTAL_WORKERS` | `=SFU_WORKERS` | 整集群 worker 总数（全局编号 `0..total`）。 |
| `SFU_WORKER_OFFSET` | `0` | 本进程 worker 的全局编号起始。 |
| `SFU_CONTROL_LISTEN` | 未设 | 本进程 `MediaControl` gRPC 监听（信令 → 远端 worker 命令）。 |
| `SFU_CONTROL_PEERS` | 未设 | `idx=host:port,...`，远端全局 worker → 其 gRPC 地址。 |
| `SFU_RELAY_LISTEN` | 未设 | 本机收发跨主机中继 RTP 的 UDP 端点。 |
| `SFU_RELAY_PEERS` | 未设 | `idx=host:port,...`，远端全局 worker → 对端中继 UDP 地址。 |
| `SFU_JWT_SECRET` | 未设 | JWT HMAC 对称密钥（HS256/384/512）。配置后默认强制鉴权。 |
| `SFU_JWT_PUBLIC_KEY` / `SFU_JWT_PUBLIC_KEY_FILE` | 未设 | JWT 非对称公钥 PEM（RS/ES/EdDSA），与 `SFU_JWT_SECRET` 二选一。 |
| `SFU_JWT_ALGORITHM` | 按密钥推断 | JWT 算法，如 `HS256` / `RS256`。 |
| `SFU_JWT_ISSUER` / `SFU_JWT_AUDIENCE` | 未设 | 校验 `iss` / `aud`。 |
| `SFU_JWT_REQUIRED` | 配置密钥后 `on` | 是否强制所有 join 携带有效 token；`off` = 有 token 才校验。 |
| `RUST_LOG` | `info` | 日志级别，如 `info` / `sfu_server::worker::event_loop=debug`。 |

> 说明：`batch_size`(64)、`send_batch_size`(128)、`busy_poll_us`(0) 目前为编译期默认，无对应环境变量。

### 5.1 信令 → 媒体：控制面 gRPC

信令进程与 worker 进程可分离（worker-only 进程设 `SFU_SIGNALING=off`）。
信令对远端 worker 的命令通道由自研 TCP 改为 **gRPC**
（`proto/sfu.proto` 的 `MediaControl`），媒体面仍走 UDP。

`CreateParticipant` 为同步 RPC（返回 SDP answer），其余命令 fire-and-forget
（`SendCommand`）。跨主机媒体中继 RTP 按 `SFU_RELAY_PEERS` 点对点 UDP 直连。

> 构建需安装 `protoc`（编译 `proto/sfu.proto`）。

### 5.2 JWT 用户鉴权

默认不开启。一旦配置 `SFU_JWT_SECRET` 或 `SFU_JWT_PUBLIC_KEY`，服务默认强制
校验，未携带 / 无效 token 的连接无法创建参与者。

```bash
SFU_JWT_SECRET='<强密钥>' ./target/release/sfu-server
# 生成测试 token:
SFU_JWT_SECRET='<强密钥>' ./scripts/gen-jwt.sh alice room-1 publisher 3600
```

token 传递位置：

| 传输 | 携带方式 |
|---|---|
| WebSocket | 请求头 `Authorization: Bearer <jwt>` 或 `?token=<jwt>` |
| QUIC | `join` 消息的 `token` 字段（QUIC 无 HTTP 头） |
| WHIP | 请求头 `Authorization: Bearer <jwt>` 或 `?token=<jwt>` |

claims 约定：`sub`（必填，用户 ID，发布者 `participant_id` 必须等于它，
订阅者必须为 `<sub>-recv-<publisherId>`）、`exp`（必填，Unix 秒）、
`room` / `role`（可选，限定房间 / 角色：`role=subscriber` 只能订阅，
`publisher` 或省略则既可发布也可订阅）。鉴权失败返回 401；密钥不会写入日志。

---

## 6. 快速启动（单机）

```bash
# 最小可用（记得设 announce ip 为本机内网/公网 IP）
SFU_ANNOUNCE_IP=192.168.1.10 RUST_LOG=info ./target/release/sfu-server
```

一条命令启动 4 worker + WHIP：

```bash
SFU_ANNOUNCE_IP=192.168.1.10 \
SFU_WORKERS=4 \
SFU_WS_ADDR=0.0.0.0:5174 \
SFU_MEDIA_ADDR=0.0.0.0:5175 \
SFU_QUIC_ADDR=0.0.0.0:5173 \
SFU_WHIP_ADDR=0.0.0.0:5180 \
RUST_LOG=info \
./target/release/sfu-server
```

启动成功日志：

```
启动 WebRTC SFU 服务 config=SfuConfig { ... announce_ip: 192.168.1.10, ... }
UDP socket 缓冲区生效值 ... rcvbuf=33554432 sndbuf=33554432
已启动 4 个 worker 线程, 监听 0.0.0.0:5175 (媒体端口范围 5175..5178), 对外通告 IP 192.168.1.10
QUIC 信令服务器启动: quic://0.0.0.0:5173
WHIP 推流接入监听: http://0.0.0.0:5180
```

### systemd 服务

见 `deploy/sfu-server.service` 与 `deploy/sfu.env.example`：

```bash
sudo cp deploy/sfu.env.example /etc/sfu/sfu.env   # 修改后
sudo cp deploy/sfu-server.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now sfu-server
journalctl -u sfu-server -f
```

---

## 7. 多 worker 横向扩展

**默认行为：按房间 id 哈希把每个房间固定到一个 worker**（同房间同 worker，
不同房间自动分散到 `SFU_WORKERS` 个 worker 上）。这样单房间始终在单核内运行，
行为可预测；多个房间天然负载均衡。也可用 `SFU_DEFAULT_WORKER` 强制未配置房间
全部落到同一 worker。

要让**单个房间**跨 worker，必须**显式配置该房间**（`SFU_ROOM_WORKERS`）：

| 配置 | 含义 |
|---|---|
| `roomA=3` | roomA 全部参与者固定到全局 worker 3 |
| `roomB=0,1,2,3` | roomB 参与者轮转分布到 worker 0..3（单机多 worker） |
| `*=0,1,2,3` | 未单独配置的房间的兑底规则 |

- 跨 worker 的订阅经 `RelayHub`（进程内）自动中继，无需其他配置。
- 建议 `SFU_WORKERS` = 物理核数（每 worker 自 pin 一核）。
- 媒体端口段相应扩大：`[base, base+N-1]`。

```bash
# 4 worker，房间按哈希自动分散；只有 roomB 额外跨 worker
SFU_ANNOUNCE_IP=192.168.1.10 SFU_WORKERS=4 SFU_ROOM_WORKERS="roomB=0,1,2,3" \
  ./target/release/sfu-server
# 放行 UDP 5175-5178
```

---

## 8. 跨主机集群部署

### 8.1 模型：集中信令 + worker-only 主机

- **信令主机（Host A）**：跑 WebSocket/QUIC/WHIP + 若干本地 worker。
- **worker 主机（Host B…）**：`SFU_SIGNALING=off`，只跑 worker + 控制面 + relay。
- 客户端只连 Host A 的信令；媒体按分配到的 worker 所在主机直连（ICE 候选里是
  该主机的 `announce_ip`）。

### 8.2 worker 全局编号

整集群 worker 用全局编号 `0..SFU_TOTAL_WORKERS`。每台主机的
`[SFU_WORKER_OFFSET, SFU_WORKER_OFFSET + SFU_WORKERS)` 为本地编号。

> **单房间跨主机需要配置房间**：`SFU_ROOM_WORKERS` 使用同一套全局编号。
> 未配置的房间按房间 id 哈希落在某个全局 worker（也可能落在 Host B 上）；
> 如果希望某个房间跨主机，需显式配置。例如 Host A 上配置
> `SFU_ROOM_WORKERS="load=0,2"` 可让 load 房间在 Host A 的 worker0 与 Host B
> 的 worker2 之间分布，媒体经跨主机 relay 中继。

### 8.3 示例：2 台主机 × 2 worker

**Host A**（信令 + worker 0,1）：
```bash
SFU_SIGNALING=on
SFU_WORKERS=2
SFU_TOTAL_WORKERS=4
SFU_WORKER_OFFSET=0
# 只有 load 房间跨主机（全局 worker 0 与 2）；其余房间全在 worker 0
SFU_ROOM_WORKERS=load=0,2
SFU_ANNOUNCE_IP=10.0.0.1
SFU_WS_ADDR=0.0.0.0:5174
SFU_MEDIA_ADDR=0.0.0.0:5175
SFU_CONTROL_LISTEN=0.0.0.0:5400
SFU_CONTROL_PEERS=2=10.0.0.2:5400,3=10.0.0.2:5400
SFU_RELAY_LISTEN=0.0.0.0:5390
SFU_RELAY_PEERS=2=10.0.0.2:5391,3=10.0.0.2:5391
```

**Host B**（worker-only + worker 2,3）：
```bash
SFU_SIGNALING=off
SFU_WORKERS=2
SFU_TOTAL_WORKERS=4
SFU_WORKER_OFFSET=2
SFU_ANNOUNCE_IP=10.0.0.2
SFU_MEDIA_ADDR=0.0.0.0:5175
SFU_CONTROL_LISTEN=0.0.0.0:5401
SFU_CONTROL_PEERS=0=10.0.0.1:5400,1=10.0.0.1:5400
SFU_RELAY_LISTEN=0.0.0.0:5391
SFU_RELAY_PEERS=0=10.0.0.1:5390,1=10.0.0.1:5390
```

### 8.4 防火墙

跨主机需额外放行：**relay UDP 5390/5391**、**control TCP 5400/5401**，
以及各主机的**媒体 UDP 端口段**。

> 注意：**发布者当前会把所有 simulcast 层中继给订阅者主机**（由订阅端选层）。
> 跨主机带宽会偏大；后续可按订阅者 BWE 在中继端裁剪。

---

## 9. 浏览器使用（RoomChatWeb）

```bash
cd RoomChatWeb
npm install
npm run dev        # 打开 http://localhost:5173
```

在界面「服务器地址」填 `ws://<SFU_IP>:5174`，然后：
1. **发布**：点「加入」→ 授摄像头/麦克风 → 发布。
2. **订阅**：房间内其他人加入后，在侧栏点订阅。

默认服务器地址在 `RoomChatWeb/src/components/JoinScreen.tsx`，可改为你的 SFU 地址后构建：

```bash
npm run build      # 产出 RoomChatWeb/dist 静态文件，用 nginx 托管
```

---

## 10. WHIP 推流接入（FFmpeg / GStreamer / OBS）

启用：`SFU_WHIP_ADDR=0.0.0.0:5180`。推流者会被当作房间内的普通发布者，
WebSocket 订阅者可直接订阅。

### 10.1 FFmpeg（8.x 原生 `-f whip`）

```bash
ffmpeg -re \
  -f lavfi -i testsrc2=size=1280x720:rate=30 \
  -f lavfi -i sine=frequency=440 \
  -c:v libopenh264 -b:v 2000k -g 60 -pix_fmt yuv420p \
  -c:a libopus -b:a 96k -ar 48000 -ac 2 \
  -f whip "http://<SFU_IP>:5180/whip?room=demo"
```

推文件/拉流同理：

```bash
ffmpeg -re -i input.mp4 \
  -c:v libopenh264 -b:v 2500k -g 60 -c:a libopus -b:a 128k \
  -f whip "http://<SFU_IP>:5180/whip?room=demo"
```

> ⚠️ FFmpeg 的 whip muxer **视频仅支持 H.264**（`libx264` / `libopenh264` / `h264_nvenc`…），
> 音频支持 Opus。H.264 + Opus 是 WebRTC 可直通编码，**无需转码**。

### 10.2 GStreamer

推荐 gst-plugins-rs 的 `whipsink`：

```bash
gst-launch-1.0 videotestsrc is-live=true ! \
  video/x-raw,width=1280,height=720,framerate=30/1 ! \
  x264enc tune=zerolatency ! rtph264pay config-interval=1 pt=96 ! \
  whipsink whip-endpoint="http://<SFU_IP>:5180/whip?room=demo"
```

无 `whipsink` 时可用 `webrtcbin` 自建 WHIP 客户端；FFmpeg 亦可作为通用兜底。

### 10.3 OBS

输出 → 添加「WHIP」→ 服务器填 `http://<SFU_IP>:5180/whip?room=demo`。

### 10.4 WHIP 接口（RFC 9725 子集）

| 方法 | 路径 | 作用 |
|---|---|---|
| `OPTIONS` | `/whip` | CORS / 能力探测 |
| `POST` | `/whip?room=<房间>` | 提交 SDP offer → `201` + SDP answer（含 SFU ICE 候选）；响应头 `Location`、`X-Participant-Id` |
| `PATCH` | `/whip/<id>` | trickle ICE（`application/trickle-ice-sdpfrag`） |
| `DELETE` | `/whip/<id>` | 停止推流 |

---

## 11. 信令协议（JSON）

WebSocket 与 QUIC 承载**完全相同**的 JSON 消息。一条连接可承载多个 `RTCPeerConnection`，
用 `pc_id` 区分。

### 11.1 客户端 → 服务端

| `type` | 字段 | 说明 |
|---|---|---|
| `hello` | `protocol_version?`, `client?` | 可选握手；服务端回 `welcome` |
| `ping` | `t?` | 应用层心跳；服务端回 `pong` |
| `join` | `pc_id`, `room_id`, `participant_id`, `sdp`, `role?`, `token?` | 建立发布/订阅连接。`role`=`publisher`(默认)/`subscriber` |
| `ice_candidate` | `pc_id`, `candidate`, `sdp_mid?`, `sdp_mline_index?` | trickle ICE |
| `subscribe` | `pc_id`, `publisher_id`, `track_mid`, `subscriber_mid?` | 订阅某发布者轨道 |
| `unsubscribe` | `pc_id`, `publisher_id`, `track_mid` | 取消订阅 |
| `update_media` | `pc_id`, `audio_enabled?`, `video_enabled?` | 静音/关摄像头，广播给房间内其他人 |
| `get_room_state` | `room_id?` | 请求房间成员快照（重连/刷新） |
| `leave` | `pc_id` | 离开 |

### 11.2 服务端 → 客户端

| `type` | 字段 | 说明 |
|---|---|---|
| `welcome` | `protocol_version`, `server`, `features[]` | 握手回复（协议版本/能力集） |
| `pong` | `t?` | 心跳回复 |
| `sdp_answer` | `pc_id`, `sdp`, `ice_candidates[]` | SDP 应答 + SFU ICE 候选（内联） |
| `joined` | `pc_id`, `participant_id`, `room_id`, `role`, `participants[]` | `join` 确认 + 成员快照 |
| `left` | `pc_id` | `leave` 确认 |
| `participant_joined` | `participant_id` | 房间新人加入（对 `joined.participants` 去重） |
| `participant_left` | `participant_id` | 有人离开 |
| `participant_updated` | `participant_id`, `audio_enabled`, `video_enabled` | 成员媒体状态变化 |
| `subscribed` | `pc_id`, `publisher_id`, `track_mid` | 订阅已受理 |
| `unsubscribed` | `pc_id`, `publisher_id`, `track_mid` | 取消订阅确认 |
| `room_state` | `room_id`, `participants[]` | 成员快照（响应 `get_room_state`） |
| `error` | `pc_id?`, `code`, `error` | 错误（`code` 见下） |

`code` 取值：`bad_request` / `unauthorized` / `not_joined` / `already_joined` /
`unknown_session` / `worker_error` / `worker_unavailable` / `unsupported_protocol` / `internal`。

> 说明：SFU 的 ICE 候选为**单端口 host 候选**，直接内联在 `sdp_answer.ice_candidates` 中，
> 当前不发送服务端 trickle `ice_candidate`。

### 11.3 `pc_id` 约定

- 发布连接：`pc_id = "pub"`（或任意唯一值），`role="publisher"`。
- 订阅连接：`pc_id = "sub-<publisherId>"`，`role="subscriber"`（每个发布者一条）。

---

## 12. 媒体功能说明

### 12.1 RTP mode / 包级转发
所有 `Rtc` 以 `set_rtp_mode(true)` 构建，SFU 做**包级转发**（不重组帧、不重新分包）。

### 12.2 Simulcast（RID 选层）
- 发布者发送多路 simulcast（每路带 RID）；SFU 用 500ms 窗口实测各层码率。
- 每 500ms 按**订阅者 BWE**（留 10% 余量）选码率不超限的最高层。
- 切层时向目标 RID 发 PLI，**等目标层关键帧到达**才切换，避免花屏。
- 逐订阅者**重写 RTP 序号**，切层/丢层后序号连续（NACK 正确性前提）。

### 12.3 SVC（时间层）
- 单条 **VP8** 流的时间层按 TID 降级（自研 VP8 payload-descriptor 解析）。
- **不支持** VP9/AV1 空间层选择（str0m 公开 API 不暴露空间层索引），已在代码中标注。

### 12.4 BWE
- 订阅者侧启用带宽估计（TWCC/REMB），`Event::EgressBitrateEstimate` 驱动选层。
- `SFU_BWE_INITIAL_BPS` 设初始值；desired bitrate 自动设为各层码率之和以探测上探。

### 12.5 跨 worker / 跨主机中继
- 同进程跨 worker：`RelayHub`（channel + 唤醒管道）。
- 跨主机：媒体走 UDP relay，控制命令走 TCP 控制面。
- 结构详见 `docs/multi-worker-relay.md`。

---

## 13. 压测工具

### 13.1 发布器

```bash
./target/release/examples/load_publisher \
  --url ws://127.0.0.1:5174 --room load \
  --count 1 --duration 60 \
  --advertise 192.168.1.10 \
  --video-kbps 1500 --fps 30
# 输出: [mids] load-0 <audio_mid> <video_mid>
```

### 13.2 订阅器

```bash
./target/release/examples/load_subscriber \
  --url ws://127.0.0.1:5174 --room load \
  --count 100 --duration 60 \
  --advertise 192.168.1.10 \
  --publisher load-0:<audio_mid>:<video_mid>
```

| 参数 | 说明 |
|---|---|
| `--url` | 信令地址（默认 `ws://127.0.0.1:5174`） |
| `--room` | 房间（须与发布者一致） |
| `--count` | 数量 |
| `--duration` | 持续秒数 |
| `--advertise` | 压测客户端对外 IP（须与 SFU 互通） |
| `--video-kbps` / `--fps` | 仅发布器：每路视频码率/帧率 |
| `--publisher` | 仅订阅器：`<id>:<audio_mid>:<video_mid>`，可重复 |

> 压测客户端本身消耗 CPU 做 SRTP 解密；测 SFU 单核能力时建议把压测进程
> 放到其它核或另一台机器。

### 13.3 QUIC 信令调试

```bash
./target/release/examples/quic_signaling_client <host:port> <cert-fingerprint-hex>
```

---

## 14. 运维与排障

### 14.1 日志

```bash
RUST_LOG=info ./target/release/sfu-server
# 更详细: RUST_LOG=sfu_server::worker::event_loop=debug
```

关键日志：
- `Worker 就绪, 监听 ... (对外通告 ...)` — 绑定/通告地址
- `UDP socket 缓冲区生效值 ... rcvbuf=... sndbuf=...` — 是否被内核截断
- `参与者 X 已连接` / `发布媒体 mid=... kind=...`
- `WHIP 推流建立 ...` / `WHIP 推流停止 ...`

### 14.2 常见问题

| 现象 | 排查 |
|---|---|
| 客户端 ICE 连不上 | `SFU_ANNOUNCE_IP` 是否客户端可达；媒体 UDP 端口段是否放行 |
| 有信令但无画面 | 检查媒体端口段防火墙；`announce_ip` 是否为真实网卡地址 |
| 高并发丢包 | 执行 `scripts/sysctl-sfu.sh`；确认日志 `rcvbuf/sndbuf` 已变大 |
| 单房间到 ~200 路劣化 | 提升 `SFU_WORKERS` 并配置 `SFU_ROOM_WORKERS` 让该房间跨 worker（经 RelayHub 中继） |
| FFmpeg WHIP 报编码错误 | whip muxer 视频**仅支持 H.264**，用 `libopenh264`/`libx264` |
| 订阅无画面（WHIP 源） | 确认订阅 `--publisher <X-Participant-Id>:<amid>:<vmid>`，mid 见日志 |

### 14.3 观测 worker 负载

```bash
# 找到媒体线程并看其 CPU
top -H -p $(pgrep -x sfu-server) | grep sfu-worker
```

---

## 15. 安全注意事项

- 本项目 **ICE Lite + host 候选**，不含 TURN/STUN 服务端与鉴权；请部署在受信网络，
  或用安全组/防火墙限制来源。
- 没有内置 TLS：WebSocket 为明文 `ws://`，QUIC 用自签名证书。
  **生产建议前置 nginx/负载均衡做 TLS 终止**。
- **服务端不校验鉴权**：WHIP 的 `Authorization: Bearer` 头虽被客户端支持
  （FFmpeg `-authorization` / OBS 对应字段），但当前服务端**不强制校验**。
  若需鉴权，请在反向代理/网关层实现（校验后再转发到 WHIP/WS 端口）。
- WHIP / 信令端口按需暴露，避免 0.0.0.0 直接暴露公网。
- 媒体端口段仅需对媒体来源开放。

---

## 附：相关文档

- 架构与实现细节：`README.md`
- 多 worker / 跨主机中继设计：`docs/multi-worker-relay.md`
- 内核调优脚本：`scripts/sysctl-sfu.sh`
- 部署样例：`deploy/`
