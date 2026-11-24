<div align="center">

# 🎥 webrtc-sfu

**用 Rust 编写的高性能 WebRTC SFU（选择性转发单元）。**

Thread-per-core · 零拷贝 RTP 转发 · 批量系统调用 · WHIP/WHEP · QUIC 信令

[![Rust](https://img.shields.io/badge/Rust-1.75%2B-orange)](#)
[![License](https://img.shields.io/badge/License-MIT-blue)](#license)
[![Platform](https://img.shields.io/badge/Platform-Linux%20x86__64-lightgrey)](#requirements)
[![WebRTC](https://img.shields.io/badge/WebRTC-str0m%20%7C%20WHIP%20%7C%20WHEP-green)](#)

[English](./README.md) · [架构设计](./docs/design.md) · [使用手册](./docs/manual.md)

</div>

---

**webrtc-sfu** 是一个面向多人实时音视频的 SFU。它基于
[`str0m`](https://github.com/algesten/str0m)（一个 Sans-I/O 的 WebRTC 协议栈），并在媒体热路径上
实现了**全同步**的事件循环，完全避开 async 开销。每个媒体 worker 独占并绑定到一个 CPU 核，使用
`recvmmsg`/`sendmmsg` 批量收发（可选 `io_uring` 后端），并以共享 `Arc<[u8]>` 负载的方式转发 RTP
包 —— 不重组帧、不重新分包、不为每个订阅者拷贝数据。

---

## ✨ 特性亮点

- **Thread-per-core、share-nothing 的 worker** —— 每个 worker 独占自己的 UDP socket、`str0m`
  `Rtc` 实例、路由表和定时器，并 pin 到独立 CPU 核。
- **全程批量 I/O** —— `recvmmsg`/`sendmmsg` 单次系统调用收发 64+ 个包；`io_uring` 收包后端
  自动探测，旧内核上自动回退到 `epoll + recvmmsg`。
- **零拷贝包级转发** —— `set_rtp_mode(true)`；RTP 负载以 `Arc<[u8]>` 在订阅者间共享，仅重写
  PT/SSRC。
- **逐订阅者带宽自适应** —— 基于 TWCC/REMB 的 BWE，支持 simulcast（RID）与 SVC（VP8/VP9 时间层
  与空间层）选层，并逐订阅者重写 RTP 序号。
- **一套信令协议、两种传输** —— WebSocket 与 QUIC 承载**完全相同**的 JSON 协议；QUIC 让移动端在
  WiFi/蜂窝切换时做到连接迁移不断线。
- **标准推流/拉流接入** —— **WHIP**（RFC 9725 子集）支持 FFmpeg/GStreamer/OBS 推流，**WHEP**
  支持拉流，均带 trickle-ICE。
- **可选 JWT 鉴权** —— 支持 HMAC（HS256/384/512）与非对称（RS/ES/EdDSA）验签，可校验
  `sub`/`exp`/`room`/`role` 声明。
- **可横向扩展** —— 房间级分片跨 worker/跨主机、gRPC 控制面（`MediaControl`）、UDP 跨主机中继、
  以及面向多信令节点的信令分配器。
- **开箱即用的参考客户端** —— Web、Android、iOS、HarmonyOS NEXT 与多个桌面端实现（见
  下方**参考客户端**）。

---

## 📊 单核容量

使用仓库自带压测工具（`examples/load_publisher` / `load_subscriber`）实测：1 路
**1.5 Mbps@30fps 视频 + 64 kbps Opus 音频**推流、订阅端多进程并行。完整测试环境与方法见
[`docs/capacity.md`](./docs/capacity.md)。

| 订阅内容 | 满质量订阅者数 / 核 | Worker CPU |
| --- | ---: | ---: |
| 音频 + 视频 | **≈ 200** | ≈ 79% |
| 仅视频 | **≈ 250** | ≈ 75% |
| 仅音频 | **≈ 500** | ≈ 70% |

- 瓶颈是「每订阅者·每包」的转发量（SRTP 加密 + `write_rtp`）：单核约
  **2.5–4 万次订阅者转发/秒**。
- 满质量区间内 `io_uring` 与 `epoll` CPU 相差在 ~2% 以内；过载时 `io_uring` 投递量高约
  28–40%。
- 单房间跨 2 个 worker 可达 **≈ 300–320** 路满质量订阅（约 1.3–1.5×），代价是总 CPU 多约
  60%（share-nothing 架构下目标 worker 需重跑每包处理）。

---

## 🏗️ 架构

```
                    ┌─────────────────────────────────────────────┐
  Browser / App ───►│  Signaling (Tokio, async)                   │
   ws / quic / http │  WS :5174 · QUIC :5173 · WHIP :5180 · WHEP  │
                    │  rooms · SDP · JWT · signaling dispatcher   │
                    └──────────────────────┬──────────────────────┘
                                           │ WorkerCommand
                                           │ (channel / gRPC)
        ┌──────────────────────────────────┼──────────────────────────────────┐
        ▼                                  ▼                                  ▼
  ┌───────────┐                      ┌───────────┐                      ┌───────────┐
  │ Worker 0  │                      │ Worker 1  │        ...           │ Worker N  │
  │ CPU 0     │                      │ CPU 1     │                      │ CPU N     │
  │ UDP 5175  │◄── RelayHub ────────►│ UDP 5176  │◄────────────────────►│ UDP 5175+N│
  │ recvmmsg  │                      │ recvmmsg  │                      │ recvmmsg  │
  │ sendmmsg  │                      │ sendmmsg  │                      │ sendmmsg  │
  └───────────┘                      └───────────┘                      └───────────┘
```

默认按 `room_id` 哈希把房间固定到某个 worker；通过 `SFU_ROOM_WORKERS` 可显式让单个房间跨
worker/主机分布，媒体经 `RelayHub` 平面中继。完整设计见 [`docs/design.md`](./docs/design.md)。

---

## 🚀 快速开始

### 环境要求

- **Linux**（依赖 `recvmmsg`/`sendmmsg`/`epoll`/`sched_setaffinity`；`io_uring` 可选，内核 5.11+）
- **Rust 1.75+**
- **protoc**（编译 `proto/sfu.proto`）—— 如 `apt install protobuf-compiler` / `dnf install protobuf-compiler`

### 编译与运行

```bash
git clone <repo-url> && cd webrtc-sfu

# 高 fan-out 部署前建议执行：
sudo bash scripts/sysctl-sfu.sh

cargo build --release

# SFU_ANNOUNCE_IP 必须是客户端可达的 IP（写入 SDP/ICE 候选）
SFU_ANNOUNCE_IP=192.168.1.10 RUST_LOG=info ./target/release/sfu-server
```

默认端口：

| 服务 | 协议 | 默认端口 |
| --- | --- | ---: |
| WebSocket 信令 | TCP | 5174 |
| QUIC 信令 | UDP | 5173 |
| 媒体（RTP/RTCP/STUN/DTLS） | UDP | 5175 … 5175+N-1 |
| WHIP 推流（可选，`SFU_WHIP_ADDR`） | TCP | 5180 |
| WHEP 拉流（可选，`SFU_WHEP_ADDR`） | TCP | 5181 |

### 用 FFmpeg 推流（WHIP）

```bash
ffmpeg -re -f lavfi -i testsrc2=size=1280x720:rate=30 -f lavfi -i sine=frequency=440 \
  -c:v libopenh264 -b:v 2000k -g 60 -pix_fmt yuv420p \
  -c:a libopus -b:a 96k -ar 48000 -ac 2 \
  -f whip "http://<SFU_IP>:5180/whip?room=demo"
```

然后打开 Web 客户端加入 `room=demo` 并订阅这个 FFmpeg 发布者即可。GStreamer、OBS 示例、WHEP
拉流以及完整信令协议见 [`docs/manual.md`](./docs/manual.md)。

---

## 🧩 参考客户端

仓库内置了多套相互兼容的客户端，它们使用同一套信令协议：

| 客户端 | 目录 | 技术栈 |
| --- | --- | --- |
| Web | [`RoomChatWeb`](./RoomChatWeb) | Solid + TypeScript |
| Android | [`RoomChatAndroid`](./RoomChatAndroid) | Kotlin + Jetpack Compose |
| iOS | [`RoomChatApple`](./RoomChatApple) | Swift |
| HarmonyOS NEXT | [`RoomChatHarmony`](./RoomChatHarmony) | ArkTS |
| 桌面（GPUI） | [`RoomChatDesktopGpui`](./RoomChatDesktopGpui) | GPUI + reactor-webrtc |
| 桌面（Slint + libwebrtc） | [`RoomChatDesktopSlint`](./RoomChatDesktopSlint) | Slint + reactor-webrtc |
| 桌面（Slint + str0m） | [`RoomChatDesktopSlintStr0m`](./RoomChatDesktopSlintStr0m) | Slint + str0m + ffmpeg-next |
| 桌面（Slint + GStreamer） | [`RoomChatDesktopGStreamer`](./RoomChatDesktopGStreamer) | Slint + GStreamer webrtcbin |

---

## ⚙️ 配置

所有配置均为环境变量（无配置文件）。核心变量：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `SFU_ANNOUNCE_IP` | 自动探测 | 对外通告 IP，写入 SDP 与 ICE 候选 |
| `SFU_WORKERS` | `1` | worker 线程数（每个占一个媒体端口）；`0` = 纯信令节点 |
| `SFU_CPU_BASE` | `0` | worker `i` pin 到 CPU `cpu_base + i` |
| `SFU_ROOM_WORKERS` | — | 房间 → 全局 worker 列表，如 `load=0,1,2,3` |
| `SFU_WS_ADDR` | `0.0.0.0:5174` | WebSocket 信令监听 |
| `SFU_QUIC_ADDR` | `0.0.0.0:5173` | QUIC 信令监听 |
| `SFU_MEDIA_ADDR` | `0.0.0.0:5175` | 媒体 UDP 基地址 |
| `SFU_WHIP_ADDR` / `SFU_WHEP_ADDR` | — | 启用 WHIP / WHEP HTTP 端点 |
| `SFU_IO_URING` | `auto` | I/O 后端：`auto` / `on` / `off`（epoll） |
| `SFU_JWT_SECRET` | — | JWT HMAC 密钥，设置后启用鉴权 |
| `SFU_JWT_PUBLIC_KEY_FILE` | — | JWT 非对称公钥（RS/ES/EdDSA） |
| `SFU_SIGNALING` | `on` | worker-only 主机设 `off` |
| `SFU_CONTROL_LISTEN` / `SFU_CONTROL_PEERS` | — | gRPC 控制面（跨主机） |
| `SFU_RELAY_LISTEN` / `SFU_RELAY_PEERS` | — | UDP 跨主机媒体中继 |
| `SFU_DISPATCHER_ADDR` | — | 运行信令分配器（多信令节点） |

完整参考、部署形态（单进程、多 worker、跨主机、纯信令节点、分配器）与排障见
[`docs/manual.md`](./docs/manual.md) 与 [`docs/deployment.md`](./docs/deployment.md)。

---

## 📚 文档

| 文档 | 内容 |
| --- | --- |
| [`docs/manual.md`](./docs/manual.md) | 使用：编译、配置、信令协议、WHIP/WHEP、压测、运维 |
| [`docs/deployment.md`](./docs/deployment.md) | 逐步部署拓扑与全部环境变量 |
| [`docs/design.md`](./docs/design.md) | 完整架构与实现细节 |
| [`docs/capacity.md`](./docs/capacity.md) | 单核容量实测与 io_uring/epoll 对比 |
| [`docs/multi-worker-relay.md`](./docs/multi-worker-relay.md) | 跨 worker / 跨主机中继设计 |
| [`docs/signaling-dispatcher.md`](./docs/signaling-dispatcher.md) | 多信令节点分配（一致性哈希） |

---

## 🗂️ 项目结构

```
webrtc-sfu/
├── proto/sfu.proto            # gRPC MediaControl 控制面定义
├── src/
│   ├── main.rs                # 入口：workers + 信令 + gRPC 控制面
│   ├── config.rs              # 全部环境变量配置
│   ├── worker/                # per-core 事件循环与跨 worker 中继
│   ├── sfu/                   # 路由表、参与者、simulcast/SVC 选层
│   ├── net/                   # recvmmsg/sendmmsg UDP、epoll、io_uring、中继编解码
│   ├── signaling/             # WebSocket / QUIC / WHIP / WHEP / JWT
│   ├── grpc/                  # 信令 → 媒体 gRPC 控制面
│   ├── cluster/               # 本地 channel / 远端 gRPC 路由
│   └── dispatcher/            # 信令分配器（房间 → 信令节点）
├── examples/                  # load_publisher、load_subscriber、whep_client、quic 客户端
├── scripts/                   # sysctl 调优、JWT 工具
└── deploy/                    # systemd 单元与环境变量示例
```

---

## 🔐 安全说明

- SFU 为 **ICE-Lite + host 候选**，不含 TURN/STUN 服务端；请部署在受信网络或用防火墙/安全组
  限制来源。
- WebSocket 默认为明文 `ws://`，QUIC 默认使用自签名证书；生产建议前置 nginx/负载均衡做 TLS
  终止。
- 接入鉴权请启用 JWT（`SFU_JWT_SECRET` / `SFU_JWT_PUBLIC_KEY*`）。

---

## 📄 License

MIT —— 见 [`LICENSE`](./LICENSE)。

---

## 🙌 致谢

- [`str0m`](https://github.com/algesten/str0m) —— 驱动媒体路径的 Sans-I/O WebRTC 协议栈。
- [`quinn`](https://github.com/quinn-rs/quinn) —— 信令的 QUIC 传输。
- [`nix`](https://github.com/nix-rust/nix) —— 底层 Linux 系统调用绑定。
