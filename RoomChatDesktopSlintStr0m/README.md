# RoomChat Desktop · Slint + str0m + ffmpeg-next

基于 **Slint** (UI) + **str0m** (纯 Rust、Sans-IO 的 WebRTC 协议栈) +
**ffmpeg-next** (libav*: 采集 / VP8 / Opus / 缩放 / 重采样) 的桌面视频通话
客户端, 对接本仓库的 WebRTC SFU 服务端。功能与 `RoomChatDesktopSlint`
(reactor-webrtc / libwebrtc 版) 完全一致。

```
┌──────────────────────────────────────────────────────────────┐
│ RoomChatDesktopSlintStr0m                                     │
│                                                               │
│  Slint 主线程                 sfu-client 线程 (str0m)         │
│  ┌───────────────┐  命令     ┌────────────────────────────┐   │
│  │ AppWindow     │ ────────► │ Rtc("pub")                 │   │
│  │ 视频网格       │           │  ├─ sendonly audio (Opus)  │   │
│  │ 参与/日志侧栏   │ ◄──────── │  └─ sendonly video (VP8)   │   │
│  │ 控制栏         │  UiEvent  │ Rtc("sub-<发布者>") ×N      │   │
│  └───────────────┘           │  recvonly audio + video    │   │
│        ▲                     └───────┬───────────┬────────┘   │
│        │ RGBA 帧                     │ 已编码帧   │ UDP        │
│  ┌─────┴───────────────────┐  ┌──────▼──────┐    │            │
│  │ ffmpeg-next 媒体线程     │  │ crossbeam   │    │            │
│  │ 采集 / 编解码 / 缩放/重采样│  │ channel     │    │            │
│  └─────────────────────────┘  └─────────────┘    │            │
└───────────────────────────────────────────────┼───────────────┘
                                                │ WS ws://host:5174
                                       ┌────────▼────────┐
                                       │  SFU 信令服务    │
                                       │  (src/signaling) │
                                       └─────────────────┘
```

## 与 `RoomChatDesktopSlint` 的差异

| 关注点 | RoomChatDesktopSlint | 本目录 (Str0m) |
|--------|----------------------|----------------|
| WebRTC 传输 | reactor-webrtc (libwebrtc 封装) | **str0m** (纯 Rust, 自带 ICE/DTLS/SRTP/RTP) |
| 采集 / 编解码 | libwebrtc 内置 + nokhwa | **ffmpeg-next** (libavdevice / libavcodec / libswscale / libswresample) |
| 视频编码 | libwebrtc VP8 | libvpx VP8 (ffmpeg-next) |
| 音频编码 | libwebrtc Opus | libopus (ffmpeg-next) |
| 远端视频渲染 | libwebrtc 解码 → BGRA | ffmpeg-next 解码 → swscale → RGBA |
| 远端音频播放 | 平台 ADM 自动播放 | ffmpeg-next 解码 → 系统音频输出设备 |
| 构建产物 | 需下载数百 MB libwebrtc | 只依赖系统 FFmpeg 开发库 |

UI (`ui/app.slint`)、信令协议 (`src/protocol.rs`)、信令客户端
(`src/signaling.rs`) 与 `RoomChatDesktopSlint` 保持一致, 因此两个客户端可以
在同一个 SFU 房间里互通。

## 功能

- 加入房间 (信令地址 / 房间号 / 昵称)
- **真实摄像头 + 真实麦克风**: 摄像头经 libavdevice 采集、libavcodec 解码后
  转 RGBA; 麦克风采集后重采样为 48kHz 单声道
- 摄像头不可用时自动回退到合成测试图案; 可取消勾选「使用真实摄像头和麦克风」
  强制使用合成视频 + 合成 440Hz 正弦波 (联调)
- 自动订阅房间内其他参与者 (音频 + 视频), 在视频网格中实时渲染
- 参与者侧栏: 手动订阅 / 取消订阅、状态指示、事件日志
- 控制栏: 麦克风开关、摄像头开关 (暂停采集)、挂断
- 单条 WebSocket 复用多个 PeerConnection (`pc_id` 区分 `pub` / `sub-<发布者>`)

## 构建与运行

### 前置条件

- Rust 1.92+ (Slint 1.18)
- **FFmpeg 开发库** (ffmpeg-next 通过 `pkg-config` 查找):
  - Fedora: `sudo dnf install ffmpeg-free-devel`
  - Debian/Ubuntu: `sudo apt install libavcodec-dev libavformat-dev \
    libavutil-dev libavfilter-dev libavdevice-dev libswscale-dev \
    libswresample-dev`
  - macOS: `brew install ffmpeg`
  - **无 sudo 的 Fedora**: `./scripts/setup-ffmpeg-dev.sh` (把 `*-free-devel`
    RPM 免 root 解压到 `~/.local/share/ffmpeg-dev`; `.cargo/config.toml` 已指向
    该目录)
- 先启动 SFU 服务端 (默认 WebSocket 信令 `ws://<host>:5174`, 媒体 UDP 5175+):
  ```bash
  # 在仓库根目录
  SFU_ANNOUNCE_IP=<可达IP> RUST_LOG=info cargo run --release
  ```

### 运行

```bash
cd RoomChatDesktopSlintStr0m
cargo run --release
```

首次运行会弹出**摄像头 / 麦克风授权**对话框, 允许后即可通话。
macOS 上可用 `./macos/bundle.sh` 打包成 `.app` (对 TCC 最可靠)。

### 可选环境变量

| 变量 | 说明 | 默认 |
|------|------|------|
| `SFU_CAMERA_DEVICE` | 摄像头设备 | Linux `/dev/video0`; macOS `0`; Windows dshow 名称 |
| `SFU_AUDIO_INPUT` | 麦克风输入 (`<格式> <设备>`) | Linux `pulse default`; macOS `avfoundation :0` |
| `SFU_AUDIO_SINK` | 音频输出 (`<格式> <设备>`) | Linux `pulse default`; macOS `audiotoolbox -` |
| `SFU_ADVERTISE_IP` | 本机对外公布的 ICE host IP | 由信令地址自动推导 |
| `RUST_LOG` | 日志级别 | `warn` |

## 音频

| 模式 | 采集 | 播放远端音频 | 权限 |
|------|------|--------------|------|
| 「使用真实摄像头和麦克风」打开 (默认) | libavdevice 麦克风 + 真实摄像头 | 解码后写入系统音频设备 | 需系统授权 |
| 关闭 | 合成 440Hz 正弦波 + 合成视频 | 仍会尝试解码播放 | 不需要 |

静音实现: 暂停/恢复向 Opus 编码器喂入 PCM (不关闭采集设备, 恢复无需重新授权)。

## 信令协议

与 `src/signaling/protocol.rs` 完全一致 (协议 v2):

- `join` (`pc_id`: `pub` / `sub-<发布者>`; `role`: `publisher` / `subscriber`)
- `ice_candidate` (trickle ICE)
- `subscribe` / `unsubscribe` (`track_mid` = 发布者 mid; `subscriber_mid` =
  本地 recvonly mid; str0m 会为每条 m-line 随机分配 mid, 因此必须显式上报)
- `joined` / `participant_joined` / `participant_tracks` (成员与轨道发现)
- `leave`

订阅流程: 从 `joined.participants` / `participant_joined.tracks` 读取发布者轨道
→ 创建 recvonly PC (音频 + 视频) → `join(role=subscriber)` → 收到 `sdp_answer`
后 `accept_answer` + 添加 SFU 候选 → 按发布者 mid 发 `subscribe` →
`Event::MediaData` 收到远端帧, 交给 ffmpeg-next 解码。

## 目录结构

```
RoomChatDesktopSlintStr0m/
├── Cargo.toml
├── build.rs                 # Slint 编译 + macOS Info.plist 嵌入
├── .cargo/config.toml       # FFmpeg 开发文件查找路径 (pkg-config)
├── macos/                   # Info.plist / bundle.sh
├── scripts/
│   └── setup-ffmpeg-dev.sh  # 免 root 准备 FFmpeg 开发文件 (Fedora)
├── ui/
│   └── app.slint            # 全部 UI (加入页 / 通话页 / 组件)
└── src/
    ├── main.rs              # Slint 启动、UI 模型、事件泵 (16ms)
    ├── protocol.rs          # 信令 JSON 类型
    ├── signaling.rs         # WebSocket 客户端 (tokio-tungstenite)
    ├── sfu.rs               # str0m worker (发布/订阅/Sans-IO 事件循环)
    ├── media.rs             # ffmpeg-next 采集 / VP8 / Opus / 缩放 / 重采样 / 播放
    └── ui.rs                # UiEvent / LogLevel / SubStatus
```

## 线程模型

- **主线程**: Slint 事件循环 + `slint::Timer` 每 16ms 排空 worker 事件, 更新
  `VecModel` 与 `image` 属性 (所有 Slint 对象只在主线程访问)。
- **sfu-client 线程**: 独占所有 `str0m::Rtc` 与 UDP socket。str0m 是 Sans-IO,
  因此在本线程内实现事件循环: `handle_input(Receive/Timeout)` →
  `poll_output` (发包 / 事件 / 下个超时)。
- **媒体线程**: 每个来源/去向一条 (`video-pub`、`camera-capture`、`video-synth`、
  `audio-pub`、`mic-capture`、`audio-synth`、`video-dec-<发布者>`、
  `audio-play`)。编解码全部在各自线程内完成, 不阻塞 worker。
- **signaling-rt**: 专用 tokio 运行时, 负责 WebSocket 读写。

各线程仅通过 crossbeam channel 通信: 主线程 → worker 用 `ClientCommand`,
worker → 主线程用 `UiEvent`, 媒体线程 → worker 用 `EncodedVideo` /
`EncodedAudio`, worker → 解码线程用原始编码帧。

## 说明

- 视频参数: 640×480 @ 30fps, VP8 ~1 Mbps; 音频: 48kHz 单声道 Opus 64kbps,
  20ms 帧。
- 远端视频统一用 `swscale` 缩放/加黑边到 640×480 RGBA 后交给 Slint。
- 远端音频用 `libavdevice` 的输出设备 (PulseAudio / ALSA / AudioToolbox / SDL)
  播放; 若设备不可用, 仅解码丢弃并记录警告 (不影响通话其他功能)。
- `ffmpeg-next` 依赖 FFmpeg 8.x (libavcodec 62); 更早的 FFmpeg 6/7 通常也可用
  (ffmpeg-sys-next 会按版本自动选择绑定)。
