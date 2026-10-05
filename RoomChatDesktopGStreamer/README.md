# RoomChat Desktop · Slint + GStreamer (webrtcbin)

基于 **Slint** (UI) + **GStreamer** (`webrtcbin` + 采集/编解码) 的桌面视频通话
客户端, 对接本仓库的 WebRTC SFU 服务端。功能与 `RoomChatDesktopSlint`
(reactor-webrtc / libwebrtc 版) 完全一致, 但 WebRTC 传输与媒体处理全部交给
GStreamer。

```
┌──────────────────────────────────────────────────────────────┐
│ RoomChatDesktopGStreamer                                     │
│                                                              │
│  Slint 主线程                 sfu-client 线程 (GLib MainLoop) │
│  ┌───────────────┐  命令     ┌────────────────────────────┐   │
│  │ AppWindow     │ ────────► │ webrtcbin "pub"            │   │
│  │ 视频网格       │           │  ├─ audiotestsrc/autoaudiosrc │
│  │ 参与/日志侧栏   │ ◄──────── │  │   → opusenc → rtpopuspay  │  │
│  │ 控制栏         │  UiEvent  │  └─ videotestsrc/autovideosrc │
│  └───────────────┘           │      → vp8enc → rtpvp8pay     │  │
│        ▲                     │ webrtcbin "sub-<发布者>" ×N     │   │
│        │ RGBA 帧 (appsink)   │  recvonly audio + video        │   │
│  ┌─────┴───────────────────┐ └───────┬───────────┬────────┘   │
│  │ GStreamer 内部流线程      │         │ SRTP/RTP   │ UDP        │
│  │ 采集 / 编解码 / appsink   │         │            │            │
│  └─────────────────────────┘         │            │            │
└──────────────────────────────────────┼────────────┼───────────┘
                                       │            │
                              WS ws://host:5174   UDP 5175+
                                    ┌──▼────────────▼──┐
                                    │  SFU 信令 + 媒体   │
                                    │  (src/signaling)  │
                                    └───────────────────┘
```

## 与 `RoomChatDesktopSlint` 的差异

| 关注点 | RoomChatDesktopSlint | 本目录 (GStreamer) |
|--------|----------------------|--------------------|
| WebRTC 传输 | reactor-webrtc (libwebrtc 封装) | **GStreamer webrtcbin** (libnice ICE + DTLS + SRTP) |
| 采集 / 编解码 | libwebrtc 内置 + nokhwa | **GStreamer**: `autovideosrc` / `autoaudiosrc`, `vp8enc` / `opusenc`, `decodebin` |
| 摄像头/麦克风开关 | libwebrtc transceiver / pause | `valve` 元素 (drop) |
| 远端视频渲染 | libwebrtc 解码 → BGRA | `decodebin → videoconvert → appsink` → RGBA |
| 远端音频播放 | 平台 ADM 自动播放 | `decodebin → audioconvert → autoaudiosink` |
| 构建产物 | 需下载数百 MB libwebrtc | 仅依赖系统 GStreamer 运行时/开发库 |

UI (`ui/app.slint`)、信令协议 (`src/protocol.rs`)、信令客户端
(`src/signaling.rs`)、事件模型 (`src/ui.rs`) 与 `RoomChatDesktopSlint`
保持一致, 因此不同客户端可在同一个 SFU 房间里互通。

## 功能

- 加入房间 (信令地址 / 房间号 / 昵称)
- **真实摄像头 + 真实麦克风** (GStreamer `autovideosrc` / `autoaudiosrc`),
  远端音频自动播放
- 摄像头/麦克风不可用时自动回退到合成源 (`videotestsrc` / `audiotestsrc`);
  也可取消勾选「使用真实摄像头和麦克风」强制合成源 (联调)
- 自动订阅房间内其他参与者 (音频 + 视频), 在视频网格中实时渲染
- 参与者侧栏: 手动订阅 / 取消订阅、状态指示、事件日志
- 控制栏: 麦克风开关、摄像头开关、挂断
- 发布静音/关摄像头时通过 `update_media` 通知房间内其他人
- 单条 WebSocket 复用多个 PeerConnection (`pc_id` 区分 `pub` / `sub-<发布者>`)

## 构建与运行

### 前置条件

- Rust 1.92+ (Slint 1.18 要求)
- **GStreamer 1.28+ 运行时 + 开发库** (gstreamer-rs 通过 `pkg-config` 查找;
  1.28 起 `webrtcbin` 的 `add-transceiver` 签名为 `(direction, caps)`):
  - Fedora: `sudo dnf install gstreamer1-devel gstreamer1-plugins-base-devel \
    gstreamer1-plugins-bad-free-devel gstreamer1-plugins-good \
    libnice-gstreamer1`
  - Debian/Ubuntu: `sudo apt install libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev libgstreamer-plugins-bad1.0-dev \
    gstreamer1.0-plugins-good gstreamer1.0-nice`
  - macOS: `brew install gstreamer`
  - **无 sudo 的 Fedora**: `./scripts/setup-gstreamer-webrtc-dev.sh`
    (把 `gstreamer1-plugins-bad-free-devel` 与 `libnice-gstreamer1` 两个 RPM
    免 root 解压到 `~/.local/share/gst-webrtc-dev`; `.cargo/config.toml` 已指向
    该目录, 程序启动时也会自动把 nice 插件目录加入 `GST_PLUGIN_PATH`)

  必需的 GStreamer 元素: `webrtcbin`、`autovideosrc`/`autoaudiosrc`、
  `videotestsrc`/`audiotestsrc`、`vp8enc`/`vp8dec`、`opusenc`/`opusdec`、
  `rtpvp8pay`/`rtpvp8depay`、`rtpopuspay`/`rtpopusdepay`、`decodebin`、
  `appsink`、`valve`。**`libnice` 的 `nicesrc`/`nicesink` (nice 插件) 是
  `webrtcbin` 的硬依赖**, 许多发行版默认不装, 需单独安装。
- 先启动 SFU 服务端 (默认 WebSocket 信令 `ws://<host>:5174`, 媒体 UDP 5175+):
  ```bash
  # 在仓库根目录
  SFU_ANNOUNCE_IP=<可达IP> RUST_LOG=info cargo run --release
  ```
  > 注意: 媒体 ICE 候选使用 `SFU_ANNOUNCE_IP`。本机联调时**不要用
  > `127.0.0.1`**, 请用局域网 IP (如 `192.168.x.y`): libnice 会绑定到具体
  > 网卡, 从非 loopback 网卡发往 `127.0.0.1` 的候选不可达。

### 运行

```bash
cd RoomChatDesktopGStreamer
cargo run --release
```

首次运行会弹出**摄像头 / 麦克风授权**对话框, 允许后即可通话。
macOS 上可用 `./macos/bundle.sh` 打包成 `.app` (对 TCC 最可靠)。

### 无界面冒烟测试

`roomchat-gst-headless` 复用与主程序相同的 worker 与媒体层, 把 `UiEvent`
打到 stdout, 用于 CI / 无显示环境验证发布 + 订阅链路:

```bash
cargo run --bin roomchat-gst-headless -- ws://127.0.0.1:5174 room-1 alice synth 30
cargo run --bin roomchat-gst-headless -- ws://127.0.0.1:5174 room-1 bob   synth 30
# 参数: [ws_url] [room] [nick] [real|synth] [seconds]
# 退出码: 收到远端视频帧则为 0, 否则为 2
```

## 媒体管线

发布端 (`webrtcbin "pub"`, 音频 m-line 在前):

```text
[autovideosrc | videotestsrc] ! videoconvert ! videoscale ! 640x480@30
    ! valve(video) ! tee
      ├─ queue ! vp8enc ! rtpvp8pay ! queue ! webrtcbin.sink
      └─ queue ! videoconvert ! RGBA ! appsink  → UiEvent::LocalVideo → Slint
[autoaudiosrc | audiotestsrc] ! audioconvert ! audioresample ! 48k mono
    ! valve(audio) ! tee
      └─ queue ! opusenc ! rtpopuspay ! queue ! webrtcbin.sink
```

订阅端 (`webrtcbin "sub-<发布者>"`, recvonly):

```text
webrtcbin.src_%u ! queue ! decodebin
    ├─(video) videoconvert ! videoscale ! 640x480 RGBA ! appsink
    │            → UiEvent::RemoteVideo → Slint
    └─(audio) audioconvert ! audioresample ! autoaudiosink | fakesink
```

- 摄像头/麦克风开关通过 `valve` 的 `drop` 属性实现 (不关闭设备, 恢复无需重新授权)。
- 所有 appsink 回调运行在 GStreamer 流线程, 仅通过 crossbeam channel 投递 RGBA
  帧给 Slint 主线程。

## 信令协议

与 `src/signaling/protocol.rs` 完全一致 (协议 v2):

- `hello` / `welcome` 握手
- `join` (`pc_id`: `pub` / `sub-<发布者>`; `role`: `publisher` / `subscriber`)
- `ice_candidate` (trickle ICE)
- `subscribe` / `unsubscribe` (`track_mid` = 发布者 mid; `subscriber_mid` =
  订阅者自身接收该轨道的 mid, 从本地 offer SDP 的 `a=mid:` 解析得到)
- `joined` / `participant_joined` / `participant_tracks` (成员与轨道发现)
- `update_media` (静音 / 关摄像头广播)
- `leave`

订阅流程: 从 `joined.participants` / `participant_joined.tracks` 读取发布者轨道
→ 创建 recvonly PC (音频 + 视频 transceiver, 必须在管线进入 PLAYING 前添加)
→ `join(role=subscriber)` → 收到 `sdp_answer` 后 `set-remote-description` +
添加 SFU 候选 → 按发布者 mid 发 `subscribe` → `webrtcbin` 产出 RTP src pad,
同步链接到 `decodebin` 渲染。

## 目录结构

```
RoomChatDesktopGStreamer/
├── Cargo.toml
├── build.rs                 # Slint 编译 + macOS Info.plist 嵌入
├── .cargo/config.toml       # GStreamer WebRTC 开发文件查找路径 + nice 插件路径
├── macos/                   # Info.plist / bundle.sh
├── scripts/
│   └── setup-gstreamer-webrtc-dev.sh  # 免 root 准备 GStreamer + nice (Fedora)
├── ui/
│   └── app.slint            # 全部 UI (加入页 / 通话页 / 组件)
└── src/
    ├── main.rs              # Slint 启动、UI 模型、事件泵 (16ms)
    ├── lib.rs               # 库入口 (主程序与 headless 共用)
    ├── bin/headless.rs      # 无界面冒烟测试
    ├── protocol.rs          # 信令 JSON 类型
    ├── signaling.rs         # WebSocket 客户端 (tokio-tungstenite)
    ├── sfu.rs               # GStreamer worker (GLib MainLoop + webrtcbin 状态机)
    ├── media.rs             # 管线构建 / appsink → RGBA / 远端 pad 解码
    └── ui.rs                # UiEvent / LogLevel / SubStatus
```

## 线程模型

- **主线程**: Slint 事件循环 + `slint::Timer` 每 16ms 排空 worker 事件。
- **sfu-client 线程**: GLib `MainLoop` + 15ms `timeout_add_local` 驱动 tick,
  排空命令 / 信令 / GStreamer 事件, 并执行 `create-offer` /
  `set-local-description` / `set-remote-description` / `add-ice-candidate`。
- **GStreamer 内部流线程**: 采集 / 编解码, 经 appsink 回调投递视频帧。
- **signaling-rt**: 专用 tokio 运行时, 负责 WebSocket 读写。

各线程仅通过 crossbeam channel 通信: 主线程 → worker 用 `ClientCommand`,
worker → 主线程用 `UiEvent`; `webrtcbin` 信号回调只投递 `GstEvent`。

## 实现要点 / 踩坑记录

- **`set-remote-description` 是异步的**: 必须在它的 promise 完成后才能添加
  远端 ICE 候选, 否则候选会被丢弃 (见 `GstEvent::RemoteDescriptionSet`)。
- **订阅端 transceiver 必须在 PLAYING 之前添加**, 且 `add-transceiver` 需要完整
  RTP caps (含 `encoding-params`), 否则生成的 offer 缺少 m-line 或
  `rtpmap` 声道数, str0m SFU 会拒绝该 m-line。
- **`webrtcbin` 的 `pad-added` 必须同步处理**: rtpbin 暴露 src pad 后立即可能有
  RTP 到达, 若延迟到下一个 tick 才链接, 未链接的 pad 会让上游收到
  `GST_FLOW_NOT_LINKED` 并终止 receive bin (`Internal data stream error`)。
- 发布端链接 sink pad 后默认为 `sendrecv`, 这里显式改为 `sendonly`
  (`media::set_publisher_sendonly`)。
- 含 `a=ice-lite` 的 SFU 可与 libnice full agent 正常互通; 但 libnice 绑定到具体
  网卡, 因此服务端候选使用 `127.0.0.1` 时不可达, 需用局域网 announce IP。

## 可选环境变量

| 变量 | 说明 | 默认 |
|------|------|------|
| `RUST_LOG` | 日志级别 | `warn` |
| `GST_DEBUG` | GStreamer 调试 | 无 |
| `GST_PLUGIN_PATH` | 额外插件目录 (nice) | 自动探测用户级目录 |
| `GST_WEBRTC_DEV_PREFIX` | `setup-gstreamer-webrtc-dev.sh` 安装前缀 | `~/.local/share/gst-webrtc-dev` |
