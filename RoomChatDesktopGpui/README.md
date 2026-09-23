# RoomChat Desktop · GPUI

基于 **GPUI** (Zed 的 GPU 加速 UI 框架) + **gpui-kit / gpui-component** (组件库) +
**reactor-webrtc** (libwebrtc 安全封装) + **nokhwa** (摄像头采集) 的桌面视频通话
客户端, 对接本仓库的 WebRTC SFU 服务端。

功能与 `RoomChatDesktop` (Slint 版)、`RoomChat` (Android/Compose)、`demo`
(Web/React) 客户端保持一致, 是 Slint 版本的 GPUI 重写。

```
┌────────────────────────────────────────────────────────────────┐
│ RoomChatDesktopGpui (本目录)                                    │
│                                                                │
│   GPUI 主线程                       sfu-client 线程             │
│   ┌──────────────────┐  命令       ┌──────────────────────────┐│
│   │ RoomChatApp       │ ─────────► │ reactor-webrtc           ││
│   │  ├─ 加入页         │            │  ├─ 发布 PC (sendonly)   ││
│   │  └─ 通话页         │ ◄───────── │  └─ 订阅 PC (recvonly)×N ││
│   │     视频网格        │  UiEvent   │ camera-capture 线程:      ││
│   │     参与者/日志侧栏  │ (async-    │   nokhwa 采集             ││
│   │     控制栏          │  channel)  │ WebSocket 信令 (tokio)    ││
│   └──────────────────┘             └──────────────────────────┘│
└───────────────────────────────────────────────┼────────────────┘
                                                │ WS ws://host:5174
                                       ┌────────▼────────┐
                                       │  SFU 信令服务    │
                                       │  (src/signaling) │
                                       └─────────────────┘
```

## 功能

- 加入房间 (信令地址 / 房间号 / 昵称), 任一输入框回车即可加入
- **真实摄像头 + 真实麦克风**: 摄像头帧经 nokhwa 采集后逐帧推给
  reactor-webrtc; 麦克风由平台 ADM (CoreAudio/ALSA/WASAPI) 采集, 远端音频自动播放
- 摄像头不可用时自动回退到合成测试图案; 可勾选关闭真实设备改用合成源 (联调)
- 自动订阅房间内其他参与者 (音频 + 视频), 在视频网格中实时渲染
- 参与者侧栏: 手动订阅 / 取消订阅、状态指示、事件日志
- 控制栏: 麦克风开关、摄像头开关 (暂停采集)、挂断
- 单条 WebSocket 复用多个 `RTCPeerConnection` (按 `pc_id` 区分)

## UI 架构 (GPUI)

- **窗口根视图**是 `gpui_component::Root`, 内部包住 `app::RoomChatApp`。
  `Root` 负责主题、焦点、对话框/通知等全局能力; `gpui_kit::init(cx)` 完成组件初始化。
- 加入页使用组件库的 `Input`(绑定 `InputState`)、`Checkbox`、`Button`;
  通话页的视频网格/侧栏/控制栏用 `div/h_flex/v_flex` 布局, 与 Slint 版配色一致。
- 视频帧: libwebrtc 回调是 BGRA, GPUI 的 `RenderImage` 内部正好是 BGRA, 因此
  无需通道交换, 直接把 `frame.bgra` 包装成 `RenderImage` 即可 (`app::frame_to_image`)。
- worker 线程通过 `async_channel` 投递 `UiEvent`; 主线程用
  `cx.spawn(async move |this, cx| { while let Ok(ev) = rx.recv().await { ... } })`
  在 GPUI 执行器里排空事件。worker 绝不触碰任何 GPUI 对象。

> `Cargo.toml` 里同时依赖 `gpui-kit` 与 `gpui` (`gpui = { package = "gpui-pre" }`):
> `gpui-kit` 内部使用的就是 `gpui-pre` 快照, 这里把同一份以 `gpui` 名字引入, 仅为让
> `#[derive(Render)]` 等过程宏展开出的 `::gpui::...` 路径可解析 (不会引入第二份 GPUI)。

## 构建与运行

### 前置条件

- Rust 1.92+ (edition 2024), macOS / Linux / Windows
- 先启动 SFU 服务端 (默认 WebSocket 信令 `ws://<host>:5174`, 媒体 UDP 5175+):
  ```bash
  # 在仓库根目录
  SFU_ANNOUNCE_IP=<可达IP> RUST_LOG=info cargo run --release
  ```
- **首次构建**会自动从 reactor-webrtc 的 GitHub Release 下载对应平台的预编译
  `libwebrtc` (较大, 数百 MB; 之后有缓存, 不会重复下载)。
  若本机已经解压过, 可用环境变量复用, 避免重新下载:
  ```bash
  export REACTOR_WEBRTC_LIB_DIR=/path/to/extracted/libwebrtc
  ```

### 运行

```bash
cd RoomChatDesktopGpui
cargo run --release
```

首次运行会弹出**摄像头 / 麦克风授权**对话框, 允许后即可通话。

### macOS 权限与链接说明

命令行程序 (非 `.app` bundle) 访问摄像头/麦克风时, macOS 需要从 Info.plist 读取
`NSCameraUsageDescription` / `NSMicrophoneUsageDescription`, 否则会直接终止进程。
本目录通过链接器把 `macos/Info.plist` 嵌入可执行文件的 `__TEXT,__info_plist`
段, 因此 `cargo run` 也能正常弹出授权框。若某版本下仍被拒, 可打包成 `.app`:

```bash
./macos/bundle.sh
open "target/release/RoomChat Desktop Gpui.app"
```

此外 `build.rs` 还会:

- 在 macOS 上定位并静态链接 compiler-rt (`libclang_rt.osx.a`), 解决
  reactor-webrtc 预编译库引用 `___isPlatformVersionAtLeast` 的链接失败;
- 在 Windows/MSVC 上补齐 C++ 静态标准库 (`libcpmt.lib`/`msvcprt.lib`);
- 在 Linux 上, 若系统只装了运行时的 `libxkbcommon-x11.so.0` 而没有开发软链,
  在 `OUT_DIR` 生成一个仅用于链接的软链 (GPUI 的 Linux 后端会链接
  `-lxkbcommon-x11`)。安装 `libxkbcommon-x11-devel` 后该逻辑自动跳过。

`.cargo/config.toml` 把 `MACOSX_DEPLOYMENT_TARGET` 对齐到 13.0, 与预编译
libwebrtc 的部署目标一致。

## 信令协议

与 `src/signaling/protocol.rs` 完全一致 (见仓库 README 第 8 节):

- `join` (`pc_id`: `pub` / `sub-<publisher_id>`; `role`: `publisher` / `subscriber`)
- `ice_candidate` (trickle ICE)
- `subscribe` / `unsubscribe` (`track_mid` = 发布者 mid, 音频 `0`、视频 `1`)
- `leave`

订阅流程: recvonly PC (音频 mid=0 + 视频 mid=1) → `join(role=subscriber)` →
收到 `sdp_answer` 后 `set_remote_description` + 添加 SFU 候选 → 对 mid 0/1 发
`subscribe` → `on_track` 收到远端轨, 视频挂 `on_frame` 回传 UI。

## 目录结构

```
RoomChatDesktopGpui/
├── Cargo.toml
├── build.rs                 # macOS Info.plist 嵌入 + compiler-rt / MSVC STL / Linux 软链
├── .cargo/config.toml       # macOS 部署目标 + 网络容错
├── macos/                   # Info.plist (嵌入可执行文件) + .app 打包脚本
└── src/
    ├── main.rs              # GPUI Application 启动、窗口与 Root
    ├── app.rs               # RoomChatApp 视图: 状态、事件泵、加入页/通话页渲染
    ├── protocol.rs          # 信令 JSON 类型
    ├── signaling.rs         # WebSocket 客户端 (tokio-tungstenite)
    ├── sfu.rs               # SfuClient worker (发布/订阅/信令状态机)
    ├── media.rs             # 摄像头采集 / 合成回退 / 合成音频 (BGRA)
    └── ui.rs                # UiEvent / UiSender / LogLevel / SubStatus
```

## 线程模型

- **主线程**: GPUI 事件循环 + 视图渲染; 一个异步任务持续
  `ui_rx.recv().await` 排空 worker 事件并 `cx.notify()` (所有 GPUI 对象只在主线程访问)。
- **sfu-client 线程**: 同步阻塞的 reactor-webrtc 调用 (create_offer /
  set_remote_description / …) 与信令状态机。
- **camera-capture / video-synth / audio-gen**: 本地媒体源线程。
- **signaling-rt**: 专用 tokio 运行时, 负责 WebSocket 读写。

各线程仅通过 channel 通信: 主线程 → worker 用 crossbeam `ClientCommand`,
worker → 主线程用 async-channel `UiEvent`。
