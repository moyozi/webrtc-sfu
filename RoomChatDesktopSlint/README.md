# RoomChat Desktop · Slint

基于 **Slint** (UI) + **reactor-webrtc** (libwebrtc 安全封装) + **nokhwa** (摄像头采集)
的桌面视频通话客户端, 对接本仓库的 WebRTC SFU 服务端。功能与 `RoomChat`
(Android/Compose)、`RoomChatWeb` (Web/React) 客户端保持一致。

```
┌──────────────────────────────────────────────────────────────┐
│ RoomChatDesktopSlint (本目录)                                      │
│                                                              │
│   Slint 主线程                    sfu-client 线程             │
│   ┌────────────────┐  命令      ┌──────────────────────────┐ │
│   │ AppWindow      │ ─────────► │ reactor-webrtc           │ │
│   │ 视频网格        │            │  ├─ 发布 PC (sendonly)    │ │
│   │ 参与/日志侧栏    │ ◄───────── │  └─ 订阅 PC (recvonly)×N  │ │
│   │ 控制栏          │  UiEvent   │ camera-capture 线程: nokhwa│ │
│   └────────────────┘  (crossbeam)│ WebSocket 信令 (tokio)    │ │
└───────────────────────────────────────────────┼───────────────┘
                                                │ WS ws://host:5174
                                       ┌────────▼────────┐
                                       │  SFU 信令服务    │
                                       │  (src/signaling) │
                                       └─────────────────┘
```

## 功能

- 加入房间 (信令地址 / 房间号 / 昵称)
- **真实摄像头 + 真实麦克风**: 摄像头帧经 nokhwa 采集后逐帧推给 reactor-webrtc;
  麦克风由平台 ADM (CoreAudio/ALSA/WASAPI) 采集, 远端音频自动播放
- 摄像头不可用时自动回退到合成测试图案; 可勾选关闭真实设备改用合成源 (联调)
- 自动订阅房间内其他参与者 (音频 + 视频), 在视频网格中实时渲染
- 参与者侧栏: 手动订阅 / 取消订阅、状态指示、事件日志
- 控制栏: 麦克风开关、摄像头开关 (暂停采集)、挂断
- 单条 WebSocket 复用多个 `RTCPeerConnection` (按 `pc_id` 区分)

## 构建与运行

### 前置条件

- Rust 1.92+ (Slint 1.18 要求), macOS / Linux / Windows
- 先启动 SFU 服务端 (默认 WebSocket 信令 `ws://<host>:5174`, 媒体 UDP 5175+):
  ```bash
  # 在仓库根目录
  SFU_ANNOUNCE_IP=<可达IP> RUST_LOG=info cargo run --release
  ```
- **首次构建**会自动从 reactor-webrtc 的 GitHub Release 下载对应平台的预编译
  `libwebrtc` (较大, 数百 MB; 之后有缓存, 不会重复下载)。

### 运行

```bash
cd RoomChatDesktopSlint
cargo run --release
```

首次运行会弹出**摄像头 / 麦克风授权**对话框, 允许后即可通话。

若某个 macOS 版本下 `cargo run` 未能弹出授权框 (或直接被拒), 用附带的脚本打包成
`.app` (带 `Contents/Info.plist` 并 ad-hoc 签名, 对 TCC 最可靠):

```bash
./macos/bundle.sh
open "target/release/RoomChat Desktop Slint.app"
```

### macOS 权限与链接说明

命令行程序 (非 `.app` bundle) 访问摄像头/麦克风时, macOS 需要从 Info.plist 读取
`NSCameraUsageDescription` / `NSMicrophoneUsageDescription`, 否则会直接终止进程。
本目录已通过链接器把 `macos/Info.plist` 嵌入可执行文件的 `__TEXT,__info_plist`
段, 因此 `cargo run` 也能正常弹出授权框:

```
cargo:rustc-link-arg=-Wl,-sectcreate,__TEXT,__info_plist,<abs path>
```

此外, reactor-webrtc 的 macOS 预编译库以 **macOS 13.0** 为目标构建, 而 rustc
默认部署目标是 11.0, 会分别导致:

1. 大量 `object file was built for newer 'macOS' version (13.0) ...` 警告;
2. 链接失败: `Undefined symbols ... ___isPlatformVersionAtLeast`。

本目录内置修复: `.cargo/config.toml` 设置 `MACOSX_DEPLOYMENT_TARGET = "13.0"`,
`build.rs` 定位并静态链接 compiler-rt (`libclang_rt.osx.a`)。

> 构建时可能出现 `duplicate symbol '_jpeg_natural_order'` 链接警告: 这是 nokhwa 的
> mozjpeg 与 libwebrtc 内置 libjpeg 的静态表重名, 属于良性警告, 不影响运行。

### Linux / Windows

nokhwa 会自动选择后端 (V4L2 / MSMF), 直接 `cargo build` 即可。Linux 上若缺少
H.264, 可启用 reactor-webrtc 的 `openh264` feature。

## 摄像头采集

reactor-webrtc 的视频轨是 **push 模型** (未暴露摄像头采集 API), 因此由
`media.rs` 使用 [nokhwa](https://github.com/l1npengtul/nokhwa) 采集:

```
nokhwa Camera.frame()
  → decode RgbAFormat (RGBA)
  → 交换 R/B 得 BGRA
  → VideoTrack::push_frame (libwebrtc 编码 / 发送)
  → 同一帧 RGBA 发到 UI 做本地预览
```

- 打开时优先请求 640×480 MJPEG / YUYV, 再退化为设备支持的最高帧率格式;
- 采集在独立线程 (`camera-capture`) 中进行, `Camera::frame()` 阻塞到下一帧, 天然限速;
- 摄像头开关 = 暂停/恢复推帧 (不关闭流, 恢复无需重新授权);
- 打开失败 (无设备/无权限/被占用) 时自动回退到合成测试图案。

## 音频

「使用真实摄像头和麦克风」勾选框控制 `PeerConnectionFactory` 的 ADM 与摄像头:

| 模式 | 采集 | 播放远端音频 | 权限 |
|------|------|--------------|------|
| 打开 (默认) | 平台麦克风 + 真实摄像头 | 自动播放 | 需系统授权 |
| 关闭 | 合成 440Hz 正弦波 + 合成视频 | 无声 (合成 ADM 无播放设备) | 不需要 |

静音实现: 真实麦克风模式切换音频 transceiver 的发送方向 (`SendOnly` ↔ `Inactive`);
合成模式直接暂停/恢复正弦波发生器。

> 无音频设备的机器上使用平台 ADM 可能在 libwebrtc 内部 `SIGABRT`; 若遇到, 取消勾选
> 使用合成源即可。

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
RoomChatDesktopSlint/
├── Cargo.toml
├── build.rs                 # Slint 编译 + macOS Info.plist 嵌入 + compiler-rt 链接
├── .cargo/config.toml       # 网络容错 + macOS 部署目标
├── macos/Info.plist         # 摄像头/麦克风用途说明 (嵌入可执行文件)
├── ui/
│   └── app.slint            # 全部 UI (加入页 / 通话页 / 组件)
└── src/
    ├── main.rs              # Slint 启动、UI 模型、事件泵 (16ms)、权限初始化
    ├── protocol.rs          # 信令 JSON 类型
    ├── signaling.rs         # WebSocket 客户端 (tokio-tungstenite)
    ├── sfu.rs               # SfuClient worker (发布/订阅/信令状态机)
    ├── media.rs             # 摄像头采集 / 合成回退 / 合成音频, BGRA↔RGBA
    └── ui.rs                # UiEvent / LogLevel / SubStatus
```

## 线程模型

- **主线程**: Slint 事件循环 + `slint::Timer` 每 16ms 排空 worker 事件, 更新
  `VecModel` 与 `image` 属性 (所有 Slint 对象只在主线程访问)。
- **sfu-client 线程**: 同步阻塞的 reactor-webrtc 调用 (create_offer /
  set_remote_description / …) 与信令状态机。
- **camera-capture**: nokhwa 逐帧采集并推轨。
- **signaling-rt**: 专用 tokio 运行时, 负责 WebSocket 读写。

各线程仅通过 crossbeam channel 通信: 主线程 → worker 用 `ClientCommand`,
worker → 主线程用 `UiEvent`。
