# RoomChatHarmony

鸿蒙 (HarmonyOS NEXT / API 12+) 版 RoomChat，**原生 ArkTS 实现**，功能与
[`../RoomChatAndroid`](../RoomChatAndroid) 保持一致。

本应用是一个多人视频通话客户端，通过单条 WebSocket 复用信令，与 SFU 服务端
(`src/signaling`) 交互：

- `pc_id = "pub"` — 发布连接 (`sendonly`)，用于加入房间、推送本地音视频；
- `pc_id = "sub-<远端id>"` — 订阅连接 (`recvonly`)，用于拉取指定参与者的音视频。

WebRTC 原生能力由 [hezhubo/OhosWebrtc](https://github.com/hezhubo/OhosWebrtc)
提供（本项目已把 `ohos_webrtc` HAR 模块 vendored 到 `./ohos_webrtc`）。

## 目录结构

```
RoomChatHarmony/
├── AppScope/                 # 应用级配置与图标
├── entry/                    # 入口 HAP 模块
│   └── src/main/ets/
│       ├── entryability/EntryAbility.ets
│       ├── model/Models.ets              # LogLevel / SubStatus / LogEntry / Participant
│       ├── webrtc/SignalingClient.ets    # @ohos.net.webSocket 封装
│       ├── webrtc/SfuClient.ets          # 单 WS + 多 RTCPeerConnection 客户端
│       ├── components/VideoTile.ets      # XComponent 视频画面
│       └── pages/Index.ets               # 加入页 + 通话页 + 参与者面板
├── ohos_webrtc/              # @ohos/webrtc 原生 HAR 库 (vendored)
├── build-profile.json5
├── hvigorfile.ts
└── oh-package.json5
```

## 与 Android 端的对应关系

| Android (`RoomChatAndroid`) | HarmonyOS (`RoomChatHarmony`) |
| --- | --- |
| `webrtc/Models.kt` | `entry/src/main/ets/model/Models.ets` |
| `webrtc/SignalingClient.kt` (OkHttp WebSocket) | `webrtc/SignalingClient.ets` (`@ohos.net.webSocket`) |
| `webrtc/SfuClient.kt` (StreamWebRTC) | `webrtc/SfuClient.ets` (`@ohos/webrtc`) |
| `ui/SfuViewModel.kt` | `pages/Index.ets` 中的状态与回调 |
| `ui/JoinScreen.kt` | `Index.ets` 的 `joinScreen()` |
| `ui/CallScreen.kt` | `Index.ets` 的 `callScreen()` |
| `ui/WebRtcVideoView.kt` (`SurfaceViewRenderer`) | `components/VideoTile.ets` (`XComponent` + `VideoRenderController`) |

功能完全对齐 Android 端：

- 加入房间（信令地址 / 房间号 / 昵称，可配置）
- 发布本地音视频（音频 mid=0、视频 mid=1）
- 自动订阅新加入的参与者（500ms 延迟，与 Web / Android 一致）
- 参与者面板：订阅状态、手动订阅、取消订阅、事件日志
- 麦克风 / 摄像头开关、前后摄像头切换、挂断
- `sdp_answer` / `ice_candidate` / `participant_joined` / `participant_left` /
  `subscribed` / `error` 信令处理

## 构建

1. 使用 DevEco Studio (5.0 / 6.0，本项目已用 26.0.0 验证) 打开本目录。
2. `ohpm install` 会自动解析 `entry` 对 `@ohos/webrtc`（本地 `../ohos_webrtc`）的依赖。
3. 真机 / 模拟器上运行前需在 DevEco Studio 中配置签名（`File → Project Structure → Signing Configs`）。

命令行构建（可选，等价于 DevEco 的 `assembleHap`）：

```bash
export DEVECO_SDK_HOME=/Applications/DevEco-Studio.app/Contents/sdk
export JAVA_HOME=/Applications/DevEco-Studio.app/Contents/jbr/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
node /Applications/DevEco-Studio.app/Contents/tools/hvigor/bin/hvigorw.js \
  assembleHap --mode module -p product=default --no-daemon
```

产物：`entry/build/default/outputs/default/entry-default-unsigned.hap`。

## 依赖说明（重要）

`ohos_webrtc` 原生库需要链接一个体积很大的静态库：

- `ohos_webrtc/src/main/cpp/deps/webrtc/libs/arm64-v8a/libwebrtc.a`（约 388 MB）

该文件在上游仓库中由 Git LFS 托管。本目录已放入完整二进制；如果你通过 Git
重新分发 / 克隆，请确保：

```bash
git lfs install
git lfs pull
```

或直接从上游下载：

```bash
curl -L -o ohos_webrtc/src/main/cpp/deps/webrtc/libs/arm64-v8a/libwebrtc.a \
  "https://media.githubusercontent.com/media/hezhubo/OhosWebrtc/main/ohos_webrtc/src/main/cpp/deps/webrtc/libs/arm64-v8a/libwebrtc.a"
```

> 另外，`libmediasoupclient` 依赖的 `deps/libsdptransform`（上游为 git 子模块，
> 未随 tarball 分发）已一并放入
> `ohos_webrtc/src/main/cpp/deps/libmediasoupclient/deps/libsdptransform`，并已关闭其
> `test/readme-helper` 可执行程序的构建，只编译 `sdptransform` 静态库。

### ⚠️ 关键修复：必须定义 `WEBRTC_LINUX`

预编译的 `libwebrtc.a` 是以 `WEBRTC_LINUX`（启用 epoll）构建的。而
`rtc_base/physical_socket_server.h` 只有在 `WEBRTC_LINUX` 时才把
`epoll_events_`（`std::array<epoll_event, 128>`，约 1.5KB）作为 `PhysicalSocketServer`
的成员；否则走 select 分支，类会小得多。

若不定义 `WEBRTC_LINUX`：

- 本工程编译出的 `sizeof(PhysicalSocketServer)` ≈ **200 字节**，
  而 `libwebrtc.a` 中的构造函数/方法按 **≈ 2256 字节** 的布局读写
  （其 `WakeUp()` 通过偏移 `2184` 访问 `signal_wakeup_`）；
- `new PhysicalSocketServer()` 只分配 200 字节，`.a` 的构造函数却写入约 2KB →
  **堆越界写**，随之出现随机崩溃：
  - `Fatal error ... recursive_critical_section.cc, line 121 / Check failed: CurrentThreadIsOwner()`（SIGABRT），或
  - `PhysicalSocketServer::WakeUp()` / RTP pacing 队列中的 SIGSEGV。

因此 `ohos_webrtc/src/main/cpp/CMakeLists.txt` 中必须同时定义
`-DWEBRTC_POSIX -DWEBRTC_LINUX`（已默认配置）。修改后请清理 `ohos_webrtc/.cxx`
重新编译，确保头文件布局与 `.a` 一致。

## 权限

`entry/src/main/module.json5` 已声明：

- `ohos.permission.INTERNET`（系统授权）
- `ohos.permission.GET_NETWORK_INFO`（系统授权）
- `ohos.permission.CAMERA`（用户授权，inuse）
- `ohos.permission.MICROPHONE`（用户授权，inuse）

摄像头 / 麦克风为运行时权限，在 `Index.aboutToAppear` 中通过
`abilityAccessCtrl.requestPermissionsFromUser` 申请。

## 信令协议

与 Rust 服务端 `src/signaling/protocol.rs` 完全对应，客户端发送：

```jsonc
// 加入 / 发布
{ "type": "join", "pc_id": "pub", "room_id": "...", "participant_id": "...", "sdp": "...", "role": "publisher" }
// 订阅 (role=subscriber，participant_id 形如 "<name>-recv-<publisherId>")
{ "type": "join", "pc_id": "sub-<id>", "room_id": "...", "participant_id": "...-recv-<id>", "sdp": "...", "role": "subscriber" }
{ "type": "ice_candidate", "pc_id": "...", "candidate": "...", "sdp_mid": "...", "sdp_mline_index": 0 }
{ "type": "subscribe", "pc_id": "...", "publisher_id": "...", "track_mid": "0|1" }
{ "type": "unsubscribe", "pc_id": "...", "publisher_id": "...", "track_mid": "0|1" }
{ "type": "leave", "pc_id": "..." }
```

接收：

```jsonc
{ "type": "sdp_answer", "pc_id": "...", "sdp": "...", "ice_candidates": ["..."] }
{ "type": "ice_candidate", "pc_id": "...", "candidate": "...", "sdp_mid": "..." }
{ "type": "participant_joined", "participant_id": "..." }
{ "type": "participant_left", "participant_id": "..." }
{ "type": "subscribed", "pc_id": "...", "publisher_id": "..." }
{ "type": "error", "pc_id": "...", "error": "..." }
```

默认信令地址为 `ws://192.168.31.112:5174`，加入页可修改。
