//! SFU 客户端 worker。
//!
//! 运行在独立线程上 (reactor-webrtc 是同步阻塞 API, 不能跑在 Slint 主线程)。
//! 通过 crossbeam channel 与主线程通信:
//!   · 命令:  主线程 → worker (`ClientCommand`)
//!   · 事件:  worker → 主线程 (`UiEvent`)
//!
//! 信令模型与 Web/Android 客户端一致: 一条 WebSocket 承载多个 PeerConnection,
//! 用 `pc_id` 区分发布连接 (`"pub"`) 与订阅连接 (`"sub-<发布者id>"`)。
//!
//! 订阅流程:
//!   1. 创建 recvonly PC (音频 mid=0, 视频 mid=1), 生成 offer 并 `join` (role=subscriber)
//!   2. 收到 `sdp_answer` 后 `set_remote_description` + 添加 SFU 候选
//!   3. 依据发布者已知轨道发送 `subscribe`
//!      (track_mid=发布者 mid, subscriber_mid=本地音频 mid 0 / 视频 mid 1)
//!   4. `on_track` 回调收到远端轨: 视频挂 `on_frame` 转发给 UI, 音频保持存活

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{unbounded, Receiver, Sender};
use reactor_webrtc::{
    AudioTrack, BundlePolicy, IceCandidate, IceGatheringState, MediaKind, PeerConnection,
    PeerConnectionFactory, PeerConnectionObserver, PeerConnectionState, RemoteTrack,
    RtcConfiguration, SessionDescription, SdpType, TransceiverDirection, VideoTrack,
};

use crate::media::{bgra_to_rgba, AudioGenerator, VideoCapture};
use crate::protocol::{ClientMessage, ServerMessage, TrackInfo};
use crate::signaling::Signaling;
use crate::ui::{LogLevel, SubStatus, UiEvent};

// ── 主线程 → worker ───────────────────────────────────────────

#[derive(Debug)]
pub enum ClientCommand {
    Join {
        server_url: String,
        room_id: String,
        participant_id: String,
        /// true = 真实摄像头 + 真实麦克风; false = 合成视频 + 合成音频
        use_real_devices: bool,
    },
    Leave,
    Subscribe { publisher_id: String },
    Unsubscribe { publisher_id: String },
    ToggleAudio { enabled: bool },
    ToggleVideo { enabled: bool },
    Shutdown,
}

// ── worker 内部 ───────────────────────────────────────────────

struct RemoteTrackEvent {
    publisher_id: String,
    track: RemoteTrack,
}

/// 与 observer 回调共享的 PC 状态。
#[derive(Default)]
struct PcShared {
    gathering_complete: AtomicBool,
    pending_ice: Mutex<Vec<IceCandidate>>,
}

struct SubEntry {
    pc_id: String,
    pc: PeerConnection,
    status: SubStatus,
    has_video: bool,
    /// 是否已收到 `sdp_answer` (收到后才能发送 subscribe)。
    negotiated: bool,
    /// 已发送 subscribe 的发布者 track_mid 列表 (用于去重与取消订阅)。
    sent_tracks: Vec<String>,
    // 远端轨句柄必须保活, 否则 native track 被销毁, on_frame 不再触发
    _video_track: Option<VideoTrack>,
    _audio_track: Option<AudioTrack>,
}

// ── 线程入口 ──────────────────────────────────────────────────

/// 启动 worker 线程, 返回命令发送端与线程句柄。
pub fn spawn_worker(ui_tx: Sender<UiEvent>) -> (Sender<ClientCommand>, JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = unbounded::<ClientCommand>();
    let handle = thread::Builder::new()
        .name("sfu-client".into())
        .spawn(move || {
            let mut client = SfuClient::new(ui_tx);
            client.run(cmd_rx);
        })
        .expect("无法启动 sfu-client 线程");
    (cmd_tx, handle)
}

// ── SfuClient ─────────────────────────────────────────────────

struct SfuClient {
    ui_tx: Sender<UiEvent>,

    // 信令
    msg_tx: Sender<ServerMessage>,
    msg_rx: Receiver<ServerMessage>,
    sig: Option<Signaling>,

    // observer → worker 的远端轨
    track_tx: Sender<RemoteTrackEvent>,
    track_rx: Receiver<RemoteTrackEvent>,

    // WebRTC
    factory: Option<Arc<PeerConnectionFactory>>,

    room_id: String,
    participant_id: String,
    active: bool,

    // 发布端
    pub_pc: Option<PeerConnection>,
    pub_shared: Option<Arc<PcShared>>,
    local_audio: Option<AudioTrack>,
    local_video: Option<Arc<VideoTrack>>,
    video_capture: Option<VideoCapture>,
    audio_gen: Option<AudioGenerator>,
    synthetic_audio: bool,
    use_real_devices: bool,

    // 订阅端
    subs: HashMap<String, SubEntry>,
    /// 各发布者已知的轨道 (mid + kind), 用于订阅与取消订阅。
    publisher_tracks: HashMap<String, Vec<TrackInfo>>,

    audio_enabled: bool,
    video_enabled: bool,
}

impl SfuClient {
    fn new(ui_tx: Sender<UiEvent>) -> Self {
        let (msg_tx, msg_rx) = unbounded::<ServerMessage>();
        let (track_tx, track_rx) = unbounded::<RemoteTrackEvent>();
        Self {
            ui_tx,
            msg_tx,
            msg_rx,
            sig: None,
            track_tx,
            track_rx,
            factory: None,
            room_id: String::new(),
            participant_id: String::new(),
            active: false,
            pub_pc: None,
            pub_shared: None,
            local_audio: None,
            local_video: None,
            video_capture: None,
            audio_gen: None,
            synthetic_audio: true,
            use_real_devices: true,
            subs: HashMap::new(),
            publisher_tracks: HashMap::new(),
            audio_enabled: true,
            video_enabled: true,
        }
    }

    // ── 事件循环 ──────────────────────────────────────────────

    fn run(&mut self, cmd_rx: Receiver<ClientCommand>) {
        loop {
            while let Ok(cmd) = cmd_rx.try_recv() {
                match cmd {
                    ClientCommand::Join {
                        server_url,
                        room_id,
                        participant_id,
                        use_real_devices,
                    } => self.handle_join(server_url, room_id, participant_id, use_real_devices),
                    ClientCommand::Leave => self.leave(),
                    ClientCommand::Subscribe { publisher_id } => self.subscribe(publisher_id),
                    ClientCommand::Unsubscribe { publisher_id } => self.unsubscribe(&publisher_id),
                    ClientCommand::ToggleAudio { enabled } => self.toggle_audio(enabled),
                    ClientCommand::ToggleVideo { enabled } => self.toggle_video(enabled),
                    ClientCommand::Shutdown => {
                        self.leave();
                        return;
                    }
                }
            }

            if self.sig.is_some() {
                while let Ok(msg) = self.msg_rx.try_recv() {
                    self.handle_signal(msg);
                }
            }
            while let Ok(ev) = self.track_rx.try_recv() {
                self.handle_remote_track(ev);
            }

            thread::sleep(Duration::from_millis(15));
        }
    }

    // ── 加入 / 离开 ───────────────────────────────────────────

    fn handle_join(
        &mut self,
        server_url: String,
        room_id: String,
        participant_id: String,
        use_real_devices: bool,
    ) {
        // 重置上一次会话
        self.leave();
        // 清空上一次会话可能残留的信令/轨道事件, 避免污染新会话
        while self.msg_rx.try_recv().is_ok() {}
        while self.track_rx.try_recv().is_ok() {}

        self.room_id = room_id.clone();
        self.participant_id = participant_id.clone();
        self.use_real_devices = use_real_devices;
        self.synthetic_audio = !use_real_devices;
        self.audio_enabled = true;
        self.video_enabled = true;

        // 1. WebRTC 工厂 (平台 ADM = 真实麦克风/扬声器; 否则合成 ADM)
        let factory = if use_real_devices {
            PeerConnectionFactory::builder().with_platform_adm().build()
        } else {
            PeerConnectionFactory::builder().build()
        };
        match factory {
            Ok(f) => self.factory = Some(Arc::new(f)),
            Err(e) => {
                self.fail(format!("创建 WebRTC 工厂失败: {e}"));
                return;
            }
        }

        // 2. 信令
        match Signaling::connect(&server_url, self.msg_tx.clone()) {
            Ok(sig) => self.sig = Some(sig),
            Err(e) => {
                self.fail(format!("信令连接失败: {e}"));
                return;
            }
        }
        // 协议握手: 声明协议版本, 服务端回 `welcome`。
        self.send(ClientMessage::Hello {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            client: format!("{}/desktop", env!("CARGO_PKG_NAME")),
        });

        // 3. 发布连接
        if let Err(e) = self.create_publisher() {
            self.fail(format!("创建发布连接失败: {e}"));
            self.leave();
            return;
        }

        self.active = true;
        let _ = self.ui_tx.send(UiEvent::Joined {
            room_id: room_id.clone(),
        });
        self.log(LogLevel::Info, format!("正在加入房间 {room_id} ..."));
    }

    fn leave(&mut self) {
        if let Some(mut c) = self.video_capture.take() {
            c.stop();
        }
        if let Some(mut g) = self.audio_gen.take() {
            g.stop();
        }

        // 通知服务端关闭各 PC (给 writer 一点时间 flush)
        if self.sig.is_some() {
            for entry in self.subs.values() {
                self.send(ClientMessage::Leave {
                    pc_id: entry.pc_id.clone(),
                });
            }
            if self.pub_pc.is_some() {
                self.send(ClientMessage::Leave {
                    pc_id: "pub".to_string(),
                });
            }
            if self.active {
                thread::sleep(Duration::from_millis(150));
            }
        }

        self.subs.clear();
        self.publisher_tracks.clear();
        self.pub_pc = None;
        self.pub_shared = None;
        self.local_audio = None;
        self.local_video = None;
        self.sig = None; // drop 信令 (关闭连接)
        self.factory = None;

        if self.active {
            let _ = self.ui_tx.send(UiEvent::Left);
        }
        self.active = false;
    }

    // ── 发布 ─────────────────────────────────────────────────

    fn create_publisher(&mut self) -> Result<(), String> {
        let factory = self
            .factory
            .clone()
            .ok_or_else(|| "WebRTC 工厂未初始化".to_string())?;

        let shared = Arc::new(PcShared::default());
        let obs = make_observer(
            self.ui_tx.clone(),
            "pub".to_string(),
            shared.clone(),
            None,
            None,
        );
        let pc = factory
            .create_peer_connection(&rtc_config(), obs)
            .map_err(|e| e.to_string())?;

        // 音频在前 (mid=0), 视频在后 (mid=1), 与服务端/其他客户端约定一致
        let audio_tx = pc
            .add_transceiver(MediaKind::Audio, TransceiverDirection::SendOnly)
            .map_err(|e| e.to_string())?;
        let audio = factory
            .create_audio_track("audio0")
            .map_err(|e| e.to_string())?;
        audio_tx.set_track(&audio).map_err(|e| e.to_string())?;
        self.local_audio = Some(audio);

        let video_tx = pc
            .add_transceiver(MediaKind::Video, TransceiverDirection::SendOnly)
            .map_err(|e| e.to_string())?;
        let video = Arc::new(
            factory
                .create_video_track("video0")
                .map_err(|e| e.to_string())?,
        );
        video_tx.set_track(&video).map_err(|e| e.to_string())?;
        self.local_video = Some(video.clone());

        // SDP offer + trickle ICE
        let offer = pc.create_offer().map_err(|e| e.to_string())?;
        pc.set_local_description(&offer).map_err(|e| e.to_string())?;
        wait_gathering(&shared, Duration::from_millis(3000));

        self.send(ClientMessage::Join {
            pc_id: "pub".to_string(),
            room_id: self.room_id.clone(),
            participant_id: self.participant_id.clone(),
            sdp: offer.sdp.clone(),
            role: "publisher".to_string(),
        });
        for c in take_candidates(&shared) {
            self.send_ice("pub", &c);
        }

        self.pub_pc = Some(pc);
        self.pub_shared = Some(shared);

        self.start_media_generators();
        Ok(())
    }

    fn start_media_generators(&mut self) {
        if let Some(video) = self.local_video.clone() {
            let (capture, is_camera, msg) =
                VideoCapture::open(video, self.ui_tx.clone(), self.use_real_devices);
            self.video_capture = Some(capture);
            let level = if self.use_real_devices && !is_camera {
                LogLevel::Warn
            } else {
                LogLevel::Info
            };
            self.log(level, msg);
        }
        if self.synthetic_audio {
            if let Some(factory) = self.factory.clone() {
                self.audio_gen = Some(AudioGenerator::start(factory));
            }
        }
    }

    // ── 订阅 ─────────────────────────────────────────────────

    fn subscribe(&mut self, publisher_id: String) {
        if self.factory.is_none() || self.sig.is_none() {
            return;
        }
        if self.subs.contains_key(&publisher_id) {
            self.log(LogLevel::Warn, format!("已订阅 {publisher_id}"));
            return;
        }
        if publisher_id == self.participant_id || publisher_id.contains("-recv-") {
            self.log(LogLevel::Warn, format!("忽略非法订阅目标: {publisher_id}"));
            return;
        }

        let factory = self.factory.clone().unwrap();
        let pc_id = format!("sub-{publisher_id}");
        let shared = Arc::new(PcShared::default());
        let obs = make_observer(
            self.ui_tx.clone(),
            pc_id.clone(),
            shared.clone(),
            Some(publisher_id.clone()),
            Some(self.track_tx.clone()),
        );

        let pc = match factory.create_peer_connection(&rtc_config(), obs) {
            Ok(pc) => pc,
            Err(e) => {
                self.log(LogLevel::Error, format!("创建订阅连接失败: {e}"));
                return;
            }
        };

        // recvonly: 音频 (mid=0) + 视频 (mid=1)
        if let Err(e) = pc.add_transceiver(MediaKind::Audio, TransceiverDirection::RecvOnly) {
            self.log(LogLevel::Error, format!("添加音频 transceiver 失败: {e}"));
        }
        if let Err(e) = pc.add_transceiver(MediaKind::Video, TransceiverDirection::RecvOnly) {
            self.log(LogLevel::Error, format!("添加视频 transceiver 失败: {e}"));
        }

        let offer = match pc.create_offer() {
            Ok(o) => o,
            Err(e) => {
                self.log(LogLevel::Error, format!("创建订阅 offer 失败: {e}"));
                return;
            }
        };
        if let Err(e) = pc.set_local_description(&offer) {
            self.log(LogLevel::Error, format!("设置订阅本地 SDP 失败: {e}"));
            return;
        }
        wait_gathering(&shared, Duration::from_millis(3000));

        self.send(ClientMessage::Join {
            pc_id: pc_id.clone(),
            room_id: self.room_id.clone(),
            participant_id: format!("{}-recv-{}", self.participant_id, publisher_id),
            sdp: offer.sdp.clone(),
            role: "subscriber".to_string(),
        });
        for c in take_candidates(&shared) {
            self.send_ice(&pc_id, &c);
        }

        self.subs.insert(
            publisher_id.clone(),
            SubEntry {
                pc_id,
                pc,
                status: SubStatus::Connecting,
                has_video: false,
                negotiated: false,
                sent_tracks: Vec::new(),
                _video_track: None,
                _audio_track: None,
            },
        );
        let _ = self.ui_tx.send(UiEvent::SubscriptionUpdate {
            id: publisher_id.clone(),
            status: SubStatus::Connecting,
            has_video: false,
        });
        self.log(LogLevel::Info, format!("正在订阅 {publisher_id} ..."));
    }

    fn unsubscribe(&mut self, publisher_id: &str) {
        let Some(entry) = self.subs.remove(publisher_id) else {
            return;
        };
        self.publisher_tracks.remove(publisher_id);
        for track_mid in &entry.sent_tracks {
            self.send(ClientMessage::Unsubscribe {
                pc_id: entry.pc_id.clone(),
                publisher_id: publisher_id.to_string(),
                track_mid: track_mid.clone(),
            });
        }
        self.send(ClientMessage::Leave {
            pc_id: entry.pc_id.clone(),
        });
        drop(entry.pc);
        let _ = self.ui_tx.send(UiEvent::SubscriptionUpdate {
            id: publisher_id.to_string(),
            status: SubStatus::Idle,
            has_video: false,
        });
        self.log(LogLevel::Info, format!("已取消订阅 {publisher_id}"));
    }

    // ── 信令处理 ──────────────────────────────────────────────

    fn handle_signal(&mut self, msg: ServerMessage) {
        match msg {
            ServerMessage::SdpAnswer {
                pc_id,
                sdp,
                ice_candidates,
            } => {
                if pc_id == "pub" {
                    self.on_pub_answer(sdp, ice_candidates);
                } else {
                    let pid = self
                        .subs
                        .iter()
                        .find(|(_, e)| e.pc_id == pc_id)
                        .map(|(k, _)| k.clone());
                    match pid {
                        Some(pid) => self.on_sub_answer(pid, sdp, ice_candidates),
                        None => self.log(
                            LogLevel::Warn,
                            format!("收到未知 pc_id 的 sdp_answer: {pc_id}"),
                        ),
                    }
                }
            }

            ServerMessage::Joined {
                participants,
                room_id,
                role,
                ..
            } => {
                self.log(
                    LogLevel::Info,
                    format!(
                        "已加入房间 {room_id} ({} 位已有成员, role={role})",
                        participants.len()
                    ),
                );
                for p in participants {
                    if self.is_self_or_shadow(&p.participant_id) {
                        continue;
                    }
                    self.publisher_tracks
                        .insert(p.participant_id.clone(), p.tracks);
                    let _ = self.ui_tx.send(UiEvent::ParticipantJoined {
                        id: p.participant_id.clone(),
                    });
                    self.subscribe(p.participant_id);
                }
            }

            ServerMessage::IceCandidate {
                pc_id,
                candidate,
                sdp_mid,
            } => {
                let cand = IceCandidate {
                    candidate,
                    sdp_mid,
                    sdp_mline_index: None,
                };
                if pc_id == "pub" {
                    if let Some(pc) = &self.pub_pc {
                        let _ = pc.add_ice_candidate(&cand);
                    }
                } else if let Some(e) = self.subs.get(&pc_id) {
                    let _ = e.pc.add_ice_candidate(&cand);
                }
            }

            ServerMessage::Subscribed {
                publisher_id,
                pc_id,
            } => {
                let has_video = if let Some(e) = self.subs.get_mut(&publisher_id) {
                    e.status = SubStatus::Subscribed;
                    e.has_video
                } else {
                    false
                };
                let _ = self.ui_tx.send(UiEvent::SubscriptionUpdate {
                    id: publisher_id.clone(),
                    status: SubStatus::Subscribed,
                    has_video,
                });
                self.log(LogLevel::Success, format!("订阅确认: {publisher_id} ({pc_id})"));
            }

            ServerMessage::ParticipantJoined {
                participant_id,
                tracks,
            } => {
                if self.is_self_or_shadow(&participant_id) {
                    return;
                }
                self.publisher_tracks.insert(participant_id.clone(), tracks);
                let _ = self.ui_tx.send(UiEvent::ParticipantJoined {
                    id: participant_id.clone(),
                });
                self.log(LogLevel::Info, format!("参与者加入: {participant_id}"));
                self.subscribe(participant_id);
            }

            ServerMessage::ParticipantTracks {
                participant_id,
                tracks,
            } => {
                if self.is_self_or_shadow(&participant_id) {
                    return;
                }
                self.publisher_tracks.insert(participant_id.clone(), tracks);
                if self.subs.contains_key(&participant_id) {
                    self.maybe_subscribe_tracks(&participant_id);
                } else {
                    self.subscribe(participant_id);
                }
            }

            ServerMessage::ParticipantUpdated {
                participant_id,
                audio_enabled,
                video_enabled,
            } => {
                self.log(
                    LogLevel::Info,
                    format!(
                        "参与者状态更新: {participant_id} (audio={audio_enabled}, video={video_enabled})"
                    ),
                );
            }

            ServerMessage::Welcome {
                protocol_version,
                server,
            } => {
                self.log(
                    LogLevel::Info,
                    format!("信令握手: {server} (v{protocol_version})"),
                );
            }

            ServerMessage::Pong { .. } => {}

            ServerMessage::Left { pc_id } => {
                self.log(LogLevel::Info, format!("连接已关闭: {pc_id}"));
            }

            ServerMessage::Unsubscribed { publisher_id, .. } => {
                self.log(LogLevel::Info, format!("已取消订阅: {publisher_id}"));
            }

            ServerMessage::RoomState { participants, .. } => {
                self.log(
                    LogLevel::Info,
                    format!("房间快照: {} 位成员", participants.len()),
                );
            }

            ServerMessage::ParticipantLeft { participant_id } => {
                self.unsubscribe(&participant_id);
                let _ = self.ui_tx.send(UiEvent::ParticipantLeft {
                    id: participant_id.clone(),
                });
                self.log(LogLevel::Warn, format!("参与者离开: {participant_id}"));
            }

            ServerMessage::Error { pc_id, error } => {
                // pc_id 为空的错误来自连接层 (连不上 / 断开 / 解析失败),
                // 视为致命错误并退出通话; 带 pc_id 的仅为某个 PC 的协商错误。
                let fatal = pc_id.is_none();
                let msg = match &pc_id {
                    Some(id) => format!("服务器错误 [{id}]: {error}"),
                    None => format!("服务器错误: {error}"),
                };
                self.log(LogLevel::Error, msg.clone());
                if fatal {
                    let _ = self.ui_tx.send(UiEvent::Error { msg });
                    self.leave();
                }
            }
        }
    }

    fn on_pub_answer(&mut self, sdp: String, candidates: Vec<String>) {
        let Some(pc) = &self.pub_pc else {
            return;
        };
        let answer = SessionDescription {
            kind: SdpType::Answer,
            sdp,
        };
        if let Err(e) = pc.set_remote_description(&answer) {
            self.log(LogLevel::Error, format!("设置发布远端 SDP 失败: {e}"));
            return;
        }
        // BUNDLE: 候选加到 mid 0 (音频) 即可
        for c in candidates {
            let _ = pc.add_ice_candidate(&IceCandidate {
                candidate: c,
                sdp_mid: Some("0".to_string()),
                sdp_mline_index: Some(0),
            });
        }
        self.log(LogLevel::Success, "发布 SDP 协商完成, 已添加服务器候选");
    }

    fn on_sub_answer(&mut self, publisher_id: String, sdp: String, candidates: Vec<String>) {
        let (_pc_id, ok) = {
            let Some(entry) = self.subs.get(&publisher_id) else {
                return;
            };
            let answer = SessionDescription {
                kind: SdpType::Answer,
                sdp,
            };
            match entry.pc.set_remote_description(&answer) {
                Ok(()) => {
                    // 订阅端有两个 mid, 都尝试添加 (BUNDLE 单端口候选)
                    for c in &candidates {
                        let _ = entry.pc.add_ice_candidate(&IceCandidate {
                            candidate: c.clone(),
                            sdp_mid: Some("0".to_string()),
                            sdp_mline_index: Some(0),
                        });
                        let _ = entry.pc.add_ice_candidate(&IceCandidate {
                            candidate: c.clone(),
                            sdp_mid: Some("1".to_string()),
                            sdp_mline_index: Some(1),
                        });
                    }
                    (entry.pc_id.clone(), true)
                }
                Err(_) => (entry.pc_id.clone(), false),
            }
        };

        if !ok {
            if let Some(e) = self.subs.get_mut(&publisher_id) {
                e.status = SubStatus::Error;
            }
            let _ = self.ui_tx.send(UiEvent::SubscriptionUpdate {
                id: publisher_id.clone(),
                status: SubStatus::Error,
                has_video: false,
            });
            self.log(
                LogLevel::Error,
                format!("订阅 {publisher_id} 远端 SDP 设置失败"),
            );
            return;
        }

        // 协商完成后, 依据发布者已知轨道发送 subscribe。
        if let Some(e) = self.subs.get_mut(&publisher_id) {
            e.negotiated = true;
        }
        self.maybe_subscribe_tracks(&publisher_id);
    }

    /// 判断是否为自身或订阅连接产生的伪参与者 (xxx-recv-yyy)。
    fn is_self_or_shadow(&self, participant_id: &str) -> bool {
        participant_id == self.participant_id
            || participant_id.starts_with(&format!("{}-recv-", self.participant_id))
    }

    /// 依据已知的发布者轨道发送 `subscribe` (音频→本地 mid 0, 视频→本地 mid 1)。
    ///
    /// 仅在 `sdp_answer` 之后发送; 可重复调用 (只补发尚未订阅的 mid),
    /// 以应对 `participant_tracks` 在协商之后才到达的场景。
    fn maybe_subscribe_tracks(&mut self, publisher_id: &str) {
        let Some(entry) = self.subs.get(publisher_id) else {
            return;
        };
        if !entry.negotiated {
            return;
        }
        let pc_id = entry.pc_id.clone();
        let sent = entry.sent_tracks.clone();
        let Some(tracks) = self.publisher_tracks.get(publisher_id).cloned() else {
            return;
        };

        // 桌面端仅呈现一路音频 + 一路视频; 每种 kind 取第一条轨道。
        let mut want: Vec<(String, &'static str)> = Vec::new();
        if let Some(a) = tracks.iter().find(|t| t.kind == "audio") {
            want.push((a.mid.clone(), "0"));
        }
        if let Some(v) = tracks.iter().find(|t| t.kind == "video") {
            want.push((v.mid.clone(), "1"));
        }

        let fresh: Vec<(String, &'static str)> = want
            .into_iter()
            .filter(|(mid, _)| !sent.iter().any(|m| m == mid))
            .collect();
        if fresh.is_empty() {
            return;
        }

        for (track_mid, subscriber_mid) in &fresh {
            self.send(ClientMessage::Subscribe {
                pc_id: pc_id.clone(),
                publisher_id: publisher_id.to_string(),
                track_mid: track_mid.clone(),
                subscriber_mid: (*subscriber_mid).to_string(),
            });
        }
        if let Some(e) = self.subs.get_mut(publisher_id) {
            e.sent_tracks.extend(fresh.iter().map(|(m, _)| m.clone()));
        }
        self.log(
            LogLevel::Success,
            format!(
                "订阅 {publisher_id} SDP 完成, 已发送 {} 条 subscribe",
                fresh.len()
            ),
        );
    }

    // ── 远端轨道 ──────────────────────────────────────────────

    fn handle_remote_track(&mut self, ev: RemoteTrackEvent) {
        match ev.track {
            RemoteTrack::Video(vt) => {
                let ui = self.ui_tx.clone();
                let pid = ev.publisher_id.clone();
                // on_frame 在 WebRTC 线程触发: 转 RGBA 后发给主线程
                vt.on_frame(move |frame| {
                    let rgba = bgra_to_rgba(frame.bgra, frame.width, frame.height);
                    let _ = ui.send(UiEvent::RemoteVideo {
                        id: pid.clone(),
                        rgba,
                        width: frame.width,
                        height: frame.height,
                    });
                });
                if let Some(e) = self.subs.get_mut(&ev.publisher_id) {
                    e.has_video = true;
                    e._video_track = Some(vt);
                }
                let _ = self.ui_tx.send(UiEvent::SubscriptionUpdate {
                    id: ev.publisher_id.clone(),
                    status: SubStatus::Subscribed,
                    has_video: true,
                });
                self.log(
                    LogLevel::Success,
                    format!("收到 {} 的 video 轨道", ev.publisher_id),
                );
            }
            RemoteTrack::Audio(at) => {
                // 平台 ADM 会自动播放远端音频; 合成 ADM 下无声 (仅保活句柄)
                if let Some(e) = self.subs.get_mut(&ev.publisher_id) {
                    e._audio_track = Some(at);
                }
                self.log(
                    LogLevel::Success,
                    format!("收到 {} 的 audio 轨道", ev.publisher_id),
                );
            }
        }
    }

    // ── 媒体控制 ──────────────────────────────────────────────

    fn toggle_audio(&mut self, enabled: bool) {
        self.audio_enabled = enabled;
        if self.synthetic_audio {
            if let Some(g) = &self.audio_gen {
                if enabled {
                    g.resume();
                } else {
                    g.pause();
                }
            }
        } else if let Some(pc) = &self.pub_pc {
            // 平台麦克风无法直接静音: 切换发送方向 => 停止/恢复发送
            let dir = if enabled {
                TransceiverDirection::SendOnly
            } else {
                TransceiverDirection::Inactive
            };
            for t in pc.transceivers() {
                if t.kind() == MediaKind::Audio {
                    let _ = t.set_direction(dir);
                }
            }
        }
    }

    fn toggle_video(&mut self, enabled: bool) {
        self.video_enabled = enabled;
        if let Some(cap) = &self.video_capture {
            if enabled {
                cap.resume();
            } else {
                cap.pause();
            }
        }
    }

    // ── 工具 ─────────────────────────────────────────────────

    fn send(&self, msg: ClientMessage) {
        if let Some(sig) = &self.sig {
            sig.send(msg);
        }
    }

    fn send_ice(&self, pc_id: &str, cand: &IceCandidate) {
        self.send(ClientMessage::IceCandidate {
            pc_id: pc_id.to_string(),
            candidate: cand.candidate.clone(),
            sdp_mid: cand.sdp_mid.clone(),
        });
    }

    fn log(&self, level: LogLevel, msg: impl Into<String>) {
        let _ = self.ui_tx.send(UiEvent::Log {
            level,
            msg: msg.into(),
        });
    }

    fn fail(&self, msg: String) {
        self.log(LogLevel::Error, msg.clone());
        let _ = self.ui_tx.send(UiEvent::Error { msg });
    }
}

// ── 辅助函数 ──────────────────────────────────────────────────

fn rtc_config() -> RtcConfiguration {
    RtcConfiguration {
        bundle_policy: BundlePolicy::MaxBundle,
        ice_connection_receiving_timeout_ms: Some(4000),
        ..Default::default()
    }
}

/// 等待 ICE 收集完成 (或超时)。
fn wait_gathering(shared: &PcShared, timeout: Duration) {
    let start = Instant::now();
    while !shared.gathering_complete.load(Ordering::SeqCst) {
        if start.elapsed() > timeout {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn take_candidates(shared: &PcShared) -> Vec<IceCandidate> {
    shared
        .pending_ice
        .lock()
        .map(|mut g| std::mem::take(&mut *g))
        .unwrap_or_default()
}

/// 构造 PeerConnection observer, 把回调转发为 UI 事件 / 内部通道。
fn make_observer(
    ui_tx: Sender<UiEvent>,
    pc_id: String,
    shared: Arc<PcShared>,
    publisher_id: Option<String>,
    track_tx: Option<Sender<RemoteTrackEvent>>,
) -> PeerConnectionObserver {
    let shared_ice = shared.clone();

    let ui_gather = ui_tx.clone();
    let pc_gather = pc_id.clone();
    let shared_gather = shared.clone();

    let ui_conn = ui_tx;
    let pc_conn = pc_id;

    let obs = PeerConnectionObserver::new()
        .on_ice_candidate(move |cand| {
            if let Ok(mut v) = shared_ice.pending_ice.lock() {
                v.push(cand);
            }
        })
        .on_ice_gathering_change(move |state| {
            if state == IceGatheringState::Complete {
                shared_gather.gathering_complete.store(true, Ordering::SeqCst);
            }
            let _ = ui_gather.send(UiEvent::Log {
                level: LogLevel::Info,
                msg: format!("[{pc_gather}] ICE 收集: {state:?}"),
            });
        })
        .on_connection_state_change(move |state| {
            let _ = ui_conn.send(UiEvent::ConnectionState {
                pc_id: pc_conn.clone(),
                state: format!("{state:?}"),
            });
            match state {
                PeerConnectionState::Connected => {
                    let _ = ui_conn.send(UiEvent::Log {
                        level: LogLevel::Success,
                        msg: format!("[{pc_conn}] WebRTC 已连接"),
                    });
                }
                PeerConnectionState::Failed => {
                    let _ = ui_conn.send(UiEvent::Log {
                        level: LogLevel::Error,
                        msg: format!("[{pc_conn}] WebRTC 连接失败"),
                    });
                }
                PeerConnectionState::Disconnected => {
                    let _ = ui_conn.send(UiEvent::Log {
                        level: LogLevel::Warn,
                        msg: format!("[{pc_conn}] WebRTC 连接断开"),
                    });
                }
                _ => {}
            }
        });

    if let Some(tx) = track_tx {
        let pid = publisher_id.unwrap_or_default();
        obs.on_track(move |track| {
            let _ = tx.send(RemoteTrackEvent {
                publisher_id: pid.clone(),
                track,
            });
        })
    } else {
        obs
    }
}
