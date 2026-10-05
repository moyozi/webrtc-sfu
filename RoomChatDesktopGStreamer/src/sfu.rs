//! SFU 客户端 worker — GStreamer (`webrtcbin`) 版。
//!
//! 运行在独立线程上, 该线程同时驱动一个 GLib 主循环:
//!   · `webrtcbin` 的 promise/future (`create-offer`) 与 bus 消息依赖主循环;
//!   · 每 15ms 的 `glib::timeout_add_local` 排空命令 / 信令 / GStreamer 事件。
//!
//! 所有对 `webrtcbin` 的操作 (create-offer / set-local-description /
//! set-remote-description / add-ice-candidate / add-transceiver) 都发生在
//! 主循环线程 (即 tick 内), 避免跨线程状态竞争。GStreamer 信号回调运行在内部
//! 流线程, 只通过 crossbeam channel 投递 `GstEvent`。
//!
//! 信令模型与 Web/Android/Slint 客户端一致: 一条 WebSocket 承载多个
//! PeerConnection, 用 `pc_id` 区分发布连接 (`"pub"`) 与订阅连接 (`"sub-<发布者id>"`)。

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{unbounded, Receiver, Sender};
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_sdp::SDPMessage;
use gstreamer_webrtc as gst_webrtc;

use crate::media::{self, SubscriberHandles};
use crate::protocol::{ClientMessage, ServerMessage, TrackInfo, PROTOCOL_VERSION};
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
    Subscribe {
        publisher_id: String,
    },
    Unsubscribe {
        publisher_id: String,
    },
    ToggleAudio {
        enabled: bool,
    },
    ToggleVideo {
        enabled: bool,
    },
    Shutdown,
}

// ── GStreamer 信号回调 → worker ───────────────────────────────

enum GstEvent {
    NegotiationNeeded {
        pc_id: String,
    },
    OfferCreated {
        pc_id: String,
        sdp: Option<String>,
    },
    /// `set-remote-description` 已生效, 此时才能安全添加服务器候选。
    RemoteDescriptionSet {
        pc_id: String,
        candidates: Vec<String>,
    },
    IceCandidate {
        pc_id: String,
        mline_index: u32,
        candidate: String,
    },
    ConnectionState {
        pc_id: String,
        state: String,
    },
    IceGatheringState {
        pc_id: String,
        state: String,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Publisher,
    Subscriber,
}

/// 一个 PeerConnection (`webrtcbin` + 其管线)。
struct Peer {
    pc_id: String,
    role: Role,
    pipeline: gst::Pipeline,
    webrtc: gst::Element,
    /// 订阅端对应的发布者 id。
    publisher_id: Option<String>,
    // 发布端媒体开关
    video_valve: Option<gst::Element>,
    audio_valve: Option<gst::Element>,
    // 订阅端状态
    status: SubStatus,
    has_video: bool,
    negotiated: bool,
    join_sent: bool,
    offer_pending: bool,
    sent_tracks: Vec<String>,
    /// 本地 offer SDP 中 kind → mid 的映射 (订阅端 `subscriber_mid`)。
    local_mids: HashMap<String, String>,
    /// 远端描述是否已应用 (应用之前收到的 trickle 候选先缓存)。
    remote_desc_set: bool,
    pending_ice: Vec<String>,
}

// ── 线程入口 ──────────────────────────────────────────────────

/// 启动 worker 线程, 返回命令发送端与线程句柄。
pub fn spawn_worker(ui_tx: Sender<UiEvent>) -> (Sender<ClientCommand>, JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = unbounded::<ClientCommand>();
    let handle = thread::Builder::new()
        .name("sfu-client".into())
        .spawn(move || run_worker(ui_tx, cmd_rx))
        .expect("无法启动 sfu-client 线程");
    (cmd_tx, handle)
}

fn run_worker(ui_tx: Sender<UiEvent>, cmd_rx: Receiver<ClientCommand>) {
    ensure_plugin_path();
    if let Err(e) = gst::init() {
        let _ = ui_tx.send(UiEvent::Error {
            msg: format!("GStreamer 初始化失败: {e}"),
        });
        return;
    }

    let missing = media::missing_elements();
    if !missing.is_empty() {
        let _ = ui_tx.send(UiEvent::Log {
            level: LogLevel::Error,
            msg: format!("缺少 GStreamer 元素: {}", missing.join(", ")),
        });
    }

    let (msg_tx, msg_rx) = unbounded::<ServerMessage>();
    let (gst_tx, gst_rx) = unbounded::<GstEvent>();
    let client = Rc::new(RefCell::new(SfuClient::new(
        ui_tx, msg_tx, msg_rx, gst_tx, gst_rx, cmd_rx,
    )));

    let main_loop = gst::glib::MainLoop::new(None, false);
    let ml = main_loop.clone();
    let c = client.clone();
    gst::glib::timeout_add_local(Duration::from_millis(15), move || {
        let keep_going = c.borrow_mut().tick();
        if keep_going {
            gst::glib::ControlFlow::Continue
        } else {
            ml.quit();
            gst::glib::ControlFlow::Break
        }
    });

    main_loop.run();

    // 退出前确保管线释放
    client.borrow_mut().leave();
}

// ── SfuClient ─────────────────────────────────────────────────

struct SfuClient {
    ui_tx: Sender<UiEvent>,
    msg_tx: Sender<ServerMessage>,
    msg_rx: Receiver<ServerMessage>,
    gst_tx: Sender<GstEvent>,
    gst_rx: Receiver<GstEvent>,
    cmd_rx: Receiver<ClientCommand>,

    sig: Option<Signaling>,

    room_id: String,
    participant_id: String,
    active: bool,
    use_real_devices: bool,

    publisher: Option<Peer>,
    subs: HashMap<String, Peer>,
    /// 各发布者已知的轨道 (mid + kind), 用于订阅与取消订阅。
    publisher_tracks: HashMap<String, Vec<TrackInfo>>,

    audio_enabled: bool,
    video_enabled: bool,
}

impl SfuClient {
    fn new(
        ui_tx: Sender<UiEvent>,
        msg_tx: Sender<ServerMessage>,
        msg_rx: Receiver<ServerMessage>,
        gst_tx: Sender<GstEvent>,
        gst_rx: Receiver<GstEvent>,
        cmd_rx: Receiver<ClientCommand>,
    ) -> Self {
        Self {
            ui_tx,
            msg_tx,
            msg_rx,
            gst_tx,
            gst_rx,
            cmd_rx,
            sig: None,
            room_id: String::new(),
            participant_id: String::new(),
            active: false,
            use_real_devices: true,
            publisher: None,
            subs: HashMap::new(),
            publisher_tracks: HashMap::new(),
            audio_enabled: true,
            video_enabled: true,
        }
    }

    // ── 事件循环 ──────────────────────────────────────────────

    /// 处理一批事件; 返回 false 表示请求退出主循环。
    fn tick(&mut self) -> bool {
        while let Ok(cmd) = self.cmd_rx.try_recv() {
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
                    return false;
                }
            }
        }

        if self.sig.is_some() {
            while let Ok(msg) = self.msg_rx.try_recv() {
                self.handle_signal(msg);
            }
        }
        while let Ok(ev) = self.gst_rx.try_recv() {
            self.handle_gst_event(ev);
        }

        self.drain_buses();
        true
    }

    // ── Peer 查找 ────────────────────────────────────────────

    fn peer(&self, pc_id: &str) -> Option<&Peer> {
        if let Some(p) = &self.publisher {
            if p.pc_id == pc_id {
                return Some(p);
            }
        }
        self.subs.values().find(|p| p.pc_id == pc_id)
    }

    fn peer_mut(&mut self, pc_id: &str) -> Option<&mut Peer> {
        match &mut self.publisher {
            Some(p) if p.pc_id == pc_id => return Some(p),
            _ => {}
        }
        self.subs.values_mut().find(|p| p.pc_id == pc_id)
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
        while self.msg_rx.try_recv().is_ok() {}
        while self.gst_rx.try_recv().is_ok() {}

        self.room_id = room_id.clone();
        self.participant_id = participant_id;
        self.use_real_devices = use_real_devices;
        self.audio_enabled = true;
        self.video_enabled = true;

        // 1. 信令
        match Signaling::connect(&server_url, self.msg_tx.clone()) {
            Ok(sig) => self.sig = Some(sig),
            Err(e) => {
                self.fail(format!("信令连接失败: {e}"));
                return;
            }
        }
        self.send(ClientMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            client: format!("{}/desktop-gst", env!("CARGO_PKG_NAME")),
        });

        // 2. 发布管线 (webrtcbin + 采集 + 编码)
        let handles = match media::build_publisher(self.ui_tx.clone(), use_real_devices) {
            Ok(h) => h,
            Err(e) => {
                self.fail(e);
                self.leave();
                return;
            }
        };
        connect_webrtc_signals(&handles.webrtc, "pub", self.gst_tx.clone(), None);
        media::set_publisher_sendonly(&handles.webrtc);
        if let Err(e) = media::set_playing(&handles.pipeline) {
            self.log(LogLevel::Warn, e);
        }
        self.publisher = Some(Peer {
            pc_id: "pub".to_string(),
            role: Role::Publisher,
            pipeline: handles.pipeline,
            webrtc: handles.webrtc,
            publisher_id: None,
            video_valve: handles.video_valve,
            audio_valve: handles.audio_valve,
            status: SubStatus::Idle,
            has_video: false,
            negotiated: false,
            join_sent: false,
            offer_pending: false,
            sent_tracks: Vec::new(),
            local_mids: HashMap::new(),
            remote_desc_set: false,
            pending_ice: Vec::new(),
        });

        self.active = true;
        let _ = self.ui_tx.send(UiEvent::Joined {
            room_id: room_id.clone(),
        });
        self.log(LogLevel::Info, format!("正在加入房间 {room_id} ..."));
    }

    fn leave(&mut self) {
        let had_signal = self.sig.is_some();
        if had_signal {
            let subs: Vec<(String, Peer)> = self.subs.drain().collect();
            for (publisher_id, peer) in subs {
                for track_mid in &peer.sent_tracks {
                    self.send(ClientMessage::Unsubscribe {
                        pc_id: peer.pc_id.clone(),
                        publisher_id: publisher_id.clone(),
                        track_mid: track_mid.clone(),
                    });
                }
                self.send(ClientMessage::Leave {
                    pc_id: peer.pc_id.clone(),
                });
                let _ = peer.pipeline.set_state(gst::State::Null);
            }
            if let Some(p) = self.publisher.take() {
                self.send(ClientMessage::Leave {
                    pc_id: "pub".to_string(),
                });
                let _ = p.pipeline.set_state(gst::State::Null);
            }
            if self.active {
                thread::sleep(Duration::from_millis(150));
            }
        } else {
            let subs: Vec<(String, Peer)> = self.subs.drain().collect();
            for (_, peer) in subs {
                let _ = peer.pipeline.set_state(gst::State::Null);
            }
            if let Some(p) = self.publisher.take() {
                let _ = p.pipeline.set_state(gst::State::Null);
            }
        }

        self.publisher_tracks.clear();
        self.sig = None;

        if self.active {
            let _ = self.ui_tx.send(UiEvent::Left);
        }
        self.active = false;
    }

    // ── 发布: offer 生成 ─────────────────────────────────────

    fn on_negotiation_needed(&mut self, pc_id: &str) {
        let Some(peer) = self.peer_mut(pc_id) else {
            return;
        };
        if peer.offer_pending || peer.join_sent {
            return;
        }
        peer.offer_pending = true;
        let webrtc = peer.webrtc.clone();
        let tx = self.gst_tx.clone();
        let id = pc_id.to_string();

        let promise = gst::Promise::with_change_func(move |reply| {
            let mut sdp = None;
            if let Ok(Some(st)) = reply {
                if let Ok(offer) = st.get::<gst_webrtc::WebRTCSessionDescription>("offer") {
                    sdp = offer.sdp().as_text().ok();
                }
            }
            let _ = tx.send(GstEvent::OfferCreated {
                pc_id: id.clone(),
                sdp,
            });
        });

        let _ = webrtc.emit_by_name::<()>("create-offer", &[&None::<gst::Structure>, &promise]);
    }

    fn on_offer_created(&mut self, pc_id: &str, sdp: Option<String>) {
        let Some(sdp) = sdp else {
            if let Some(p) = self.peer_mut(pc_id) {
                p.offer_pending = false;
            }
            self.log(
                LogLevel::Error,
                format!("[{pc_id}] create-offer 未返回 SDP"),
            );
            return;
        };

        let parsed = match SDPMessage::parse_buffer(sdp.as_bytes()) {
            Ok(m) => m,
            Err(e) => {
                self.log(
                    LogLevel::Error,
                    format!("[{pc_id}] 解析本地 SDP 失败: {e:?}"),
                );
                return;
            }
        };
        let offer =
            gst_webrtc::WebRTCSessionDescription::new(gst_webrtc::WebRTCSDPType::Offer, parsed);

        let (role, publisher_id) = {
            let Some(p) = self.peer_mut(pc_id) else {
                return;
            };
            let _ = p
                .webrtc
                .emit_by_name::<()>("set-local-description", &[&offer, &None::<gst::Promise>]);
            p.local_mids = parse_mids(&sdp);
            p.offer_pending = false;
            p.join_sent = true;
            (p.role, p.publisher_id.clone())
        };

        let participant_id = match role {
            Role::Publisher => self.participant_id.clone(),
            Role::Subscriber => format!(
                "{}-recv-{}",
                self.participant_id,
                publisher_id.as_deref().unwrap_or_default()
            ),
        };
        let role_str = match role {
            Role::Publisher => "publisher",
            Role::Subscriber => "subscriber",
        };
        tracing::debug!("[{pc_id}] 本地 offer:\n{sdp}");
        self.send(ClientMessage::Join {
            pc_id: pc_id.to_string(),
            room_id: self.room_id.clone(),
            participant_id,
            sdp,
            role: role_str.to_string(),
        });
        self.log(LogLevel::Info, format!("[{pc_id}] 已发送 SDP offer"));
    }

    // ── 订阅 ─────────────────────────────────────────────────

    fn subscribe(&mut self, publisher_id: String) {
        if self.sig.is_none() {
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

        let SubscriberHandles { pipeline, webrtc } = match media::build_subscriber() {
            Ok(h) => h,
            Err(e) => {
                self.log(LogLevel::Error, format!("创建订阅连接失败: {e}"));
                return;
            }
        };

        let pc_id = format!("sub-{publisher_id}");
        // recvonly transceiver 已在 build_subscriber 中添加 (必须在 PLAYING 之前)。
        connect_webrtc_signals(
            &webrtc,
            &pc_id,
            self.gst_tx.clone(),
            Some(RemotePadContext {
                pipeline: pipeline.clone(),
                publisher_id: publisher_id.clone(),
                ui_tx: self.ui_tx.clone(),
                play_audio: self.use_real_devices,
            }),
        );
        let pipeline_for_state = pipeline.clone();

        self.subs.insert(
            publisher_id.clone(),
            Peer {
                pc_id: pc_id.clone(),
                role: Role::Subscriber,
                pipeline,
                webrtc,
                publisher_id: Some(publisher_id.clone()),
                video_valve: None,
                audio_valve: None,
                status: SubStatus::Connecting,
                has_video: false,
                negotiated: false,
                join_sent: false,
                offer_pending: false,
                sent_tracks: Vec::new(),
                local_mids: HashMap::new(),
                remote_desc_set: false,
                pending_ice: Vec::new(),
            },
        );

        // 先 PAUSED 协商; 收到 answer / 远端描述就绪后再切 PLAYING。
        if let Err(e) = media::set_paused(&pipeline_for_state) {
            self.log(LogLevel::Warn, e);
        }

        let _ = self.ui_tx.send(UiEvent::SubscriptionUpdate {
            id: publisher_id.clone(),
            status: SubStatus::Connecting,
            has_video: false,
        });
        self.log(LogLevel::Info, format!("正在订阅 {publisher_id} ..."));
    }

    fn unsubscribe(&mut self, publisher_id: &str) {
        let Some(peer) = self.subs.remove(publisher_id) else {
            return;
        };
        self.publisher_tracks.remove(publisher_id);
        for track_mid in &peer.sent_tracks {
            self.send(ClientMessage::Unsubscribe {
                pc_id: peer.pc_id.clone(),
                publisher_id: publisher_id.to_string(),
                track_mid: track_mid.clone(),
            });
        }
        self.send(ClientMessage::Leave {
            pc_id: peer.pc_id.clone(),
        });
        let _ = peer.pipeline.set_state(gst::State::Null);

        let _ = self.ui_tx.send(UiEvent::SubscriptionUpdate {
            id: publisher_id.to_string(),
            status: SubStatus::Idle,
            has_video: false,
        });
        self.log(LogLevel::Info, format!("已取消订阅 {publisher_id}"));
    }

    /// 依据已知的发布者轨道发送 `subscribe`
    /// (音频→本地 audio mid, 视频→本地 video mid)。
    ///
    /// 仅在 `sdp_answer` 之后发送; 可重复调用 (只补发尚未订阅的 mid)。
    fn maybe_subscribe_tracks(&mut self, publisher_id: &str) {
        let Some(peer) = self.subs.get(publisher_id) else {
            return;
        };
        if !peer.negotiated || !peer.join_sent {
            return;
        }
        let pc_id = peer.pc_id.clone();
        let sent = peer.sent_tracks.clone();
        let mids = peer.local_mids.clone();
        let Some(tracks) = self.publisher_tracks.get(publisher_id).cloned() else {
            return;
        };

        // 桌面端仅呈现一路音频 + 一路视频; 每种 kind 取第一条轨道。
        let mut want: Vec<(String, String)> = Vec::new();
        if let Some(a) = tracks.iter().find(|t| t.kind == "audio") {
            if let Some(mid) = mids.get("audio") {
                want.push((a.mid.clone(), mid.clone()));
            }
        }
        if let Some(v) = tracks.iter().find(|t| t.kind == "video") {
            if let Some(mid) = mids.get("video") {
                want.push((v.mid.clone(), mid.clone()));
            }
        }

        let fresh: Vec<(String, String)> = want
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
                subscriber_mid: subscriber_mid.clone(),
            });
        }
        if let Some(p) = self.subs.get_mut(publisher_id) {
            p.sent_tracks.extend(fresh.iter().map(|(m, _)| m.clone()));
        }
        self.log(
            LogLevel::Success,
            format!(
                "订阅 {publisher_id} SDP 完成, 已发送 {} 条 subscribe",
                fresh.len()
            ),
        );
    }

    // ── 信令处理 ──────────────────────────────────────────────

    fn handle_signal(&mut self, msg: ServerMessage) {
        match msg {
            ServerMessage::SdpAnswer {
                pc_id,
                sdp,
                ice_candidates,
            } => self.on_answer(&pc_id, sdp, ice_candidates),

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
                let index = sdp_mid
                    .as_deref()
                    .and_then(|s| s.parse::<u32>().ok())
                    .unwrap_or(0);
                if let Some(peer) = self.peer_mut(&pc_id) {
                    if peer.remote_desc_set {
                        let webrtc = peer.webrtc.clone();
                        let _ =
                            webrtc.emit_by_name::<()>("add-ice-candidate", &[&index, &candidate]);
                    } else {
                        // 远端描述尚未应用, 先缓存, 待 RemoteDescriptionSet 后再添加。
                        peer.pending_ice.push(candidate);
                    }
                }
            }

            ServerMessage::Subscribed {
                publisher_id,
                pc_id,
            } => {
                let has_video = if let Some(p) = self.subs.get_mut(&publisher_id) {
                    p.status = SubStatus::Subscribed;
                    p.has_video
                } else {
                    false
                };
                let _ = self.ui_tx.send(UiEvent::SubscriptionUpdate {
                    id: publisher_id.clone(),
                    status: SubStatus::Subscribed,
                    has_video,
                });
                self.log(
                    LogLevel::Success,
                    format!("订阅确认: {publisher_id} ({pc_id})"),
                );
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
                // pc_id 为空的错误来自连接层, 视为致命错误并退出通话;
                // 带 pc_id 的仅为某个 PC 的协商错误。
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

    fn on_answer(&mut self, pc_id: &str, sdp: String, candidates: Vec<String>) {
        let Some(peer) = self.peer(pc_id) else {
            self.log(
                LogLevel::Warn,
                format!("收到未知 pc_id 的 sdp_answer: {pc_id}"),
            );
            return;
        };
        let webrtc = peer.webrtc.clone();
        let role = peer.role;
        let publisher_id = peer.publisher_id.clone();

        let parsed = match SDPMessage::parse_buffer(sdp.as_bytes()) {
            Ok(m) => m,
            Err(e) => {
                self.log(
                    LogLevel::Error,
                    format!("[{pc_id}] 解析 answer SDP 失败: {e:?}"),
                );
                return;
            }
        };
        tracing::debug!("[{pc_id}] 远端 answer:\n{sdp}");
        let answer =
            gst_webrtc::WebRTCSessionDescription::new(gst_webrtc::WebRTCSDPType::Answer, parsed);

        // set-remote-description 是异步的: 必须在它的 promise 完成 (远端描述已
        // 应用、ICE transport 已就绪) 之后才能添加远端候选, 否则候选会被丢弃。
        let tx = self.gst_tx.clone();
        let pid = pc_id.to_string();
        let promise = gst::Promise::with_change_func(move |reply| {
            // set-remote-description 成功时 promise 不带 reply structure (Ok(None))。
            let ok = reply.is_ok();
            if !ok {
                tracing::warn!("[{pid}] set-remote-description 失败");
            }
            let _ = tx.send(GstEvent::RemoteDescriptionSet {
                pc_id: pid.clone(),
                candidates: if ok { candidates } else { Vec::new() },
            });
        });
        let _ = webrtc.emit_by_name::<()>("set-remote-description", &[&answer, &promise]);

        if role == Role::Subscriber {
            if let Some(p) = self.peer_mut(pc_id) {
                p.negotiated = true;
            }
            if let Some(pid) = publisher_id {
                self.maybe_subscribe_tracks(&pid);
            }
        } else {
            self.log(LogLevel::Success, format!("[{pc_id}] 发布 SDP 协商完成"));
        }
    }

    /// 远端描述已应用: 现在添加服务器候选与缓存的 trickle 候选。
    fn on_remote_description_set(&mut self, pc_id: &str, candidates: Vec<String>) {
        let Some(peer) = self.peer_mut(pc_id) else {
            return;
        };
        peer.remote_desc_set = true;
        let webrtc = peer.webrtc.clone();
        let pending = std::mem::take(&mut peer.pending_ice);

        // BUNDLE: 服务器候选统一加到 mline 0。
        for c in candidates.iter().chain(pending.iter()) {
            tracing::debug!("[{pc_id}] add-ice-candidate(idx=0): {c}");
            let _ = webrtc.emit_by_name::<()>("add-ice-candidate", &[&0u32, c]);
        }
        // 远端描述已就绪, 订阅连接切到 PLAYING。
        let pipeline = self.peer(pc_id).map(|p| p.pipeline.clone());
        if let Some(pipeline) = pipeline {
            if let Err(e) = media::set_playing(&pipeline) {
                tracing::warn!("[{pc_id}] {e}");
            }
        }
        if !candidates.is_empty() || !pending.is_empty() {
            self.log(
                LogLevel::Success,
                format!(
                    "[{pc_id}] 已添加 {} 条远端候选",
                    candidates.len() + pending.len()
                ),
            );
        }
    }

    // ── GStreamer 回调事件 ───────────────────────────────────

    fn handle_gst_event(&mut self, ev: GstEvent) {
        match ev {
            GstEvent::NegotiationNeeded { pc_id } => self.on_negotiation_needed(&pc_id),
            GstEvent::OfferCreated { pc_id, sdp } => self.on_offer_created(&pc_id, sdp),
            GstEvent::RemoteDescriptionSet { pc_id, candidates } => {
                self.on_remote_description_set(&pc_id, candidates)
            }
            GstEvent::IceCandidate {
                pc_id,
                mline_index,
                candidate,
            } => {
                tracing::debug!("[{pc_id}] ICE candidate (mline={mline_index}): {candidate}");
                if candidate.is_empty() {
                    return;
                }
                self.send(ClientMessage::IceCandidate {
                    pc_id,
                    candidate,
                    sdp_mid: Some(mline_index.to_string()),
                });
            }
            GstEvent::ConnectionState { pc_id, state } => {
                let _ = self.ui_tx.send(UiEvent::ConnectionState {
                    pc_id: pc_id.clone(),
                    state: state.clone(),
                });
                match state.as_str() {
                    "Connected" => self.log(LogLevel::Success, format!("[{pc_id}] WebRTC 已连接")),
                    "Failed" => self.log(LogLevel::Error, format!("[{pc_id}] WebRTC 连接失败")),
                    "Disconnected" => {
                        self.log(LogLevel::Warn, format!("[{pc_id}] WebRTC 连接断开"))
                    }
                    _ => {}
                }
            }
            GstEvent::IceGatheringState { pc_id, state } => {
                self.log(LogLevel::Info, format!("[{pc_id}] ICE 收集: {state}"));
            }
        }
    }

    // ── 媒体控制 ──────────────────────────────────────────────

    fn toggle_audio(&mut self, enabled: bool) {
        self.audio_enabled = enabled;
        if let Some(peer) = &self.publisher {
            if let Some(valve) = &peer.audio_valve {
                valve.set_property("drop", !enabled);
            }
        }
        if self.active {
            self.send(ClientMessage::UpdateMedia {
                pc_id: "pub".to_string(),
                audio_enabled: Some(enabled),
                video_enabled: None,
            });
        }
    }

    fn toggle_video(&mut self, enabled: bool) {
        self.video_enabled = enabled;
        if let Some(peer) = &self.publisher {
            if let Some(valve) = &peer.video_valve {
                valve.set_property("drop", !enabled);
            }
        }
        if self.active {
            self.send(ClientMessage::UpdateMedia {
                pc_id: "pub".to_string(),
                audio_enabled: None,
                video_enabled: Some(enabled),
            });
        }
    }

    // ── 工具 ─────────────────────────────────────────────────

    fn is_self_or_shadow(&self, participant_id: &str) -> bool {
        participant_id == self.participant_id
            || participant_id.starts_with(&format!("{}-recv-", self.participant_id))
    }

    fn send(&self, msg: ClientMessage) {
        if let Some(sig) = &self.sig {
            sig.send(msg);
        }
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

    /// 排空各管线的 bus, 记录错误 / 警告 / EOS (不因单个 sink 错误终止会话)。
    fn drain_buses(&mut self) {
        let mut messages: Vec<(String, gst::Message)> = Vec::new();
        if let Some(p) = &self.publisher {
            if let Some(bus) = p.pipeline.bus() {
                while let Some(msg) = bus.pop() {
                    messages.push((p.pc_id.clone(), msg));
                }
            }
        }
        for peer in self.subs.values() {
            if let Some(bus) = peer.pipeline.bus() {
                while let Some(msg) = bus.pop() {
                    messages.push((peer.pc_id.clone(), msg));
                }
            }
        }

        for (pc_id, msg) in messages {
            match msg.view() {
                gst::MessageView::Error(err) => {
                    let src = err
                        .src()
                        .map(|s| s.path_string().to_string())
                        .unwrap_or_default();
                    let dbg = err.debug().map(|d| d.to_string()).unwrap_or_default();
                    self.log(
                        LogLevel::Error,
                        format!("GStreamer 错误 [{pc_id}/{src}]: {} | {dbg}", err.error()),
                    );
                }
                gst::MessageView::Warning(w) => {
                    let src = w
                        .src()
                        .map(|s| s.path_string().to_string())
                        .unwrap_or_default();
                    self.log(
                        LogLevel::Warn,
                        format!("GStreamer 警告 [{pc_id}/{src}]: {}", w.error()),
                    );
                }
                gst::MessageView::Eos(_) => {
                    self.log(LogLevel::Warn, format!("[{pc_id}] 媒体流结束"));
                }
                _ => {}
            }
        }
    }
}

// ── webrtcbin 信号连接 ────────────────────────────────────────

/// 订阅连接的远端 pad 处理上下文。
///
/// `pad-added` 必须**同步**处理: rtpbin 在 pad 暴露后立即可能有 RTP 到达,
/// 若等到 15ms 后的 tick 才链接, 未链接的 pad 会让上游得到 NOT_LINKED 并致命。
#[derive(Clone)]
struct RemotePadContext {
    pipeline: gst::Pipeline,
    publisher_id: String,
    ui_tx: Sender<UiEvent>,
    play_audio: bool,
}

/// 连接 `webrtcbin` 的信号。除 `pad-added` 外只把事件投递到 `gst_tx`
/// (回调运行在 GStreamer 内部线程, 不触碰 worker 状态)。
fn connect_webrtc_signals(
    webrtc: &gst::Element,
    pc_id: &str,
    tx: Sender<GstEvent>,
    remote: Option<RemotePadContext>,
) {
    // 需要 (重新) 协商
    {
        let tx = tx.clone();
        let id = pc_id.to_string();
        webrtc.connect("on-negotiation-needed", false, move |_values| {
            let _ = tx.send(GstEvent::NegotiationNeeded { pc_id: id.clone() });
            None
        });
    }

    // trickle ICE
    {
        let tx = tx.clone();
        let id = pc_id.to_string();
        webrtc.connect("on-ice-candidate", false, move |values| {
            let mline_index = values.get(1).and_then(|v| v.get::<u32>().ok()).unwrap_or(0);
            let candidate = values
                .get(2)
                .and_then(|v| v.get::<String>().ok())
                .unwrap_or_default();
            let _ = tx.send(GstEvent::IceCandidate {
                pc_id: id.clone(),
                mline_index,
                candidate,
            });
            None
        });
    }

    // 连接状态
    {
        let tx = tx.clone();
        let id = pc_id.to_string();
        let w = webrtc.clone();
        webrtc.connect_notify(Some("connection-state"), move |_, _| {
            let state = w.property::<gst_webrtc::WebRTCPeerConnectionState>("connection-state");
            let _ = tx.send(GstEvent::ConnectionState {
                pc_id: id.clone(),
                state: format!("{state:?}"),
            });
        });
    }

    // ICE 收集状态
    {
        let tx = tx.clone();
        let id = pc_id.to_string();
        let w = webrtc.clone();
        webrtc.connect_notify(Some("ice-gathering-state"), move |_, _| {
            let state = w.property::<gst_webrtc::WebRTCICEGatheringState>("ice-gathering-state");
            let _ = tx.send(GstEvent::IceGatheringState {
                pc_id: id.clone(),
                state: format!("{state:?}"),
            });
        });
    }

    // 远端媒体 src pad: 立即同步链接到解码分支 (见 RemotePadContext 注释)。
    {
        webrtc.connect_pad_added(move |_, pad| {
            if pad.direction() != gst::PadDirection::Src {
                return;
            }
            if let Some(ctx) = &remote {
                media::handle_remote_pad(
                    &ctx.pipeline,
                    pad,
                    ctx.ui_tx.clone(),
                    ctx.publisher_id.clone(),
                    ctx.play_audio,
                );
            }
        });
    }
}

/// 把用户级 GStreamer 插件目录 (免 root 安装的 nice 插件) 加入搜索路径。
///
/// `webrtcbin` 依赖 libnice 的 `nicesrc`/`nicesink`; Fedora 将它们放在单独的
/// `libnice-gstreamer1` 包中。`scripts/setup-gstreamer-webrtc-dev.sh` 会把它
/// 解压到该目录。若系统已全局安装, 则无需此步骤。
fn ensure_plugin_path() {
    let Some(home) = std::env::var_os("HOME") else {
        return;
    };
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    let base = std::path::PathBuf::from(&home).join(".local/share/gst-webrtc-dev/usr");
    candidates.push(base.join("lib64/gstreamer-1.0"));
    candidates.push(base.join("lib/gstreamer-1.0"));

    let existing = std::env::var("GST_PLUGIN_PATH").unwrap_or_default();
    let mut paths: Vec<String> = existing
        .split(':')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let mut changed = false;
    for dir in candidates {
        if dir.is_dir() {
            let s = dir.to_string_lossy().to_string();
            if !paths.iter().any(|p| p == &s) {
                paths.push(s);
                changed = true;
            }
        }
    }
    if changed {
        std::env::set_var("GST_PLUGIN_PATH", paths.join(":"));
    }
}

/// 从 SDP 提取 `kind → mid` 映射。
fn parse_mids(sdp: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Ok(msg) = SDPMessage::parse_buffer(sdp.as_bytes()) {
        for media in msg.medias() {
            let kind = media.media().unwrap_or_default();
            if let Some(mid) = media.attribute_val("mid") {
                out.insert(kind.to_string(), mid.to_string());
            }
        }
    }
    out
}
