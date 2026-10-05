//! SFU 客户端 worker — str0m 版。
//!
//! str0m 是 Sans-IO 的 WebRTC 协议栈: 所有输入 (UDP 收包 / 超时) 通过
//! `Rtc::handle_input` 送入, 所有输出 (发包 / 事件) 通过 `Rtc::poll_output`
//! 取走。因此 worker 在一个同步线程里自建事件循环:
//!
//! ```text
//!   loop {
//!     命令 (主线程) ──► 处理
//!     信令消息 (tokio) ──► 处理 (accept_answer / subscribe / …)
//!     已编码媒体 ──► rtc.writer(mid).write(...)   (发布)
//!     for 每个 Rtc: 收 UDP → handle_input; 到点 → Timeout; poll_output → 发包/事件
//!     sleep 3ms
//!   }
//! ```
//!
//! 线程与通道 (与 `RoomChatDesktopSlint` 保持一致的模型):
//!   · 主线程          : Slint UI, `ClientCommand` → worker, `UiEvent` → 主线程
//!   · sfu-client 线程 : 本模块, 独占所有 `Rtc` / `UdpSocket`
//!   · 信令 tokio 线程 : WebSocket 读写 (`crate::signaling`)
//!   · 媒体线程        : ffmpeg 采集/编解码 (`crate::media`)
//!
//! 每个 PeerConnection 一个 `Rtc` + 一个独立 UDP socket:
//!   · `pub`         发布连接 (sendonly 音频 + sendonly 视频)
//!   · `sub-<发布者>` 订阅连接 (recvonly 音频 + recvonly 视频)
//! 与 Web/Android 客户端及服务端 `pc_id` 约定一致。

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{unbounded, Receiver, Sender};
use str0m::change::{SdpAnswer, SdpPendingOffer};
use str0m::format::Codec;
use str0m::media::{Direction, MediaKind, MediaTime, Mid, Pt};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output, Rtc};

use crate::media::{
    AudioPublisher, EncodedAudio, EncodedVideo, RemoteAudioPlayer, RemoteVideoDecoder,
    VideoPublisher,
};
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

// ── worker 内部 ───────────────────────────────────────────────

/// 一个 PeerConnection 会话 (一个 str0m `Rtc` + 一个 UDP socket)。
struct PcSession {
    pc_id: String,

    rtc: Rtc,
    udp: UdpSocket,
    pending: Option<SdpPendingOffer>,
    next_timeout: Instant,

    // ── 发布端 ──
    audio_mid: Option<Mid>,
    video_mid: Option<Mid>,
    audio_pt: Option<Pt>,
    video_pt: Option<Pt>,

    // ── 订阅端 ──
    /// 对端发布者 id (仅订阅端有值)。
    publisher_id: Option<String>,
    sub_audio_mid: Option<Mid>,
    sub_video_mid: Option<Mid>,
    /// 是否已收到 `sdp_answer` (收到后才能发送 subscribe)。
    negotiated: bool,
    /// 已发送 subscribe 的发布者 track_mid (去重 / 取消订阅)。
    sent_tracks: Vec<String>,

    status: SubStatus,
    has_video: bool,

    // 远端媒体解码器 (懒创建, drop 时自动停止)
    video_decoder: Option<RemoteVideoDecoder>,
    audio_player: Option<RemoteAudioPlayer>,
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

    // 本地已编码媒体 (由 media 线程生产, 本线程消费写入 Rtc)
    video_tx: Sender<EncodedVideo>,
    video_rx: Receiver<EncodedVideo>,
    audio_tx: Sender<EncodedAudio>,
    audio_rx: Receiver<EncodedAudio>,

    // 本地媒体管线
    video_pub: Option<VideoPublisher>,
    audio_pub: Option<AudioPublisher>,

    // 会话
    pub_session: Option<PcSession>,
    subs: HashMap<String, PcSession>,
    publisher_tracks: HashMap<String, Vec<TrackInfo>>,

    room_id: String,
    participant_id: String,
    advertise_ip: IpAddr,
    active: bool,
    use_real_devices: bool,

    audio_enabled: bool,
    video_enabled: bool,
}

impl SfuClient {
    fn new(ui_tx: Sender<UiEvent>) -> Self {
        let (msg_tx, msg_rx) = unbounded::<ServerMessage>();
        let (video_tx, video_rx) = unbounded::<EncodedVideo>();
        let (audio_tx, audio_rx) = unbounded::<EncodedAudio>();
        Self {
            ui_tx,
            msg_tx,
            msg_rx,
            sig: None,
            video_tx,
            video_rx,
            audio_tx,
            audio_rx,
            video_pub: None,
            audio_pub: None,
            pub_session: None,
            subs: HashMap::new(),
            publisher_tracks: HashMap::new(),
            room_id: String::new(),
            participant_id: String::new(),
            advertise_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            active: false,
            use_real_devices: true,
            audio_enabled: true,
            video_enabled: true,
        }
    }

    // ── 事件循环 ──────────────────────────────────────────────

    fn run(&mut self, cmd_rx: Receiver<ClientCommand>) {
        loop {
            while let Ok(cmd) = cmd_rx.try_recv() {
                match cmd {
                    ClientCommand::Shutdown => {
                        self.leave();
                        return;
                    }
                    other => self.handle_command(other),
                }
            }

            if self.sig.is_some() {
                while let Ok(msg) = self.msg_rx.try_recv() {
                    self.handle_signal(msg);
                }
            }

            self.pump_encoded_media();

            let ui_tx = self.ui_tx.clone();
            let now = Instant::now();
            if let Some(p) = self.pub_session.as_mut() {
                pump_session(p, now, &ui_tx);
            }
            for s in self.subs.values_mut() {
                pump_session(s, now, &ui_tx);
            }

            thread::sleep(Duration::from_millis(3));
        }
    }

    fn handle_command(&mut self, cmd: ClientCommand) {
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
            ClientCommand::Shutdown => {}
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
        while self.msg_rx.try_recv().is_ok() {}

        self.room_id = room_id.clone();
        self.participant_id = participant_id.clone();
        self.use_real_devices = use_real_devices;
        self.audio_enabled = true;
        self.video_enabled = true;
        self.advertise_ip = resolve_advertise_ip(&server_url);

        // 信令
        match Signaling::connect(&server_url, self.msg_tx.clone()) {
            Ok(sig) => self.sig = Some(sig),
            Err(e) => {
                self.fail(format!("信令连接失败: {e}"));
                return;
            }
        }
        self.send(ClientMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            client: format!("{}/desktop-str0m", env!("CARGO_PKG_NAME")),
        });

        // 发布连接 + 本地媒体管线
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
        if let Some(mut v) = self.video_pub.take() {
            v.stop();
        }
        if let Some(mut a) = self.audio_pub.take() {
            a.stop();
        }

        // 通知服务端关闭各 PC (给 writer 一点时间 flush)
        if self.sig.is_some() {
            let sub_pc_ids: Vec<String> = self.subs.values().map(|s| s.pc_id.clone()).collect();
            for pc_id in sub_pc_ids {
                self.send(ClientMessage::Leave { pc_id });
            }
            if self.pub_session.is_some() {
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
        self.pub_session = None;
        self.sig = None; // drop 信令 (关闭连接)

        if self.active {
            let _ = self.ui_tx.send(UiEvent::Left);
        }
        self.active = false;
    }

    // ── 发布 ─────────────────────────────────────────────────

    fn create_publisher(&mut self) -> Result<(), String> {
        let (mut rtc, udp, cand) = self.new_transport()?;

        // 音频在前 (mid 由 str0m 生成), 视频在后; 服务端按发布者 mid 广播。
        let (audio_mid, video_mid, offer_sdp, pending) = {
            let mut api = rtc.sdp_api();
            let a = api.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
            let v = api.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
            let (offer, pending) = api.apply().ok_or("生成 SDP offer 失败")?;
            (a, v, offer.to_sdp_string(), pending)
        };
        let audio_pt = local_pt(&rtc, Codec::Opus);
        let video_pt = local_pt(&rtc, Codec::Vp8);

        self.send(ClientMessage::Join {
            pc_id: "pub".to_string(),
            room_id: self.room_id.clone(),
            participant_id: self.participant_id.clone(),
            sdp: offer_sdp,
            role: "publisher".to_string(),
        });
        self.send_ice("pub", &cand);

        self.pub_session = Some(PcSession {
            pc_id: "pub".to_string(),
            rtc,
            udp,
            pending: Some(pending),
            next_timeout: Instant::now(),
            audio_mid: Some(audio_mid),
            video_mid: Some(video_mid),
            audio_pt,
            video_pt,
            publisher_id: None,
            sub_audio_mid: None,
            sub_video_mid: None,
            negotiated: false,
            sent_tracks: Vec::new(),
            status: SubStatus::Idle,
            has_video: false,
            video_decoder: None,
            audio_player: None,
        });

        self.start_media_generators();
        Ok(())
    }

    fn start_media_generators(&mut self) {
        let (video_pub, is_camera, vmsg) = VideoPublisher::start(
            self.video_tx.clone(),
            self.ui_tx.clone(),
            self.use_real_devices,
        );
        self.video_pub = Some(video_pub);
        let level = if self.use_real_devices && !is_camera {
            LogLevel::Warn
        } else {
            LogLevel::Info
        };
        self.log(level, vmsg);

        let (audio_pub, is_mic, amsg) =
            AudioPublisher::start(self.audio_tx.clone(), self.use_real_devices);
        self.audio_pub = Some(audio_pub);
        let level = if self.use_real_devices && !is_mic {
            LogLevel::Warn
        } else {
            LogLevel::Info
        };
        self.log(level, amsg);
        if !is_mic {
            self.log(
                LogLevel::Info,
                "未启用真实麦克风: 使用合成 440Hz 正弦波".to_string(),
            );
        }
    }

    // ── 订阅 ─────────────────────────────────────────────────

    fn subscribe(&mut self, publisher_id: String) {
        if self.sig.is_none() || self.pub_session.is_none() {
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

        let pc_id = format!("sub-{publisher_id}");
        let (mut rtc, udp, cand) = match self.new_transport() {
            Ok(t) => t,
            Err(e) => {
                self.log(LogLevel::Error, format!("创建订阅连接失败: {e}"));
                return;
            }
        };

        // recvonly: 音频 + 视频 (mid 由 str0m 分配, 作为 subscriber_mid 上报)。
        let (a_mid, v_mid, offer_sdp, pending) = {
            let mut api = rtc.sdp_api();
            let a = api.add_media(MediaKind::Audio, Direction::RecvOnly, None, None, None);
            let v = api.add_media(MediaKind::Video, Direction::RecvOnly, None, None, None);
            let (offer, pending) = match api.apply() {
                Some(x) => x,
                None => {
                    self.log(LogLevel::Error, "生成订阅 offer 失败".to_string());
                    return;
                }
            };
            (a, v, offer.to_sdp_string(), pending)
        };

        self.send(ClientMessage::Join {
            pc_id: pc_id.clone(),
            room_id: self.room_id.clone(),
            participant_id: format!("{}-recv-{}", self.participant_id, publisher_id),
            sdp: offer_sdp,
            role: "subscriber".to_string(),
        });
        self.send_ice(&pc_id, &cand);

        self.subs.insert(
            publisher_id.clone(),
            PcSession {
                pc_id,
                rtc,
                udp,
                pending: Some(pending),
                next_timeout: Instant::now(),
                audio_mid: None,
                video_mid: None,
                audio_pt: None,
                video_pt: None,
                publisher_id: Some(publisher_id.clone()),
                sub_audio_mid: Some(a_mid),
                sub_video_mid: Some(v_mid),
                negotiated: false,
                sent_tracks: Vec::new(),
                status: SubStatus::Connecting,
                has_video: false,
                video_decoder: None,
                audio_player: None,
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
        let Some(mut entry) = self.subs.remove(publisher_id) else {
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
        // 显式停止解码器, 释放 ffmpeg 子进程
        if let Some(mut d) = entry.video_decoder.take() {
            d.stop();
        }
        if let Some(mut p) = entry.audio_player.take() {
            p.stop();
        }
        drop(entry); // drop Rtc → 关闭该 PeerConnection
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
                pc_id, candidate, ..
            } => {
                let cand = match Candidate::from_sdp_string(&candidate) {
                    Ok(c) => c,
                    Err(_) => return,
                };
                if pc_id == "pub" {
                    if let Some(s) = self.pub_session.as_mut() {
                        s.rtc.add_remote_candidate(cand);
                    }
                } else if let Some((_, s)) = self.subs.iter_mut().find(|(_, s)| s.pc_id == pc_id) {
                    s.rtc.add_remote_candidate(cand);
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
                self.log(LogLevel::Info, format!("参与者轨道更新: {participant_id}"));
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
        let res: Result<(), String> = (|| {
            let s = self.pub_session.as_mut().ok_or("发布会话不存在")?;
            let answer = SdpAnswer::from_sdp_string(&sdp).map_err(|e| e.to_string())?;
            let pending = s.pending.take().ok_or("缺少 pending offer")?;
            s.rtc
                .sdp_api()
                .accept_answer(pending, answer)
                .map_err(|e| e.to_string())?;
            for c in &candidates {
                if let Ok(cand) = Candidate::from_sdp_string(c) {
                    s.rtc.add_remote_candidate(cand);
                }
            }
            s.audio_pt = s.audio_pt.or_else(|| local_pt(&s.rtc, Codec::Opus));
            s.video_pt = s.video_pt.or_else(|| local_pt(&s.rtc, Codec::Vp8));
            Ok(())
        })();
        match res {
            Ok(()) => self.log(LogLevel::Success, "发布 SDP 协商完成, 已添加服务器候选"),
            Err(e) => self.log(LogLevel::Error, format!("发布 SDP 协商失败: {e}")),
        }
    }

    fn on_sub_answer(&mut self, publisher_id: String, sdp: String, candidates: Vec<String>) {
        let res: Result<(), String> = (|| {
            let s = self.subs.get_mut(&publisher_id).ok_or("订阅会话不存在")?;
            let answer = SdpAnswer::from_sdp_string(&sdp).map_err(|e| e.to_string())?;
            let pending = s.pending.take().ok_or("缺少 pending offer")?;
            s.rtc
                .sdp_api()
                .accept_answer(pending, answer)
                .map_err(|e| e.to_string())?;
            for c in &candidates {
                if let Ok(cand) = Candidate::from_sdp_string(c) {
                    s.rtc.add_remote_candidate(cand);
                }
            }
            s.negotiated = true;
            Ok(())
        })();

        if let Err(e) = res {
            if let Some(s) = self.subs.get_mut(&publisher_id) {
                s.status = SubStatus::Error;
            }
            let _ = self.ui_tx.send(UiEvent::SubscriptionUpdate {
                id: publisher_id.clone(),
                status: SubStatus::Error,
                has_video: false,
            });
            self.log(
                LogLevel::Error,
                format!("订阅 {publisher_id} 远端 SDP 设置失败: {e}"),
            );
            return;
        }
        self.maybe_subscribe_tracks(&publisher_id);
    }

    /// 依据已知的发布者轨道发送 `subscribe`。
    ///
    /// `track_mid` 为发布者 mid, `subscriber_mid` 为本端 recvonly mid;
    /// 仅在 `sdp_answer` 之后发送, 且只补发尚未发送的 mid。
    fn maybe_subscribe_tracks(&mut self, publisher_id: &str) {
        let Some(tracks) = self.publisher_tracks.get(publisher_id).cloned() else {
            return;
        };

        let msgs: Vec<(String, String)> = {
            let Some(s) = self.subs.get_mut(publisher_id) else {
                return;
            };
            if !s.negotiated {
                return;
            }
            // 桌面端只呈现一路音频 + 一路视频: 每种 kind 取第一条。
            let mut want: Vec<(String, Option<Mid>)> = Vec::new();
            if let Some(a) = tracks.iter().find(|t| t.kind == "audio") {
                want.push((a.mid.clone(), s.sub_audio_mid));
            }
            if let Some(v) = tracks.iter().find(|t| t.kind == "video") {
                want.push((v.mid.clone(), s.sub_video_mid));
            }

            let pc_id = s.pc_id.clone();
            let mut msgs = Vec::new();
            for (track_mid, own_mid) in want {
                if s.sent_tracks.iter().any(|m| m == &track_mid) {
                    continue;
                }
                let Some(own_mid) = own_mid else {
                    continue;
                };
                msgs.push((format!("{pc_id}\u{0}{track_mid}"), own_mid.to_string()));
                s.sent_tracks.push(track_mid);
            }
            msgs
        };

        if msgs.is_empty() {
            return;
        }
        let n = msgs.len();
        for (key, subscriber_mid) in msgs {
            let (pc_id, track_mid) = key.split_once('\u{0}').unwrap_or((key.as_str(), ""));
            self.send(ClientMessage::Subscribe {
                pc_id: pc_id.to_string(),
                publisher_id: publisher_id.to_string(),
                track_mid: track_mid.to_string(),
                subscriber_mid,
            });
        }
        self.log(
            LogLevel::Success,
            format!("订阅 {publisher_id} SDP 完成, 已发送 {n} 条 subscribe"),
        );
    }

    // ── 媒体写入 ──────────────────────────────────────────────

    fn pump_encoded_media(&mut self) {
        let video_rx = self.video_rx.clone();
        let audio_rx = self.audio_rx.clone();

        let Some(s) = self.pub_session.as_mut() else {
            // 不在通话中: 丢弃生产者残留帧, 避免堆积。
            while video_rx.try_recv().is_ok() {}
            while audio_rx.try_recv().is_ok() {}
            return;
        };

        let now = Instant::now();

        while let Ok(f) = video_rx.try_recv() {
            let pt = s.video_pt.or_else(|| local_pt(&s.rtc, Codec::Vp8));
            s.video_pt = pt;
            if let (Some(mid), Some(pt)) = (s.video_mid, pt) {
                if let Some(w) = s.rtc.writer(mid) {
                    let _ = w.write(pt, now, MediaTime::from_micros(f.timestamp_us), f.data);
                }
            }
        }

        while let Ok(a) = audio_rx.try_recv() {
            let pt = s.audio_pt.or_else(|| local_pt(&s.rtc, Codec::Opus));
            s.audio_pt = pt;
            if let (Some(mid), Some(pt)) = (s.audio_mid, pt) {
                if let Some(w) = s.rtc.writer(mid) {
                    let _ = w.write(pt, now, MediaTime::from_micros(a.timestamp_us), a.data);
                }
            }
        }
    }

    // ── 媒体控制 ──────────────────────────────────────────────

    fn toggle_audio(&mut self, enabled: bool) {
        self.audio_enabled = enabled;
        if let Some(p) = self.audio_pub.as_ref() {
            if enabled {
                p.resume();
            } else {
                p.pause();
            }
        }
    }

    fn toggle_video(&mut self, enabled: bool) {
        self.video_enabled = enabled;
        if let Some(p) = self.video_pub.as_ref() {
            if enabled {
                p.resume();
            } else {
                p.pause();
            }
        }
    }

    // ── 工具 ─────────────────────────────────────────────────

    fn new_transport(&self) -> Result<(Rtc, UdpSocket, Candidate), String> {
        let udp = UdpSocket::bind("0.0.0.0:0").map_err(|e| format!("绑定 UDP 失败: {e}"))?;
        udp.set_nonblocking(true).map_err(|e| e.to_string())?;
        let port = udp.local_addr().map_err(|e| e.to_string())?.port();
        let advertised = SocketAddr::new(self.advertise_ip, port);
        let mut rtc = Rtc::builder().build(Instant::now());
        let cand =
            Candidate::host(advertised, Protocol::Udp).map_err(|e| format!("创建候选失败: {e}"))?;
        rtc.add_local_candidate(cand.clone());
        Ok((rtc, udp, cand))
    }

    fn is_self_or_shadow(&self, participant_id: &str) -> bool {
        participant_id == self.participant_id
            || participant_id.starts_with(&format!("{}-recv-", self.participant_id))
    }

    fn send(&self, msg: ClientMessage) {
        if let Some(sig) = &self.sig {
            sig.send(msg);
        }
    }

    fn send_ice(&self, pc_id: &str, cand: &Candidate) {
        self.send(ClientMessage::IceCandidate {
            pc_id: pc_id.to_string(),
            candidate: cand.to_sdp_string(),
            sdp_mid: Some("0".to_string()),
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

// ── 单会话 IO 泵 ──────────────────────────────────────────────

/// 收 UDP → 送入 str0m; 到点 → Timeout; 然后 drain 输出 (发包 + 事件)。
fn pump_session(s: &mut PcSession, now: Instant, ui_tx: &Sender<UiEvent>) {
    let mut buf = [0u8; 2048];
    let local = s
        .udp
        .local_addr()
        .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));

    loop {
        match s.udp.recv_from(&mut buf) {
            Ok((n, src)) => {
                if let Ok(recv) = Receive::new(Protocol::Udp, src, local, &buf[..n]) {
                    let _ = s.rtc.handle_input(Input::Receive(now, recv));
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        }
    }

    if now >= s.next_timeout {
        let _ = s.rtc.handle_input(Input::Timeout(now));
    }

    loop {
        match s.rtc.poll_output() {
            Ok(Output::Transmit(t)) => {
                let _ = s.udp.send_to(&t.contents, t.destination);
            }
            Ok(Output::Event(ev)) => handle_rtc_event(s, ev, ui_tx),
            Ok(Output::Timeout(t)) => {
                s.next_timeout = t;
                break;
            }
            Err(_) => break,
        }
    }
}

/// 处理 str0m 事件: 连接状态 → UI, 远端媒体 → 解码器。
fn handle_rtc_event(s: &mut PcSession, ev: Event, ui_tx: &Sender<UiEvent>) {
    match ev {
        Event::IceConnectionStateChange(state) => {
            let _ = ui_tx.send(UiEvent::ConnectionState {
                pc_id: s.pc_id.clone(),
                state: format!("{state:?}"),
            });
        }
        Event::MediaAdded(media) => {
            let _ = ui_tx.send(UiEvent::Log {
                level: LogLevel::Info,
                msg: format!(
                    "[{}] 媒体协商: mid={:?} kind={:?}",
                    s.pc_id, media.mid, media.kind
                ),
            });
        }
        Event::MediaData(md) => {
            let codec = md.params.spec().codec;
            match codec {
                Codec::Vp8 => {
                    let pid = s.publisher_id.clone().unwrap_or_default();
                    if s.video_decoder.is_none() {
                        match RemoteVideoDecoder::new(pid.clone(), ui_tx.clone()) {
                            Ok(d) => {
                                s.video_decoder = Some(d);
                                s.has_video = true;
                                let _ = ui_tx.send(UiEvent::SubscriptionUpdate {
                                    id: pid.clone(),
                                    status: SubStatus::Subscribed,
                                    has_video: true,
                                });
                                let _ = ui_tx.send(UiEvent::Log {
                                    level: LogLevel::Success,
                                    msg: format!("收到 {pid} 的 video 轨道"),
                                });
                            }
                            Err(e) => {
                                let _ = ui_tx.send(UiEvent::Log {
                                    level: LogLevel::Error,
                                    msg: format!("创建视频解码器失败: {e}"),
                                });
                            }
                        }
                    }
                    if let Some(d) = s.video_decoder.as_mut() {
                        d.push(&md.data);
                    }
                }
                Codec::Opus => {
                    if s.audio_player.is_none() {
                        match RemoteAudioPlayer::new() {
                            Ok(p) => {
                                s.audio_player = Some(p);
                                let pid = s.publisher_id.clone().unwrap_or_default();
                                let _ = ui_tx.send(UiEvent::Log {
                                    level: LogLevel::Success,
                                    msg: format!("收到 {pid} 的 audio 轨道"),
                                });
                            }
                            Err(e) => {
                                let _ = ui_tx.send(UiEvent::Log {
                                    level: LogLevel::Warn,
                                    msg: format!("音频播放不可用 (仅接收不解码): {e}"),
                                });
                            }
                        }
                    }
                    if let Some(p) = s.audio_player.as_mut() {
                        p.push(&md.data);
                    }
                }
                _ => {}
            }
        }
        Event::KeyframeRequest(_) => {
            // 订阅者请求关键帧 — 本客户端仅消费媒体, 忽略。
        }
        _ => {}
    }
}

// ── 辅助函数 ──────────────────────────────────────────────────

/// 在本地编解码配置中查找指定 codec 的 PT。
fn local_pt(rtc: &Rtc, want: Codec) -> Option<Pt> {
    rtc.codec_config()
        .iter()
        .find(|p| p.spec().codec == want)
        .map(|p| p.pt())
}

/// 解析信令地址, 推导本机对外可达的 IP (用于 ICE host 候选)。
fn resolve_advertise_ip(server_url: &str) -> IpAddr {
    if let Ok(v) = std::env::var("SFU_ADVERTISE_IP") {
        if let Ok(ip) = v.parse() {
            return ip;
        }
    }

    let host_port = server_url
        .strip_prefix("ws://")
        .or_else(|| server_url.strip_prefix("wss://"))
        .unwrap_or(server_url);
    let host_port = host_port.split('/').next().unwrap_or(host_port);

    let target: Option<SocketAddr> = if let Some((h, p)) = host_port.rsplit_once(':') {
        let port = p.parse::<u16>().unwrap_or(5174);
        if let Ok(ip) = h.parse::<IpAddr>() {
            Some(SocketAddr::new(ip, port))
        } else {
            use std::net::ToSocketAddrs;
            (h, port)
                .to_socket_addrs()
                .ok()
                .and_then(|mut it| it.next())
        }
    } else if let Ok(ip) = host_port.parse::<IpAddr>() {
        Some(SocketAddr::new(ip, 5174))
    } else {
        use std::net::ToSocketAddrs;
        (host_port, 5174u16)
            .to_socket_addrs()
            .ok()
            .and_then(|mut it| it.next())
    };

    if let Some(target) = target {
        if let Ok(sock) = UdpSocket::bind("0.0.0.0:0") {
            if sock.connect(target).is_ok() {
                if let Ok(local) = sock.local_addr() {
                    return local.ip();
                }
            }
        }
    }
    IpAddr::V4(Ipv4Addr::LOCALHOST)
}
