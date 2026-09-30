//! RoomChat 桌面客户端的 GPUI 视图。
//!
//! 与 Slint 版本 (`RoomChatDesktopSlint`) 的结构一一对应:
//!   · 加入页: 信令地址 / 房间号 / 昵称 / 是否使用真实设备 + 加入按钮
//!   · 通话页: 顶栏 (房间/人数/连接状态) + 视频网格 + 参与者/日志侧栏 + 控制栏
//!
//! 线程与数据流:
//!   · WebRTC worker 线程 (sfu.rs) 通过 [`UiSender`](crate::ui::UiSender) 投递 UiEvent;
//!   · 本视图在 GPUI 执行器里 spawn 一个异步任务 `recv().await` 排空事件;
//!   · worker 绝不触碰任何 GPUI 对象, 主线程也只在事件处理里更新视图状态。

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::Sender;
use gpui::prelude::*;
use gpui::{
    div, img, px, rgb, rgba, AnyElement, App, ClickEvent, Context, Entity, Focusable as _,
    ObjectFit, RenderImage, SharedString, Window,
};
use gpui_kit::component::button::{Button, ButtonVariants as _};
use gpui_kit::component::checkbox::Checkbox;
use gpui_kit::component::input::{Input, InputEvent, InputState};
use gpui_kit::component::{h_flex, v_flex, Disableable as _};

use crate::sfu::{self, ClientCommand};
use crate::ui::{LogLevel, SubStatus, UiEvent, UiSender};

// ── 主题色 (与 Slint 版本一致, 深色) ─────────────────────────
const BG: u32 = 0x16181d;
const CARD: u32 = 0x22252c;
const PANEL: u32 = 0x101216;
const SIDEBAR: u32 = 0x1a1c22;
const BAR: u32 = 0x1e2128;
const TEXT: u32 = 0xffffff;
const TEXT_DIM: u32 = 0x9aa0a6;
const LABEL: u32 = 0xc0c4ca;
const MUTED: u32 = 0x6b7076;
const SUBSCRIBED: u32 = 0x2e7d32;
const CONNECTING: u32 = 0xf9a825;
const ERROR: u32 = 0xe53935;
const IDLE: u32 = 0x5f6368;
const LOG_INFO: u32 = 0x8ab4f8;
const LOG_SUCCESS: u32 = 0x66bb6a;
const LOG_WARN: u32 = 0xf9a825;
const LOG_ERROR: u32 = 0xef5350;

// ── 视图数据 ─────────────────────────────────────────────────

/// 远端参与者在网格/侧栏中的展示状态。
#[derive(Clone)]
struct ParticipantView {
    id: SharedString,
    status: SubStatus,
    has_video: bool,
    frame: Option<Arc<RenderImage>>,
}

#[derive(Clone)]
struct LogView {
    time: SharedString,
    msg: SharedString,
    level: LogLevel,
}

/// 持有 worker 命令发送端与线程句柄, `Drop` 时优雅退出。
struct Worker {
    cmd_tx: Sender<ClientCommand>,
    handle: Option<JoinHandle<()>>,
}

impl Worker {
    fn send(&self, cmd: ClientCommand) {
        let _ = self.cmd_tx.send(cmd);
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(ClientCommand::Shutdown);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub struct RoomChatApp {
    // 声明顺序决定 Drop 顺序: 先取消事件泵与订阅, 再关闭 worker 线程, 避免
    // worker 在退出时投递的事件回调到正在销毁的视图。
    _pump: gpui::Task<()>,
    _subscriptions: Vec<gpui::Subscription>,

    worker: Worker,
    cmd_tx: Sender<ClientCommand>,

    // 顶层状态
    in_call: bool,
    joining: bool,
    error_text: SharedString,
    room_id: SharedString,
    participant_count: i32,
    connection_state: SharedString,
    audio_enabled: bool,
    video_enabled: bool,

    // 媒体
    local_frame: Option<Arc<RenderImage>>,
    participants: Vec<ParticipantView>,
    logs: Vec<LogView>,

    // 加入页输入
    server_input: Entity<InputState>,
    room_input: Entity<InputState>,
    nickname_input: Entity<InputState>,
    use_real_devices: bool,
}

impl RoomChatApp {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let (ui_tx, ui_rx) = UiSender::channel();
        let (cmd_tx, handle) = sfu::spawn_worker(ui_tx);

        let server_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("ws://host:5174")
                .default_value("ws://127.0.0.1:5174")
        });
        let room_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("room-1")
                .default_value("room-1")
        });
        let nickname_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder("desktop-user")
                .default_value("desktop-user")
        });

        // worker 事件泵: 在 GPUI 执行器里 await 事件通道。
        let pump = cx.spawn(async move |this, cx| {
            while let Ok(event) = ui_rx.recv().await {
                if this
                    .update(cx, |app, cx| app.handle_event(event, cx))
                    .is_err()
                {
                    break; // 视图已销毁
                }
            }
        });

        // 在任一输入框按回车即触发加入。
        let mut subscriptions = Vec::new();
        for input in [&server_input, &room_input, &nickname_input] {
            subscriptions.push(cx.subscribe(input, |app, _input, event: &InputEvent, cx| {
                if matches!(event, InputEvent::PressEnter { .. }) {
                    app.start_join(cx);
                }
            }));
        }

        // 让第一个输入框获得焦点。
        let focus = server_input.read(cx).focus_handle(cx);
        window.focus(&focus, cx);

        Self {
            _pump: pump,
            _subscriptions: subscriptions,
            worker: Worker {
                cmd_tx: cmd_tx.clone(),
                handle: Some(handle),
            },
            cmd_tx,
            in_call: false,
            joining: false,
            error_text: SharedString::default(),
            room_id: SharedString::default(),
            participant_count: 1,
            connection_state: "未连接".into(),
            audio_enabled: true,
            video_enabled: true,
            local_frame: None,
            participants: Vec::new(),
            logs: Vec::new(),
            server_input,
            room_input,
            nickname_input,
            use_real_devices: true,
        }
    }

    // ── worker 事件处理 (始终在主线程) ───────────────────────

    fn handle_event(&mut self, event: UiEvent, cx: &mut Context<Self>) {
        match event {
            UiEvent::Log { level, msg } => {
                self.logs.push(LogView {
                    time: now_hms().into(),
                    msg: msg.into(),
                    level,
                });
                if self.logs.len() > 300 {
                    let excess = self.logs.len() - 300;
                    self.logs.drain(0..excess);
                }
            }
            UiEvent::Joined { room_id } => {
                self.in_call = true;
                self.joining = false;
                self.room_id = room_id.into();
                self.participant_count = self.participants.len() as i32 + 1;
            }
            UiEvent::Left => {
                self.in_call = false;
                self.joining = false;
                self.connection_state = "未连接".into();
                self.participants.clear();
                self.local_frame = None;
                self.participant_count = 1;
            }
            UiEvent::ParticipantJoined { id } => {
                if self.participants.iter().all(|p| p.id != id.as_str()) {
                    self.participants.push(ParticipantView {
                        id: id.into(),
                        status: SubStatus::Idle,
                        has_video: false,
                        frame: None,
                    });
                    self.participant_count = self.participants.len() as i32 + 1;
                }
            }
            UiEvent::ParticipantLeft { id } => {
                self.participants.retain(|p| p.id != id.as_str());
                self.participant_count = self.participants.len() as i32 + 1;
            }
            UiEvent::SubscriptionUpdate {
                id,
                status,
                has_video,
            } => match self.participants.iter_mut().find(|p| p.id == id.as_str()) {
                Some(p) => {
                    p.status = status;
                    p.has_video = has_video;
                }
                None => {
                    self.participants.push(ParticipantView {
                        id: id.into(),
                        status,
                        has_video,
                        frame: None,
                    });
                    self.participant_count = self.participants.len() as i32 + 1;
                }
            },
            UiEvent::LocalVideo {
                bgra,
                width,
                height,
            } => {
                self.local_frame = frame_to_image(bgra, width, height);
            }
            UiEvent::RemoteVideo {
                id,
                bgra,
                width,
                height,
            } => {
                let image = frame_to_image(bgra, width, height);
                if let Some(p) = self.participants.iter_mut().find(|p| p.id == id.as_str()) {
                    p.frame = image;
                    p.has_video = true;
                }
            }
            UiEvent::ConnectionState { pc_id, state } => {
                self.connection_state = format!("{pc_id}: {state}").into();
            }
            UiEvent::Error { msg } => {
                self.joining = false;
                self.error_text = msg.into();
            }
        }
        cx.notify();
    }

    // ── 交互回调 ─────────────────────────────────────────────

    fn start_join(&mut self, cx: &mut Context<Self>) {
        let server_url = self.server_input.read(cx).value().trim().to_string();
        let room_id = self.room_input.read(cx).value().trim().to_string();
        let nickname = self.nickname_input.read(cx).value().trim().to_string();

        if nickname.is_empty() {
            self.error_text = "请输入昵称".into();
            cx.notify();
            return;
        }
        if room_id.is_empty() {
            self.error_text = "请输入房间号".into();
            cx.notify();
            return;
        }

        self.error_text = SharedString::default();
        self.joining = true;
        self.worker.send(ClientCommand::Join {
            server_url,
            room_id,
            participant_id: nickname,
            use_real_devices: self.use_real_devices,
        });
        cx.notify();
    }

    fn on_join_click(&mut self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.start_join(cx);
    }

    fn on_leave_click(&mut self, _: &ClickEvent, _: &mut Window, _: &mut Context<Self>) {
        self.worker.send(ClientCommand::Leave);
    }

    fn on_toggle_real_devices(&mut self, checked: &bool, _: &mut Window, cx: &mut Context<Self>) {
        self.use_real_devices = *checked;
        cx.notify();
    }

    fn on_toggle_audio(&mut self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.audio_enabled = !self.audio_enabled;
        self.worker.send(ClientCommand::ToggleAudio {
            enabled: self.audio_enabled,
        });
        cx.notify();
    }

    fn on_toggle_video(&mut self, _: &ClickEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.video_enabled = !self.video_enabled;
        self.worker.send(ClientCommand::ToggleVideo {
            enabled: self.video_enabled,
        });
        cx.notify();
    }

    // ── 渲染 ─────────────────────────────────────────────────

    fn render_join(&self, cx: &mut Context<Self>) -> AnyElement {
        let card = v_flex()
            .w(px(440.))
            .bg(rgb(CARD))
            .rounded_xl()
            .p_6()
            .gap_2()
            .child(
                div()
                    .text_size(px(22.))
                    .text_color(rgb(TEXT))
                    .child("WebRTC SFU 桌面客户端"),
            )
            .child(
                div()
                    .text_size(px(13.))
                    .text_color(rgb(TEXT_DIM))
                    .child("多人视频通话 · GPUI + gpui-kit + reactor-webrtc"),
            )
            .child(div().h(px(8.)).w_full())
            .child(field("信令服务器", Input::new(&self.server_input).cleanable(true)))
            .child(field("房间号", Input::new(&self.room_input).cleanable(true)))
            .child(field("昵称", Input::new(&self.nickname_input).cleanable(true)))
            .child(
                Checkbox::new("use-real-devices")
                    .label("使用真实摄像头和麦克风 (需系统授权)")
                    .checked(self.use_real_devices)
                    .on_change(cx.listener(Self::on_toggle_real_devices)),
            )
            .child(div().h(px(4.)).w_full())
            .child(
                Button::new("join-room")
                    .primary()
                    .label(if self.joining { "加入中..." } else { "加入房间" })
                    .disabled(self.joining)
                    .w_full()
                    .on_click(cx.listener(Self::on_join_click)),
            )
            .when(!self.error_text.is_empty(), |this| {
                this.child(
                    div()
                        .text_size(px(13.))
                        .text_color(rgb(LOG_ERROR))
                        .child(self.error_text.clone()),
                )
            })
            .child(
                div()
                    .text_size(px(11.))
                    .text_color(rgb(MUTED))
                    .child("媒体端口 UDP 5175+ · 信令端口 WS 5174"),
            );

        v_flex()
            .size_full()
            .bg(rgb(BG))
            .justify_center()
            .items_center()
            .p_6()
            .child(card)
            .into_any_element()
    }

    fn render_call(&self, cx: &mut Context<Self>) -> AnyElement {
        // ── 顶栏 ──
        let top_bar = h_flex()
            .h(px(54.))
            .w_full()
            .bg(rgb(BAR))
            .px_4()
            .gap_3()
            .items_center()
            .child(
                div()
                    .flex_1()
                    .text_size(px(16.))
                    .text_color(rgb(TEXT))
                    .truncate()
                    .child(format!("房间 {} · {} 人", self.room_id, self.participant_count)),
            )
            .child(
                div()
                    .text_size(px(12.))
                    .text_color(rgb(TEXT_DIM))
                    .child(format!("连接: {}", self.connection_state)),
            );

        // ── 本地视频块 ──
        let local_subtitle = if self.video_enabled {
            "等待本地画面..."
        } else {
            "摄像头已关闭"
        };
        let local_tile = video_tile(
            "我 (本地)".into(),
            local_subtitle.into(),
            self.local_frame.clone(),
            self.video_enabled,
        );

        // ── 远端视频块 ──
        let remote_tiles: Vec<AnyElement> = self
            .participants
            .iter()
            .map(|p| {
                let subtitle = match p.status {
                    SubStatus::Connecting => "连接中...".to_string(),
                    SubStatus::Error => "连接失败".to_string(),
                    _ if p.has_video => "等待视频...".to_string(),
                    _ => "未收到视频".to_string(),
                };
                video_tile(
                    p.id.clone(),
                    subtitle.into(),
                    p.frame.clone(),
                    p.has_video,
                )
            })
            .collect();

        let video_area = div()
            .id("video-grid")
            .flex_1()
            .h_full()
            .bg(rgb(PANEL))
            .overflow_y_scroll()
            .child(
                div()
                    .flex()
                    .flex_row()
                    .flex_wrap()
                    .gap_3()
                    .p_4()
                    .child(local_tile)
                    .children(remote_tiles),
            );

        // ── 侧栏 ──
        let participant_rows: Vec<AnyElement> = self
            .participants
            .iter()
            .map(|p| participant_row(p.id.clone(), p.status, self.cmd_tx.clone()))
            .collect();

        let log_lines: Vec<AnyElement> = self
            .logs
            .iter()
            .map(|l| {
                div()
                    .text_size(px(12.))
                    .text_color(rgb(log_color(l.level)))
                    .truncate()
                    .child(format!("{}  {}", l.time, l.msg))
                    .into_any_element()
            })
            .collect();

        let sidebar = v_flex()
            .w(px(300.))
            .h_full()
            .bg(rgb(SIDEBAR))
            .p_3()
            .gap_2()
            .child(div().text_size(px(15.)).text_color(rgb(TEXT)).child("参与者"))
            .child(
                h_flex()
                    .gap_2()
                    .items_center()
                    .child(status_dot(SubStatus::Subscribed))
                    .child(
                        div()
                            .flex_1()
                            .text_size(px(13.))
                            .text_color(rgb(0xd0d3d8))
                            .child("我 (本地)"),
                    ),
            )
            .child(
                div()
                    .id("participant-list")
                    .w_full()
                    .max_h(px(220.))
                    .overflow_y_scroll()
                    .child(v_flex().w_full().gap_1().children(participant_rows)),
            )
            .child(div().h(px(4.)).w_full())
            .child(div().text_size(px(15.)).text_color(rgb(TEXT)).child("事件日志"))
            .child(
                div()
                    .id("event-logs")
                    .flex_1()
                    .w_full()
                    .overflow_y_scroll()
                    .child(v_flex().w_full().gap_1().children(log_lines)),
            );

        let body = h_flex().w_full().flex_1().child(video_area).child(sidebar);

        // ── 控制栏 ──
        let control_bar = h_flex()
            .h(px(68.))
            .w_full()
            .bg(rgb(BAR))
            .justify_center()
            .items_center()
            .gap_3()
            .child(
                Button::new("toggle-audio")
                    .label(if self.audio_enabled {
                        "麦克风 开"
                    } else {
                        "麦克风 关"
                    })
                    .on_click(cx.listener(Self::on_toggle_audio)),
            )
            .child(
                Button::new("toggle-video")
                    .label(if self.video_enabled {
                        "摄像头 开"
                    } else {
                        "摄像头 关"
                    })
                    .on_click(cx.listener(Self::on_toggle_video)),
            )
            .child(
                Button::new("leave-call")
                    .danger()
                    .label("挂断并离开")
                    .on_click(cx.listener(Self::on_leave_click)),
            );

        v_flex()
            .size_full()
            .bg(rgb(BG))
            .child(top_bar)
            .child(body)
            .child(control_bar)
            .into_any_element()
    }
}

impl Render for RoomChatApp {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.in_call {
            self.render_call(cx)
        } else {
            self.render_join(cx)
        }
    }
}

// ── 辅助函数 ─────────────────────────────────────────────────

/// BGRA 原始像素 → GPUI 的 [`RenderImage`]。
///
/// GPUI 的 `RenderImage` 内部存储的就是 BGRA, 因此这里无需通道交换。
fn frame_to_image(bgra: Vec<u8>, width: u32, height: u32) -> Option<Arc<RenderImage>> {
    let buffer: image::RgbaImage = image::ImageBuffer::from_raw(width, height, bgra)?;
    let frame = image::Frame::new(buffer);
    Some(Arc::new(RenderImage::new(vec![frame])))
}

fn now_hms() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

fn status_color(status: SubStatus) -> u32 {
    match status {
        SubStatus::Subscribed => SUBSCRIBED,
        SubStatus::Connecting => CONNECTING,
        SubStatus::Error => ERROR,
        SubStatus::Idle => IDLE,
    }
}

fn log_color(level: LogLevel) -> u32 {
    match level {
        LogLevel::Info => LOG_INFO,
        LogLevel::Success => LOG_SUCCESS,
        LogLevel::Warn => LOG_WARN,
        LogLevel::Error => LOG_ERROR,
    }
}

fn status_dot(status: SubStatus) -> AnyElement {
    div()
        .w(px(10.))
        .h(px(10.))
        .rounded_full()
        .bg(rgb(status_color(status)))
        .into_any_element()
}

/// 带标签的输入行。
fn field(label: &'static str, input: impl IntoElement) -> AnyElement {
    v_flex()
        .w_full()
        .gap_1()
        .child(
            div()
                .text_size(px(13.))
                .text_color(rgb(LABEL))
                .child(label),
        )
        .child(input)
        .into_any_element()
}

/// 视频块: 有帧就显示画面, 否则显示占位文案; 底部叠加标题。
fn video_tile(
    title: SharedString,
    subtitle: SharedString,
    frame: Option<Arc<RenderImage>>,
    has_video: bool,
) -> AnyElement {
    let media = match frame {
        Some(image) if has_video => img(image)
            .size_full()
            .object_fit(ObjectFit::Contain)
            .into_any_element(),
        _ => div()
            .size_full()
            .bg(rgb(0x202329))
            .flex()
            .items_center()
            .justify_center()
            .child(
                div()
                    .text_size(px(13.))
                    .text_color(rgb(0xb0b3b8))
                    .child(subtitle),
            )
            .into_any_element(),
    };

    div()
        .relative()
        .w(px(440.))
        .h(px(300.))
        .rounded_lg()
        .overflow_hidden()
        .bg(rgb(0x000000))
        .child(media)
        .child(
            div()
                .absolute()
                .bottom_0()
                .left_0()
                .w_full()
                .h(px(30.))
                .px_3()
                .bg(rgba(0x000000cc))
                .flex()
                .items_center()
                .child(
                    div()
                        .w_full()
                        .text_size(px(13.))
                        .text_color(rgb(TEXT))
                        .truncate()
                        .child(title),
                ),
        )
        .into_any_element()
}

/// 侧栏中的远端参与者行: 状态点 + 昵称 + 订阅/取消按钮。
fn participant_row(
    id: SharedString,
    status: SubStatus,
    cmd_tx: Sender<ClientCommand>,
) -> AnyElement {
    let subscribed = status == SubStatus::Subscribed;
    let connecting = status == SubStatus::Connecting;
    let label = if subscribed {
        "取消"
    } else if connecting {
        "连接中..."
    } else {
        "订阅"
    };
    let button_id = format!("participant-button-{id}");
    let target = id.clone();
    let cmd = cmd_tx;
    let on_click = move |_: &ClickEvent, _: &mut Window, _: &mut App| {
        let _ = cmd.send(if subscribed {
            ClientCommand::Unsubscribe {
                publisher_id: target.to_string(),
            }
        } else {
            ClientCommand::Subscribe {
                publisher_id: target.to_string(),
            }
        });
    };

    h_flex()
        .w_full()
        .h(px(40.))
        .gap_2()
        .px_1()
        .items_center()
        .child(status_dot(status))
        .child(
            div()
                .flex_1()
                .text_size(px(13.))
                .text_color(rgb(0xe6e6e6))
                .truncate()
                .child(id),
        )
        .child(
            Button::new(button_id)
                .label(label)
                .disabled(connecting)
                .on_click(on_click),
        )
        .into_any_element()
}

/// 让 `Entity` 在需要时也能当作 GPUI 上下文使用 (保留占位, 便于后续扩展)。
#[allow(dead_code)]
fn _assert_entity_is_view(_: &Entity<RoomChatApp>) {}
