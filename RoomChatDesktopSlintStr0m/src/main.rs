//! RoomChat 桌面客户端入口 (str0m + ffmpeg 版)。
//!
//! 与 `RoomChatDesktopSlint` 功能一致, 但 WebRTC 引擎由 reactor-webrtc
//! (libwebrtc) 换成纯 Rust 的 **str0m**, 媒体采集/编解码由 **ffmpeg** 完成。
//!
//! 线程模型:
//!   · 主线程: Slint 事件循环 + 16ms 定时器排空 worker 事件并刷新 UI 模型
//!   · sfu-client 线程: str0m (Sans-IO 事件循环) + 信令状态机
//!   · 媒体线程: ffmpeg 摄像头/麦克风采集与 VP8/Opus 编解码
//!
//! UI 与 worker 只通过 crossbeam channel 通信, worker 绝不直接触碰 Slint 对象。

mod media;
mod protocol;
mod sfu;
mod signaling;
mod ui;

slint::include_modules!();

use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::unbounded;
use slint::{
    ComponentHandle, Image, Model, ModelRc, Rgba8Pixel, SharedPixelBuffer, Timer, TimerMode,
    VecModel,
};

use sfu::ClientCommand;
use ui::{SubStatus, UiEvent};

/// RGBA 字节 → Slint 图像。
fn image_from_rgba(rgba: &[u8], width: u32, height: u32) -> Image {
    let mut buffer = SharedPixelBuffer::<Rgba8Pixel>::new(width, height);
    let bytes = buffer.make_mut_bytes();
    let n = bytes.len().min(rgba.len());
    bytes[..n].copy_from_slice(&rgba[..n]);
    Image::from_rgba8(buffer)
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

fn main() -> Result<(), slint::PlatformError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .try_init()
        .ok();

    // 权限占位 (真实授权由 ffmpeg 子进程首次打开设备时触发)
    media::init_camera_permission();

    let app = AppWindow::new()?;

    // ── UI 模型 ───────────────────────────────────────────────
    let participants: Rc<VecModel<Participant>> = Rc::new(VecModel::default());
    let logs: Rc<VecModel<LogEntry>> = Rc::new(VecModel::default());
    app.set_remote_participants(ModelRc::from(participants.clone()));
    app.set_logs(ModelRc::from(logs.clone()));
    app.set_local_image(Image::default());

    // ── 与 worker 的通道 ──────────────────────────────────────
    let (ui_tx, ui_rx) = unbounded::<UiEvent>();
    let (cmd_tx, worker_handle) = sfu::spawn_worker(ui_tx);

    // ── UI 回调 ──────────────────────────────────────────────
    app.on_join({
        let cmd = cmd_tx.clone();
        let weak = app.as_weak();
        move |server_url, room_id, nickname, use_real_devices| {
            let Some(app) = weak.upgrade() else {
                return;
            };
            let nickname = nickname.trim().to_string();
            let room_id = room_id.trim().to_string();
            if nickname.is_empty() {
                app.set_error_text("请输入昵称".into());
                return;
            }
            if room_id.is_empty() {
                app.set_error_text("请输入房间号".into());
                return;
            }
            app.set_error_text("".into());
            app.set_joining(true);
            let _ = cmd.send(ClientCommand::Join {
                server_url: server_url.to_string(),
                room_id,
                participant_id: nickname,
                use_real_devices,
            });
        }
    });

    app.on_leave({
        let cmd = cmd_tx.clone();
        move || {
            let _ = cmd.send(ClientCommand::Leave);
        }
    });

    app.on_subscribe({
        let cmd = cmd_tx.clone();
        move |id| {
            let _ = cmd.send(ClientCommand::Subscribe {
                publisher_id: id.to_string(),
            });
        }
    });

    app.on_unsubscribe({
        let cmd = cmd_tx.clone();
        move |id| {
            let _ = cmd.send(ClientCommand::Unsubscribe {
                publisher_id: id.to_string(),
            });
        }
    });

    app.on_toggle_audio({
        let cmd = cmd_tx.clone();
        let weak = app.as_weak();
        move |enabled| {
            if let Some(app) = weak.upgrade() {
                app.set_audio_enabled(enabled);
            }
            let _ = cmd.send(ClientCommand::ToggleAudio { enabled });
        }
    });

    app.on_toggle_video({
        let cmd = cmd_tx.clone();
        let weak = app.as_weak();
        move |enabled| {
            if let Some(app) = weak.upgrade() {
                app.set_video_enabled(enabled);
            }
            let _ = cmd.send(ClientCommand::ToggleVideo { enabled });
        }
    });

    // 窗口关闭时优雅退出 worker 并结束事件循环
    {
        let cmd = cmd_tx.clone();
        app.window().on_close_requested(move || {
            let _ = cmd.send(ClientCommand::Shutdown);
            let _ = slint::quit_event_loop();
            slint::CloseRequestResponse::HideWindow
        });
    }

    // ── 事件泵: 16ms 排空 worker 事件 ─────────────────────────
    let timer = Timer::default();
    {
        let weak = app.as_weak();
        let participants = participants.clone();
        let logs = logs.clone();
        timer.start(TimerMode::Repeated, Duration::from_millis(16), move || {
            let Some(app) = weak.upgrade() else {
                return;
            };
            while let Ok(ev) = ui_rx.try_recv() {
                handle_event(&app, &participants, &logs, ev);
            }
        });
    }

    // ── 运行 ─────────────────────────────────────────────────
    app.run()?;

    let _ = cmd_tx.send(ClientCommand::Shutdown);
    let _ = worker_handle.join();
    Ok(())
}

// ── 事件处理 (始终在主线程) ───────────────────────────────────

fn handle_event(
    app: &AppWindow,
    participants: &Rc<VecModel<Participant>>,
    logs: &Rc<VecModel<LogEntry>>,
    ev: UiEvent,
) {
    match ev {
        UiEvent::Log { level, msg } => {
            logs.push(LogEntry {
                time: now_hms().into(),
                msg: msg.into(),
                level: level.as_str().into(),
            });
            while logs.row_count() > 300 {
                logs.remove(0);
            }
        }

        UiEvent::Joined { room_id } => {
            app.set_in_call(true);
            app.set_joining(false);
            app.set_room_id(room_id.into());
            update_count(app, participants);
        }

        UiEvent::Left => {
            app.set_in_call(false);
            app.set_joining(false);
            app.set_connection_state("未连接".into());
            participants.clear();
            app.set_local_image(Image::default());
            app.set_participant_count(1);
        }

        UiEvent::ParticipantJoined { id } => {
            if find_participant(participants, &id).is_none() {
                participants.push(new_participant(&id, SubStatus::Idle, false));
                update_count(app, participants);
            }
        }

        UiEvent::ParticipantLeft { id } => {
            if let Some(i) = find_participant(participants, &id) {
                participants.remove(i);
                update_count(app, participants);
            }
        }

        UiEvent::SubscriptionUpdate {
            id,
            status,
            has_video,
        } => match find_participant(participants, &id) {
            Some(i) => {
                if let Some(mut p) = participants.row_data(i) {
                    p.status = status.as_str().into();
                    p.has_video = has_video;
                    participants.set_row_data(i, p);
                }
            }
            None => {
                participants.push(new_participant(&id, status, has_video));
                update_count(app, participants);
            }
        },

        UiEvent::LocalVideo {
            rgba,
            width,
            height,
        } => {
            app.set_local_image(image_from_rgba(&rgba, width, height));
        }

        UiEvent::RemoteVideo {
            id,
            rgba,
            width,
            height,
        } => {
            if let Some(i) = find_participant(participants, &id) {
                if let Some(mut p) = participants.row_data(i) {
                    p.image = image_from_rgba(&rgba, width, height);
                    p.has_video = true;
                    participants.set_row_data(i, p);
                }
            }
        }

        UiEvent::ConnectionState { pc_id, state } => {
            app.set_connection_state(format!("{pc_id}: {state}").into());
        }

        UiEvent::Error { msg } => {
            app.set_joining(false);
            app.set_error_text(msg.into());
        }
    }
}

fn find_participant(participants: &Rc<VecModel<Participant>>, id: &str) -> Option<usize> {
    for i in 0..participants.row_count() {
        if let Some(p) = participants.row_data(i) {
            if p.id == id {
                return Some(i);
            }
        }
    }
    None
}

fn new_participant(id: &str, status: SubStatus, has_video: bool) -> Participant {
    Participant {
        id: id.into(),
        status: status.as_str().into(),
        has_video,
        image: Image::default(),
    }
}

fn update_count(app: &AppWindow, participants: &Rc<VecModel<Participant>>) {
    app.set_participant_count((participants.row_count() + 1) as i32);
}
