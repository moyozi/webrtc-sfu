//! 无界面冒烟测试 (不依赖 Slint / 显示环境)。
//!
//! 复用与主程序完全相同的 worker (`sfu`) 与 GStreamer 媒体层 (`media`),
//! 加入房间、发布合成 (或真实) 音视频、自动订阅其他参与者, 把 `UiEvent`
//! 打印到 stdout。用于:
//!   · CI / 无显示环境验证 WebRTC 发布 + 订阅链路;
//!   · 排查信令 / SDP / ICE 问题。
//!
//! 用法:
//!   cargo run --bin roomchat-gst-headless -- [ws_url] [room] [nick] [real|synth] [seconds]
//!
//! 例: 开两个终端, 第二个会自动订阅第一个并收到远端视频帧:
//!   cargo run --bin roomchat-gst-headless -- ws://127.0.0.1:5174 room-1 alice synth 30
//!   cargo run --bin roomchat-gst-headless -- ws://127.0.0.1:5174 room-1 bob   synth 30

use std::time::{Duration, Instant};

use crossbeam_channel::unbounded;

use roomchat_desktop_gstreamer::sfu::{self, ClientCommand};
use roomchat_desktop_gstreamer::ui::UiEvent;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .try_init()
        .ok();

    let args: Vec<String> = std::env::args().collect();
    let server_url = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "ws://127.0.0.1:5174".to_string());
    let room_id = args.get(2).cloned().unwrap_or_else(|| "room-1".to_string());
    let participant_id = args
        .get(3)
        .cloned()
        .unwrap_or_else(|| format!("gst-headless-{}", std::process::id()));
    let use_real_devices = args.get(4).map(|s| s == "real").unwrap_or(false);
    let seconds: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(25);

    println!(
        "== headless: server={server_url} room={room_id} nick={participant_id} real={use_real_devices} {seconds}s =="
    );

    let (ui_tx, ui_rx) = unbounded();
    let (cmd_tx, worker) = sfu::spawn_worker(ui_tx);
    let _ = cmd_tx.send(ClientCommand::Join {
        server_url,
        room_id,
        participant_id,
        use_real_devices,
    });

    let start = Instant::now();
    let mut local_frames: u64 = 0;
    let mut remote_frames: u64 = 0;
    while start.elapsed() < Duration::from_secs(seconds) {
        while let Ok(ev) = ui_rx.try_recv() {
            match ev {
                UiEvent::Log { level, msg } => println!("[{level:?}] {msg}"),
                UiEvent::Joined { room_id } => println!("JOINED {room_id}"),
                UiEvent::Left => println!("LEFT"),
                UiEvent::ParticipantJoined { id } => println!("PARTICIPANT+ {id}"),
                UiEvent::ParticipantLeft { id } => println!("PARTICIPANT- {id}"),
                UiEvent::SubscriptionUpdate {
                    id,
                    status,
                    has_video,
                } => println!("SUB {id} status={status:?} has_video={has_video}"),
                UiEvent::ConnectionState { pc_id, state } => println!("CONN {pc_id} {state}"),
                UiEvent::Error { msg } => println!("ERROR {msg}"),
                UiEvent::LocalVideo { .. } => local_frames += 1,
                UiEvent::RemoteVideo { id, .. } => {
                    remote_frames += 1;
                    if remote_frames % 30 == 1 {
                        println!("REMOTE FRAME {id} #{remote_frames}");
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    println!("== summary: local_frames={local_frames} remote_frames={remote_frames} ==");
    let _ = cmd_tx.send(ClientCommand::Shutdown);
    let _ = worker.join();

    if remote_frames == 0 {
        eprintln!("没有收到任何远端视频帧 (可能没有其他参与者, 或订阅/解码失败)");
        std::process::exit(2);
    }
}
