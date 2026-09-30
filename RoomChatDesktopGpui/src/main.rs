//! RoomChat 桌面客户端入口 (GPUI + gpui-kit)。
//!
//! 同一套功能移植自 `RoomChatDesktopSlint` (Slint 版), 线程模型保持一致:
//!   · 主线程: GPUI 事件循环 + 视图 `RoomChatApp`
//!   · sfu-client 线程: reactor-webrtc (同步阻塞) + WebSocket 信令
//!   · camera-capture / audio-gen / video-synth 线程: 本地媒体源
//!
//! UI 与 worker 仅通过 async-channel 传递事件。

mod app;
mod media;
mod protocol;
mod signaling;
mod sfu;
mod ui;

use gpui::{
    px, size, AppContext as _, Bounds, SharedString, TitlebarOptions, WindowBounds, WindowOptions,
};
use gpui_kit::component::{Root, Theme, ThemeMode};

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .try_init()
        .ok();

    // 尽早请求摄像头权限 (macOS 弹系统授权框; 其他平台为空操作)
    media::init_camera_permission();

    gpui_kit::application()
        .with_assets(gpui_kit::assets::AllAssets)
        .run(|cx| {
            gpui_kit::init(cx);
            // Slint 版本使用深色主题, 这里保持一致。
            Theme::change(ThemeMode::Dark, None, cx);

            let bounds = Bounds::centered(None, size(px(1180.), px(780.)), cx);
            cx.spawn(async move |cx| {
                cx.open_window(
                    WindowOptions {
                        titlebar: Some(TitlebarOptions {
                            title: Some(SharedString::from("RoomChat Desktop · WebRTC SFU")),
                            ..Default::default()
                        }),
                        window_bounds: Some(WindowBounds::Windowed(bounds)),
                        app_id: Some("roomchat-desktop-gpui".to_string()),
                        ..Default::default()
                    },
                    |window, cx| {
                        let app = cx.new(|cx| app::RoomChatApp::new(window, cx));
                        cx.new(|cx| Root::new(app, window, cx))
                    },
                )
                .expect("无法创建主窗口");
            })
            .detach();

            cx.activate(true);
        });
}
