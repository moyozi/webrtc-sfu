//! GStreamer 媒体管线。
//!
//! 发布端 (`webrtcbin` + 采集 + 编码):
//! ```text
//!   [autovideosrc | videotestsrc] ! videoconvert ! videoscale ! 640x480@30
//!       ! valve(video) ! tee
//!         ├─ queue ! vp8enc ! rtpvp8pay ! queue ! webrtcbin.sink_1
//!         └─ queue ! videoconvert ! RGBA ! appsink(local_video)   → UI
//!   [autoaudiosrc | audiotestsrc] ! audioconvert ! audioresample ! 48k mono
//!       ! valve(audio) ! tee
//!         └─ queue ! opusenc ! rtpopuspay ! queue ! webrtcbin.sink_0
//! ```
//!
//! 订阅端 (`webrtcbin` + 解码):
//! ```text
//!   webrtcbin.src_%u ! queue ! decodebin
//!       ├─(video) videoconvert ! videoscale ! RGBA ! appsink(remote_video_<id>) → UI
//!       └─(audio) audioconvert ! audioresample ! autoaudiosink | fakesink
//! ```
//!
//! 摄像头 / 麦克风开关通过 `valve` 元素 (drop) 实现, 不关闭设备, 恢复无需重新授权。
//! 所有 appsink 回调运行在 GStreamer 流线程中, 只通过 crossbeam channel 投递 RGBA
//! 帧给 Slint 主线程。

use crossbeam_channel::Sender;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app::{AppSink, AppSinkCallbacks};
use gstreamer_video::VideoInfo;

use crate::ui::UiEvent;

/// 请求的摄像头分辨率 (GStreamer 会缩放到该尺寸)。
pub const VIDEO_WIDTH: u32 = 640;
pub const VIDEO_HEIGHT: u32 = 480;

/// 解码后统一缩放/裁剪到的 RGBA 尺寸。
const REMOTE_WIDTH: u32 = 640;
const REMOTE_HEIGHT: u32 = 480;

/// 必需的 GStreamer 元素 (缺失时给出明确提示)。
const REQUIRED_ELEMENTS: &[&str] = &[
    "webrtcbin",
    "autovideosrc",
    "autoaudiosrc",
    "videotestsrc",
    "audiotestsrc",
    "videoconvert",
    "videoscale",
    "audioconvert",
    "audioresample",
    "vp8enc",
    "vp8dec",
    "rtpvp8pay",
    "rtpvp8depay",
    "opusenc",
    "opusdec",
    "rtpopuspay",
    "rtpopusdepay",
    "decodebin",
    "appsink",
    "valve",
    "queue",
    "tee",
];

/// 权限占位。
///
/// 真实摄像头/麦克风授权由 GStreamer 首次打开设备时触发 (macOS TCC 依据
/// 可执行文件嵌入的 `Info.plist`, 见 `build.rs`)。
pub fn init_camera_permission() {
    tracing::debug!("摄像头/麦克风权限将在 GStreamer 打开设备时请求");
}

/// 检查必需插件是否齐全; 返回缺失元素列表。
pub fn missing_elements() -> Vec<String> {
    REQUIRED_ELEMENTS
        .iter()
        .filter(|name| gst::ElementFactory::find(name).is_none())
        .map(|s| s.to_string())
        .collect()
}

/// 探测本机是否存在摄像头 / 麦克风设备。
///
/// 返回 `(has_video, has_audio)`。探测失败时保守返回 `(false, false)`,
/// 调用方据此回退到合成源。
pub fn detect_devices() -> (bool, bool) {
    let monitor = gst::DeviceMonitor::new();
    monitor.add_filter(Some("Video/Source"), None);
    monitor.add_filter(Some("Audio/Source"), None);
    if monitor.start().is_err() {
        return (false, false);
    }
    let mut has_video = false;
    let mut has_audio = false;
    for device in monitor.devices() {
        if let Some(caps) = device.caps() {
            for s in caps.iter() {
                let name = s.name();
                if name.starts_with("video/") {
                    has_video = true;
                } else if name.starts_with("audio/") {
                    has_audio = true;
                }
            }
        }
    }
    monitor.stop();
    (has_video, has_audio)
}

// ── 发布端 ────────────────────────────────────────────────────

pub struct PublisherHandles {
    pub pipeline: gst::Pipeline,
    /// `webrtcbin` (信号的连接由 sfu worker 完成)。
    pub webrtc: gst::Element,
    /// 摄像头开关: drop=true 时暂停发送 (黑屏)。
    pub video_valve: Option<gst::Element>,
    /// 麦克风开关: drop=true 时暂停发送 (静音)。
    pub audio_valve: Option<gst::Element>,
}

/// 构建发布管线。`prefer_real` 为真且检测到设备时使用真实摄像头/麦克风,
/// 否则 (或设备缺失时) 使用合成源。
pub fn build_publisher(
    ui_tx: Sender<UiEvent>,
    prefer_real: bool,
) -> Result<PublisherHandles, String> {
    let (has_video_dev, has_audio_dev) = if prefer_real {
        detect_devices()
    } else {
        (false, false)
    };

    let video_src = if has_video_dev {
        "autovideosrc"
    } else {
        "videotestsrc is-live=true pattern=ball"
    };
    let audio_src = if has_audio_dev {
        "autoaudiosrc"
    } else {
        "audiotestsrc is-live=true wave=sine freq=440"
    };

    // 音频放在前面 => 音频 transceiver 先创建, SDP 中 mid 顺序为 音频在前。
    let desc = format!(
        "webrtcbin name=pub_webrtc bundle-policy=max-bundle \
         {audio_src} ! audioconvert ! audioresample ! \
             audio/x-raw,rate=48000,channels=1,format=S16LE ! \
             valve name=avalve ! tee name=atee \
         atee. ! queue ! opusenc bitrate=64000 ! rtpopuspay pt=97 ! \
             application/x-rtp,media=audio,encoding-name=OPUS,payload=97,clock-rate=48000 ! \
             queue ! pub_webrtc. \
         {video_src} ! videoconvert ! videoscale ! \
             video/x-raw,width={VIDEO_WIDTH},height={VIDEO_HEIGHT},framerate=30/1 ! \
             valve name=vvalve ! tee name=vtee \
         vtee. ! queue ! vp8enc deadline=1 target-bitrate=1000000 keyframe-max-dist=30 ! \
             rtpvp8pay pt=96 ! \
             application/x-rtp,media=video,encoding-name=VP8,payload=96,clock-rate=90000 ! \
             queue ! pub_webrtc. \
         vtee. ! queue ! videoconvert ! video/x-raw,format=RGBA ! \
             appsink name=local_video max-buffers=1 drop=true sync=false"
    );

    let element = gst::parse::launch(&desc).map_err(|e| format!("构建发布管线失败: {e}"))?;
    let pipeline = element
        .downcast::<gst::Pipeline>()
        .map_err(|_| "发布管线不是 GstPipeline".to_string())?;

    let webrtc = pipeline
        .by_name("pub_webrtc")
        .ok_or_else(|| "发布管线缺少 webrtcbin".to_string())?;
    let video_valve = pipeline.by_name("vvalve");
    let audio_valve = pipeline.by_name("avalve");

    // 本地预览 appsink → UiEvent::LocalVideo
    if let Some(sink) = pipeline.by_name("local_video") {
        if let Ok(appsink) = sink.downcast::<AppSink>() {
            let ui = ui_tx.clone();
            appsink.set_callbacks(
                AppSinkCallbacks::builder()
                    .new_sample(move |sink| {
                        if let Ok(sample) = sink.pull_sample() {
                            if let Some((rgba, width, height)) = sample_to_rgba(&sample) {
                                let _ = ui.send(UiEvent::LocalVideo {
                                    rgba,
                                    width,
                                    height,
                                });
                            }
                        }
                        Ok(gst::FlowSuccess::Ok)
                    })
                    .build(),
            );
        }
    }

    // 状态由 sfu worker 在连接完信号后设置 (见 connect_webrtc_signals / set_playing)。

    if prefer_real && (!has_video_dev || !has_audio_dev) {
        tracing::warn!(
            "真实设备不完整 (video={has_video_dev}, audio={has_audio_dev}), 缺失部分回退到合成源"
        );
    } else {
        tracing::info!(
            "发布媒体源: video={}, audio={}",
            if has_video_dev { "camera" } else { "synthetic" },
            if has_audio_dev {
                "microphone"
            } else {
                "synthetic"
            }
        );
    }

    Ok(PublisherHandles {
        pipeline,
        webrtc,
        video_valve,
        audio_valve,
    })
}

// ── 订阅端 ────────────────────────────────────────────────────

pub struct SubscriberHandles {
    pub pipeline: gst::Pipeline,
    pub webrtc: gst::Element,
}

/// 构建订阅管线: `webrtcbin` + recvonly 音频/视频 transceiver。
///
/// transceiver 必须在管线进入 PLAYING **之前**添加, 否则第一个
/// `on-negotiation-needed` 会生成不含 m-line 的空 offer。状态由 worker 设置。
pub fn build_subscriber() -> Result<SubscriberHandles, String> {
    let pipeline = gst::Pipeline::new();
    let webrtc = gst::ElementFactory::make("webrtcbin")
        .name("sub_webrtc")
        .build()
        .map_err(|e| format!("创建订阅 webrtcbin 失败: {e}"))?;
    webrtc.set_property_from_str("bundle-policy", "max-bundle");
    pipeline
        .add(&webrtc)
        .map_err(|e| format!("无法把 webrtcbin 加入订阅管线: {e}"))?;

    add_recvonly_transceivers(&webrtc)?;

    Ok(SubscriberHandles { pipeline, webrtc })
}

/// 为订阅连接添加 recvonly 的音频 + 视频 transceiver (音频 mid 在前)。
///
/// `add-transceiver` 需要完整的 RTP caps (含 encoding-name), 否则 webrtcbin
/// 无法确定编解码器, 生成的 offer 不会包含对应 m-line。
fn add_recvonly_transceivers(webrtc: &gst::Element) -> Result<(), String> {
    use gstreamer_webrtc as gst_webrtc;

    // 必须带 encoding-params (声道数), 否则 rtpmap 为 `OPUS/48000` 而非
    // `OPUS/48000/2`, str0m SFU 会认为编解码不匹配而拒绝该 m-line。
    let audio = gst::Caps::builder("application/x-rtp")
        .field("media", "audio")
        .field("encoding-name", "OPUS")
        .field("encoding-params", "2")
        .field("payload", 97i32)
        .field("clock-rate", 48000i32)
        .build();
    let video = gst::Caps::builder("application/x-rtp")
        .field("media", "video")
        .field("encoding-name", "VP8")
        .field("payload", 96i32)
        .field("clock-rate", 90000i32)
        .build();

    let _ = webrtc.emit_by_name::<gst_webrtc::WebRTCRTPTransceiver>(
        "add-transceiver",
        &[&gst_webrtc::WebRTCRTPTransceiverDirection::Recvonly, &audio],
    );
    let _ = webrtc.emit_by_name::<gst_webrtc::WebRTCRTPTransceiver>(
        "add-transceiver",
        &[&gst_webrtc::WebRTCRTPTransceiverDirection::Recvonly, &video],
    );
    Ok(())
}

/// 把发布连接的 transceiver 方向设为 sendonly (links 默认 sendrecv)。
pub fn set_publisher_sendonly(webrtc: &gst::Element) {
    use gstreamer_webrtc as gst_webrtc;
    for i in 0..2i32 {
        let tr = webrtc.emit_by_name::<gst_webrtc::WebRTCRTPTransceiver>("get-transceiver", &[&i]);
        let _ = tr.set_direction(gst_webrtc::WebRTCRTPTransceiverDirection::Sendonly);
    }
}

/// 把管线切到 PLAYING。
pub fn set_playing(pipeline: &gst::Pipeline) -> Result<(), String> {
    pipeline
        .set_state(gst::State::Playing)
        .map(|_| ())
        .map_err(|e| format!("管线无法进入 PLAYING: {e}"))
}

/// 把管线切到 PAUSED。订阅连接先以 PAUSED 协商, 待远端描述就绪后再切
/// PLAYING, 以规避 `transportreceivebin` 在 PLAYING 下新增 nicesrc 的竞态。
pub fn set_paused(pipeline: &gst::Pipeline) -> Result<(), String> {
    pipeline
        .set_state(gst::State::Paused)
        .map(|_| ())
        .map_err(|e| format!("管线无法进入 PAUSED: {e}"))
}

/// 处理 `webrtcbin` 新增的 RTP src pad: `queue ! decodebin`, 再按解码后的
/// caps 分支到视频 (appsink → UI) 或音频 (扬声器 / fakesink)。
pub fn handle_remote_pad(
    pipeline: &gst::Pipeline,
    pad: &gst::Pad,
    ui_tx: Sender<UiEvent>,
    publisher_id: String,
    play_audio: bool,
) {
    let queue = match gst::ElementFactory::make("queue").build() {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!("创建 queue 失败: {e}");
            return;
        }
    };
    let decodebin = match gst::ElementFactory::make("decodebin").build() {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!("创建 decodebin 失败: {e}");
            return;
        }
    };
    if pipeline.add_many([&queue, &decodebin]).is_err() {
        tracing::warn!("无法把解码元素加入订阅管线");
        return;
    }
    let _ = queue.sync_state_with_parent();
    let _ = decodebin.sync_state_with_parent();

    tracing::debug!(
        "handle_remote_pad {publisher_id}: pad={} caps={:?}",
        pad.name(),
        pad.current_caps()
    );

    // 先连接 decodebin 的 pad-added, 再链接, 避免 pad 在链接过程中已暴露而丢失事件。
    let pipeline = pipeline.clone();
    let dec_ui = ui_tx.clone();
    let dec_pid = publisher_id.clone();
    decodebin.connect("pad-added", false, move |values| {
        let Some(dpad) = values.get(1).and_then(|v| v.get::<gst::Pad>().ok()) else {
            return None;
        };
        handle_decoded_pad(&pipeline, &dpad, &dec_ui, &dec_pid, play_audio);
        None
    });

    let Some(qsink) = queue.static_pad("sink") else {
        return;
    };
    if pad.link(&qsink).is_err() {
        tracing::warn!("无法把 webrtc src pad 链接到 queue");
        return;
    }
    if queue.link(&decodebin).is_err() {
        tracing::warn!("无法把 queue 链接到 decodebin");
        return;
    }
}

fn handle_decoded_pad(
    pipeline: &gst::Pipeline,
    pad: &gst::Pad,
    ui_tx: &Sender<UiEvent>,
    publisher_id: &str,
    play_audio: bool,
) {
    let caps = pad.current_caps().or_else(|| Some(pad.query_caps(None)));
    let name = caps
        .as_ref()
        .and_then(|c| c.structure(0).map(|s| s.name().to_string()))
        .unwrap_or_default();
    tracing::debug!("decodebin pad-added {publisher_id}: caps={name}");

    if name.starts_with("video/") {
        let safe = sanitize(publisher_id);
        let appsink_name = format!("remote_video_{safe}");
        let desc = format!(
            "videoconvert ! videoscale ! video/x-raw,width={REMOTE_WIDTH},height={REMOTE_HEIGHT},\
             format=RGBA ! appsink name={appsink_name} max-buffers=2 drop=true sync=false"
        );
        let bin = match gst::parse::bin_from_description(&desc, true) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("构建远端视频分支失败: {e}");
                return;
            }
        };
        if pipeline.add(&bin).is_err() {
            tracing::warn!("无法把远端视频分支加入订阅管线");
            return;
        }
        let _ = bin.sync_state_with_parent();
        let Some(sink) = bin.static_pad("sink") else {
            return;
        };
        if pad.link(&sink).is_err() {
            tracing::warn!("无法链接远端视频解码到显示分支");
            return;
        }

        let Some(appsink) = bin
            .by_name(&appsink_name)
            .and_then(|e| e.downcast::<AppSink>().ok())
        else {
            return;
        };
        let ui = ui_tx.clone();
        let pid = publisher_id.to_string();
        appsink.set_callbacks(
            AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    if let Ok(sample) = sink.pull_sample() {
                        if let Some((rgba, width, height)) = sample_to_rgba(&sample) {
                            let _ = ui.send(UiEvent::RemoteVideo {
                                id: pid.clone(),
                                rgba,
                                width,
                                height,
                            });
                        }
                    }
                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );
        tracing::info!("已为 {publisher_id} 挂载远端视频解码分支");
    } else if name.starts_with("audio/") {
        let sink = if play_audio {
            "queue ! audioconvert ! audioresample ! autoaudiosink"
        } else {
            "queue ! fakesink sync=false"
        };
        let bin = match gst::parse::bin_from_description(sink, true) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("构建远端音频分支失败: {e}");
                return;
            }
        };
        if pipeline.add(&bin).is_err() {
            return;
        }
        let _ = bin.sync_state_with_parent();
        let Some(sp) = bin.static_pad("sink") else {
            return;
        };
        if pad.link(&sp).is_err() {
            tracing::warn!("无法链接远端音频到输出分支");
            return;
        }
        tracing::info!("已为 {publisher_id} 挂载远端音频播放分支");
    }
}

// ── 工具 ─────────────────────────────────────────────────────

/// `GstSample` (RGBA) → 紧凑 `Vec<u8>` (处理 stride)。
fn sample_to_rgba(sample: &gst::Sample) -> Option<(Vec<u8>, u32, u32)> {
    let caps = sample.caps()?;
    let info = VideoInfo::from_caps(caps).ok()?;
    let buffer = sample.buffer()?;
    let map = buffer.map_readable().ok()?;
    let data = map.as_slice();

    let width = info.width();
    let height = info.height();
    let stride = info.stride()[0].max(0) as usize;
    let row_bytes = width as usize * 4;
    let mut rgba = Vec::with_capacity(row_bytes * height as usize);

    if stride == row_bytes || stride == 0 {
        let need = row_bytes * height as usize;
        let n = need.min(data.len());
        rgba.extend_from_slice(&data[..n]);
    } else {
        for y in 0..height as usize {
            let start = y * stride;
            let end = start + row_bytes;
            if end > data.len() {
                break;
            }
            rgba.extend_from_slice(&data[start..end]);
        }
    }

    Some((rgba, width, height))
}

/// 把任意 id 变成可安全用于 GStreamer 元素名的字符串。
fn sanitize(id: &str) -> String {
    id.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}
