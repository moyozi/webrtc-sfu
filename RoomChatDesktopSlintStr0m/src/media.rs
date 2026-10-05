//! 本地媒体采集/编解码与远端媒体解码 — 全部通过 **ffmpeg-next** (libav*) 完成。
//!
//! 与 `RoomChatDesktopSlint` 使用 reactor-webrtc (libwebrtc) 自带采集/编解码不同,
//! 本 crate 用 str0m 负责 WebRTC 传输 (ICE/DTLS/SRTP/RTP), 把**采集与编解码**
//! 交给 ffmpeg 的 Rust 绑定 [`ffmpeg_next`]:
//!
//! ```text
//!   摄像头 (libavdevice) ─► 解码 ─► RGBA ─┬─► Slint UI 预览
//!                                        └─► swscale(YUV420P) ─► libvpx(VP8) ─► str0m
//!   合成图案 ──────────────RGBA──────────┘
//!
//!   麦克风 (libavdevice) ─► 解码 ─► swresample ─► libopus ─► str0m
//!   合成正弦波 ─────────────────────► libopus ─► str0m
//!
//!   远端 VP8  ─► libvpx 解码 ─► swscale(RGBA) ─► Slint UI
//!   远端 Opus ─► libopus 解码 ─► swresample ─► 系统音频输出设备 (libavdevice)
//! ```
//!
//! 线程模型: 每一种媒体来源/去向各一条线程, 仅通过 crossbeam channel 与 str0m
//! worker 通信。这样 worker 线程永远不会被采集/编解码阻塞, 能及时驱动 str0m
//! 的定时器。
//!
//! 设备名 / 后端均可通过环境变量覆盖:
//!   · `SFU_CAMERA_DEVICE` (默认 `/dev/video0`; macOS `0`; Windows dshow 名称)
//!   · `SFU_AUDIO_INPUT`   (默认 `pulse default`; macOS `avfoundation :0`)
//!   · `SFU_AUDIO_SINK`    (默认 `pulse default`; macOS `audiotoolbox -`)

use std::ffi::CString;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{unbounded, Receiver, Sender};

use ffmpeg_next as ffmpeg;
use ffmpeg_next::codec;
use ffmpeg_next::format as avformat;
use ffmpeg_next::format::sample::Type as SampleType;
use ffmpeg_next::format::Pixel;
use ffmpeg_next::format::Sample;
use ffmpeg_next::frame;
use ffmpeg_next::media as avmedia;
use ffmpeg_next::software;
use ffmpeg_next::software::scaling::Flags as ScaleFlags;
use ffmpeg_next::{ChannelLayout, Dictionary, Packet, Rational};

use crate::ui::UiEvent;

// ── 媒体参数 ──────────────────────────────────────────────────

/// 本地采集 / 预览分辨率。
pub const VIDEO_WIDTH: u32 = 640;
pub const VIDEO_HEIGHT: u32 = 480;
/// 本地视频帧率。
pub const VIDEO_FPS: u32 = 30;
/// 视频编码码率。
const VIDEO_BITRATE: usize = 1_000_000;

/// 音频采样率。
pub const SAMPLE_RATE: u32 = 48_000;
/// 单个 Opus 帧的采样点数 (20ms @ 48kHz)。
pub const AUDIO_FRAME_SAMPLES: usize = 960;

/// 摄像头/麦克风权限初始化占位。
///
/// 采集由 ffmpeg 的 libavdevice 在本进程内完成; 首次打开设备时由操作系统触发
/// 授权 (macOS 读取可执行文件内嵌的 `Info.plist`)。保留该函数与 main.rs 的
/// 初始化流程保持一致。
pub fn init_camera_permission() {}

// ── 全局初始化 ────────────────────────────────────────────────

static FFMPEG_INIT: OnceLock<Result<(), String>> = OnceLock::new();

fn ensure_ffmpeg() -> Result<(), String> {
    FFMPEG_INIT
        .get_or_init(|| {
            ffmpeg::init().map_err(|e| format!("初始化 ffmpeg 失败: {e}"))?;
            // libavdevice 的输入/输出设备 (v4l2 / avfoundation / alsa / pulse …)
            ffmpeg::device::register_all();
            Ok(())
        })
        .clone()
}

// ── worker 消费的已编码媒体 ───────────────────────────────────

/// 一帧已编码的 VP8 (来自视频编码器)。
#[derive(Debug)]
pub struct EncodedVideo {
    pub data: Arc<[u8]>,
    /// 微秒时间戳 (单调递增)。
    pub timestamp_us: u64,
}

/// 一个已编码的 Opus 包 (来自音频编码器)。
#[derive(Debug)]
pub struct EncodedAudio {
    pub data: Arc<[u8]>,
    /// 微秒时间戳 (单调递增)。
    pub timestamp_us: u64,
}

// ── 设备打开 (libavdevice) ────────────────────────────────────

/// 用指定输入格式打开设备 (允许传入 framerate / video_size / sample_rate 等选项)。
///
/// ffmpeg-next 的高层 `format::input*` 无法强制输入格式 (avfoundation / dshow
/// 需要), 因此这里直接调用 libav* 的 C API 并包装成 `Input`。
unsafe fn open_input_device(
    format_name: &str,
    url: &str,
    options: &[(&str, &str)],
) -> Result<avformat::context::Input, String> {
    let c_format = CString::new(format_name).map_err(|_| "输入格式名包含 NUL".to_string())?;
    let ifmt = ffmpeg::ffi::av_find_input_format(c_format.as_ptr());
    if ifmt.is_null() {
        return Err(format!("未知输入设备格式: {format_name}"));
    }

    let mut dict: *mut ffmpeg::ffi::AVDictionary = ptr::null_mut();
    for (k, v) in options {
        let ck = CString::new(*k).unwrap();
        let cv = CString::new(*v).unwrap();
        ffmpeg::ffi::av_dict_set(&mut dict, ck.as_ptr(), cv.as_ptr(), 0);
    }

    let c_url = CString::new(url).map_err(|_| "设备名包含 NUL".to_string())?;
    let mut ps: *mut ffmpeg::ffi::AVFormatContext = ptr::null_mut();
    let ret = ffmpeg::ffi::avformat_open_input(&mut ps, c_url.as_ptr(), ifmt, &mut dict);
    ffmpeg::ffi::av_dict_free(&mut dict);
    if ret < 0 {
        return Err(format!(
            "打开设备失败 ({format_name} {url}): {}",
            ffmpeg::Error::from(ret)
        ));
    }

    let r = ffmpeg::ffi::avformat_find_stream_info(ps, ptr::null_mut());
    if r < 0 {
        ffmpeg::ffi::avformat_close_input(&mut ps);
        return Err(format!("读取设备流信息失败: {}", ffmpeg::Error::from(r)));
    }

    Ok(avformat::context::Input::wrap(ps))
}

/// 摄像头输入规格: `(格式, 设备名, 选项)`。
fn camera_input_spec() -> (String, String, Vec<(&'static str, String)>) {
    let size = format!("{VIDEO_WIDTH}x{VIDEO_HEIGHT}");
    let fps = VIDEO_FPS.to_string();
    #[cfg(target_os = "linux")]
    {
        let dev = std::env::var("SFU_CAMERA_DEVICE").unwrap_or_else(|_| "/dev/video0".into());
        (
            "v4l2".into(),
            dev,
            vec![("framerate", fps), ("video_size", size)],
        )
    }
    #[cfg(target_os = "macos")]
    {
        let dev = std::env::var("SFU_CAMERA_DEVICE").unwrap_or_else(|_| "0".into());
        (
            "avfoundation".into(),
            dev,
            vec![("framerate", fps), ("video_size", size)],
        )
    }
    #[cfg(target_os = "windows")]
    {
        let dev =
            std::env::var("SFU_CAMERA_DEVICE").unwrap_or_else(|_| "video=Integrated Camera".into());
        (
            "dshow".into(),
            dev,
            vec![("framerate", fps), ("video_size", size)],
        )
    }
}

/// 麦克风输入规格: `(格式, 设备名, 选项)`。
fn audio_input_spec() -> (String, String, Vec<(&'static str, String)>) {
    let spec = std::env::var("SFU_AUDIO_INPUT").unwrap_or_else(|_| {
        #[cfg(target_os = "linux")]
        {
            "pulse default".to_string()
        }
        #[cfg(target_os = "macos")]
        {
            "avfoundation :0".to_string()
        }
        #[cfg(target_os = "windows")]
        {
            "dshow audio=Microphone".to_string()
        }
    });
    let mut parts = spec.split_whitespace();
    let fmt = parts.next().unwrap_or("pulse").to_string();
    let dev = parts.next().unwrap_or("default").to_string();
    (
        fmt,
        dev,
        vec![
            ("sample_rate", "48000".to_string()),
            ("channels", "1".to_string()),
        ],
    )
}

/// 音频输出 (播放) 规格: `(格式, 设备名)`。
fn audio_sink_spec() -> (String, String) {
    let spec = std::env::var("SFU_AUDIO_SINK").unwrap_or_else(|_| {
        #[cfg(target_os = "linux")]
        {
            "pulse default".to_string()
        }
        #[cfg(target_os = "macos")]
        {
            "audiotoolbox -".to_string()
        }
        #[cfg(target_os = "windows")]
        {
            "sdl -".to_string()
        }
    });
    let mut parts = spec.split_whitespace();
    (
        parts.next().unwrap_or("pulse").to_string(),
        parts.next().unwrap_or("default").to_string(),
    )
}

// ── 像素格式转换 ──────────────────────────────────────────────

/// 把 FFmpeg `Video` 帧的 plane 0 复制为紧凑 RGBA 字节 (处理 stride)。
fn frame_to_rgba(frame: &frame::Video) -> Vec<u8> {
    let width = frame.width() as usize;
    let height = frame.height() as usize;
    let row = width * 4;
    let stride = frame.stride(0);
    let data = frame.data(0);
    let mut out = vec![0u8; row * height];
    for y in 0..height {
        let start = y * stride;
        out[y * row..(y + 1) * row].copy_from_slice(&data[start..start + row]);
    }
    out
}

/// 把紧凑 RGBA 字节写入 `Video` 帧 (处理 stride)。
fn fill_rgba_frame(frame: &mut frame::Video, rgba: &[u8]) {
    let width = frame.width() as usize;
    let height = frame.height() as usize;
    let row = width * 4;
    let stride = frame.stride(0);
    let data = frame.data_mut(0);
    for y in 0..height {
        let start = y * stride;
        data[start..start + row].copy_from_slice(&rgba[y * row..(y + 1) * row]);
    }
}

// ── 合成画面 ──────────────────────────────────────────────────

/// 绘制一帧合成测试图案 (RGBA)。
fn draw_pattern_rgba(buf: &mut [u8], frame_no: u64) {
    let w = VIDEO_WIDTH as usize;
    let h = VIDEO_HEIGHT as usize;
    let t = frame_no as f32 / VIDEO_FPS as f32;
    let bar_x = ((frame_no * 4) % (w as u64 + 120)) as i64 - 60;

    for y in 0..h {
        for x in 0..w {
            let i = (y * w + x) * 4;
            let fx = x as f32;
            let fy = y as f32;
            let r = (128.0 + 100.0 * (t * 1.3 + fx * 0.010).sin()) as u8;
            let g = (128.0 + 100.0 * (t * 1.7 + fy * 0.012).cos()) as u8;
            let b = (128.0 + 100.0 * (t * 2.1 + (fx + fy) * 0.008).sin()) as u8;
            let (r, g, b) = if (x as i64) >= bar_x && (x as i64) < bar_x + 14 {
                (255u8, 255u8, 255u8)
            } else {
                (r, g, b)
            };
            buf[i] = r;
            buf[i + 1] = g;
            buf[i + 2] = b;
            buf[i + 3] = 255;
        }
    }

    // 左上角 32 位帧计数指示 (8×4 方块)。
    for bit in 0..32usize {
        let bx = 6 + (bit % 8) * 12;
        let by = 6 + (bit / 8) * 12;
        let on = (frame_no >> bit) & 1 == 1;
        for yy in 0..9usize {
            for xx in 0..9usize {
                let x = bx + xx;
                let y = by + yy;
                if x >= w || y >= h {
                    continue;
                }
                let i = (y * w + x) * 4;
                let v = if on { 255u8 } else { 0u8 };
                buf[i] = v;
                buf[i + 1] = v;
                buf[i + 2] = v;
                buf[i + 3] = 255;
            }
        }
    }
}

// ── 视频: VP8 编码 + 预览转换 ─────────────────────────────────

/// 视频帧转换器: 任意像素格式 → RGBA, RGBA → YUV420P。
struct VideoConverter {
    // 解码帧 → RGBA
    to_rgba: Option<software::scaling::Context>,
    src_def: Option<(Pixel, u32, u32)>,
    rgba: frame::Video,
    // RGBA → YUV420P (编码输入)
    to_yuv: software::scaling::Context,
    yuv: frame::Video,
}

impl VideoConverter {
    fn new() -> Result<Self, String> {
        let to_yuv = software::scaling::Context::get(
            Pixel::RGBA,
            VIDEO_WIDTH,
            VIDEO_HEIGHT,
            Pixel::YUV420P,
            VIDEO_WIDTH,
            VIDEO_HEIGHT,
            ScaleFlags::BILINEAR,
        )
        .map_err(|e| format!("创建缩放器失败: {e}"))?;
        Ok(Self {
            to_rgba: None,
            src_def: None,
            rgba: frame::Video::new(Pixel::RGBA, VIDEO_WIDTH, VIDEO_HEIGHT),
            to_yuv,
            yuv: frame::Video::new(Pixel::YUV420P, VIDEO_WIDTH, VIDEO_HEIGHT),
        })
    }

    /// 任意解码帧 → 内部 RGBA 帧。返回其紧凑字节 (供 UI)。
    fn decode_to_rgba(&mut self, src: &frame::Video) -> Result<Vec<u8>, String> {
        let def = (src.format(), src.width(), src.height());
        if self.src_def != Some(def) {
            self.to_rgba = Some(
                software::scaling::Context::get(
                    src.format(),
                    src.width(),
                    src.height(),
                    Pixel::RGBA,
                    VIDEO_WIDTH,
                    VIDEO_HEIGHT,
                    ScaleFlags::BILINEAR,
                )
                .map_err(|e| format!("创建缩放器失败: {e}"))?,
            );
            self.src_def = Some(def);
        }
        let scaler = self.to_rgba.as_mut().ok_or("缩放器未初始化")?;
        scaler
            .run(src, &mut self.rgba)
            .map_err(|e| format!("像素转换失败: {e}"))?;
        Ok(frame_to_rgba(&self.rgba))
    }

    /// 内部 RGBA 帧 → 内部 YUV420P 帧 (编码输入)。
    fn rgba_to_yuv(&mut self) -> Result<(), String> {
        self.to_yuv
            .run(&self.rgba, &mut self.yuv)
            .map_err(|e| format!("像素转换失败: {e}"))
    }
}

/// VP8 视频编码器。
struct VideoEncoder {
    encoder: codec::encoder::Video,
    pts: i64,
}

impl VideoEncoder {
    fn new() -> Result<Self, String> {
        let codec = codec::encoder::find_by_name("libvpx")
            .or_else(|| codec::encoder::find(codec::Id::VP8))
            .ok_or("未找到 VP8 编码器 (libvpx)")?;

        let ctx = codec::context::Context::new_with_codec(codec);
        let mut enc = ctx.encoder().video().map_err(|e| e.to_string())?;
        enc.set_width(VIDEO_WIDTH);
        enc.set_height(VIDEO_HEIGHT);
        enc.set_format(Pixel::YUV420P);
        enc.set_time_base(Rational(1, VIDEO_FPS as i32));
        enc.set_frame_rate(Some(Rational(VIDEO_FPS as i32, 1)));
        enc.set_bit_rate(VIDEO_BITRATE);
        enc.set_gop(VIDEO_FPS);

        let mut opts = Dictionary::new();
        opts.set("deadline", "realtime");
        opts.set("cpu-used", "8");
        opts.set("lag-in-frames", "0");
        opts.set("auto-alt-ref", "0");
        opts.set("error-resilient", "1");

        let encoder = enc
            .open_as_with(codec, opts)
            .map_err(|e| format!("打开 VP8 编码器失败: {e}"))?;
        Ok(Self { encoder, pts: 0 })
    }

    fn encode(&mut self, yuv: &mut frame::Video, tx: &Sender<EncodedVideo>) {
        yuv.set_pts(Some(self.pts));
        let timestamp_us = (self.pts.max(0) as u64) * 1_000_000 / VIDEO_FPS as u64;
        self.pts += 1;

        if self.encoder.send_frame(yuv).is_err() {
            return;
        }
        let mut pkt = Packet::empty();
        while self.encoder.receive_packet(&mut pkt).is_ok() {
            if let Some(data) = pkt.data() {
                let _ = tx.send(EncodedVideo {
                    data: Arc::from(data),
                    timestamp_us,
                });
            }
        }
    }
}

// ── 视频发布器 ────────────────────────────────────────────────

/// 视频发布器: 采集/合成 + 编码, 支持暂停/恢复/停止。
pub struct VideoPublisher {
    paused: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl VideoPublisher {
    /// 启动视频发布。返回 `(发布器, 是否真实摄像头, 日志信息)`。
    pub fn start(
        tx: Sender<EncodedVideo>,
        ui_tx: Sender<UiEvent>,
        prefer_camera: bool,
    ) -> (Self, bool, String) {
        let paused = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let (ready_tx, ready_rx) = std_mpsc::channel::<(bool, String)>();

        let handle = thread::Builder::new()
            .name("video-pub".into())
            .spawn({
                let (paused, stopped) = (paused.clone(), stopped.clone());
                move || video_publisher_loop(prefer_camera, tx, ui_tx, paused, stopped, ready_tx)
            })
            .ok();

        let (is_camera, msg) = ready_rx
            .recv_timeout(Duration::from_secs(8))
            .unwrap_or_else(|_| (false, "视频初始化超时; 已回退到合成视频".to_string()));

        (
            Self {
                paused,
                stopped,
                handle,
            },
            is_camera,
            msg,
        )
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
    }

    pub fn stop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn video_publisher_loop(
    prefer_camera: bool,
    tx: Sender<EncodedVideo>,
    ui_tx: Sender<UiEvent>,
    paused: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    ready_tx: std_mpsc::Sender<(bool, String)>,
) {
    if let Err(e) = ensure_ffmpeg() {
        let _ = ready_tx.send((false, e));
        return;
    }

    let mut converter = match VideoConverter::new() {
        Ok(c) => c,
        Err(e) => {
            let _ = ready_tx.send((false, e));
            return;
        }
    };
    let mut encoder = match VideoEncoder::new() {
        Ok(e) => e,
        Err(e) => {
            let _ = ready_tx.send((false, e));
            return;
        }
    };

    if prefer_camera {
        match open_camera_source() {
            Ok(source) => {
                let _ = ready_tx.send((true, "已启用真实摄像头".to_string()));
                camera_source_loop(
                    source,
                    &mut converter,
                    &mut encoder,
                    &tx,
                    &ui_tx,
                    &paused,
                    &stopped,
                );
                return;
            }
            Err(e) => {
                let _ = ready_tx.send((false, format!("{e}; 已回退到合成视频")));
            }
        }
    } else {
        let _ = ready_tx.send((false, "使用合成视频".to_string()));
    }

    synthetic_video_loop(&mut converter, &mut encoder, &tx, &ui_tx, &paused, &stopped);
}

struct CameraSource {
    input: avformat::context::Input,
    index: usize,
    decoder: codec::decoder::Video,
}

fn open_camera_source() -> Result<CameraSource, String> {
    let (fmt, dev, options) = camera_input_spec();
    let opts: Vec<(&str, &str)> = options.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let input = unsafe { open_input_device(&fmt, &dev, &opts)? };
    let stream = input
        .streams()
        .best(avmedia::Type::Video)
        .ok_or("摄像头无视频流")?;
    let index = stream.index();
    let params = stream.parameters();
    let ctx = codec::context::Context::from_parameters(params)
        .map_err(|e| format!("创建摄像头解码器上下文失败: {e}"))?;
    let decoder = ctx
        .decoder()
        .video()
        .map_err(|e| format!("打开摄像头解码器失败: {e}"))?;
    Ok(CameraSource {
        input,
        index,
        decoder,
    })
}

fn camera_source_loop(
    mut source: CameraSource,
    converter: &mut VideoConverter,
    encoder: &mut VideoEncoder,
    tx: &Sender<EncodedVideo>,
    ui_tx: &Sender<UiEvent>,
    paused: &Arc<AtomicBool>,
    stopped: &Arc<AtomicBool>,
) {
    let mut packets = source.input.packets();
    while !stopped.load(Ordering::Relaxed) {
        let Some((stream, packet)) = packets.next() else {
            break;
        };
        if stream.index() != source.index {
            continue;
        }
        if source.decoder.send_packet(&packet).is_err() {
            continue;
        }
        let mut decoded = frame::Video::empty();
        while source.decoder.receive_frame(&mut decoded).is_ok() {
            let Ok(rgba) = converter.decode_to_rgba(&decoded) else {
                continue;
            };
            let _ = ui_tx.send(UiEvent::LocalVideo {
                rgba,
                width: VIDEO_WIDTH,
                height: VIDEO_HEIGHT,
            });
            if paused.load(Ordering::Relaxed) {
                continue;
            }
            if converter.rgba_to_yuv().is_ok() {
                encoder.encode(&mut converter.yuv, tx);
            }
        }
    }
}

fn synthetic_video_loop(
    converter: &mut VideoConverter,
    encoder: &mut VideoEncoder,
    tx: &Sender<EncodedVideo>,
    ui_tx: &Sender<UiEvent>,
    paused: &Arc<AtomicBool>,
    stopped: &Arc<AtomicBool>,
) {
    let frame_dur = Duration::from_millis(1000 / VIDEO_FPS as u64);
    let mut frame_no: u64 = 0;
    let mut next = Instant::now();
    let mut rgba = vec![0u8; (VIDEO_WIDTH * VIDEO_HEIGHT * 4) as usize];

    while !stopped.load(Ordering::Relaxed) {
        if paused.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(15));
            next = Instant::now() + frame_dur;
            continue;
        }
        let now = Instant::now();
        if now < next {
            thread::sleep(next - now);
        }
        next += frame_dur;
        if next < Instant::now() {
            next = Instant::now() + frame_dur;
        }

        frame_no = frame_no.wrapping_add(1);
        draw_pattern_rgba(&mut rgba, frame_no);
        fill_rgba_frame(&mut converter.rgba, &rgba);
        let _ = ui_tx.send(UiEvent::LocalVideo {
            rgba: rgba.clone(),
            width: VIDEO_WIDTH,
            height: VIDEO_HEIGHT,
        });
        if converter.rgba_to_yuv().is_ok() {
            encoder.encode(&mut converter.yuv, tx);
        }
    }
}

// ── 音频: Opus 编码 ───────────────────────────────────────────

/// Opus 音频编码器 (48kHz 单声道)。
struct AudioEncoder {
    encoder: codec::encoder::Audio,
    pts: i64,
}

impl AudioEncoder {
    fn new() -> Result<Self, String> {
        let codec = codec::encoder::find_by_name("libopus")
            .or_else(|| codec::encoder::find(codec::Id::OPUS))
            .ok_or("未找到 Opus 编码器")?;

        let ctx = codec::context::Context::new_with_codec(codec);
        let mut enc = ctx.encoder().audio().map_err(|e| e.to_string())?;
        enc.set_rate(SAMPLE_RATE as i32);
        enc.set_channel_layout(ChannelLayout::MONO);
        enc.set_format(Sample::F32(SampleType::Planar));
        enc.set_bit_rate(64_000);
        enc.set_time_base(Rational(1, SAMPLE_RATE as i32));

        let mut opts = Dictionary::new();
        opts.set("application", "voip");
        opts.set("frame_duration", "20");

        let encoder = enc
            .open_as_with(codec, opts)
            .map_err(|e| format!("打开 Opus 编码器失败: {e}"))?;
        Ok(Self { encoder, pts: 0 })
    }

    /// 送入 `samples` 个 f32 采样 (单声道), 输出编码包。
    fn encode(&mut self, samples: &[f32], tx: &Sender<EncodedAudio>) {
        if samples.is_empty() {
            return;
        }
        let mut pcm = frame::Audio::new(
            Sample::F32(SampleType::Planar),
            samples.len(),
            ChannelLayout::MONO,
        );
        pcm.set_pts(Some(self.pts));
        let timestamp_us = (self.pts.max(0) as u64) * 1_000_000 / SAMPLE_RATE as u64;
        self.pts += samples.len() as i64;

        let dst = pcm.data_mut(0);
        let src =
            unsafe { std::slice::from_raw_parts(samples.as_ptr() as *const u8, samples.len() * 4) };
        let n = dst.len().min(src.len());
        dst[..n].copy_from_slice(&src[..n]);

        if self.encoder.send_frame(&pcm).is_err() {
            return;
        }
        let mut pkt = Packet::empty();
        while self.encoder.receive_packet(&mut pkt).is_ok() {
            if let Some(data) = pkt.data() {
                let _ = tx.send(EncodedAudio {
                    data: Arc::from(data),
                    timestamp_us,
                });
            }
        }
    }
}

// ── 音频发布器 ────────────────────────────────────────────────

/// 音频发布器: 麦克风/合成 + 编码。
pub struct AudioPublisher {
    paused: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl AudioPublisher {
    /// 启动音频发布。返回 `(发布器, 是否真实麦克风, 日志信息)`。
    pub fn start(tx: Sender<EncodedAudio>, prefer_real: bool) -> (Self, bool, String) {
        let paused = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let (ready_tx, ready_rx) = std_mpsc::channel::<(bool, String)>();

        let handle = thread::Builder::new()
            .name("audio-pub".into())
            .spawn({
                let (paused, stopped) = (paused.clone(), stopped.clone());
                move || audio_publisher_loop(prefer_real, tx, paused, stopped, ready_tx)
            })
            .ok();

        let (is_mic, msg) = ready_rx
            .recv_timeout(Duration::from_secs(8))
            .unwrap_or_else(|_| (false, "音频初始化超时; 已回退到合成音频".to_string()));

        (
            Self {
                paused,
                stopped,
                handle,
            },
            is_mic,
            msg,
        )
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
    }

    pub fn stop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn audio_publisher_loop(
    prefer_real: bool,
    tx: Sender<EncodedAudio>,
    paused: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    ready_tx: std_mpsc::Sender<(bool, String)>,
) {
    if let Err(e) = ensure_ffmpeg() {
        let _ = ready_tx.send((false, e));
        return;
    }

    if prefer_real {
        match open_mic_source() {
            Ok(source) => {
                let _ = ready_tx.send((true, "已启用真实麦克风".to_string()));
                mic_source_loop(source, &tx, &paused, &stopped);
                return;
            }
            Err(e) => {
                let _ = ready_tx.send((false, format!("{e}; 已回退到合成音频")));
            }
        }
    } else {
        let _ = ready_tx.send((false, "使用合成音频".to_string()));
    }

    synthetic_audio_loop(&tx, &paused, &stopped);
}

struct MicSource {
    input: avformat::context::Input,
    index: usize,
    decoder: codec::decoder::Audio,
}

fn open_mic_source() -> Result<MicSource, String> {
    let (fmt, dev, options) = audio_input_spec();
    let opts: Vec<(&str, &str)> = options.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let input = unsafe { open_input_device(&fmt, &dev, &opts)? };
    let stream = input
        .streams()
        .best(avmedia::Type::Audio)
        .ok_or("麦克风无音频流")?;
    let index = stream.index();
    let params = stream.parameters();
    let ctx = codec::context::Context::from_parameters(params)
        .map_err(|e| format!("创建麦克风解码器上下文失败: {e}"))?;
    let decoder = ctx
        .decoder()
        .audio()
        .map_err(|e| format!("打开麦克风解码器失败: {e}"))?;
    Ok(MicSource {
        input,
        index,
        decoder,
    })
}

fn mic_source_loop(
    mut source: MicSource,
    tx: &Sender<EncodedAudio>,
    paused: &Arc<AtomicBool>,
    stopped: &Arc<AtomicBool>,
) {
    let mut encoder = match AudioEncoder::new() {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("{e}");
            return;
        }
    };
    let mut resampler: Option<software::resampling::Context> = None;
    let mut resampler_def: Option<(Sample, ChannelLayout, u32)> = None;
    let mut pending: Vec<f32> = Vec::new();

    let mut packets = source.input.packets();
    while !stopped.load(Ordering::Relaxed) {
        let Some((stream, packet)) = packets.next() else {
            break;
        };
        if stream.index() != source.index {
            continue;
        }
        if source.decoder.send_packet(&packet).is_err() {
            continue;
        }
        let mut decoded = frame::Audio::empty();
        while source.decoder.receive_frame(&mut decoded).is_ok() {
            if paused.load(Ordering::Relaxed) {
                continue;
            }
            let def = (decoded.format(), decoded.channel_layout(), decoded.rate());
            if resampler_def != Some(def) {
                resampler = software::resampling::Context::get(
                    decoded.format(),
                    decoded.channel_layout(),
                    decoded.rate(),
                    Sample::F32(SampleType::Planar),
                    ChannelLayout::MONO,
                    SAMPLE_RATE,
                )
                .ok();
                resampler_def = Some(def);
            }
            let Some(rs) = resampler.as_mut() else {
                continue;
            };
            let mut out = frame::Audio::empty();
            if rs.run(&decoded, &mut out).is_err() {
                continue;
            }
            let n = out.samples();
            if n == 0 {
                continue;
            }
            let bytes = out.data(0);
            let samples = unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const f32, n) };
            pending.extend_from_slice(samples);
            while pending.len() >= AUDIO_FRAME_SAMPLES {
                let chunk: Vec<f32> = pending.drain(..AUDIO_FRAME_SAMPLES).collect();
                encoder.encode(&chunk, tx);
            }
        }
    }
}

fn synthetic_audio_loop(
    tx: &Sender<EncodedAudio>,
    paused: &Arc<AtomicBool>,
    stopped: &Arc<AtomicBool>,
) {
    use std::f32::consts::PI;
    const FREQ: f32 = 440.0;

    let mut encoder = match AudioEncoder::new() {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("{e}");
            return;
        }
    };
    let period = Duration::from_millis(20);
    let mut phase = 0.0f32;
    let mut pcm = vec![0f32; AUDIO_FRAME_SAMPLES];
    let mut next = Instant::now();

    while !stopped.load(Ordering::Relaxed) {
        if paused.load(Ordering::Relaxed) {
            thread::sleep(period);
            next = Instant::now() + period;
            continue;
        }
        for s in pcm.iter_mut() {
            *s = phase.sin() * 0.15;
            phase += 2.0 * PI * FREQ / SAMPLE_RATE as f32;
            if phase > 2.0 * PI {
                phase -= 2.0 * PI;
            }
        }
        encoder.encode(&pcm, tx);

        let now = Instant::now();
        if now < next {
            thread::sleep(next - now);
        }
        next += period;
        if next < Instant::now() {
            next = Instant::now() + period;
        }
    }
}

// ── 远端视频解码 ──────────────────────────────────────────────

/// 远端 VP8 解码器: 后台线程接收 VP8 帧 → RGBA → UI。
pub struct RemoteVideoDecoder {
    tx: Option<Sender<Vec<u8>>>,
    handle: Option<JoinHandle<()>>,
}

impl RemoteVideoDecoder {
    pub fn new(publisher_id: String, ui_tx: Sender<UiEvent>) -> Result<Self, String> {
        ensure_ffmpeg()?;
        let decoder = create_vp8_decoder()?;
        let (tx, rx) = unbounded::<Vec<u8>>();
        let name = format!("video-dec-{publisher_id}");
        let handle = thread::Builder::new()
            .name(name)
            .spawn(move || remote_video_loop(rx, decoder, publisher_id, ui_tx))
            .map_err(|e| format!("启动视频解码线程失败: {e}"))?;
        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
        })
    }

    /// 送入一个完整 VP8 帧。
    pub fn push(&mut self, frame: &[u8]) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(frame.to_vec());
        }
    }

    pub fn stop(&mut self) {
        self.tx.take();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for RemoteVideoDecoder {
    fn drop(&mut self) {
        self.stop();
    }
}

fn create_vp8_decoder() -> Result<codec::decoder::Video, String> {
    let codec = codec::decoder::find(codec::Id::VP8).ok_or("未找到 VP8 解码器")?;
    let ctx = codec::context::Context::new_with_codec(codec);
    ctx.decoder()
        .video()
        .map_err(|e| format!("打开 VP8 解码器失败: {e}"))
}

fn remote_video_loop(
    rx: Receiver<Vec<u8>>,
    mut decoder: codec::decoder::Video,
    publisher_id: String,
    ui_tx: Sender<UiEvent>,
) {
    let mut scaler: Option<software::scaling::Context> = None;
    let mut scaler_def: Option<(Pixel, u32, u32)> = None;
    let mut rgba = frame::Video::new(Pixel::RGBA, VIDEO_WIDTH, VIDEO_HEIGHT);

    while let Ok(data) = rx.recv() {
        let packet = Packet::copy(&data);
        if decoder.send_packet(&packet).is_err() {
            continue;
        }
        let mut decoded = frame::Video::empty();
        while decoder.receive_frame(&mut decoded).is_ok() {
            let def = (decoded.format(), decoded.width(), decoded.height());
            if scaler_def != Some(def) {
                scaler = software::scaling::Context::get(
                    decoded.format(),
                    decoded.width(),
                    decoded.height(),
                    Pixel::RGBA,
                    VIDEO_WIDTH,
                    VIDEO_HEIGHT,
                    ScaleFlags::BILINEAR,
                )
                .ok();
                scaler_def = Some(def);
            }
            let Some(s) = scaler.as_mut() else {
                continue;
            };
            if s.run(&decoded, &mut rgba).is_err() {
                continue;
            }
            let _ = ui_tx.send(UiEvent::RemoteVideo {
                id: publisher_id.clone(),
                rgba: frame_to_rgba(&rgba),
                width: VIDEO_WIDTH,
                height: VIDEO_HEIGHT,
            });
        }
    }
}

// ── 远端音频解码 + 播放 ───────────────────────────────────────

/// 远端 Opus 播放器: 后台线程解码 Opus → PCM → 系统音频输出设备。
pub struct RemoteAudioPlayer {
    tx: Option<Sender<Vec<u8>>>,
    handle: Option<JoinHandle<()>>,
}

impl RemoteAudioPlayer {
    pub fn new() -> Result<Self, String> {
        ensure_ffmpeg()?;
        let decoder = create_opus_decoder()?;
        let (tx, rx) = unbounded::<Vec<u8>>();
        let handle = thread::Builder::new()
            .name("audio-play".into())
            .spawn(move || remote_audio_loop(rx, decoder))
            .map_err(|e| format!("启动音频播放线程失败: {e}"))?;
        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
        })
    }

    /// 送入一个 Opus 包。
    pub fn push(&mut self, packet: &[u8]) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(packet.to_vec());
        }
    }

    pub fn stop(&mut self) {
        self.tx.take();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for RemoteAudioPlayer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn create_opus_decoder() -> Result<codec::decoder::Audio, String> {
    let codec = codec::decoder::find(codec::Id::OPUS).ok_or("未找到 Opus 解码器")?;
    let ctx = codec::context::Context::new_with_codec(codec);
    ctx.decoder()
        .audio()
        .map_err(|e| format!("打开 Opus 解码器失败: {e}"))
}

/// 系统音频输出 (通过 libavdevice 的 alsa/pulse/audiotoolbox 输出设备)。
struct AudioSink {
    octx: avformat::context::Output,
    encoder: codec::encoder::Audio,
    stream_index: usize,
    resampler: Option<software::resampling::Context>,
    resampler_def: Option<(Sample, ChannelLayout, u32)>,
    pts: i64,
}

impl AudioSink {
    fn open() -> Result<Self, String> {
        let (fmt, dev) = audio_sink_spec();
        let c_fmt = CString::new(fmt.clone()).unwrap();
        let c_dev = CString::new(dev.clone()).unwrap();

        unsafe {
            let mut ps: *mut ffmpeg::ffi::AVFormatContext = ptr::null_mut();
            let ret = ffmpeg::ffi::avformat_alloc_output_context2(
                &mut ps,
                ptr::null_mut(),
                c_fmt.as_ptr(),
                c_dev.as_ptr(),
            );
            if ret < 0 {
                return Err(format!(
                    "创建音频输出失败 ({fmt} {dev}): {}",
                    ffmpeg::Error::from(ret)
                ));
            }
            let mut octx = avformat::context::Output::wrap(ps);

            let codec = codec::encoder::find(codec::Id::PCM_S16LE).ok_or("未找到 PCM 编码器")?;
            let (encoder, stream_index) = {
                let mut stream = octx.add_stream(codec).map_err(|e| e.to_string())?;
                let ctx = codec::context::Context::from_parameters(stream.parameters())
                    .map_err(|e| e.to_string())?;
                let mut enc = ctx.encoder().audio().map_err(|e| e.to_string())?;
                enc.set_rate(SAMPLE_RATE as i32);
                enc.set_channel_layout(ChannelLayout::STEREO);
                enc.set_format(Sample::I16(SampleType::Packed));
                enc.set_time_base(Rational(1, SAMPLE_RATE as i32));
                let enc = enc.open_as(codec).map_err(|e| e.to_string())?;
                stream.set_parameters(&enc);
                (enc, stream.index())
            };

            octx.write_header().map_err(|e| e.to_string())?;

            Ok(Self {
                octx,
                encoder,
                stream_index,
                resampler: None,
                resampler_def: None,
                pts: 0,
            })
        }
    }

    /// 解码得到的任意格式音频帧 → 输出设备。
    fn write_frame(&mut self, src: &frame::Audio) -> Result<(), String> {
        let def = (src.format(), src.channel_layout(), src.rate());
        if self.resampler_def != Some(def) {
            self.resampler = Some(
                software::resampling::Context::get(
                    src.format(),
                    src.channel_layout(),
                    src.rate(),
                    Sample::I16(SampleType::Packed),
                    ChannelLayout::STEREO,
                    SAMPLE_RATE,
                )
                .map_err(|e| e.to_string())?,
            );
            self.resampler_def = Some(def);
        }
        let resampler = self.resampler.as_mut().ok_or("重采样器未初始化")?;

        let mut pcm = frame::Audio::new(
            Sample::I16(SampleType::Packed),
            src.samples(),
            ChannelLayout::STEREO,
        );
        resampler.run(src, &mut pcm).map_err(|e| e.to_string())?;
        pcm.set_pts(Some(self.pts));
        self.pts += pcm.samples() as i64;

        self.encoder.send_frame(&pcm).map_err(|e| e.to_string())?;
        let mut packet = Packet::empty();
        while self.encoder.receive_packet(&mut packet).is_ok() {
            packet.set_stream(self.stream_index);
            packet
                .write_interleaved(&mut self.octx)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

fn remote_audio_loop(rx: Receiver<Vec<u8>>, mut decoder: codec::decoder::Audio) {
    let mut sink = match AudioSink::open() {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::warn!("音频播放不可用 (仅接收不解码): {e}");
            None
        }
    };

    while let Ok(data) = rx.recv() {
        let packet = Packet::copy(&data);
        if decoder.send_packet(&packet).is_err() {
            continue;
        }
        let mut frame = frame::Audio::empty();
        while decoder.receive_frame(&mut frame).is_ok() {
            if let Some(s) = sink.as_mut() {
                if let Err(e) = s.write_frame(&frame) {
                    tracing::warn!("写入音频设备失败, 停止播放: {e}");
                    sink = None;
                }
            }
        }
    }
}

// ── 往返测试 (验证 ffmpeg-next 编解码路径) ────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_rgba(frame_no: u64) -> Vec<u8> {
        let mut rgba = vec![0u8; (VIDEO_WIDTH * VIDEO_HEIGHT * 4) as usize];
        draw_pattern_rgba(&mut rgba, frame_no);
        rgba
    }

    #[test]
    fn video_encode_decode_roundtrip() {
        ensure_ffmpeg().unwrap();
        let (tx, rx) = unbounded::<EncodedVideo>();
        let mut conv = VideoConverter::new().unwrap();
        let mut enc = VideoEncoder::new().unwrap();

        for i in 0..10u64 {
            let rgba = test_rgba(i);
            fill_rgba_frame(&mut conv.rgba, &rgba);
            conv.rgba_to_yuv().unwrap();
            enc.encode(&mut conv.yuv, &tx);
        }

        let encoded = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("视频编码器未产生码流");
        assert!(!encoded.data.is_empty(), "编码结果为空");

        let mut dec = create_vp8_decoder().unwrap();
        let pkt = Packet::copy(&encoded.data);
        dec.send_packet(&pkt).expect("send vp8 packet");
        let mut out = frame::Video::empty();
        dec.receive_frame(&mut out).expect("decode vp8 frame");
        assert_eq!(out.width(), VIDEO_WIDTH);
        assert_eq!(out.height(), VIDEO_HEIGHT);
    }

    #[test]
    fn audio_encode_decode_roundtrip() {
        ensure_ffmpeg().unwrap();
        let (tx, rx) = unbounded::<EncodedAudio>();
        let mut enc = AudioEncoder::new().unwrap();

        let mut pcm = vec![0f32; AUDIO_FRAME_SAMPLES];
        for (i, s) in pcm.iter_mut().enumerate() {
            *s = ((i as f32) * 0.05).sin() * 0.1;
        }
        for _ in 0..5 {
            enc.encode(&pcm, &tx);
        }

        let encoded = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("音频编码器未产生码流");
        assert!(!encoded.data.is_empty(), "编码结果为空");

        let mut dec = create_opus_decoder().unwrap();
        let pkt = Packet::copy(&encoded.data);
        dec.send_packet(&pkt).expect("send opus packet");
        let mut out = frame::Audio::empty();
        dec.receive_frame(&mut out).expect("decode opus frame");
        assert!(out.samples() > 0);
    }
}
