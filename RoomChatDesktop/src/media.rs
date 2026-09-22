//! 本地媒体源。
//!
//! 视频:
//!   · 默认使用 **真实摄像头** (nokhwa: AVFoundation / V4L2 / MSMF), 逐帧推给
//!     reactor-webrtc 视频轨 (`VideoTrack::push_frame`), 同时转 RGBA 发到 UI。
//!   · 摄像头不可用 (无设备 / 无权限 / 打开失败) 时自动回退到 **合成测试图案**。
//!
//! 音频:
//!   · 平台 ADM: `PeerConnectionFactory::with_platform_adm()` 提供真实麦克风采集
//!     与远端音频自动播放, 不需要本模块。
//!   · 合成 ADM: 本模块 `AudioGenerator` 持续推入 440Hz 正弦波供联调。

use std::f32::consts::PI;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use nokhwa::pixel_format::RgbAFormat;
use nokhwa::utils::{CameraFormat, CameraIndex, FrameFormat, RequestedFormat, RequestedFormatType};
use nokhwa::Camera;
use reactor_webrtc::{PeerConnectionFactory, VideoFrame, VideoTrack};
use zune_jpeg::zune_core::colorspace::ColorSpace;
use zune_jpeg::zune_core::options::DecoderOptions;
use zune_jpeg::JpegDecoder;

use crate::ui::UiEvent;

/// 请求的摄像头分辨率 (实际以设备支持的最接近格式为准)。
pub const VIDEO_WIDTH: u32 = 640;
pub const VIDEO_HEIGHT: u32 = 480;

/// libwebrtc 回调给出的是 BGRA, Slint 需要 RGBA。
pub fn bgra_to_rgba(bgra: &[u8], width: u32, height: u32) -> Vec<u8> {
    let pixels = (width as usize) * (height as usize);
    let mut out = vec![0u8; pixels * 4];
    let n = pixels.min(bgra.len() / 4);
    for i in 0..n {
        out[i * 4] = bgra[i * 4 + 2];
        out[i * 4 + 1] = bgra[i * 4 + 1];
        out[i * 4 + 2] = bgra[i * 4];
        out[i * 4 + 3] = bgra[i * 4 + 3];
    }
    out
}

/// 就地交换首尾通道: RGBA ↔ BGRA。
fn swap_rb(bytes: &mut [u8]) {
    for px in bytes.chunks_exact_mut(4) {
        px.swap(0, 2);
    }
}

// ── 摄像头权限 (macOS) ────────────────────────────────────────

/// 请求摄像头权限 (macOS 会弹系统授权框; 其他平台立即回调 true)。
/// 应在打开摄像头之前调用。
pub fn init_camera_permission() {
    nokhwa::nokhwa_initialize(|granted| {
        tracing::info!("摄像头权限: {}", granted);
    });
}

// ── 视频: 摄像头 ──────────────────────────────────────────────

/// 打开默认摄像头 (index 0)。
///
/// 依次尝试 640×480 的 MJPEG / YUYV, 最后退化为摄像头支持的最高帧率格式;
/// 直接用 `RgbAFormat` 约束, 保证拿到可解码的格式。
fn open_camera(index: u32) -> Result<Camera, String> {
    let candidates = [
        RequestedFormatType::Closest(CameraFormat::new_from(
            VIDEO_WIDTH,
            VIDEO_HEIGHT,
            FrameFormat::MJPEG,
            30,
        )),
        RequestedFormatType::Closest(CameraFormat::new_from(
            VIDEO_WIDTH,
            VIDEO_HEIGHT,
            FrameFormat::YUYV,
            30,
        )),
        RequestedFormatType::AbsoluteHighestFrameRate,
    ];

    let mut last_err = String::new();
    for request in candidates {
        let format = RequestedFormat::new::<RgbAFormat>(request);
        match Camera::new(CameraIndex::Index(index), format) {
            Ok(mut camera) => match camera.open_stream() {
                Ok(()) => return Ok(camera),
                Err(e) => last_err = e.to_string(),
            },
            Err(e) => last_err = e.to_string(),
        }
    }
    Err(format!("无法打开摄像头 (index={index}): {last_err}"))
}

/// 把一帧解码为 RGBA。
///
/// nokhwa 默认的 `decoding` feature 依赖 mozjpeg, 其导出的 `jpeg_*` 符号与
/// libwebrtc 内置 libjpeg 冲突, 因此这里关闭该 feature:
///  · MJPEG 帧用纯 Rust 的 zune-jpeg 解码;
///  · YUYV 等原始格式仍走 nokhwa 自带的转换。
fn decode_frame(frame: &nokhwa::Buffer, rgba: &mut [u8]) -> Result<(), String> {
    if frame.source_frame_format() == FrameFormat::MJPEG {
        let options = DecoderOptions::default().jpeg_set_out_colorspace(ColorSpace::RGBA);
        let mut dec = JpegDecoder::new_with_options(Cursor::new(frame.buffer()), options);
        dec.decode_into(rgba).map_err(|e| e.to_string())
    } else {
        frame
            .decode_image_to_buffer::<RgbAFormat>(rgba)
            .map_err(|e| e.to_string())
    }
}

pub struct CameraCapture {
    paused: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl CameraCapture {
    /// 在独立线程中逐帧采集并推送。
    pub fn start(camera: Camera, track: Arc<VideoTrack>, ui_tx: Sender<UiEvent>) -> Self {
        let paused = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let (paused_task, stopped_task) = (paused.clone(), stopped.clone());

        let handle = thread::Builder::new()
            .name("camera-capture".into())
            .spawn(move || camera_loop(camera, track, ui_tx, paused_task, stopped_task))
            .ok();

        Self {
            paused,
            stopped,
            handle,
        }
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

fn camera_loop(
    mut camera: Camera,
    track: Arc<VideoTrack>,
    ui_tx: Sender<UiEvent>,
    paused: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
) {
    let mut rgba: Vec<u8> = Vec::new();
    let mut bgra: Vec<u8> = Vec::new();

    while !stopped.load(Ordering::Relaxed) {
        // frame() 阻塞直到下一帧 (天然限速)
        let frame = match camera.frame() {
            Ok(f) => f,
            Err(e) => {
                let _ = ui_tx.send(UiEvent::Log {
                    level: crate::ui::LogLevel::Error,
                    msg: format!("摄像头采集失败: {e}"),
                });
                break;
            }
        };
        if stopped.load(Ordering::Relaxed) {
            break;
        }
        // 摄像头关闭时继续取帧但丢弃, 保留流 (恢复时无需重新打开)
        if paused.load(Ordering::Relaxed) {
            continue;
        }

        let res = frame.resolution();
        let (w, h) = (res.width(), res.height());
        let need = (w as usize) * (h as usize) * 4;
        if rgba.len() != need {
            rgba.resize(need, 0);
        }
        if decode_frame(&frame, &mut rgba).is_err() {
            continue;
        }

        // RGBA → BGRA (推给 WebRTC), 复用 puffer 避免每帧分配
        bgra.clear();
        bgra.extend_from_slice(&rgba);
        swap_rb(&mut bgra);

        let _ = track.push_frame(VideoFrame::new(&bgra, w, h));
        let _ = ui_tx.send(UiEvent::LocalVideo {
            rgba: rgba.clone(),
            width: w,
            height: h,
        });
    }

    let _ = camera.stop_stream();
}

// ── 视频: 合成回退 ────────────────────────────────────────────

fn draw_pattern(buf: &mut [u8], frame_no: u64) {
    let w = VIDEO_WIDTH as usize;
    let h = VIDEO_HEIGHT as usize;
    let t = frame_no as f32 / 30.0;
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
            buf[i] = b;
            buf[i + 1] = g;
            buf[i + 2] = r;
            buf[i + 3] = 255;
        }
    }

    // 左上角 32 位帧计数指示 (8×4 方块)
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

pub struct SyntheticVideoCapture {
    paused: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl SyntheticVideoCapture {
    pub fn start(track: Arc<VideoTrack>, ui_tx: Sender<UiEvent>) -> Self {
        let paused = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let (paused_task, stopped_task) = (paused.clone(), stopped.clone());

        let handle = thread::Builder::new()
            .name("video-synth".into())
            .spawn(move || {
                let mut buf = vec![0u8; (VIDEO_WIDTH * VIDEO_HEIGHT * 4) as usize];
                let frame_dur = Duration::from_millis(33);
                let mut frame_no: u64 = 0;
                let mut next = Instant::now();
                while !stopped_task.load(Ordering::Relaxed) {
                    if paused_task.load(Ordering::Relaxed) {
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
                    draw_pattern(&mut buf, frame_no);
                    let _ = track.push_frame(VideoFrame::new(&buf, VIDEO_WIDTH, VIDEO_HEIGHT));

                    let rgba = bgra_to_rgba(&buf, VIDEO_WIDTH, VIDEO_HEIGHT);
                    let _ = ui_tx.send(UiEvent::LocalVideo {
                        rgba,
                        width: VIDEO_WIDTH,
                        height: VIDEO_HEIGHT,
                    });
                }
            })
            .ok();

        Self {
            paused,
            stopped,
            handle,
        }
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

// ── 视频源统一封装 ────────────────────────────────────────────

pub enum VideoCapture {
    Camera(CameraCapture),
    Synthetic(SyntheticVideoCapture),
}

impl VideoCapture {
    /// 优先摄像头, 失败回退合成。返回 (capture, 是否是真实摄像头, 提示信息)。
    pub fn open(
        track: Arc<VideoTrack>,
        ui_tx: Sender<UiEvent>,
        prefer_camera: bool,
    ) -> (Self, bool, String) {
        if prefer_camera {
            match open_camera(0) {
                Ok(camera) => {
                    let cap = CameraCapture::start(camera, track, ui_tx);
                    return (Self::Camera(cap), true, "已启用真实摄像头".to_string());
                }
                Err(e) => {
                    let cap = SyntheticVideoCapture::start(track, ui_tx);
                    return (Self::Synthetic(cap), false, format!("{e}; 已回退到合成视频"));
                }
            }
        }
        let cap = SyntheticVideoCapture::start(track, ui_tx);
        (Self::Synthetic(cap), false, "使用合成视频".to_string())
    }

    pub fn pause(&self) {
        match self {
            Self::Camera(c) => c.pause(),
            Self::Synthetic(s) => s.pause(),
        }
    }

    pub fn resume(&self) {
        match self {
            Self::Camera(c) => c.resume(),
            Self::Synthetic(s) => s.resume(),
        }
    }

    pub fn stop(&mut self) {
        match self {
            Self::Camera(c) => c.stop(),
            Self::Synthetic(s) => s.stop(),
        }
    }
}

// ── 音频 (合成) ───────────────────────────────────────────────

pub struct AudioGenerator {
    paused: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl AudioGenerator {
    /// 以 10ms 为块向共享 ADM 推入 440Hz 正弦波。
    pub fn start(factory: Arc<PeerConnectionFactory>) -> Self {
        let paused = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let (paused_task, stopped_task) = (paused.clone(), stopped.clone());

        let handle = thread::Builder::new()
            .name("audio-gen".into())
            .spawn(move || {
                const SAMPLE_RATE: u32 = 48_000;
                const FREQ: f32 = 440.0;
                const FRAMES: usize = 480; // 10ms @48k
                let period = Duration::from_millis(10);

                let mut phase = 0.0f32;
                let mut pcm = vec![0i16; FRAMES];
                let mut next = Instant::now();

                while !stopped_task.load(Ordering::Relaxed) {
                    if paused_task.load(Ordering::Relaxed) {
                        thread::sleep(period);
                        next = Instant::now() + period;
                        continue;
                    }
                    for s in pcm.iter_mut() {
                        *s = (phase.sin() * 0.15 * 32767.0) as i16;
                        phase += 2.0 * PI * FREQ / SAMPLE_RATE as f32;
                        if phase > 2.0 * PI {
                            phase -= 2.0 * PI;
                        }
                    }
                    factory.push_audio_frame(&pcm, SAMPLE_RATE, 1);

                    let now = Instant::now();
                    if now < next {
                        thread::sleep(next - now);
                    }
                    next += period;
                }
            })
            .ok();

        Self {
            paused,
            stopped,
            handle,
        }
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
