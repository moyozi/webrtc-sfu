//! 最小压测发布器 — 以 str0m 作为 WebRTC 客户端向 SFU 发布合成音视频
//!
//! 每个「发布者」拥有独立的 UDP socket 与 WebSocket 信令连接，完整走一遍
//! ICE + DTLS + SRTP 握手，然后按固定帧率持续写入合成的 Opus/VP8 数据。
//! 用它可以在没有浏览器的情况下压测 SFU 的媒体转发路径与单核承载能力。
//!
//! 用法:
//! ```bash
//! # 发布 50 路 720p@30fps/1.5Mbps 到 room=load，持续 30 秒
//! cargo run --release --example load_publisher -- \
//!     --url ws://127.0.0.1:5174 --room load --count 50 --duration 30
//!
//! # 发布 500 路
//! cargo run --release --example load_publisher -- --count 500
//! ```
//!
//! 参数:
//!   --url       WebSocket 信令地址 (默认 ws://127.0.0.1:5174)
//!   --room      房间号 (默认 load)
//!   --count     发布者数量 (默认 10)
//!   --duration  持续秒数 (默认 20)
//!   --advertise 客户端对外公布的 IP (默认 127.0.0.1, 需与服务端连通)
//!   --video-kbps 每路视频码率 (默认 1500)
//!   --fps       视频帧率 (默认 30)
//!
//! 注意: 每个发布者占用 1 个 UDP fd + 1 个 TCP fd，大规模压测前请调高
//! `ulimit -n` (如 `ulimit -n 65535`)。

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use str0m::change::{SdpAnswer, SdpPendingOffer};
use str0m::format::Codec;
use str0m::media::{Direction, MediaKind, MediaTime, Mid, Pt};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output, Rtc};
use tokio::net::UdpSocket;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// 全局统计 (跨发布者共享)
#[derive(Default)]
struct Stats {
    /// 当前已建立连接的发布者数
    connected: AtomicI64,
    /// 发出的 RTP/RTCP/STUN 包数
    tx_packets: AtomicU64,
    /// 发出的字节数
    tx_bytes: AtomicU64,
    /// 收到的包数
    rx_packets: AtomicU64,
}

struct Config {
    url: String,
    room: String,
    count: usize,
    duration: Duration,
    advertise_ip: IpAddr,
    video_kbps: u64,
    fps: u64,
    /// 额外发布一路屏幕共享视频 (验证一个参与者多 track)。
    screen: bool,
}

impl Config {
    fn parse() -> Self {
        let mut cfg = Config {
            url: "ws://127.0.0.1:5174".into(),
            room: "load".into(),
            count: 10,
            duration: Duration::from_secs(20),
            advertise_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            video_kbps: 1500,
            fps: 30,
            screen: false,
        };

        let mut args = std::env::args().skip(1);
        while let Some(key) = args.next() {
            let mut val = || args.next().unwrap_or_default();
            match key.as_str() {
                "--url" => cfg.url = val(),
                "--room" => cfg.room = val(),
                "--count" => cfg.count = val().parse().unwrap_or(cfg.count),
                "--duration" => {
                    cfg.duration = Duration::from_secs(val().parse().unwrap_or(20))
                }
                "--advertise" => {
                    if let Ok(ip) = val().parse() {
                        cfg.advertise_ip = ip;
                    }
                }
                "--video-kbps" => cfg.video_kbps = val().parse().unwrap_or(cfg.video_kbps),
                "--fps" => cfg.fps = val().parse().unwrap_or(cfg.fps).max(1),
                "--screen" => cfg.screen = true,
                "--help" | "-h" => {
                    println!("{}", include_str!("load_publisher.rs"));
                    std::process::exit(0);
                }
                other => eprintln!("忽略未知参数: {}", other),
            }
        }
        cfg
    }
}

#[tokio::main]
async fn main() {
    let cfg = Arc::new(Config::parse());
    let stats = Arc::new(Stats::default());

    println!(
        "压测发布器: count={} room={} url={} 每路视频={}kbps@{}fps, 持续={}s",
        cfg.count,
        cfg.room,
        cfg.url,
        cfg.video_kbps,
        cfg.fps,
        cfg.duration.as_secs()
    );

    // 周期打印统计
    let s = stats.clone();
    tokio::spawn(async move {
        let mut last_tx = 0u64;
        let mut last_bytes = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let tx = s.tx_packets.load(Ordering::Relaxed);
            let bytes = s.tx_bytes.load(Ordering::Relaxed);
            let tx_kpps = (tx - last_tx) as f64 / 2.0 / 1000.0;
            let mbps = (bytes - last_bytes) as f64 * 8.0 / 2.0 / 1e6;
            last_tx = tx;
            last_bytes = bytes;
            println!(
                "[stats] connected={}  发送={:.1} kpps / {:.1} Mbps  (累计 {} 包, {:.1} MB)  收包={}",
                s.connected.load(Ordering::Relaxed),
                tx_kpps,
                mbps,
                tx,
                bytes as f64 / 1e6,
                s.rx_packets.load(Ordering::Relaxed),
            );
        }
    });

    let deadline = Instant::now() + cfg.duration + Duration::from_secs(5);

    let mut handles = Vec::with_capacity(cfg.count);
    for idx in 0..cfg.count {
        let cfg = cfg.clone();
        let stats = stats.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = publisher(idx, cfg, stats).await {
                eprintln!("[load-{}] 退出: {}", idx, e);
            }
        }));
    }

    // 到点后统一收尾 (不强制 abort，让任务自然结束)
    for (idx, h) in handles.into_iter().enumerate() {
        if tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), h)
            .await
            .is_err()
        {
            eprintln!("[load-{}] 超时未结束", idx);
        }
    }

    let bytes = stats.tx_bytes.load(Ordering::Relaxed);
    println!(
        "共发送 {} 包 / {:.1} MB",
        stats.tx_packets.load(Ordering::Relaxed),
        bytes as f64 / 1e6
    );
}

/// 单个发布者的完整生命周期
async fn publisher(
    idx: usize,
    cfg: Arc<Config>,
    stats: Arc<Stats>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let participant_id = format!("load-{}", idx);

    // ── 1. 独立 UDP socket (每个发布者一个源端口, 供 SFU 按源地址路由) ──
    let udp = UdpSocket::bind("0.0.0.0:0").await?;
    enlarge_udp_buffers(&udp);
    let local_port = udp.local_addr()?.port();
    let advertised = SocketAddr::new(cfg.advertise_ip, local_port);

    // ── 2. 创建 str0m Rtc 及本地候选 ──
    let mut rtc = Rtc::builder().build(Instant::now());
    let local_candidate = Candidate::host(advertised, Protocol::Udp)?;
    rtc.add_local_candidate(local_candidate.clone());

    // ── 3. 生成 offer: 音频(mid0) + 视频(mid1), 均 SendOnly ──
    let mut api = rtc.sdp_api();
    let audio_mid = api.add_media(MediaKind::Audio, Direction::SendOnly, None, None, None);
    let video_mid = api.add_media(MediaKind::Video, Direction::SendOnly, None, None, None);
    // 可选: 第 3 路 (屏幕共享视频)。
    let screen_mid = cfg
        .screen
        .then(|| api.add_media(MediaKind::Video, Direction::SendOnly, None, None, None));
    let (offer, pending) = api.apply().ok_or("生成 SDP offer 失败")?;
    let mut pending: Option<SdpPendingOffer> = Some(pending);

    // 打印本发布者的 track mid，供 load_subscriber 订阅使用。
    match screen_mid {
        Some(s) => println!("[mids] {} {} {} {}", participant_id, audio_mid, video_mid, s),
        None => println!("[mids] {} {} {}", participant_id, audio_mid, video_mid),
    }

    // 本地编解码协商出的 PT (write 必须使用本地 PT)
    let audio_pt = local_pt(&rtc, Codec::Opus);
    let video_pt = local_pt(&rtc, Codec::Vp8);

    // ── 4. WebSocket 信令 ──
    let (ws, _resp) = connect_async(cfg.url.as_str()).await?;
    let (mut ws_tx, mut ws_rx) = ws.split();

    ws_tx
        .send(Message::text(
            json!({
                "type": "join",
                "pc_id": "pub",
                "room_id": cfg.room,
                "participant_id": participant_id,
                "sdp": offer.to_sdp_string(),
                "role": "publisher",
            })
            .to_string(),
        ))
        .await?;

    // 公布本地候选 (trickle ICE)
    ws_tx
        .send(Message::text(
            json!({
                "type": "ice_candidate",
                "pc_id": "pub",
                "candidate": local_candidate.to_sdp_string(),
                "sdp_mid": "0",
                "sdp_mline_index": 0,
            })
            .to_string(),
        ))
        .await?;

    // ── 5. 媒体参数 ──
    let fps = cfg.fps;
    let audio_payload: Arc<[u8]> = vec![0u8; 160].into();
    let video_frame_bytes = (cfg.video_kbps * 1000 / 8 / fps).max(64) as usize;
    let keyframe: Arc<[u8]> = {
        let mut v = vec![0u8; video_frame_bytes];
        v[0] = 0x00; // VP8 关键帧 (frame type 位 = 0)
        v.into()
    };
    let interframe: Arc<[u8]> = {
        let mut v = vec![0u8; video_frame_bytes];
        v[0] = 0x01; // VP8 帧间帧
        v.into()
    };

    let mut audio_tick = tokio::time::interval(Duration::from_millis(20));
    let mut video_tick = tokio::time::interval(Duration::from_millis(1000 / fps));
    // 屏幕共享: 低帧率 (5fps), 仅 --screen 时真正发送。
    let mut screen_tick = tokio::time::interval(Duration::from_millis(200));
    audio_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    video_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    screen_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    audio_tick.tick().await; // 立即开始
    video_tick.tick().await;
    screen_tick.tick().await;

    let mut audio_us = 0u64;
    let mut video_us = 0u64;
    let mut frame_no = 0u64;
    let mut screen_us = 0u64;
    let mut screen_frame_no = 0u64;
    let mut next_deadline = Instant::now();
    let mut connected = false;

    let mut buf = vec![0u8; 2000];
    let deadline = Instant::now() + cfg.duration;

    while Instant::now() < deadline {
        // 先 drain str0m 输出 (发包 + 重排超时)
        drain(&mut rtc, &udp, &mut next_deadline, &stats);

        // 连接状态变化计数
        let now_connected = rtc.is_connected();
        if now_connected != connected {
            stats
                .connected
                .fetch_add(if now_connected { 1 } else { -1 }, Ordering::Relaxed);
            connected = now_connected;
            if connected {
                println!("[{}] 已连接", participant_id);
            }
        }

        enum Act {
            Ws(Message),
            WsClosed,
            Recv(SocketAddr, usize),
            Timeout,
            Audio,
            Video,
            Screen,
        }

        let act = tokio::select! {
            msg = ws_rx.next() => match msg {
                Some(Ok(m)) => Act::Ws(m),
                _ => Act::WsClosed,
            },
            r = udp.recv_from(&mut buf) => match r {
                Ok((n, src)) => Act::Recv(src, n),
                Err(_) => Act::Recv(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0), 0),
            },
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_deadline)) => {
                Act::Timeout
            }
            _ = audio_tick.tick() => Act::Audio,
            _ = video_tick.tick() => Act::Video,
            _ = screen_tick.tick() => Act::Screen,
        };

        match act {
            Act::WsClosed => break,
            Act::Ws(msg) => {
                if let Message::Text(text) = msg {
                    handle_signal(&text, &mut rtc, &mut pending, &participant_id);
                }
            }
            Act::Recv(src, n) if n > 0 => {
                let local = udp.local_addr().unwrap_or(src);
                if let Ok(recv) = Receive::new(Protocol::Udp, src, local, &buf[..n]) {
                    let now = Instant::now();
                    if rtc.handle_input(Input::Receive(now, recv)).is_ok() {
                        stats.rx_packets.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Act::Recv(..) => {}
            Act::Timeout => {
                let _ = rtc.handle_input(Input::Timeout(Instant::now()));
            }
            Act::Audio => {
                if connected {
                    write_media(
                        &mut rtc,
                        audio_mid,
                        audio_pt,
                        MediaTime::from_micros(audio_us),
                        &audio_payload,
                    );
                    audio_us += 20_000;
                }
            }
            Act::Video => {
                if connected {
                    let payload = if frame_no.is_multiple_of(fps * 5) {
                        &keyframe
                    } else {
                        &interframe
                    };
                    write_media(
                        &mut rtc,
                        video_mid,
                        video_pt,
                        MediaTime::from_micros(video_us),
                        payload,
                    );
                    video_us += 1_000_000 / fps;
                    frame_no += 1;
                }
            }
            Act::Screen => {
                if connected {
                    if let Some(smid) = screen_mid {
                        let payload = if screen_frame_no.is_multiple_of(25) {
                            &keyframe
                        } else {
                            &interframe
                        };
                        write_media(
                            &mut rtc,
                            smid,
                            video_pt,
                            MediaTime::from_micros(screen_us),
                            payload,
                        );
                        screen_us += 200_000;
                        screen_frame_no += 1;
                    }
                }
            }
        }
    }

    // 收尾: 通知服务端离开
    let _ = ws_tx
        .send(Message::text(
            json!({"type": "leave", "pc_id": "pub"}).to_string(),
        ))
        .await;

    if connected {
        stats.connected.fetch_sub(1, Ordering::Relaxed);
    }
    Ok(())
}

/// 把 str0m 的输出全部 drain 到 `Timeout`。
///
/// 同步执行: 用 `try_send_to` 直接发包 (socket 非阻塞)，避免在热路径 await。
fn drain(rtc: &mut Rtc, udp: &UdpSocket, next_deadline: &mut Instant, stats: &Stats) {
    loop {
        match rtc.poll_output() {
            Ok(Output::Transmit(t)) => {
                stats.tx_packets.fetch_add(1, Ordering::Relaxed);
                stats.tx_bytes.fetch_add(t.contents.len() as u64, Ordering::Relaxed);
                let _ = udp.try_send_to(&t.contents, t.destination);
            }
            Ok(Output::Event(Event::Connected)) => {
                // 连接建立, 由调用方通过 is_connected() 统计
            }
            Ok(Output::Event(_)) => {}
            Ok(Output::Timeout(t)) => {
                *next_deadline = t;
                break;
            }
            Err(_) => break,
        }
    }
}

/// 写入一帧媒体 (str0m 会按 MTU 自动分片打包)
/// 调大压测客户端 UDP socket 收发缓冲区 (默认仅 ~208KB)。
/// 实际生效值受内核 `rmem_max`/`wmem_max` 限制。
fn enlarge_udp_buffers(udp: &UdpSocket) {
    use std::os::fd::AsRawFd;
    let bytes: i32 = 8 * 1024 * 1024;
    unsafe {
        let fd = udp.as_raw_fd();
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &bytes as *const _ as *const _,
            std::mem::size_of::<i32>() as u32,
        );
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &bytes as *const _ as *const _,
            std::mem::size_of::<i32>() as u32,
        );
    }
}

fn write_media(
    rtc: &mut Rtc,
    mid: Mid,
    pt: Option<Pt>,
    time: MediaTime,
    payload: &Arc<[u8]>,
) {
    let Some(pt) = pt else { return };
    if let Some(writer) = rtc.writer(mid) {
        let _ = writer.write(pt, Instant::now(), time, payload.clone());
    }
}

/// 在本地编解码配置中查找指定 codec 的 PT
fn local_pt(rtc: &Rtc, want: Codec) -> Option<Pt> {
    rtc.codec_config()
        .iter()
        .find(|p| p.spec().codec == want)
        .map(|p| p.pt())
}

/// 处理一条服务端信令消息
fn handle_signal(
    text: &str,
    rtc: &mut Rtc,
    pending: &mut Option<SdpPendingOffer>,
    participant_id: &str,
) {
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };

    match v["type"].as_str() {
        Some("sdp_answer") => {
            let Some(sdp) = v["sdp"].as_str() else { return };
            let answer = match SdpAnswer::from_sdp_string(sdp) {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("[{}] 解析 SDP answer 失败: {}", participant_id, e);
                    return;
                }
            };
            if let Some(p) = pending.take() {
                if let Err(e) = rtc.sdp_api().accept_answer(p, answer) {
                    eprintln!("[{}] accept_answer 失败: {}", participant_id, e);
                }
            }
            // 服务端通过 answer 内联的候选
            if let Some(cands) = v["ice_candidates"].as_array() {
                for c in cands.iter().filter_map(|c| c.as_str()) {
                    if let Ok(cand) = Candidate::from_sdp_string(c) {
                        rtc.add_remote_candidate(cand);
                    }
                }
            }
        }
        Some("ice_candidate") => {
            if let Some(cs) = v["candidate"].as_str() {
                if let Ok(cand) = Candidate::from_sdp_string(cs) {
                    rtc.add_remote_candidate(cand);
                }
            }
        }
        Some("error") => {
            eprintln!(
                "[{}] 服务端错误: {}",
                participant_id,
                v["error"].as_str().unwrap_or("?")
            );
        }
        _ => {}
    }
}
